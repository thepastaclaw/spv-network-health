//! Per-node probe: an SPV client pinned to exactly one peer, synced a
//! configurable amount, with timing and failure metrics collected along the
//! way.
//!
//! dash-spv's exclusive mode (`restrict_to_configured_peers` + a single
//! configured peer + `max_peers = 1`) guarantees the probe talks ONLY to the
//! node under test: no DNS discovery, no peer persistence. `ValidationMode::
//! Full` turns bad data the node serves into countable validation failures.
//! Every probe runs against a fresh scratch storage directory (wiped before
//! and after) so all nodes start cold and grades are comparable.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use dash_spv::network::{NetworkEvent, PeerNetworkManager, PeerStatsSnapshot};
use dash_spv::storage::DiskStorageManager;
use dash_spv::sync::{ProgressPercentage, SyncEvent, SyncProgress};
use dash_spv::{ClientConfig, DashSpvClient, EventHandler, ValidationMode};
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use key_wallet_manager::WalletManager;
use tokio::sync::RwLock;

use crate::backend::{phase_synced, shutdown_spv_client, AppEvent, SpvHealthClient, SpvRunTask};
use crate::config::{AppConfig, ProbeConfig, SyncDepth};
use crate::grading;
use crate::types::{ProbeMetrics, ProbePhase, ProbeResult, ProbeSnapshot};

/// How often sync progress is polled (and UI snapshots emitted, when changed).
const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Give up if the node hasn't completed a handshake within this window.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// End the probe early if sync makes no progress at all for this long.
const STALL_TIMEOUT: Duration = Duration::from_secs(45);
/// Floor for rate computations so single-batch bursts don't divide by ~zero.
const MIN_RATE_WINDOW: Duration = Duration::from_millis(500);

/// Probe a single node and grade how well it served data.
///
/// `tip_height` is the network tip observed during discovery; it anchors
/// `SyncDepth::RecentBlocks` depths and the completeness target.
pub async fn probe_node(
    address: SocketAddr,
    tip_height: u32,
    config: &AppConfig,
    probe_config: &ProbeConfig,
    events: &Sender<AppEvent>,
) -> anyhow::Result<ProbeResult> {
    let scratch_dir = config
        .data_dir
        .join("probes")
        .join(scratch_dir_name(&address));
    // Start cold even if a previous run left data behind.
    let _ = tokio::fs::remove_dir_all(&scratch_dir).await;

    let start_height = match probe_config.depth {
        SyncDepth::RecentBlocks(blocks) => tip_height.saturating_sub(blocks),
        SyncDepth::Full => 0,
    };

    let mut spv_config = ClientConfig::new(config.network)
        .with_storage_path(scratch_dir.clone())
        .with_validation_mode(ValidationMode::Full)
        .with_restrict_to_configured_peers(true)
        .without_masternodes()
        .with_start_height(start_height);
    if !probe_config.enable_filters {
        spv_config = spv_config.without_filters();
    }
    spv_config.add_peer(address);
    spv_config.max_peers = 1;

    let result = run_probe(address, tip_height, spv_config, probe_config, events).await;

    let _ = tokio::fs::remove_dir_all(&scratch_dir).await;
    result
}

async fn run_probe(
    address: SocketAddr,
    tip_height: u32,
    spv_config: ClientConfig,
    probe_config: &ProbeConfig,
    events: &Sender<AppEvent>,
) -> anyhow::Result<ProbeResult> {
    let network = spv_config.network;
    let observer = Arc::new(ProbeObserver::default());

    let network_manager = PeerNetworkManager::new(&spv_config)
        .await
        .context("failed to create probe network manager")?;
    let storage_manager = DiskStorageManager::new(&spv_config)
        .await
        .context("failed to create probe storage")?;
    let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(
        network,
    )));
    let client: SpvHealthClient = DashSpvClient::new(
        spv_config,
        network_manager,
        storage_manager,
        wallet,
        vec![Arc::clone(&observer) as Arc<dyn EventHandler>],
    )
    .await
    .context("failed to create probe SPV client")?;

    // Height the storage is anchored at (checkpoint at/below the requested
    // start); everything the node serves is measured relative to this.
    let baseline_height = client.tip_height().await;
    let started = Instant::now();
    observer.mark_run_start(started);
    let runner = client.clone();
    let mut run_task: SpvRunTask = tokio::spawn(async move { runner.run().await });

    let outcome = watch_probe(
        &client,
        &mut run_task,
        address,
        baseline_height,
        started,
        probe_config,
        &observer,
        events,
    )
    .await;

    let final_progress = client.sync_progress().await;
    // The single configured peer's connection stats (ping RTT via the
    // client's periodic pings, bytes served). Captured before shutdown drops
    // the connection; empty if the peer never connected.
    let peer_stats = client.peer_stats().await.into_iter().next();
    let total_time = started.elapsed();

    // Wind the client down unless its run task already exited on its own.
    match &outcome {
        ProbeOutcome::RunEnded(_) => {}
        _ => shutdown_spv_client(&client, run_task, "probe").await,
    }

    let connected = observer.connected_at().is_some();
    match outcome {
        ProbeOutcome::NeverConnected => Err(anyhow!(
            "no connection within {}s",
            CONNECT_TIMEOUT.as_secs()
        )),
        ProbeOutcome::RunEnded(error) if !connected => {
            Err(error.context("probe client failed before connecting"))
        }
        outcome => {
            // Completed, or ended early (timeout / stall / client failure
            // after connecting): grade whatever the node served.
            let extra_timeouts = match outcome {
                ProbeOutcome::Completed => 0,
                _ => 1,
            };
            let metrics = build_metrics(
                &final_progress,
                &observer,
                peer_stats.as_ref(),
                baseline_height,
                tip_height,
                total_time,
                extra_timeouts,
            );
            let grade = grading::grade(&metrics);
            Ok(ProbeResult {
                metrics,
                grade,
                probed_at: std::time::SystemTime::now(),
                stale: false,
            })
        }
    }
}

enum ProbeOutcome {
    /// Every requested phase reached `Synced`.
    Completed,
    /// The configured probe budget elapsed first.
    TimedOut,
    /// No progress at all for [`STALL_TIMEOUT`].
    Stalled,
    /// The node never completed a handshake.
    NeverConnected,
    /// `client.run()` returned before sync completed.
    RunEnded(anyhow::Error),
}

#[allow(clippy::too_many_arguments)]
async fn watch_probe(
    client: &SpvHealthClient,
    run_task: &mut SpvRunTask,
    address: SocketAddr,
    baseline_height: u32,
    started: Instant,
    probe_config: &ProbeConfig,
    observer: &ProbeObserver,
    events: &Sender<AppEvent>,
) -> ProbeOutcome {
    let mut ticker = tokio::time::interval(POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_progress: Option<SyncProgress> = None;
    let mut last_change = Instant::now();
    let mut last_ui_key = None;

    loop {
        tokio::select! {
            result = &mut *run_task => {
                return ProbeOutcome::RunEnded(match result {
                    Ok(Ok(())) => anyhow!("probe client stopped before sync completed"),
                    Ok(Err(e)) => anyhow::Error::new(e).context("probe sync failed"),
                    Err(e) => anyhow::Error::new(e).context("probe task panicked"),
                });
            }
            _ = ticker.tick() => {
                let progress = client.sync_progress().await;
                let connected = observer.connected_at().is_some();

                let snapshot = build_snapshot(
                    &progress,
                    connected,
                    baseline_height,
                    started,
                    probe_config.enable_filters,
                );
                let ui_key = (snapshot.phase, snapshot.headers_synced, snapshot.filters_synced);
                if last_ui_key != Some(ui_key) {
                    last_ui_key = Some(ui_key);
                    let _ = events.send(AppEvent::ProbeUpdate {
                        address,
                        snapshot,
                    });
                }

                if probe_complete(&progress, probe_config.enable_filters) {
                    return ProbeOutcome::Completed;
                }
                if !connected && started.elapsed() > CONNECT_TIMEOUT {
                    return ProbeOutcome::NeverConnected;
                }
                if started.elapsed() > probe_config.timeout {
                    return ProbeOutcome::TimedOut;
                }

                if last_progress.as_ref() != Some(&progress) {
                    last_progress = Some(progress);
                    last_change = Instant::now();
                } else if connected && last_change.elapsed() > STALL_TIMEOUT {
                    return ProbeOutcome::Stalled;
                }
            }
        }
    }
}

fn probe_complete(progress: &SyncProgress, filters_enabled: bool) -> bool {
    let headers = phase_synced(progress.headers().map(|h| h.state()));
    if !filters_enabled {
        return headers;
    }
    headers
        && phase_synced(progress.filter_headers().map(|f| f.state()))
        && phase_synced(progress.filters().map(|f| f.state()))
}

fn build_snapshot(
    progress: &SyncProgress,
    connected: bool,
    baseline_height: u32,
    started: Instant,
    filters_enabled: bool,
) -> ProbeSnapshot {
    let (headers_synced, headers_target) = progress
        .headers()
        .map(|h| span(h.current_height(), h.target_height(), baseline_height))
        .unwrap_or((0, 0));
    let filters_synced = progress
        .filters()
        .map(|f| f.current_height().saturating_sub(baseline_height))
        .unwrap_or(0);

    let phase = if !connected {
        ProbePhase::Connecting
    } else if !phase_synced(progress.headers().map(|h| h.state())) {
        ProbePhase::Headers
    } else if filters_enabled && !phase_synced(progress.filter_headers().map(|f| f.state())) {
        ProbePhase::FilterHeaders
    } else if filters_enabled && !phase_synced(progress.filters().map(|f| f.state())) {
        ProbePhase::Filters
    } else {
        ProbePhase::Finishing
    };

    ProbeSnapshot {
        phase,
        headers_synced,
        headers_target,
        filters_synced,
        elapsed: started.elapsed(),
    }
}

fn span(current: u32, target: u32, baseline: u32) -> (u32, u32) {
    (
        current.saturating_sub(baseline),
        target.saturating_sub(baseline),
    )
}

#[allow(clippy::too_many_arguments)]
fn build_metrics(
    progress: &SyncProgress,
    observer: &ProbeObserver,
    peer_stats: Option<&PeerStatsSnapshot>,
    baseline_height: u32,
    tip_height: u32,
    total_time: Duration,
    extra_timeouts: u32,
) -> ProbeMetrics {
    let advertised = observer
        .advertised_height()
        .or_else(|| peer_stats.and_then(|s| s.best_height));

    // The node is asked for everything from the anchor up to the network tip
    // observed at discovery time. A node legitimately behind that tip is only
    // graded on what it advertises having (its staleness still shows in
    // `advertised_height`), never on chain it doesn't claim to have.
    let mut target_end = tip_height;
    if let Some(advertised) = advertised {
        target_end = target_end.min(advertised.max(baseline_height));
    }
    let headers_target = target_end.saturating_sub(baseline_height);
    let headers_synced = progress
        .headers()
        .map(|h| h.current_height().saturating_sub(baseline_height))
        .unwrap_or(0);
    let filter_headers_synced = progress
        .filter_headers()
        .map(|f| f.current_height().saturating_sub(baseline_height))
        .unwrap_or(0);
    let filters_synced = progress
        .filters()
        .map(|f| f.current_height().saturating_sub(baseline_height))
        .unwrap_or(0);

    let headers_per_sec = rate(headers_synced, observer.headers_window());
    let filters_per_sec = rate(
        filter_headers_synced + filters_synced,
        observer.filters_window(),
    );

    ProbeMetrics {
        connect_time: observer.connected_at(),
        handshake_time: None,
        avg_ping: peer_stats.and_then(|s| s.avg_ping_rtt),
        bytes_received: peer_stats.map(|s| s.bytes_received).unwrap_or(0),
        headers_synced,
        headers_target,
        headers_per_sec,
        filter_headers_synced,
        filters_synced,
        filters_per_sec,
        advertised_height: advertised,
        validation_failures: observer.validation_failures.load(Ordering::Relaxed),
        timeouts: observer.timeouts.load(Ordering::Relaxed) + extra_timeouts,
        total_time,
    }
}

fn rate(count: u32, window: Option<(Instant, Instant)>) -> f64 {
    match window {
        Some((first, last)) if count > 0 => {
            f64::from(count) / (last - first).max(MIN_RATE_WINDOW).as_secs_f64()
        }
        _ => 0.0,
    }
}

/// Collects timings and error counts from client events. All methods are
/// called from the client's monitor tasks; keep them lock-light.
#[derive(Default)]
struct ProbeObserver {
    /// Time from run start to the first completed handshake.
    connected_after: Mutex<Option<Duration>>,
    run_started: Mutex<Option<Instant>>,
    /// (first, last) times data of each kind was stored.
    headers_window: Mutex<Option<(Instant, Instant)>>,
    filters_window: Mutex<Option<(Instant, Instant)>>,
    /// Best height the node advertised (0 = unknown).
    advertised: AtomicU32,
    timeouts: AtomicU32,
    validation_failures: AtomicU32,
}

impl ProbeObserver {
    fn connected_at(&self) -> Option<Duration> {
        self.connected_after.lock().ok().and_then(|g| *g)
    }

    fn advertised_height(&self) -> Option<u32> {
        match self.advertised.load(Ordering::Relaxed) {
            0 => None,
            h => Some(h),
        }
    }

    fn headers_window(&self) -> Option<(Instant, Instant)> {
        self.headers_window.lock().ok().and_then(|g| *g)
    }

    fn filters_window(&self) -> Option<(Instant, Instant)> {
        self.filters_window.lock().ok().and_then(|g| *g)
    }

    fn mark_run_start(&self, started: Instant) {
        if let Ok(mut run_started) = self.run_started.lock() {
            *run_started = Some(started);
        }
    }

    fn mark_connected(&self) {
        let started = self.run_started.lock().ok().and_then(|g| *g);
        if let (Some(started), Ok(mut connected)) = (started, self.connected_after.lock()) {
            connected.get_or_insert_with(|| started.elapsed());
        }
    }

    fn bump_window(window: &Mutex<Option<(Instant, Instant)>>) {
        if let Ok(mut window) = window.lock() {
            let now = Instant::now();
            match window.as_mut() {
                Some((_, last)) => *last = now,
                None => *window = Some((now, now)),
            }
        }
    }

    /// Attribute an error the node caused to the matching grading bucket.
    fn record_error(&self, error: &str) {
        let error = error.to_lowercase();
        if error.contains("timeout") || error.contains("timed out") {
            self.timeouts.fetch_add(1, Ordering::Relaxed);
        } else if error.contains("invalid")
            || error.contains("validation")
            || error.contains("pow")
            || error.contains("mismatch")
            || error.contains("continuity")
        {
            self.validation_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl EventHandler for ProbeObserver {
    fn on_network_event(&self, event: &NetworkEvent) {
        match event {
            NetworkEvent::PeerConnected { .. } => self.mark_connected(),
            NetworkEvent::PeersUpdated {
                best_height: Some(height),
                ..
            } => {
                self.advertised.fetch_max(*height, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn on_sync_event(&self, event: &SyncEvent) {
        match event {
            SyncEvent::BlockHeadersStored { .. } => Self::bump_window(&self.headers_window),
            SyncEvent::FilterHeadersStored { .. } | SyncEvent::FiltersStored { .. } => {
                Self::bump_window(&self.filters_window)
            }
            SyncEvent::ManagerError { error, .. } => self.record_error(error),
            _ => {}
        }
    }

    fn on_error(&self, error: &str) {
        self.record_error(error);
    }
}

/// Filesystem-safe directory name for a probe's scratch storage.
///
/// Must contain NO dots: dash-spv derives its storage lockfile with
/// `Path::set_extension("lock")`, so a dot in the directory name makes
/// everything after it get replaced — `95.183.53.19-9999` and
/// `95.183.53.20-9999` would both lock `95.183.53.lock` and concurrent
/// probes of same-/24 nodes would spuriously fail with "already in use".
fn scratch_dir_name(address: &SocketAddr) -> String {
    address
        .to_string()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_dir_names_are_filesystem_safe_and_dot_free() {
        let v4: SocketAddr = "95.183.53.19:9999".parse().unwrap();
        assert_eq!(scratch_dir_name(&v4), "95-183-53-19-9999");
        let v6: SocketAddr = "[2001:db8::1]:9999".parse().unwrap();
        let name = scratch_dir_name(&v6);
        // No dots: `set_extension("lock")` in dash-spv's storage would
        // truncate at the last dot and collide lockfiles across probes.
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "{name}"
        );

        // Same-/24 neighbours must produce distinct names (and therefore
        // distinct `<name>.lock` files).
        let neighbour: SocketAddr = "95.183.53.20:9999".parse().unwrap();
        assert_ne!(scratch_dir_name(&v4), scratch_dir_name(&neighbour));
    }

    #[test]
    fn rate_uses_minimum_window() {
        let now = Instant::now();
        // A single burst reports (first == last): the floor prevents division
        // by zero and absurd rates.
        let r = rate(1000, Some((now, now)));
        assert!(r <= 2000.0 + f64::EPSILON, "rate was {r}");
        assert_eq!(rate(0, Some((now, now))), 0.0);
        assert_eq!(rate(100, None), 0.0);
    }

    /// Live-network smoke test: discover, then probe a handful of nodes.
    /// Run manually with:
    /// `cargo test -p spv-network-health --release probe_smoke -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "hits the live Dash network"]
    async fn probe_smoke_mainnet() {
        use crate::backend::discovery;
        use crate::config::AppConfig;
        use crate::types::NodeStatus;

        let config = AppConfig {
            network: dashcore::Network::Mainnet,
            data_dir: std::env::temp_dir().join("spv-health-discovery-smoke"),
            probe: ProbeConfig {
                depth: SyncDepth::RecentBlocks(1000),
                enable_filters: true,
                concurrency: 1,
                timeout: Duration::from_secs(120),
            },
        };
        let (events, updates) = std::sync::mpsc::channel();
        let printer = std::thread::spawn(move || {
            while let Ok(event) = updates.recv() {
                match event {
                    AppEvent::DiscoveryProgress { message } => println!("{message}"),
                    AppEvent::ProbeUpdate { address, snapshot } => println!(
                        "{address}: {} {}/{} ({} filters, {:.0}s)",
                        snapshot.phase.label(),
                        snapshot.headers_synced,
                        snapshot.headers_target,
                        snapshot.filters_synced,
                        snapshot.elapsed.as_secs_f32(),
                    ),
                    _ => {}
                }
            }
        });

        let discovered = discovery::fetch_masternode_list(&config, &events)
            .await
            .expect("discovery failed");
        println!(
            "discovered {} nodes at tip {}",
            discovered.nodes.len(),
            discovered.tip_height
        );

        let targets: Vec<SocketAddr> = discovered
            .nodes
            .iter()
            .filter(|n| matches!(n.status, NodeStatus::Pending) && n.is_valid)
            .take(5)
            .map(|n| n.address)
            .collect();

        let mut graded = 0;
        for address in targets {
            match probe_node(
                address,
                discovered.tip_height,
                &config,
                &config.probe,
                &events,
            )
            .await
            {
                Ok(result) => {
                    graded += 1;
                    println!(
                        "{address}: grade {} ({:.1}) — {}/{} headers @ {:.0}/s, {} filter hdrs, \
                         {} filters, connect {:?}, ping {:?}, {} received, {} timeouts, \
                         {} validation failures, {:.1}s",
                        result.grade.letter.label(),
                        result.grade.score,
                        result.metrics.headers_synced,
                        result.metrics.headers_target,
                        result.metrics.headers_per_sec,
                        result.metrics.filter_headers_synced,
                        result.metrics.filters_synced,
                        result.metrics.connect_time,
                        result.metrics.avg_ping,
                        result.metrics.bytes_received,
                        result.metrics.timeouts,
                        result.metrics.validation_failures,
                        result.metrics.total_time.as_secs_f32(),
                    );
                }
                Err(e) => println!("{address}: probe failed: {e:#}"),
            }
        }
        drop(events);
        printer.join().unwrap();

        assert!(graded >= 1, "expected at least one node to be graded");
    }
}
