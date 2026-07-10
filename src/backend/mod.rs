//! Backend: owns the tokio runtime work — masternode discovery and the probe
//! queue — and talks to the egui thread over channels.
//!
//! UI -> backend: [`Command`] via a tokio unbounded channel.
//! Backend -> UI: [`AppEvent`] via a `std::sync::mpsc` channel, drained by the
//! UI every frame.

pub mod discovery;
pub mod probe;
pub mod store;

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use dash_spv::network::PeerNetworkManager;
use dash_spv::storage::DiskStorageManager;
use dash_spv::sync::SyncState;
use dash_spv::DashSpvClient;
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use key_wallet_manager::WalletManager;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use cancellation::CancellationFlag;

use crate::config::{AppConfig, ProbeConfig};
use crate::types::{NodeRecord, ProbeResult, ProbeSnapshot};

/// The concrete SPV client both discovery and probes use.
type SpvHealthClient =
    DashSpvClient<WalletManager<ManagedWalletInfo>, PeerNetworkManager, DiskStorageManager>;
type SpvRunTask = JoinHandle<Result<(), dash_spv::SpvError>>;

/// How long to wait for an SPV client's run task after `stop()`.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

/// At most this many concurrent probes per /16 (IPv4) or /32 (IPv6) subnet,
/// so hosting providers aren't hammered even at high global concurrency.
const SUBNET_CONCURRENCY: usize = 2;
/// Full syncs hold hundreds of MB of headers each until cleanup; cap how many
/// run at once regardless of the configured concurrency.
const FULL_SYNC_MAX_CONCURRENCY: usize = 2;
/// Upper bound of the per-probe start jitter.
const MAX_START_JITTER_MS: u64 = 400;
/// How often unwritten results are flushed to disk.
const STORE_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

fn phase_synced(state: Result<SyncState, impl std::error::Error>) -> bool {
    matches!(state, Ok(SyncState::Synced))
}

/// Stop an SPV client and wait for its run task to finish so its storage lock
/// is released.
async fn shutdown_spv_client(client: &SpvHealthClient, run_task: SpvRunTask, what: &str) {
    if let Err(e) = client.stop().await {
        tracing::warn!("failed to stop {what} client cleanly: {e}");
    }
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, run_task)
        .await
        .is_err()
    {
        tracing::warn!("{what} client did not shut down within {SHUTDOWN_TIMEOUT:?}");
    }
}

/// Requests from the UI thread.
#[derive(Debug)]
pub enum Command {
    /// (Re)fetch the masternode list from the network.
    RefreshMasternodeList,
    /// Probe the given nodes, or every known node when `targets` is `None`.
    StartProbes {
        targets: Option<Vec<SocketAddr>>,
    },
    /// Cancel all queued and running probes.
    CancelProbes,
    /// Forget all stored probe results and per-node history.
    ClearResults,
    /// Live-update probe settings (depth, filters, concurrency, timeout).
    UpdateProbeConfig(ProbeConfig),
    Shutdown,
}

/// Notifications to the UI thread.
#[derive(Debug)]
pub enum AppEvent {
    DiscoveryStarted,
    DiscoveryProgress {
        message: String,
    },
    /// Discovery finished: the full node list and the header tip it was read at.
    MasternodeList {
        nodes: Vec<NodeRecord>,
        tip_height: u32,
        /// Masternode entries skipped: no routable address / duplicate address.
        unroutable: usize,
        duplicates: usize,
    },
    DiscoveryFailed(String),
    /// Informational message for the status bar.
    Notice(String),
    ProbeQueued(SocketAddr),
    ProbeUpdate {
        address: SocketAddr,
        snapshot: ProbeSnapshot,
    },
    ProbeFinished {
        address: SocketAddr,
        result: Box<ProbeResult>,
    },
    ProbeFailed {
        address: SocketAddr,
        error: String,
    },
    BackendError(String),
}

/// Minimal cancellation flag (avoids pulling in tokio-util just for this).
mod cancellation {
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug, Default)]
    pub struct CancellationFlag(AtomicBool);

    impl CancellationFlag {
        pub fn cancel(&self) {
            self.0.store(true, Ordering::SeqCst);
        }
        pub fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
        pub fn reset(&self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }
}

/// Backend main loop. Runs on a dedicated thread inside a tokio runtime until
/// [`Command::Shutdown`] arrives or the UI drops the command channel.
pub async fn run(
    mut commands: UnboundedReceiver<Command>,
    events: Sender<AppEvent>,
    config: AppConfig,
) {
    let mut probe_config = config.probe.clone();
    // Known probe targets, refreshed by discovery. Height the list was read at
    // is used to compute `start_from_height` for RecentBlocks depths.
    let mut known_nodes: Vec<NodeRecord> = Vec::new();
    let mut tip_height: u32 = 0;
    let cancel = Arc::new(CancellationFlag::default());
    let results = Arc::new(store::SharedStore::load(store::store_path(&config)));

    // Surface results from previous runs immediately as cached rows.
    let cached = results.cached_nodes();
    if !cached.is_empty() {
        tip_height = results.tip_height();
        known_nodes = cached.clone();
        let count = cached.len();
        let _ = events.send(AppEvent::MasternodeList {
            nodes: cached,
            tip_height,
            unroutable: 0,
            duplicates: 0,
        });
        let _ = events.send(AppEvent::Notice(format!(
            "Loaded {count} cached results from the previous run — refresh to update the node list."
        )));
    }

    // Background flusher so debounce-skipped saves still reach disk.
    let flusher = tokio::spawn({
        let results = Arc::clone(&results);
        async move {
            loop {
                tokio::time::sleep(STORE_FLUSH_INTERVAL).await;
                results.flush_if_dirty();
            }
        }
    });

    while let Some(command) = commands.recv().await {
        match command {
            Command::RefreshMasternodeList => {
                let _ = events.send(AppEvent::DiscoveryStarted);
                match discovery::fetch_masternode_list(&config, &events).await {
                    Ok(discovered) => {
                        let nodes =
                            results.merge_discovered(discovered.nodes, discovered.tip_height);
                        known_nodes = nodes.clone();
                        tip_height = discovered.tip_height;
                        let _ = events.send(AppEvent::MasternodeList {
                            nodes,
                            tip_height: discovered.tip_height,
                            unroutable: discovered.unroutable,
                            duplicates: discovered.duplicates,
                        });
                    }
                    Err(e) => {
                        let _ = events.send(AppEvent::DiscoveryFailed(format!("{e:#}")));
                    }
                }
            }
            Command::StartProbes { targets } => {
                cancel.reset();
                let targets: Vec<SocketAddr> = match targets {
                    Some(t) => t,
                    None => known_nodes.iter().map(|n| n.address).collect(),
                };
                if targets.is_empty() {
                    let _ = events.send(AppEvent::BackendError(
                        "no nodes to probe — refresh the masternode list first".into(),
                    ));
                    continue;
                }
                let identities: HashMap<SocketAddr, store::NodeIdentity> = known_nodes
                    .iter()
                    .map(|n| {
                        (
                            n.address,
                            store::NodeIdentity {
                                pro_tx_hash: n.pro_tx_hash,
                                kind: n.kind,
                                is_valid: n.is_valid,
                            },
                        )
                    })
                    .collect();
                spawn_probes(
                    targets,
                    tip_height,
                    identities,
                    &config,
                    &probe_config,
                    &events,
                    &cancel,
                    &results,
                );
            }
            Command::CancelProbes => {
                cancel.cancel();
            }
            Command::ClearResults => {
                let cleared = results.clear();
                for node in &mut known_nodes {
                    node.status = crate::types::NodeStatus::Pending;
                    node.history.clear();
                }
                let _ = events.send(AppEvent::MasternodeList {
                    nodes: known_nodes.clone(),
                    tip_height,
                    unroutable: 0,
                    duplicates: 0,
                });
                let _ = events.send(AppEvent::Notice(format!(
                    "Cleared {cleared} stored results."
                )));
            }
            Command::UpdateProbeConfig(new_config) => {
                probe_config = new_config;
            }
            Command::Shutdown => break,
        }
    }
    flusher.abort();
    results.flush_if_dirty();
}

/// Fan probes out over semaphores: a global cap plus a per-subnet cap, with a
/// small deterministic start jitter so bursts don't hit the network in
/// lockstep.
#[allow(clippy::too_many_arguments)]
fn spawn_probes(
    targets: Vec<SocketAddr>,
    tip_height: u32,
    identities: HashMap<SocketAddr, store::NodeIdentity>,
    app_config: &AppConfig,
    probe_config: &ProbeConfig,
    events: &Sender<AppEvent>,
    cancel: &Arc<CancellationFlag>,
    results: &Arc<store::SharedStore>,
) {
    let mut concurrency = probe_config.concurrency;
    if probe_config.depth == crate::config::SyncDepth::Full
        && concurrency > FULL_SYNC_MAX_CONCURRENCY
    {
        concurrency = FULL_SYNC_MAX_CONCURRENCY;
        let _ = events.send(AppEvent::Notice(format!(
            "Full-sync probes: concurrency capped at {FULL_SYNC_MAX_CONCURRENCY} to bound disk usage."
        )));
    }
    let global = Arc::new(Semaphore::new(concurrency));
    let mut subnets: HashMap<u64, Arc<Semaphore>> = HashMap::new();

    for address in targets {
        let _ = events.send(AppEvent::ProbeQueued(address));
        let subnet = Arc::clone(
            subnets
                .entry(subnet_key(&address))
                .or_insert_with(|| Arc::new(Semaphore::new(SUBNET_CONCURRENCY))),
        );
        let global = Arc::clone(&global);
        let cancel = Arc::clone(cancel);
        let events = events.clone();
        let app_config = app_config.clone();
        let probe_config = probe_config.clone();
        let results = Arc::clone(results);
        let identity = identities.get(&address).copied();
        tokio::spawn(async move {
            // Subnet first, then global: tasks blocked on a crowded subnet
            // must not hold global slots that other subnets could use.
            let Ok(_subnet_permit) = subnet.acquire().await else {
                return;
            };
            let Ok(_global_permit) = global.acquire().await else {
                return;
            };
            if cancel.is_cancelled() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(start_jitter_ms(&address))).await;
            match probe::probe_node(address, tip_height, &app_config, &probe_config, &events).await
            {
                Ok(result) => {
                    if let Some(identity) = identity {
                        results.record(address, identity, &result);
                    }
                    let _ = events.send(AppEvent::ProbeFinished {
                        address,
                        result: Box::new(result),
                    });
                }
                Err(e) => {
                    let _ = events.send(AppEvent::ProbeFailed {
                        address,
                        error: format!("{e:#}"),
                    });
                }
            }
        });
    }
}

/// Group addresses by /16 for IPv4 and /32 for IPv6.
fn subnet_key(address: &SocketAddr) -> u64 {
    match address.ip() {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            (u64::from(octets[0]) << 8) | u64::from(octets[1])
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            // Tag bit keeps IPv6 keys disjoint from IPv4 keys.
            (1 << 63) | (u64::from(segments[0]) << 16) | u64::from(segments[1])
        }
    }
}

/// Deterministic per-address start jitter (no RNG needed).
fn start_jitter_ms(address: &SocketAddr) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    address.hash(&mut hasher);
    hasher.finish() % MAX_START_JITTER_MS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SyncDepth;

    #[test]
    fn subnet_keys_group_by_prefix() {
        let a: SocketAddr = "95.183.53.19:9999".parse().unwrap();
        let b: SocketAddr = "95.183.99.1:9999".parse().unwrap();
        let c: SocketAddr = "96.183.53.19:9999".parse().unwrap();
        assert_eq!(subnet_key(&a), subnet_key(&b), "same /16 shares a key");
        assert_ne!(subnet_key(&a), subnet_key(&c));

        let v6: SocketAddr = "[2001:db8::1]:9999".parse().unwrap();
        assert_ne!(
            subnet_key(&a),
            subnet_key(&v6),
            "v4 and v6 keys are disjoint"
        );
    }

    #[test]
    fn start_jitter_is_bounded_and_deterministic() {
        let addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
        let jitter = start_jitter_ms(&addr);
        assert!(jitter < MAX_START_JITTER_MS);
        assert_eq!(jitter, start_jitter_ms(&addr));
    }

    /// Headless smoke test of the backend command loop: no UI, no network.
    #[tokio::test(flavor = "multi_thread")]
    async fn backend_loop_runs_headless() {
        let config = AppConfig {
            network: dashcore::Network::Mainnet,
            data_dir: std::env::temp_dir()
                .join(format!("spv-health-backend-test-{}", std::process::id())),
            probe: ProbeConfig {
                depth: SyncDepth::RecentBlocks(10),
                enable_filters: false,
                concurrency: 1,
                timeout: Duration::from_secs(1),
            },
        };
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let backend = tokio::spawn(run(command_rx, event_tx, config.clone()));

        // With no known nodes, StartProbes must answer with an error event
        // rather than hanging or panicking.
        command_tx
            .send(Command::StartProbes { targets: None })
            .expect("backend alive");
        let mut saw_error = false;
        for _ in 0..100 {
            while let Ok(event) = event_rx.try_recv() {
                if matches!(event, AppEvent::BackendError(_)) {
                    saw_error = true;
                }
            }
            if saw_error {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(saw_error, "expected BackendError for an empty target list");

        // Config updates and shutdown are accepted cleanly.
        command_tx
            .send(Command::UpdateProbeConfig(config.probe.clone()))
            .expect("backend alive");
        command_tx.send(Command::Shutdown).expect("backend alive");
        tokio::time::timeout(Duration::from_secs(5), backend)
            .await
            .expect("backend should shut down promptly")
            .expect("backend task should not panic");
        let _ = std::fs::remove_dir_all(&config.data_dir);
    }
}
