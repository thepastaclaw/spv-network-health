//! Masternode-list discovery: one well-connected SPV client whose only job is
//! to sync the masternode list, from which we enumerate every node to probe.
//!
//! The discovery client bootstraps from DNS seeds / the embedded seed list
//! (its `peers` list is left empty), anchors headers at the latest checkpoint
//! (we need the current list, not history), and persists its storage under
//! `<data_dir>/discovery` so refreshes are incremental.

use std::collections::BTreeSet;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use dash_spv::network::PeerNetworkManager;
use dash_spv::storage::DiskStorageManager;
use dash_spv::sync::{ProgressPercentage, SyncProgress};
use dash_spv::{ClientConfig, DashSpvClient};
use dashcore::sml::masternode_list_entry::EntryMasternodeType;
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use key_wallet_manager::WalletManager;
use tokio::sync::RwLock;

use crate::backend::{phase_synced, shutdown_spv_client, AppEvent, SpvHealthClient, SpvRunTask};
use crate::config::AppConfig;
use crate::types::{NodeKind, NodeRecord, NodeStatus};

/// How often sync progress is polled and forwarded to the UI.
const POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Abort discovery when sync makes no progress at all for this long.
const STALL_TIMEOUT: Duration = Duration::from_secs(180);

/// Result of a discovery run.
#[derive(Debug)]
pub struct Discovered {
    pub nodes: Vec<NodeRecord>,
    /// Header tip height at the time the list was read.
    pub tip_height: u32,
    /// Entries skipped because they advertise no routable Core P2P address
    /// (Tor/I2P/domain-only entries).
    pub unroutable: usize,
    /// Entries skipped because another masternode already claimed the same
    /// socket address.
    pub duplicates: usize,
}

/// Sync the masternode list and enumerate all nodes on the network.
pub async fn fetch_masternode_list(
    config: &AppConfig,
    events: &Sender<AppEvent>,
) -> anyhow::Result<Discovered> {
    let spv_config = ClientConfig::new(config.network)
        .with_storage_path(config.data_dir.join("discovery"))
        .without_filters()
        // u32::MAX = anchor at the latest checkpoint: we want the current
        // masternode list, not chain history.
        .with_start_height(u32::MAX);

    let network_manager = PeerNetworkManager::new(&spv_config)
        .await
        .context("failed to create discovery network manager")?;
    let storage_manager = DiskStorageManager::new(&spv_config).await.context(
        "failed to open discovery storage (is a previous discovery run still shutting down?)",
    )?;
    let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(
        config.network,
    )));
    let client: SpvHealthClient =
        DashSpvClient::new(spv_config, network_manager, storage_manager, wallet, vec![])
            .await
            .context("failed to create discovery SPV client")?;

    let runner = client.clone();
    let mut run_task: SpvRunTask = tokio::spawn(async move { runner.run().await });

    match wait_until_masternodes_synced(&client, &mut run_task, events).await {
        WaitOutcome::Synced => {
            shutdown_spv_client(&client, run_task, "discovery").await;
            extract_nodes(&client).await
        }
        WaitOutcome::Stalled(last_state) => {
            shutdown_spv_client(&client, run_task, "discovery").await;
            Err(anyhow!(
                "discovery made no progress for {STALL_TIMEOUT:?} (last state: {last_state})"
            ))
        }
        // The run task already exited on its own; nothing left to stop.
        WaitOutcome::RunEnded(error) => Err(error),
    }
}

enum WaitOutcome {
    /// Headers and the masternode list are fully synced.
    Synced,
    /// No progress for [`STALL_TIMEOUT`]; the client is still running.
    Stalled(String),
    /// `client.run()` returned before sync completed.
    RunEnded(anyhow::Error),
}

async fn wait_until_masternodes_synced(
    client: &SpvHealthClient,
    run_task: &mut SpvRunTask,
    events: &Sender<AppEvent>,
) -> WaitOutcome {
    let mut ticker = tokio::time::interval(POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_message = String::new();
    let mut last_progress: Option<SyncProgress> = None;
    let mut last_change = Instant::now();

    loop {
        tokio::select! {
            result = &mut *run_task => {
                return WaitOutcome::RunEnded(match result {
                    Ok(Ok(())) => {
                        anyhow!("discovery client stopped before the masternode list finished syncing")
                    }
                    Ok(Err(e)) => anyhow::Error::new(e).context("discovery sync failed"),
                    Err(e) => anyhow::Error::new(e).context("discovery task panicked"),
                });
            }
            _ = ticker.tick() => {
                let progress = client.sync_progress().await;

                let message = progress_message(&progress);
                if message != last_message {
                    let _ = events.send(AppEvent::DiscoveryProgress {
                        message: message.clone(),
                    });
                    last_message = message;
                }

                if phase_synced(progress.headers().map(|h| h.state()))
                    && phase_synced(progress.masternodes().map(|m| m.state()))
                {
                    return WaitOutcome::Synced;
                }

                if last_progress.as_ref() != Some(&progress) {
                    last_progress = Some(progress);
                    last_change = Instant::now();
                } else if last_change.elapsed() > STALL_TIMEOUT {
                    return WaitOutcome::Stalled(format!("{:?}", progress.state()));
                }
            }
        }
    }
}

fn progress_message(progress: &SyncProgress) -> String {
    let headers = progress
        .headers()
        .map(|h| format!("headers {}/{}", h.current_height(), h.target_height()))
        .unwrap_or_else(|_| "headers pending".to_string());
    let masternodes = progress
        .masternodes()
        .map(|m| {
            format!(
                "masternode list {}/{} ({} diffs)",
                m.current_height(),
                m.target_height(),
                m.diffs_processed()
            )
        })
        .unwrap_or_else(|_| "masternode list pending".to_string());
    format!("Discovery: {headers} · {masternodes}")
}

/// Map the synced masternode list to probe targets.
async fn extract_nodes(client: &SpvHealthClient) -> anyhow::Result<Discovered> {
    let engine = client
        .masternode_list_engine()
        .context("masternode list engine unavailable")?;
    let engine = engine.read().await;
    let list = engine
        .latest_masternode_list()
        .ok_or_else(|| anyhow!("masternode sync completed but produced no list"))?;

    let mut nodes = Vec::with_capacity(list.masternodes.len());
    let mut seen_addresses = BTreeSet::new();
    let mut unroutable = 0usize;
    let mut duplicates = 0usize;
    for entry in list.masternodes.values() {
        let mn = &entry.masternode_list_entry;
        let Some(address) = mn.service_address.primary_service_address() else {
            unroutable += 1;
            continue;
        };
        if !seen_addresses.insert(address) {
            duplicates += 1;
            continue;
        }
        nodes.push(NodeRecord {
            pro_tx_hash: mn.pro_reg_tx_hash,
            address,
            kind: match mn.mn_type {
                EntryMasternodeType::Regular => NodeKind::Regular,
                EntryMasternodeType::HighPerformance { .. } => NodeKind::Evo,
            },
            is_valid: mn.is_valid,
            status: NodeStatus::Pending,
            history: Vec::new(),
        });
    }
    drop(engine);

    Ok(Discovered {
        nodes,
        tip_height: client.tip_height().await,
        unroutable,
        duplicates,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::{ProbeConfig, SyncDepth};

    /// Live-network smoke test. Run manually with:
    /// `cargo test -p spv-network-health -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "hits the live Dash network"]
    async fn discovery_smoke_mainnet() {
        let config = AppConfig {
            network: dashcore::Network::Mainnet,
            data_dir: std::env::temp_dir().join("spv-health-discovery-smoke"),
            skip_pose_banned: false,
            probe: ProbeConfig {
                depth: SyncDepth::RecentBlocks(100),
                enable_filters: false,
                concurrency: 1,
                timeout: Duration::from_secs(60),
            },
        };
        let (events, progress) = std::sync::mpsc::channel();
        let printer = std::thread::spawn(move || {
            while let Ok(event) = progress.recv() {
                if let AppEvent::DiscoveryProgress { message } = event {
                    println!("{message}");
                }
            }
        });

        let discovered = fetch_masternode_list(&config, &events)
            .await
            .expect("discovery should succeed");
        drop(events);
        printer.join().unwrap();

        println!(
            "discovered {} nodes at tip {} ({} unroutable, {} duplicates)",
            discovered.nodes.len(),
            discovered.tip_height,
            discovered.unroutable,
            discovered.duplicates,
        );
        assert!(
            discovered.nodes.len() > 100,
            "mainnet should have far more than 100 routable masternodes"
        );
        assert!(discovered.tip_height > 2_000_000);
    }
}
