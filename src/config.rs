//! CLI arguments and runtime configuration.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use dashcore::Network;

/// Grade every node in the Dash masternode list by probing it with an SPV sync.
#[derive(Debug, Parser)]
#[command(name = "spv-network-health", version, about)]
pub struct Args {
    /// Network to inspect: mainnet or testnet.
    #[arg(long, default_value = "mainnet")]
    pub network: String,

    /// Directory for discovery storage and per-probe scratch storage.
    #[arg(long, default_value = "./spv-health-data")]
    pub data_dir: PathBuf,

    /// How many recent blocks each probe syncs, or "full" for a full sync
    /// from genesis.
    #[arg(long, default_value = "1000")]
    pub sync_depth: String,

    /// Also sync BIP157 compact filters during probes (measures filter serving).
    #[arg(long, default_value_t = true)]
    pub filters: bool,

    /// How many nodes to probe concurrently.
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,

    /// Per-probe timeout in seconds (a probe that hasn't finished by then is
    /// graded on what it managed to serve).
    #[arg(long, default_value_t = 180)]
    pub probe_timeout_secs: u64,

    /// Exclude PoSe-banned masternodes entirely (they won't appear in any
    /// list or be probed).
    #[arg(long, default_value_t = false)]
    pub skip_pose_banned: bool,
}

/// How much chain each probe asks the node to serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDepth {
    /// Sync approximately this many recent blocks (anchored at the nearest
    /// checkpoint at or before `tip - blocks`).
    RecentBlocks(u32),
    /// Full sync from genesis.
    Full,
}

impl SyncDepth {
    pub fn parse(s: &str) -> Result<Self, String> {
        if s.eq_ignore_ascii_case("full") {
            Ok(SyncDepth::Full)
        } else {
            s.parse::<u32>().map(SyncDepth::RecentBlocks).map_err(|_| {
                format!("invalid sync depth {s:?}: expected a block count or \"full\"")
            })
        }
    }

    pub fn label(&self) -> String {
        match self {
            SyncDepth::RecentBlocks(n) => format!("last {n} blocks"),
            SyncDepth::Full => "full sync".to_string(),
        }
    }
}

/// Settings for the per-node probes. Editable live from the UI.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub depth: SyncDepth,
    pub enable_filters: bool,
    pub concurrency: usize,
    pub timeout: Duration,
}

/// Full application configuration derived from [`Args`].
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub network: Network,
    pub data_dir: PathBuf,
    pub probe: ProbeConfig,
    /// Drop PoSe-banned (invalid) masternodes from every list and probe run.
    pub skip_pose_banned: bool,
}

impl AppConfig {
    pub fn from_args(args: &Args) -> Result<Self, String> {
        let network = match args.network.to_lowercase().as_str() {
            "mainnet" | "dash" => Network::Mainnet,
            "testnet" => Network::Testnet,
            other => {
                return Err(format!(
                    "unsupported network {other:?}: use mainnet or testnet"
                ))
            }
        };
        Ok(AppConfig {
            network,
            data_dir: args.data_dir.clone(),
            skip_pose_banned: args.skip_pose_banned,
            probe: ProbeConfig {
                depth: SyncDepth::parse(&args.sync_depth)?,
                enable_filters: args.filters,
                concurrency: args.concurrency.max(1),
                timeout: Duration::from_secs(args.probe_timeout_secs.max(1)),
            },
        })
    }
}
