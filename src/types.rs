//! Shared data model: nodes discovered from the masternode list, probe
//! progress/metrics, and the resulting grades.

use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use dashcore::ProTxHash;
use serde::{Deserialize, Serialize};

/// A node discovered from the network's masternode list.
#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub pro_tx_hash: ProTxHash,
    /// Core P2P address (from the masternode entry's service address).
    pub address: SocketAddr,
    pub kind: NodeKind,
    /// Whether the masternode entry is marked valid (not PoSe-banned).
    pub is_valid: bool,
    pub status: NodeStatus,
    /// Past probe outcomes for this node (persisted across runs, newest last).
    pub history: Vec<HistoryEntry>,
}

/// One line of a node's probe history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub probed_at: SystemTime,
    pub score: f64,
    pub letter: LetterGrade,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    Regular,
    /// High-performance / Evo node.
    Evo,
}

impl NodeKind {
    pub fn label(&self) -> &'static str {
        match self {
            NodeKind::Regular => "Regular",
            NodeKind::Evo => "Evo",
        }
    }
}

/// Lifecycle of a node within the dashboard.
#[derive(Debug, Clone)]
pub enum NodeStatus {
    /// Discovered but not yet probed.
    Pending,
    /// Waiting for a free probe slot.
    Queued,
    /// A probe is currently running against this node.
    Probing(ProbeSnapshot),
    /// Probe finished and the node was graded.
    Done(ProbeResult),
    /// Probe failed outright before any data was served (unreachable,
    /// handshake failure, setup error, ...).
    Failed { error: String },
}

impl NodeStatus {
    pub fn label(&self) -> &'static str {
        match self {
            NodeStatus::Pending => "pending",
            NodeStatus::Queued => "queued",
            NodeStatus::Probing(_) => "probing",
            NodeStatus::Done(_) => "done",
            NodeStatus::Failed { .. } => "failed",
        }
    }
}

/// Which sync phase a running probe is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbePhase {
    Connecting,
    Headers,
    FilterHeaders,
    Filters,
    Finishing,
}

impl ProbePhase {
    pub fn label(&self) -> &'static str {
        match self {
            ProbePhase::Connecting => "connecting",
            ProbePhase::Headers => "headers",
            ProbePhase::FilterHeaders => "filter headers",
            ProbePhase::Filters => "filters",
            ProbePhase::Finishing => "finishing",
        }
    }
}

/// Live progress of an in-flight probe, streamed to the UI.
#[derive(Debug, Clone)]
pub struct ProbeSnapshot {
    pub phase: ProbePhase,
    pub headers_synced: u32,
    pub headers_target: u32,
    pub filters_synced: u32,
    pub elapsed: Duration,
}

impl ProbeSnapshot {
    pub fn fraction(&self) -> f32 {
        if self.headers_target == 0 {
            0.0
        } else {
            (self.headers_synced as f32 / self.headers_target as f32).clamp(0.0, 1.0)
        }
    }
}

/// Everything measured while probing a single node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProbeMetrics {
    /// TCP connect time.
    pub connect_time: Option<Duration>,
    /// Version/verack handshake round-trip.
    pub handshake_time: Option<Duration>,
    /// Average ping latency observed during the probe.
    pub avg_ping: Option<Duration>,
    /// Headers actually synced vs. requested.
    pub headers_synced: u32,
    pub headers_target: u32,
    pub headers_per_sec: f64,
    /// Filter headers / filters synced (0 when filters are disabled).
    pub filter_headers_synced: u32,
    pub filters_synced: u32,
    pub filters_per_sec: f64,
    /// Best height the node advertised in its version message.
    pub advertised_height: Option<u32>,
    /// Validation failures attributable to data this node served
    /// (bad PoW, broken filter-header chain, non-continuous headers, ...).
    pub validation_failures: u32,
    /// Requests that timed out and had to be re-issued.
    pub timeouts: u32,
    /// Total bytes the node sent us over the probe's connection.
    pub bytes_received: u64,
    /// Total wall-clock time of the probe.
    pub total_time: Duration,
}

/// Final outcome of a completed probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub metrics: ProbeMetrics,
    pub grade: Grade,
    /// When the probe finished.
    pub probed_at: SystemTime,
    /// True when this result was loaded from a previous run's persisted
    /// store and the node hasn't been re-probed yet this session.
    #[serde(default)]
    pub stale: bool,
}

/// A node's grade: a 0–100 score with per-dimension breakdown.
///
/// `latency` and `connectivity` are `None` when the underlying measurement is
/// unavailable (e.g. ping is not yet exposed by dash-spv); missing dimensions
/// are excluded from the overall score rather than counted as zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grade {
    /// Overall weighted score in `0.0..=100.0`.
    pub score: f64,
    pub letter: LetterGrade,
    /// Component scores (each `0.0..=100.0`), for the detail view.
    pub connectivity: Option<f64>,
    pub throughput: f64,
    pub completeness: f64,
    pub latency: Option<f64>,
    pub reliability: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LetterGrade {
    A,
    B,
    C,
    D,
    F,
}

impl LetterGrade {
    pub fn label(&self) -> &'static str {
        match self {
            LetterGrade::A => "A",
            LetterGrade::B => "B",
            LetterGrade::C => "C",
            LetterGrade::D => "D",
            LetterGrade::F => "F",
        }
    }
}
