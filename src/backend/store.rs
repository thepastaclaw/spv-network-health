//! Persistence of probe results across runs.
//!
//! One JSON file per network under the data dir. Results recorded during a
//! session are written back with a small time debounce (plus a periodic
//! flush), so a crash loses at most a few seconds of grades. On startup the
//! stored results surface immediately as "cached" (stale) rows until the
//! node is re-probed.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use dashcore::ProTxHash;
use serde::{Deserialize, Serialize};

use crate::config::AppConfig;
use crate::types::{HistoryEntry, NodeKind, NodeRecord, NodeStatus, ProbeResult};

/// Most history entries kept per node.
const HISTORY_CAP: usize = 20;
/// Save at most this often while probes are streaming in; the periodic
/// flusher picks up whatever the debounce skipped.
const SAVE_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(2);

/// On-disk format.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ResultsStore {
    /// Header tip height when the store was last updated from discovery.
    pub tip_height: u32,
    pub nodes: BTreeMap<SocketAddr, StoredNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredNode {
    pub pro_tx_hash: ProTxHash,
    pub kind: NodeKind,
    pub is_valid: bool,
    /// Latest probe outcome (`stale` is stored false; it's set true when the
    /// result is surfaced in a later session).
    pub result: ProbeResult,
    /// Past outcomes, newest last, capped at [`HISTORY_CAP`].
    pub history: Vec<HistoryEntry>,
}

/// Identity of a node being recorded, carried alongside probe tasks.
#[derive(Debug, Clone, Copy)]
pub struct NodeIdentity {
    pub pro_tx_hash: ProTxHash,
    pub kind: NodeKind,
    pub is_valid: bool,
}

pub fn store_path(config: &AppConfig) -> PathBuf {
    config
        .data_dir
        .join(format!("results-{}.json", config.network))
}

/// Thread-safe handle shared between the backend loop and probe tasks.
pub struct SharedStore {
    path: PathBuf,
    state: Mutex<ResultsStore>,
    dirty: AtomicBool,
    last_save: Mutex<Option<Instant>>,
}

impl SharedStore {
    /// Load the store from disk (empty on missing or unreadable file).
    pub fn load(path: PathBuf) -> Self {
        let state = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
                tracing::warn!("ignoring corrupt results store {}: {e}", path.display());
                ResultsStore::default()
            }),
            Err(_) => ResultsStore::default(),
        };
        SharedStore {
            path,
            state: Mutex::new(state),
            dirty: AtomicBool::new(false),
            last_save: Mutex::new(None),
        }
    }

    pub fn tip_height(&self) -> u32 {
        self.state.lock().map(|s| s.tip_height).unwrap_or(0)
    }

    /// Materialize the stored results as cached (stale) node rows.
    pub fn cached_nodes(&self) -> Vec<NodeRecord> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .nodes
            .iter()
            .map(|(address, stored)| {
                let mut result = stored.result.clone();
                result.stale = true;
                NodeRecord {
                    pro_tx_hash: stored.pro_tx_hash,
                    address: *address,
                    kind: stored.kind,
                    is_valid: stored.is_valid,
                    status: NodeStatus::Done(result),
                    history: stored.history.clone(),
                }
            })
            .collect()
    }

    /// Fold stored results into a freshly discovered node list: nodes with a
    /// stored result come back as cached rows, and store entries for nodes
    /// that left the masternode list are pruned.
    pub fn merge_discovered(&self, mut nodes: Vec<NodeRecord>, tip_height: u32) -> Vec<NodeRecord> {
        if let Ok(mut state) = self.state.lock() {
            state.tip_height = tip_height;
            let discovered: std::collections::BTreeSet<SocketAddr> =
                nodes.iter().map(|n| n.address).collect();
            state
                .nodes
                .retain(|address, _| discovered.contains(address));
            for node in &mut nodes {
                if let Some(stored) = state.nodes.get_mut(&node.address) {
                    // The masternode list is the authority on identity.
                    stored.pro_tx_hash = node.pro_tx_hash;
                    stored.kind = node.kind;
                    stored.is_valid = node.is_valid;
                    let mut result = stored.result.clone();
                    result.stale = true;
                    node.status = NodeStatus::Done(result);
                    node.history = stored.history.clone();
                }
            }
            self.dirty.store(true, Ordering::Relaxed);
        }
        self.maybe_save();
        nodes
    }

    /// Record a fresh probe result and save (debounced).
    pub fn record(&self, address: SocketAddr, identity: NodeIdentity, result: &ProbeResult) {
        if let Ok(mut state) = self.state.lock() {
            let entry = state.nodes.entry(address).or_insert_with(|| StoredNode {
                pro_tx_hash: identity.pro_tx_hash,
                kind: identity.kind,
                is_valid: identity.is_valid,
                result: result.clone(),
                history: Vec::new(),
            });
            entry.pro_tx_hash = identity.pro_tx_hash;
            entry.kind = identity.kind;
            entry.is_valid = identity.is_valid;
            entry.result = ProbeResult {
                stale: false,
                ..result.clone()
            };
            entry.history.push(HistoryEntry {
                probed_at: result.probed_at,
                score: result.grade.score,
                letter: result.grade.letter,
            });
            if entry.history.len() > HISTORY_CAP {
                let excess = entry.history.len() - HISTORY_CAP;
                entry.history.drain(..excess);
            }
            self.dirty.store(true, Ordering::Relaxed);
        }
        self.maybe_save();
    }

    /// Remove every stored result and history (the recorded tip height is
    /// kept — the node list itself is still valid). Returns how many nodes
    /// were cleared. Persists immediately.
    pub fn clear(&self) -> usize {
        let cleared = self
            .state
            .lock()
            .map(|mut state| {
                let count = state.nodes.len();
                state.nodes.clear();
                count
            })
            .unwrap_or(0);
        self.dirty.store(true, Ordering::Relaxed);
        self.flush_if_dirty();
        cleared
    }

    /// Save if dirty and the debounce window has passed.
    fn maybe_save(&self) {
        let due = self
            .last_save
            .lock()
            .map(|guard| {
                guard
                    .map(|at| at.elapsed() >= SAVE_DEBOUNCE)
                    .unwrap_or(true)
            })
            .unwrap_or(false);
        if due {
            self.flush_if_dirty();
        }
    }

    /// Unconditionally save if there are unwritten changes.
    pub fn flush_if_dirty(&self) {
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return;
        }
        // Serialize under the lock, write outside it.
        let json = match self.state.lock() {
            Ok(state) => match serde_json::to_vec_pretty(&*state) {
                Ok(json) => json,
                Err(e) => {
                    tracing::warn!("failed to serialize results store: {e}");
                    return;
                }
            },
            Err(_) => return,
        };
        if let Ok(mut last_save) = self.last_save.lock() {
            *last_save = Some(Instant::now());
        }
        if let Err(e) = write_atomically(&self.path, &json) {
            tracing::warn!("failed to save results store {}: {e}", self.path.display());
            self.dirty.store(true, Ordering::Relaxed);
        }
    }
}

fn write_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::grading;
    use crate::types::ProbeMetrics;

    fn identity() -> NodeIdentity {
        NodeIdentity {
            pro_tx_hash: "4444444444444444444444444444444444444444444444444444444444444444"
                .parse()
                .unwrap(),
            kind: NodeKind::Regular,
            is_valid: true,
        }
    }

    fn result(score_hint: f64) -> ProbeResult {
        let metrics = ProbeMetrics {
            headers_synced: 1000,
            headers_target: 1000,
            headers_per_sec: score_hint,
            ..Default::default()
        };
        ProbeResult {
            grade: grading::grade(&metrics),
            metrics,
            probed_at: SystemTime::now(),
            stale: false,
        }
    }

    fn temp_store_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "spv-health-store-test-{name}-{}",
            std::process::id()
        ))
    }

    #[test]
    fn record_flush_load_round_trips() {
        let path = temp_store_path("roundtrip").join("results-mainnet.json");
        let _ = std::fs::remove_file(&path);
        let address: SocketAddr = "10.1.2.3:9999".parse().unwrap();

        let store = SharedStore::load(path.clone());
        store.record(address, identity(), &result(400.0));
        store.flush_if_dirty();

        let reloaded = SharedStore::load(path.clone());
        let cached = reloaded.cached_nodes();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].address, address);
        let NodeStatus::Done(cached_result) = &cached[0].status else {
            panic!("cached node should be Done");
        };
        assert!(cached_result.stale, "loaded results must be marked stale");
        assert_eq!(cached[0].history.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn history_is_capped() {
        let path = temp_store_path("history").join("results-mainnet.json");
        let address: SocketAddr = "10.1.2.4:9999".parse().unwrap();
        let store = SharedStore::load(path);
        for _ in 0..(HISTORY_CAP + 5) {
            store.record(address, identity(), &result(100.0));
        }
        let cached = store.cached_nodes();
        assert_eq!(cached[0].history.len(), HISTORY_CAP);
    }

    #[test]
    fn merge_prunes_departed_nodes_and_marks_cached() {
        let path = temp_store_path("merge").join("results-mainnet.json");
        let kept: SocketAddr = "10.1.2.5:9999".parse().unwrap();
        let departed: SocketAddr = "10.1.2.6:9999".parse().unwrap();
        let store = SharedStore::load(path);
        store.record(kept, identity(), &result(300.0));
        store.record(departed, identity(), &result(300.0));

        let discovered = vec![NodeRecord {
            pro_tx_hash: identity().pro_tx_hash,
            address: kept,
            kind: NodeKind::Evo,
            is_valid: true,
            status: NodeStatus::Pending,
            history: Vec::new(),
        }];
        let merged = store.merge_discovered(discovered, 123_456);

        assert_eq!(merged.len(), 1);
        assert!(matches!(&merged[0].status, NodeStatus::Done(r) if r.stale));
        assert_eq!(
            merged[0].kind,
            NodeKind::Evo,
            "list identity wins over stored identity"
        );
        assert_eq!(store.tip_height(), 123_456);
        assert_eq!(store.cached_nodes().len(), 1, "departed node pruned");
    }
}
