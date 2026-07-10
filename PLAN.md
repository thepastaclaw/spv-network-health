# spv-network-health — implementation plan

Goal: an egui dashboard showing every node from the Dash masternode list,
each graded by how well it served data to a bounded SPV sync. Sync depth is
configurable from a handful of blocks up to a full sync.

## Milestone 1 — scaffolding ✅ (done)

- Workspace member `spv-network-health` with eframe/egui UI shell, clap CLI,
  backend thread + channel plumbing, shared types, and the grading model
  (implemented and unit-tested).
- `backend/discovery.rs` and `backend/probe.rs` are stubs that return errors;
  their doc comments record the exact dash-spv APIs to use.

## Milestone 2 — masternode discovery ✅ (done)

Verified against live mainnet (`discovery_smoke_mainnet`, run with
`cargo test --release -- --ignored --nocapture`):
2,605 routable nodes discovered at tip 2,502,463 in ~9 s (0 unroutable,
351 duplicate addresses collapsed).

Implemented in `discovery::fetch_masternode_list`:

1. Build a discovery `ClientConfig::new(network)`:
   - masternode sync on (default), `.without_filters()`,
   - `.with_start_height(u32::MAX)` → latest checkpoint (we need the current
     list, not history),
   - storage at `<data_dir>/discovery` (persistent across runs so refreshes
     are incremental),
   - empty `peers` → DNS seeds + embedded seed list bootstrap discovery.
2. Assemble the stack exactly as `dash-spv/examples/simple_sync.rs` does:
   `PeerNetworkManager::new(&cfg)`, `DiskStorageManager::new(&cfg)`, empty
   `WalletManager::<ManagedWalletInfo>::new(network)`, and
   `DashSpvClient::new(cfg, net, storage, wallet, handlers)`.
3. Pass an `EventHandler` that forwards `on_progress(SyncProgress)` to the UI
   as `AppEvent::DiscoveryProgress` (headers %, masternode diffs processed —
   `MasternodesProgress` has `diffs_processed` / `target_height`).
4. Spawn `client.run()`; poll `client.sync_progress().await` until the
   masternodes phase is synced, then `client.stop()`.
5. Read the list: `client.masternode_list_engine()?.read().await
   .latest_masternode_list()`, and map each `QualifiedMasternodeListEntry` to
   a `NodeRecord`:
   - `address` ← `entry.masternode_list_entry.service_address
     .primary_service_address()` — skip `None` (Tor/I2P/domain entries) but
     count them, so the UI can say "N unroutable nodes skipped";
   - `kind` ← `EntryMasternodeType::Regular` vs `HighPerformance` (Evo);
   - `is_valid` ← entry flag (PoSe-banned nodes stay listed, flagged invalid).
6. Return `Discovered { nodes, tip_height: client.tip_height().await }`.

Acceptance met: pressing "Refresh masternode list" on mainnet fills the
table with thousands of nodes and a tip height; a second refresh is fast
(incremental diff from persisted state). Additions beyond the original
sketch: a 3-minute stall guard so discovery can't hang the UI forever, and
duplicate-address collapsing (351 mainnet masternodes share an IP:port with
another entry).

## Milestone 3 — per-node probe ✅ (done)

Verified against live mainnet (`probe_smoke_mainnet`, run with
`cargo test --release probe_smoke -- --ignored
--nocapture`): 5 nodes probed at depth 1000 with filters on; each served
2,469 headers + 2,469 filter headers + 2,469 filters (checkpoint anchoring
starts below the requested depth) in 0.8–3.3 s, connect times 74–630 ms,
all graded A. Notes from the run:
- header batches land faster than the 500 ms rate floor at shallow depths,
  so headers/sec saturates the throughput scale — thresholds need retuning
  once deeper probes produce real spread (tracked in milestone 5);
- probes ended via completion detection, not timeout; stall/connect guards
  untested against a hostile peer so far.

Implemented in `probe::probe_node`:

1. Per-probe `ClientConfig::new(network)`:
   - `peers = vec![address]`, `.with_restrict_to_configured_peers(true)`,
     `max_peers = 1` — dash-spv's exclusive mode: no DNS discovery, no peer
     persistence, so the probe measures ONLY the node under test;
   - depth: `SyncDepth::RecentBlocks(n)` → `.with_start_height(tip_height -
     n)` (checkpoint-anchored), `SyncDepth::Full` → start height 0;
   - `enable_filters` per config (exercises BIP157 serving);
     `.without_masternodes()` for now (see milestone 5 extension);
   - `ValidationMode::Full` so bad data the node serves becomes a countable
     validation failure rather than silent acceptance;
   - fresh scratch storage `<data_dir>/probes/<addr-sanitized>/`, deleted
     after the probe so every node starts cold and grades are comparable.
2. Metrics collection via a custom `EventHandler` writing into a shared
   `Mutex<ProbeMetrics>`:
   - `NetworkEvent::PeerConnected` → connect+handshake wall-clock (timer
     starts before `client.run()`);
   - `SyncEvent::BlockHeadersStored { tip_height }` → headers progress and
     headers/sec; throttled `AppEvent::ProbeUpdate` snapshots to the UI
     (≤ ~4/sec per node);
   - `FilterHeadersStored` / `FiltersStored` → filter progress;
   - `SyncEvent::ManagerError` / `on_error` → classify into `timeouts` vs
     `validation_failures` (string/enum matching on `SyncError` variants);
   - ping latency: dash-spv's `Peer` tracks ping/pong internally but doesn't
     expose it on the client — first version derives latency from
     handshake time + request/response gaps; see milestone 5 for the
     upstream `dash-spv` patch that exposes `Peer::ping_stats()` properly.
3. Completion: race three futures —
   - depth reached (poll `sync_progress()`; headers synced AND, if enabled,
     filters synced),
   - `probe_config.timeout` elapsed (grade what was served; count as timeout),
   - client error (unreachable / handshake refused → `ProbeFailed`).
   Then `client.stop()`, compute rates, `grading::grade(&metrics)`, clean up
   the scratch dir, return `ProbeResult`.
4. Edge cases: node advertising a lower height than requested depth (clamp
   target to its advertised height — don't punish completeness for chain the
   node legitimately lacks... but DO record `advertised_height`); nodes on
   pre-v70220 protocols without filter support (skip filter phase, note it);
   duplicate addresses in the list (probe once, share the result).

Additions beyond the original sketch: a 15 s connect deadline (dead nodes
cost seconds, not the full probe budget), a 45 s stall guard, grading of
partial results on timeout/stall/mid-sync client death (only never-connected
surfaces as a failure), and weight renormalization in grading so unmeasured
dimensions (ping, until the upstream patch) are excluded rather than scored
as zero.

## Milestone 4 — UI polish ✅ (done)

- ✅ Sortable columns (`ui/table.rs`): click any header to sort; score/speed
  default descending, others ascending; ungraded rows sort after graded;
  ties break by address so ordering is stable. Default: score descending.
- ✅ Node detail panel (`ui/detail.rs`): click a row to open — identity
  (ProTx, type, PoSe status), big letter grade, per-dimension score bars
  (unmeasured dimensions shown as "not measured"), full raw-metrics grid,
  failure error text, and a re-probe button. Click the row again or ✖ to
  close.
- ✅ Summary strip (`ui/mod.rs::Summary`): total nodes, tip height, graded /
  active / failed counts, median score, % unreachable (failed ÷ attempted),
  and per-letter grade counts as colored chips (chips instead of a drawn
  histogram — revisit if the distribution needs more resolution).
- ✅ Filter chips: status (All/Graded/Active/Pending/Failed), type
  (All/Regular/Evo), per-letter grade toggles, plus the search box
  (address or ProTx).
- ✅ Export (`ui/export.rs`): "Copy CSV" / "Copy JSON" of the visible
  (filtered + sorted) rows to the clipboard, 19 columns incl. all metrics;
  CSV quoting and JSON shape are unit-tested.
- ✅ "Probe shown" probes exactly the filtered row set; re-probe per node
  lives in the detail panel.
- Deferred: per-node probe history across re-probes (only the latest result
  is kept per session) — fold into milestone 5's result persistence.

## Milestone 5 — hardening & upstream improvements ✅ (done)

- ✅ **dash-spv (in-repo upstream change)**: `Peer` now tracks
  `bytes_received` (counted at the socket read) and ping RTTs (last +
  running average, recorded in `handle_pong`); the manager sends an initial
  ping right after the handshake so latency is measured within the first
  round-trip instead of after the first 10 s maintenance tick. New
  `PeerStatsSnapshot` surfaces through `NetworkManager::peer_stats()`
  (default empty impl keeps mocks compiling) up to
  `DashSpvClient::peer_stats()`.
- ✅ Probe wiring: `avg_ping` and `bytes_received` come from the peer
  snapshot; the latency grade dimension is now measured (shown in the table,
  detail panel, and exports).
- ✅ Persistence (`backend/store.rs`): results saved per network to
  `<data_dir>/results-<network>.json` (atomic tmp+rename, 2 s debounce plus
  a 5 s flusher and a final flush on shutdown). On startup the cached
  results appear immediately as dimmed "cached" rows with their age; a
  refresh merges the cache into the fresh list and prunes departed nodes.
  Per-node history (last 20 scores) persists and shows in the detail panel.
- ✅ Etiquette: at most 2 concurrent probes per /16 (IPv4) or /32 (IPv6),
  acquired before the global slot so a crowded subnet can't starve the rest;
  deterministic per-address start jitter (≤400 ms); full-sync depth caps
  effective concurrency at 2 with a status-bar notice (disk guard).
- ✅ CI-friendly headless test: `backend_loop_runs_headless` drives the
  command loop with no UI or network; store round-trip/prune/cap tests
  cover persistence.
- Future (unscheduled): masternode-diff serving grade for Evo nodes,
  pause/resume for hours-long full syncs, a global bandwidth ceiling, and
  throughput-threshold retuning once deep-probe data accumulates.

## Grading model (implemented in `src/grading.rs`)

| dimension    | weight | source                                             |
|--------------|--------|----------------------------------------------------|
| completeness | 0.45   | headers served / requested                         |
| throughput   | 0.20   | headers per second (100 pts at ≥2500/s)            |
| reliability  | 0.20   | −25 pts per timeout, −50 per validation failure    |
| latency      | 0.10   | avg ping, 100 pts ≤50 ms → 0 pts ≥1 s              |
| connectivity | 0.05   | connect+handshake, 100 pts ≤150 ms → 0 pts ≥5 s    |

Incompleteness caps the overall score regardless of the other dimensions:
any shortfall → ≤69 (C at best), under half served → ≤49 (D at best),
under a tenth → ≤29 (F). A node that stops serving mid-range would hang a
real syncing wallet, so speed can't buy the grade back. Thresholds were
tightened after the first full mainnet run graded 35%-served stalls as C
("too nice").

Letters: A ≥90, B ≥75, C ≥60, D ≥40, else F. Thresholds are constants at the
top of `grading.rs`; expect to tune them against real probe data.
Dimensions without a measurement (e.g. no ping samples on a very short
probe) are excluded and the remaining weights renormalized.
