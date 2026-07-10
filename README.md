# spv-network-health

An egui dashboard that measures the health of the Dash network node-by-node.

It discovers every node from the network's masternode list (synced over SPV),
then probes each node individually: a fresh `dash-spv` client is pinned to
that single peer and asked to sync a configurable slice of the chain — from a
few hundred recent blocks all the way up to a full sync. How well the node
serves that data (connect/handshake time, header and filter throughput,
latency, timeouts, validation failures) is turned into a 0–100 score and a
letter grade shown in the UI.

Status: **complete** (milestones 1–5). Discovery, per-node probing with
measured latency and bytes served, grading, result persistence across runs
(cached rows + per-node history), probe etiquette (per-subnet caps, jittered
starts, full-sync disk guard), and the full UI (sortable table, filter
chips, node detail panel, network summary, CSV/JSON export) all work
end-to-end against mainnet. See [PLAN.md](PLAN.md) for what each milestone
delivered and the remaining future ideas.

## Run

```bash
cargo run -- --network mainnet --sync-depth 1000
```

Options:

| flag | default | meaning |
|------|---------|---------|
| `--network` | `mainnet` | `mainnet` or `testnet` |
| `--data-dir` | `./spv-health-data` | discovery storage + per-probe scratch dirs |
| `--sync-depth` | `1000` | recent blocks each probe syncs, or `full` for genesis-to-tip |
| `--filters` | `true` | also sync BIP157 compact filters during probes |
| `--concurrency` | `8` | nodes probed in parallel |
| `--probe-timeout-secs` | `180` | per-probe budget; slower nodes are graded on what they served |

All of these (except network and data dir) are also editable live in the UI.

## Architecture

```
┌─ UI thread (eframe/egui) ── src/ui/ ───────────────────────────┐
│  node table · probe controls · status bar                      │
└──── Command ▼ (tokio mpsc)          ▲ AppEvent (std mpsc) ─────┘
┌─ backend thread (tokio) ── src/backend/ ───────────────────────┐
│  discovery.rs  one well-connected SPV client → masternode list │
│  probe.rs      per-node SPV client (exclusive peer mode)       │
│  grading.rs    metrics → weighted 0–100 score → letter grade   │
└────────────────────────────────────────────────────────────────┘
```
