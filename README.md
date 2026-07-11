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

## Headless mode

For CI or scheduled runs, `--headless` skips the UI entirely: it discovers
the masternode list, probes every node (or the first `--node-limit` of
them), writes a static report, and exits.

```bash
cargo run -- --headless --network mainnet --sync-depth 1000 \
  --node-limit 50 --output-dir ./spv-health-report
```

| flag | default | meaning |
|------|---------|---------|
| `--headless` | off | run without the UI: discover, probe, write a report, exit |
| `--node-limit` | none | probe at most this many discovered nodes (headless only) |
| `--output-dir` | `./spv-health-report` | where the report is written |

The report directory contains:

- `index.html` — a self-contained, offline-viewable summary table (no
  external resources or scripts), sorted best-graded first
- `results.json` / `results.csv` — the same rows the UI's export buttons
  produce, for further processing

`index.html` always leads with a metadata line so a `--node-limit` sample
can never be mistaken for whole-network results: network, generated-at
(UTC), tip height, nodes discovered before `--node-limit` was applied,
scope (`complete network` or `sample N of M`), sync depth, whether filters
were enabled, concurrency, and the per-probe timeout.

The process exits non-zero on discovery failure or if there are no nodes to
probe; per-node probe failures are recorded in the report instead of
aborting the run.

### Publishing to GitHub Pages

To publish a report to a `gh-pages` branch:

```bash
cargo run -- --headless --network mainnet --output-dir ./spv-health-report

git worktree add /tmp/gh-pages gh-pages 2>/dev/null || \
  git worktree add -B gh-pages /tmp/gh-pages origin/main
rm -rf /tmp/gh-pages/*
cp -r spv-health-report/* /tmp/gh-pages/
cd /tmp/gh-pages
git add index.html results.json results.csv
git commit -m "chore: publish network health report"
git push origin gh-pages
cd -
git worktree remove /tmp/gh-pages
```

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
