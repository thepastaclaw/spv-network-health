//! Headless mode: discover the masternode list, probe every (or the first
//! `--node-limit`) node, and write a static report — no UI, no egui.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result};

use crate::backend::{discovery, spawn_probes, store, AppEvent, CancellationFlag};
use crate::config::AppConfig;
use crate::export;
use crate::types::{NodeRecord, NodeStatus};

/// Outcome of a headless run, printed to the console by `main`.
pub struct Summary {
    pub probed: usize,
    pub graded: usize,
    pub failed: usize,
    pub output_dir: PathBuf,
}

/// Whether a report covers every discovered node or only a `--node-limit`
/// slice of them. Kept distinct from a plain count so a report can never be
/// mistaken for whole-network results when it isn't one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportScope {
    /// Every discovered node was probed.
    Complete,
    /// Only `probed` of `total` discovered nodes were probed, per
    /// `--node-limit`.
    Sample { probed: usize, total: usize },
}

impl ReportScope {
    fn label(&self) -> String {
        match self {
            ReportScope::Complete => "complete network".to_string(),
            ReportScope::Sample { probed, total } => format!("sample {probed} of {total}"),
        }
    }
}

/// Everything about *how* a report was produced, as distinct from the probe
/// rows themselves — so a report is never read as whole-network results
/// when it's actually a `--node-limit` sample. Populated in [`run`] before
/// any truncation happens, and threaded through unchanged to
/// [`write_report`] / `render_html`.
#[derive(Debug, Clone)]
pub struct ReportMetadata {
    pub network: String,
    pub generated_at_utc: String,
    pub tip_height: u32,
    /// Nodes discovered from the masternode list, before `--node-limit` is
    /// applied.
    pub discovered_count: usize,
    pub scope: ReportScope,
    pub sync_depth: String,
    pub filters_enabled: bool,
    pub concurrency: usize,
    pub probe_timeout_secs: u64,
}

/// Discover, probe, write the report, and return counts. No UI, no egui —
/// this is the entire headless code path.
pub async fn run(
    config: AppConfig,
    node_limit: Option<usize>,
    output_dir: PathBuf,
) -> Result<Summary> {
    // Bridge the backend's std::sync::mpsc events onto a tokio channel so a
    // single async loop can both log progress and drive completion.
    let (events_tx, sync_rx) = std::sync::mpsc::channel::<AppEvent>();
    let (async_tx, mut async_rx) = tokio::sync::mpsc::unbounded_channel::<AppEvent>();
    tokio::task::spawn_blocking(move || {
        while let Ok(event) = sync_rx.recv() {
            if async_tx.send(event).is_err() {
                break;
            }
        }
    });

    tracing::info!("discovering the {} masternode list...", config.network);
    let discovered = discovery::fetch_masternode_list(&config, &events_tx)
        .await
        .context("discovery failed")?;
    tracing::info!(
        "discovered {} nodes at tip {} ({} unroutable, {} duplicates)",
        discovered.nodes.len(),
        discovered.tip_height,
        discovered.unroutable,
        discovered.duplicates,
    );

    let results_store = Arc::new(store::SharedStore::load(store::store_path(&config)));
    let tip_height = discovered.tip_height;
    let mut nodes = results_store.merge_discovered(discovered.nodes, tip_height);
    let discovered_count = nodes.len();
    if let Some(limit) = node_limit {
        nodes.truncate(limit);
    }
    if nodes.is_empty() {
        anyhow::bail!("no nodes to probe");
    }
    let scope = if nodes.len() < discovered_count {
        ReportScope::Sample {
            probed: nodes.len(),
            total: discovered_count,
        }
    } else {
        ReportScope::Complete
    };
    let metadata = ReportMetadata {
        network: config.network.to_string(),
        generated_at_utc: format_utc_rfc3339(SystemTime::now()),
        tip_height,
        discovered_count,
        scope,
        sync_depth: config.probe.depth.label(),
        filters_enabled: config.probe.enable_filters,
        concurrency: config.probe.concurrency,
        probe_timeout_secs: config.probe.timeout.as_secs(),
    };

    let targets: Vec<SocketAddr> = nodes.iter().map(|n| n.address).collect();
    let identities: HashMap<SocketAddr, store::NodeIdentity> = nodes
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
    let index_by_address: HashMap<SocketAddr, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.address, i))
        .collect();

    let total = targets.len();
    tracing::info!("probing {total} nodes...");
    let cancel = Arc::new(CancellationFlag::default());
    spawn_probes(
        targets,
        tip_height,
        identities,
        &config,
        &config.probe,
        &events_tx,
        &cancel,
        &results_store,
    );
    // Drop our clone so the bridge thread's `recv()` ends once every spawned
    // probe task's own clone is dropped too.
    drop(events_tx);

    let mut done = 0usize;
    let mut graded = 0usize;
    let mut failed = 0usize;
    while done < total {
        let Some(event) = async_rx.recv().await else {
            anyhow::bail!("probe event channel closed early ({done}/{total} finished)");
        };
        match event {
            AppEvent::ProbeFinished { address, result } => {
                done += 1;
                graded += 1;
                tracing::info!(
                    "[{done}/{total}] {address} -> {} ({:.1})",
                    result.grade.letter.label(),
                    result.grade.score,
                );
                if let Some(&i) = index_by_address.get(&address) {
                    nodes[i].status = NodeStatus::Done(*result);
                }
            }
            AppEvent::ProbeFailed { address, error } => {
                done += 1;
                failed += 1;
                tracing::warn!("[{done}/{total}] {address} -> failed: {error}");
                if let Some(&i) = index_by_address.get(&address) {
                    nodes[i].status = NodeStatus::Failed { error };
                }
            }
            AppEvent::Notice(message) => tracing::info!("{message}"),
            AppEvent::BackendError(error) => tracing::warn!("{error}"),
            _ => {}
        }
    }
    results_store.flush_if_dirty();

    write_report(&nodes, &metadata, &output_dir)?;
    Ok(Summary {
        probed: total,
        graded,
        failed,
        output_dir,
    })
}

/// Write `index.html`, `results.json`, and `results.csv` into `output_dir`
/// (created if missing). Pure and network-free — exercised directly by tests.
fn write_report(nodes: &[NodeRecord], metadata: &ReportMetadata, output_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    std::fs::write(
        output_dir.join("results.json"),
        export::to_json(nodes.iter()),
    )
    .context("failed to write results.json")?;
    std::fs::write(output_dir.join("results.csv"), export::to_csv(nodes.iter()))
        .context("failed to write results.csv")?;
    std::fs::write(output_dir.join("index.html"), render_html(nodes, metadata))
        .context("failed to write index.html")?;
    Ok(())
}

/// Format a `SystemTime` as a UTC RFC 3339 timestamp (e.g.
/// `2026-07-10T12:34:56Z`), without pulling in a chrono/time dependency for
/// one call site.
fn format_utc_rfc3339(t: SystemTime) -> String {
    let secs = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (hour, min, sec) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Days-since-epoch to (year, month, day), proleptic Gregorian calendar.
/// Howard Hinnant's `civil_from_days` algorithm:
/// <http://howardhinnant.github.io/date_algorithms.html>.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = if month <= 2 { y + 1 } else { y };
    (year, month, day)
}

/// A column's cell renderer.
type ColumnFn = fn(&export::ExportRow) -> String;

/// Each report column as a (header label, cell renderer) pair, so a column
/// can never desync from its header — reordering or adding one is a single
/// edit here instead of two parallel lists kept in sync by hand.
const COLUMNS: &[(&str, ColumnFn)] = &[
    ("address", |r| escape_html(&r.address)),
    ("pro tx hash", |r| escape_html(&r.pro_tx_hash)),
    ("kind", |r| r.kind.to_string()),
    ("valid", |r| r.valid.to_string()),
    ("status", |r| r.status.to_string()),
    ("stale", |r| opt_disp(r.stale)),
    ("score", |r| opt_fmt(r.score, |v| format!("{v:.1}"))),
    ("grade", |r| grade_badge(r.grade)),
    ("headers", |r| opt_disp(r.headers_synced)),
    ("hdr target", |r| opt_disp(r.headers_target)),
    ("hdr/s", |r| {
        opt_fmt(r.headers_per_sec, |v| format!("{v:.1}"))
    }),
    ("filter hdrs", |r| opt_disp(r.filter_headers_synced)),
    ("filters", |r| opt_disp(r.filters_synced)),
    ("filt/s", |r| {
        opt_fmt(r.filters_per_sec, |v| format!("{v:.1}"))
    }),
    ("advertised", |r| opt_disp(r.advertised_height)),
    ("connect ms", |r| opt_disp(r.connect_ms)),
    ("ping ms", |r| opt_disp(r.avg_ping_ms)),
    ("bytes", |r| opt_disp(r.bytes_received)),
    ("timeouts", |r| opt_disp(r.timeouts)),
    ("val failures", |r| opt_disp(r.validation_failures)),
    ("secs", |r| opt_fmt(r.total_secs, |v| format!("{v:.2}"))),
    ("error", |r| {
        r.error.as_deref().map(escape_html).unwrap_or_default()
    }),
];

const STYLE: &str = r#"
:root { color-scheme: light dark; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; margin: 2rem; }
h1 { margin-bottom: 0.25rem; }
.meta { opacity: 0.7; margin-bottom: 1.5rem; }
.summary { display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 1.5rem; }
.stat { border: 1px solid currentColor; border-radius: 8px; padding: 0.5rem 1rem; min-width: 90px; text-align: center; opacity: 0.85; }
.stat .n { font-size: 1.5rem; font-weight: 700; display: block; }
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; font-size: 0.85rem; }
th, td { padding: 4px 8px; border-bottom: 1px solid currentColor; text-align: right; white-space: nowrap; }
th:first-child, td:first-child { text-align: left; }
th { position: sticky; top: 0; background: Canvas; }
.grade { font-weight: 700; border-radius: 4px; padding: 2px 6px; color: #111; }
.grade-a { background: #2ecc71; }
.grade-b { background: #9acd32; }
.grade-c { background: #f1c40f; }
.grade-d { background: #e67e22; color: #fff; }
.grade-f { background: #e74c3c; color: #fff; }
"#;

/// Render a self-contained, offline-safe HTML report (no external resources,
/// no script). Every untrusted string (address, pro_tx_hash, error text) is
/// HTML-escaped; only compile-time-constant labels are embedded raw.
fn render_html(nodes: &[NodeRecord], metadata: &ReportMetadata) -> String {
    let mut rows = export::rows(nodes.iter());
    rows.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let graded = rows.iter().filter(|r| r.score.is_some()).count();
    let failed = rows.iter().filter(|r| r.status == "failed").count();
    let avg_score = {
        let scores: Vec<f64> = rows.iter().filter_map(|r| r.score).collect();
        if scores.is_empty() {
            None
        } else {
            Some(scores.iter().sum::<f64>() / scores.len() as f64)
        }
    };

    const LETTERS: [&str; 5] = ["A", "B", "C", "D", "F"];
    let mut letter_counts = [0usize; 5];
    for r in &rows {
        if let Some(g) = r.grade {
            if let Some(i) = LETTERS.iter().position(|l| *l == g) {
                letter_counts[i] += 1;
            }
        }
    }

    let mut html = String::with_capacity(4096 + rows.len() * 256);
    html.push_str("<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    html.push_str("<title>Dash SPV Network Health Report</title>\n<style>");
    html.push_str(STYLE);
    html.push_str("</style>\n</head>\n<body>\n<h1>Dash SPV Network Health</h1>\n");
    html.push_str(&format!(
        "<p class=\"meta\">network: {} &middot; generated: {} &middot; tip: {} &middot; \
         discovered: {} nodes &middot; scope: {} &middot; sync depth: {} &middot; \
         filters: {} &middot; concurrency: {} &middot; probe timeout: {}s</p>\n",
        escape_html(&metadata.network),
        escape_html(&metadata.generated_at_utc),
        metadata.tip_height,
        metadata.discovered_count,
        escape_html(&metadata.scope.label()),
        escape_html(&metadata.sync_depth),
        if metadata.filters_enabled {
            "enabled"
        } else {
            "disabled"
        },
        metadata.concurrency,
        metadata.probe_timeout_secs,
    ));

    html.push_str("<div class=\"summary\">\n");
    push_stat(&mut html, rows.len().to_string(), "probed");
    push_stat(&mut html, graded.to_string(), "graded");
    push_stat(&mut html, failed.to_string(), "failed");
    push_stat(
        &mut html,
        avg_score
            .map(|s| format!("{s:.1}"))
            .unwrap_or_else(|| "—".to_string()),
        "avg score",
    );
    for (letter, count) in LETTERS.iter().zip(letter_counts) {
        push_stat(&mut html, count.to_string(), &format!("grade {letter}"));
    }
    html.push_str("</div>\n");

    html.push_str("<div class=\"table-wrap\">\n<table>\n<thead><tr>");
    for (header, _) in COLUMNS {
        html.push_str(&format!("<th>{header}</th>"));
    }
    html.push_str("</tr></thead>\n<tbody>\n");
    for row in &rows {
        html.push_str(&row_html(row));
    }
    html.push_str("</tbody>\n</table>\n</div>\n</body>\n</html>\n");
    html
}

fn push_stat(html: &mut String, value: String, label: &str) {
    html.push_str(&format!(
        "<div class=\"stat\"><span class=\"n\">{value}</span>{label}</div>\n"
    ));
}

fn td(content: &str) -> String {
    format!("<td>{content}</td>")
}

fn opt_disp<T: std::fmt::Display>(value: Option<T>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "—".to_string())
}

fn opt_fmt<T>(value: Option<T>, f: impl Fn(T) -> String) -> String {
    value.map(f).unwrap_or_else(|| "—".to_string())
}

fn grade_badge(grade: Option<&str>) -> String {
    match grade {
        Some(g) => format!(
            "<span class=\"grade grade-{}\">{g}</span>",
            g.to_lowercase()
        ),
        None => "—".to_string(),
    }
}

fn row_html(r: &export::ExportRow) -> String {
    let mut out = String::from("<tr>");
    for (_, cell) in COLUMNS {
        out.push_str(&td(&cell(r)));
    }
    out.push_str("</tr>\n");
    out
}

fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::grading;
    use crate::types::{NodeKind, ProbeMetrics, ProbeResult};

    fn sample_nodes() -> Vec<NodeRecord> {
        let metrics = ProbeMetrics {
            connect_time: Some(Duration::from_millis(80)),
            headers_synced: 1000,
            headers_target: 1000,
            headers_per_sec: 450.0,
            total_time: Duration::from_secs(3),
            ..Default::default()
        };
        let grade = grading::grade(&metrics);
        vec![
            NodeRecord {
                pro_tx_hash: "2222222222222222222222222222222222222222222222222222222222222222"
                    .parse()
                    .unwrap(),
                address: "10.0.0.1:9999".parse().unwrap(),
                kind: NodeKind::Regular,
                is_valid: true,
                history: Vec::new(),
                status: NodeStatus::Done(ProbeResult {
                    metrics,
                    grade,
                    probed_at: std::time::SystemTime::now(),
                    stale: false,
                }),
            },
            NodeRecord {
                pro_tx_hash: "1111111111111111111111111111111111111111111111111111111111111111"
                    .parse()
                    .unwrap(),
                address: "10.0.0.2:9999".parse().unwrap(),
                kind: NodeKind::Evo,
                is_valid: false,
                status: NodeStatus::Failed {
                    error: "<script>alert(1)</script> \"broke\" & 'quoted'".to_string(),
                },
                history: Vec::new(),
            },
            NodeRecord {
                pro_tx_hash: "3333333333333333333333333333333333333333333333333333333333333333"
                    .parse()
                    .unwrap(),
                address: "10.0.0.3:9999".parse().unwrap(),
                kind: NodeKind::Regular,
                is_valid: true,
                status: NodeStatus::Pending,
                history: Vec::new(),
            },
        ]
    }

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "spv-health-headless-test-{name}-{}",
            std::process::id()
        ))
    }

    fn sample_metadata(scope: ReportScope) -> ReportMetadata {
        ReportMetadata {
            network: "dash".to_string(),
            generated_at_utc: "2026-07-10T12:34:56Z".to_string(),
            tip_height: 123_456,
            discovered_count: match scope {
                ReportScope::Complete => 3,
                ReportScope::Sample { total, .. } => total,
            },
            scope,
            sync_depth: "last 1000 blocks".to_string(),
            filters_enabled: true,
            concurrency: 8,
            probe_timeout_secs: 180,
        }
    }

    #[test]
    fn write_report_creates_all_three_files() {
        let dir = temp_dir("write-report");
        let _ = std::fs::remove_dir_all(&dir);
        let nodes = sample_nodes();
        let metadata = sample_metadata(ReportScope::Complete);

        write_report(&nodes, &metadata, &dir).expect("report should write");

        let html = std::fs::read_to_string(dir.join("index.html")).expect("index.html");
        let csv = std::fs::read_to_string(dir.join("results.csv")).expect("results.csv");
        let json = std::fs::read_to_string(dir.join("results.json")).expect("results.json");

        assert_eq!(csv, export::to_csv(nodes.iter()));
        assert_eq!(json, export::to_json(nodes.iter()));
        assert!(html.contains("123456") || html.contains("123_456"));
        assert!(html.contains("Dash SPV Network Health"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn html_escapes_untrusted_error_text() {
        let nodes = sample_nodes();
        let metadata = sample_metadata(ReportScope::Complete);
        let html = render_html(&nodes, &metadata);
        assert!(
            !html.contains("<script>"),
            "raw script tag leaked into the report"
        );
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(html.contains("&quot;broke&quot;"));
        assert!(html.contains("&#39;quoted&#39;"));
    }

    #[test]
    fn html_summarizes_counts_and_grades() {
        let nodes = sample_nodes();
        let metadata = sample_metadata(ReportScope::Complete);
        let html = render_html(&nodes, &metadata);
        // One graded (Done), one failed, one pending (neither).
        assert!(html.contains("<span class=\"n\">1</span>graded"));
        assert!(html.contains("<span class=\"n\">1</span>failed"));
        assert!(html.contains("<span class=\"n\">3</span>probed"));
        assert!(html.contains("class=\"grade grade-"));
    }

    #[test]
    fn header_and_row_have_equal_column_count() {
        let nodes = sample_nodes();
        let metadata = sample_metadata(ReportScope::Complete);
        let html = render_html(&nodes, &metadata);
        let th_count = html.matches("<th>").count();
        let td_count = html.matches("<td>").count();
        assert_eq!(th_count, COLUMNS.len());
        assert_eq!(td_count, COLUMNS.len() * nodes.len());
    }

    #[test]
    fn html_labels_complete_scope() {
        let nodes = sample_nodes();
        let metadata = sample_metadata(ReportScope::Complete);
        let html = render_html(&nodes, &metadata);
        assert!(html.contains("scope: complete network"));
        assert!(!html.contains("sample"));
    }

    #[test]
    fn html_labels_sample_scope() {
        let nodes = sample_nodes();
        let metadata = sample_metadata(ReportScope::Sample {
            probed: 3,
            total: 500,
        });
        let html = render_html(&nodes, &metadata);
        assert!(html.contains("scope: sample 3 of 500"));
    }

    #[test]
    fn html_metadata_paragraph_is_escaped() {
        let nodes = sample_nodes();
        let mut metadata = sample_metadata(ReportScope::Complete);
        metadata.network = "<script>alert(1)</script>".to_string();
        let html = render_html(&nodes, &metadata);
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
    }
}
