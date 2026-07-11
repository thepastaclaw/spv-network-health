//! CSV / JSON export of node rows, shared by the egui "copy" buttons and the
//! headless report writer.

use serde::Serialize;

use crate::types::{NodeRecord, NodeStatus};

/// Flattened, serialization-friendly view of one node row.
#[derive(Serialize)]
pub(crate) struct ExportRow {
    pub(crate) address: String,
    pub(crate) pro_tx_hash: String,
    pub(crate) kind: &'static str,
    pub(crate) valid: bool,
    pub(crate) status: &'static str,
    pub(crate) stale: Option<bool>,
    pub(crate) score: Option<f64>,
    pub(crate) grade: Option<&'static str>,
    pub(crate) headers_synced: Option<u32>,
    pub(crate) headers_target: Option<u32>,
    pub(crate) headers_per_sec: Option<f64>,
    pub(crate) filter_headers_synced: Option<u32>,
    pub(crate) filters_synced: Option<u32>,
    pub(crate) filters_per_sec: Option<f64>,
    pub(crate) advertised_height: Option<u32>,
    pub(crate) connect_ms: Option<u64>,
    pub(crate) avg_ping_ms: Option<u64>,
    pub(crate) bytes_received: Option<u64>,
    pub(crate) timeouts: Option<u32>,
    pub(crate) validation_failures: Option<u32>,
    pub(crate) total_secs: Option<f64>,
    pub(crate) error: Option<String>,
}

const CSV_HEADER: &str = "address,pro_tx_hash,kind,valid,status,stale,score,grade,headers_synced,\
headers_target,headers_per_sec,filter_headers_synced,filters_synced,filters_per_sec,\
advertised_height,connect_ms,avg_ping_ms,bytes_received,timeouts,validation_failures,\
total_secs,error";

pub(crate) fn row(node: &NodeRecord) -> ExportRow {
    let mut row = ExportRow {
        address: node.address.to_string(),
        pro_tx_hash: node.pro_tx_hash.to_string(),
        kind: node.kind.label(),
        valid: node.is_valid,
        status: node.status.label(),
        stale: None,
        score: None,
        grade: None,
        headers_synced: None,
        headers_target: None,
        headers_per_sec: None,
        filter_headers_synced: None,
        filters_synced: None,
        filters_per_sec: None,
        advertised_height: None,
        connect_ms: None,
        avg_ping_ms: None,
        bytes_received: None,
        timeouts: None,
        validation_failures: None,
        total_secs: None,
        error: None,
    };
    match &node.status {
        NodeStatus::Done(result) => {
            let m = &result.metrics;
            row.stale = Some(result.stale);
            row.score = Some(result.grade.score);
            row.grade = Some(result.grade.letter.label());
            row.headers_synced = Some(m.headers_synced);
            row.headers_target = Some(m.headers_target);
            row.headers_per_sec = Some(m.headers_per_sec);
            row.filter_headers_synced = Some(m.filter_headers_synced);
            row.filters_synced = Some(m.filters_synced);
            row.filters_per_sec = Some(m.filters_per_sec);
            row.advertised_height = m.advertised_height;
            row.connect_ms = m.connect_time.map(|d| d.as_millis() as u64);
            row.avg_ping_ms = m.avg_ping.map(|d| d.as_millis() as u64);
            row.bytes_received = Some(m.bytes_received);
            row.timeouts = Some(m.timeouts);
            row.validation_failures = Some(m.validation_failures);
            row.total_secs = Some(m.total_time.as_secs_f64());
        }
        NodeStatus::Failed { error } => {
            row.error = Some(error.clone());
        }
        _ => {}
    }
    row
}

/// Flatten every node into an [`ExportRow`], preserving iteration order.
pub(crate) fn rows<'a>(nodes: impl Iterator<Item = &'a NodeRecord>) -> Vec<ExportRow> {
    nodes.map(row).collect()
}

pub fn to_json<'a>(nodes: impl Iterator<Item = &'a NodeRecord>) -> String {
    serde_json::to_string_pretty(&rows(nodes)).unwrap_or_else(|e| format!("export failed: {e}"))
}

pub fn to_csv<'a>(nodes: impl Iterator<Item = &'a NodeRecord>) -> String {
    let mut out = String::from(CSV_HEADER);
    for node in nodes {
        let r = row(node);
        out.push('\n');
        let fields: [String; 22] = [
            r.address,
            r.pro_tx_hash,
            r.kind.to_string(),
            r.valid.to_string(),
            r.status.to_string(),
            opt(r.stale.map(|s| s.to_string())),
            opt(r.score.map(|s| format!("{s:.1}"))),
            opt(r.grade.map(str::to_string)),
            opt_num(r.headers_synced),
            opt_num(r.headers_target),
            opt(r.headers_per_sec.map(|v| format!("{v:.1}"))),
            opt_num(r.filter_headers_synced),
            opt_num(r.filters_synced),
            opt(r.filters_per_sec.map(|v| format!("{v:.1}"))),
            opt_num(r.advertised_height),
            opt_num(r.connect_ms),
            opt_num(r.avg_ping_ms),
            opt_num(r.bytes_received),
            opt_num(r.timeouts),
            opt_num(r.validation_failures),
            opt(r.total_secs.map(|v| format!("{v:.2}"))),
            opt(r.error),
        ];
        out.push_str(
            &fields
                .iter()
                .map(|f| csv_escape(f))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    out
}

fn opt(value: Option<String>) -> String {
    value.unwrap_or_default()
}

fn opt_num<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn csv_escape(field: &str) -> String {
    if field.contains([',', '"', '\n']) {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
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
                    error: "no connection, with \"quotes\", and commas".to_string(),
                },
                history: Vec::new(),
            },
        ]
    }

    #[test]
    fn csv_has_header_and_one_line_per_node() {
        let nodes = sample_nodes();
        let csv = to_csv(nodes.iter());
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("address,pro_tx_hash,"));
        assert!(lines[1].starts_with("10.0.0.1:9999,"));
        // Quoted field with escaped quotes survives.
        assert!(lines[2].contains("\"no connection, with \"\"quotes\"\", and commas\""));
    }

    #[test]
    fn json_round_trips() {
        let nodes = sample_nodes();
        let json = to_json(nodes.iter());
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        let rows = parsed.as_array().expect("array");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["address"], "10.0.0.1:9999");
        assert!(rows[0]["score"].is_number());
        assert_eq!(rows[1]["status"], "failed");
        assert!(rows[1]["score"].is_null());
    }
}
