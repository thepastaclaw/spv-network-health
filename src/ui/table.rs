//! The sortable node table.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use eframe::egui;
use egui_extras::{Column, TableBuilder};

use crate::types::{NodeRecord, NodeStatus};

use super::grade_color;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortKey {
    Address,
    Kind,
    Status,
    Score,
    Speed,
    Ping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sort {
    pub key: SortKey,
    pub descending: bool,
}

impl Default for Sort {
    fn default() -> Self {
        Sort {
            key: SortKey::Score,
            descending: true,
        }
    }
}

impl Sort {
    /// The sort that results from clicking a column header: toggles direction
    /// on the active column, otherwise switches to that column with its
    /// natural default direction.
    fn clicked(self, key: SortKey) -> Sort {
        if self.key == key {
            Sort {
                key,
                descending: !self.descending,
            }
        } else {
            Sort {
                key,
                // Metrics read best sorted high-to-low by default.
                descending: matches!(key, SortKey::Score | SortKey::Speed),
            }
        }
    }
}

/// Sort a list of node addresses according to `sort`. Ties break by address
/// so the ordering is stable across frames.
pub fn sort_addresses(
    addresses: &mut [SocketAddr],
    nodes: &BTreeMap<SocketAddr, NodeRecord>,
    sort: Sort,
) {
    addresses.sort_by(|a, b| {
        let (Some(na), Some(nb)) = (nodes.get(a), nodes.get(b)) else {
            return a.cmp(b);
        };
        let ordering = compare(na, nb, sort.key);
        let ordering = if sort.descending {
            ordering.reverse()
        } else {
            ordering
        };
        ordering.then_with(|| a.cmp(b))
    });
}

fn compare(a: &NodeRecord, b: &NodeRecord, key: SortKey) -> Ordering {
    match key {
        SortKey::Address => a.address.cmp(&b.address),
        SortKey::Kind => a.kind.label().cmp(b.kind.label()),
        SortKey::Status => status_rank(&a.status).cmp(&status_rank(&b.status)),
        SortKey::Score => score_of(a).total_cmp(&score_of(b)),
        SortKey::Speed => speed_of(a).total_cmp(&speed_of(b)),
        SortKey::Ping => ping_of(a).cmp(&ping_of(b)),
    }
}

fn status_rank(status: &NodeStatus) -> u8 {
    match status {
        NodeStatus::Done(_) => 0,
        NodeStatus::Probing(_) => 1,
        NodeStatus::Queued => 2,
        NodeStatus::Pending => 3,
        NodeStatus::Failed { .. } => 4,
    }
}

fn score_of(node: &NodeRecord) -> f64 {
    match &node.status {
        NodeStatus::Done(result) => result.grade.score,
        _ => f64::NEG_INFINITY,
    }
}

fn speed_of(node: &NodeRecord) -> f64 {
    match &node.status {
        NodeStatus::Done(result) => result.metrics.headers_per_sec,
        _ => f64::NEG_INFINITY,
    }
}

fn ping_of(node: &NodeRecord) -> Duration {
    match &node.status {
        NodeStatus::Done(result) => result.metrics.avg_ping.unwrap_or(Duration::MAX),
        _ => Duration::MAX,
    }
}

/// What the user did to the table this frame.
#[derive(Default)]
pub struct TableAction {
    pub clicked: Option<SocketAddr>,
    pub sort_changed: Option<Sort>,
}

const COLUMNS: [(&str, Option<SortKey>); 8] = [
    ("Address", Some(SortKey::Address)),
    ("Type", Some(SortKey::Kind)),
    ("Status", Some(SortKey::Status)),
    ("Grade", Some(SortKey::Score)),
    ("Score", Some(SortKey::Score)),
    ("Headers/s", Some(SortKey::Speed)),
    ("Ping", Some(SortKey::Ping)),
    ("Detail", None),
];

pub fn show(
    ui: &mut egui::Ui,
    nodes: &BTreeMap<SocketAddr, NodeRecord>,
    visible: &[SocketAddr],
    sort: Sort,
    selected: Option<SocketAddr>,
) -> TableAction {
    let mut action = TableAction::default();

    TableBuilder::new(ui)
        .striped(true)
        .sense(egui::Sense::click())
        .column(Column::auto().at_least(160.0)) // address
        .column(Column::auto().at_least(70.0)) // kind
        .column(Column::auto().at_least(70.0)) // status
        .column(Column::auto().at_least(50.0)) // grade
        .column(Column::auto().at_least(55.0)) // score
        .column(Column::auto().at_least(80.0)) // headers/s
        .column(Column::auto().at_least(60.0)) // ping
        .column(Column::remainder()) // progress / error
        .header(22.0, |mut header| {
            for (title, sort_key) in COLUMNS {
                header.col(|ui| match sort_key {
                    Some(key) => {
                        let active = sort.key == key;
                        let label = if active {
                            format!("{title} {}", if sort.descending { "▼" } else { "▲" })
                        } else {
                            title.to_string()
                        };
                        if ui
                            .selectable_label(active, egui::RichText::new(label).strong())
                            .clicked()
                        {
                            action.sort_changed = Some(sort.clicked(key));
                        }
                    }
                    None => {
                        ui.strong(title);
                    }
                });
            }
        })
        .body(|body| {
            body.rows(20.0, visible.len(), |mut row| {
                let Some(node) = nodes.get(&visible[row.index()]) else {
                    return;
                };
                row.set_selected(selected == Some(node.address));
                node_row(&mut row, node);
                if row.response().clicked() {
                    action.clicked = Some(node.address);
                }
            });
        });

    action
}

fn node_row(row: &mut egui_extras::TableRow<'_, '_>, node: &NodeRecord) {
    row.col(|ui| {
        ui.monospace(node.address.to_string());
    });
    row.col(|ui| {
        if node.is_valid {
            ui.label(node.kind.label());
        } else {
            ui.colored_label(egui::Color32::ORANGE, format!("{} ⚠", node.kind.label()))
                .on_hover_text("PoSe-banned (marked invalid in the masternode list)");
        }
    });
    row.col(|ui| match &node.status {
        NodeStatus::Done(result) if result.stale => {
            ui.weak("cached");
        }
        status => {
            ui.label(status.label());
        }
    });
    match &node.status {
        NodeStatus::Done(result) => {
            let grade = &result.grade;
            let mut color = grade_color(grade.letter);
            if result.stale {
                color = color.gamma_multiply(0.5);
            }
            row.col(|ui| {
                ui.colored_label(color, grade.letter.label());
            });
            row.col(|ui| {
                let text = format!("{:.1}", grade.score);
                if result.stale {
                    ui.weak(text);
                } else {
                    ui.label(text);
                }
            });
            row.col(|ui| {
                ui.label(format!("{:.0}", result.metrics.headers_per_sec));
            });
            row.col(|ui| {
                ui.label(
                    result
                        .metrics
                        .avg_ping
                        .map(|p| format!("{} ms", p.as_millis()))
                        .unwrap_or_else(|| "—".into()),
                );
            });
            row.col(|ui| {
                ui.label(format!(
                    "{}/{} headers in {:.1}s",
                    result.metrics.headers_synced,
                    result.metrics.headers_target,
                    result.metrics.total_time.as_secs_f32(),
                ));
            });
        }
        NodeStatus::Probing(snapshot) => {
            row.col(|ui| {
                ui.label("…");
            });
            row.col(|ui| {
                ui.label("…");
            });
            row.col(|ui| {
                ui.label(snapshot.phase.label());
            });
            row.col(|ui| {
                ui.label("…");
            });
            row.col(|ui| {
                let mut text = format!(
                    "{}/{} headers · {:.0}s",
                    snapshot.headers_synced,
                    snapshot.headers_target,
                    snapshot.elapsed.as_secs_f32(),
                );
                if snapshot.filters_synced > 0 {
                    text = format!("{text} · {} filters", snapshot.filters_synced);
                }
                ui.add(
                    egui::ProgressBar::new(snapshot.fraction())
                        .text(text)
                        .desired_height(14.0),
                );
            });
        }
        NodeStatus::Failed { error } => {
            row.col(|ui| {
                ui.colored_label(egui::Color32::RED, "F");
            });
            for _ in 0..3 {
                row.col(|ui| {
                    ui.label("—");
                });
            }
            row.col(|ui| {
                ui.colored_label(egui::Color32::LIGHT_RED, error);
            });
        }
        NodeStatus::Pending | NodeStatus::Queued => {
            for _ in 0..4 {
                row.col(|ui| {
                    ui.label("—");
                });
            }
            row.col(|ui| {
                ui.label("");
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grading;
    use crate::types::{NodeKind, ProbeMetrics, ProbeResult};

    fn node(address: &str, status: NodeStatus) -> NodeRecord {
        NodeRecord {
            pro_tx_hash: "3333333333333333333333333333333333333333333333333333333333333333"
                .parse()
                .unwrap(),
            address: address.parse().unwrap(),
            kind: NodeKind::Regular,
            is_valid: true,
            status,
            history: Vec::new(),
        }
    }

    fn done(address: &str, score_hint: f64) -> NodeRecord {
        // headers_per_sec drives throughput, which drives the score ordering.
        let metrics = ProbeMetrics {
            headers_synced: 1000,
            headers_target: 1000,
            headers_per_sec: score_hint,
            ..Default::default()
        };
        let grade = grading::grade(&metrics);
        node(
            address,
            NodeStatus::Done(ProbeResult {
                metrics,
                grade,
                probed_at: std::time::SystemTime::now(),
                stale: false,
            }),
        )
    }

    #[test]
    fn default_sort_puts_best_scores_first_and_ungraded_last() {
        let nodes: BTreeMap<SocketAddr, NodeRecord> = [
            done("10.0.0.1:9999", 100.0),
            done("10.0.0.2:9999", 400.0),
            node("10.0.0.3:9999", NodeStatus::Pending),
            node("10.0.0.4:9999", NodeStatus::Failed { error: "x".into() }),
        ]
        .into_iter()
        .map(|n| (n.address, n))
        .collect();

        let mut addresses: Vec<SocketAddr> = nodes.keys().copied().collect();
        sort_addresses(&mut addresses, &nodes, Sort::default());

        assert_eq!(addresses[0], "10.0.0.2:9999".parse().unwrap());
        assert_eq!(addresses[1], "10.0.0.1:9999".parse().unwrap());
        // Ungraded nodes (NEG_INFINITY score) sort after all graded ones.
        assert!(addresses[2..]
            .iter()
            .all(|a| { !matches!(nodes[a].status, NodeStatus::Done(_)) }));
    }

    #[test]
    fn clicking_active_column_toggles_direction() {
        let sort = Sort::default();
        assert!(sort.descending);
        let toggled = sort.clicked(SortKey::Score);
        assert!(!toggled.descending);
        let switched = toggled.clicked(SortKey::Address);
        assert_eq!(switched.key, SortKey::Address);
        assert!(!switched.descending, "address sorts ascending by default");
        let speed = switched.clicked(SortKey::Speed);
        assert!(speed.descending, "speed sorts descending by default");
    }
}
