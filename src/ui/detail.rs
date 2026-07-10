//! Right-hand detail panel for the selected node.

use eframe::egui;

use crate::types::{NodeRecord, NodeStatus, ProbeMetrics, ProbeResult};

use super::grade_color;

/// What the user did in the detail panel this frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetailAction {
    None,
    Close,
    Reprobe,
}

pub fn show(ctx: &egui::Context, node: &NodeRecord) -> DetailAction {
    let mut action = DetailAction::None;

    egui::SidePanel::right("node_detail")
        .resizable(true)
        .default_width(340.0)
        .show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(node.address.to_string());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("✖").on_hover_text("Close").clicked() {
                        action = DetailAction::Close;
                    }
                });
            });
            ui.separator();

            egui::Grid::new("node_identity")
                .num_columns(2)
                .show(ui, |ui| {
                    ui.label("ProTx");
                    let protx = node.pro_tx_hash.to_string();
                    ui.monospace(format!("{}…", &protx[..16.min(protx.len())]))
                        .on_hover_text(protx.clone());
                    ui.end_row();
                    ui.label("Type");
                    ui.label(node.kind.label());
                    ui.end_row();
                    ui.label("PoSe status");
                    if node.is_valid {
                        ui.label("valid");
                    } else {
                        ui.colored_label(egui::Color32::ORANGE, "banned");
                    }
                    ui.end_row();
                });
            ui.separator();

            match &node.status {
                NodeStatus::Done(result) => {
                    if result.stale {
                        ui.colored_label(
                            egui::Color32::GRAY,
                            format!(
                                "Cached result from {} — re-probe to refresh.",
                                ago(result.probed_at)
                            ),
                        );
                    } else {
                        ui.weak(format!("Probed {}", ago(result.probed_at)));
                    }
                    grade_section(ui, result);
                    ui.separator();
                    metrics_section(ui, &result.metrics);
                }
                NodeStatus::Failed { error } => {
                    ui.colored_label(egui::Color32::LIGHT_RED, "Probe failed");
                    ui.label(error);
                }
                NodeStatus::Probing(snapshot) => {
                    ui.label(format!("Probing: {} phase", snapshot.phase.label()));
                    ui.add(egui::ProgressBar::new(snapshot.fraction()).text(format!(
                        "{}/{} headers",
                        snapshot.headers_synced, snapshot.headers_target
                    )));
                }
                NodeStatus::Queued => {
                    ui.label("Queued for probing…");
                }
                NodeStatus::Pending => {
                    ui.label("Not probed yet.");
                }
            }

            if !node.history.is_empty() {
                ui.separator();
                ui.strong("Probe history");
                egui::ScrollArea::vertical()
                    .id_salt("probe_history")
                    .max_height(160.0)
                    .show(ui, |ui| {
                        for entry in node.history.iter().rev() {
                            ui.horizontal(|ui| {
                                ui.colored_label(grade_color(entry.letter), entry.letter.label());
                                ui.label(format!("{:.1}", entry.score));
                                ui.weak(ago(entry.probed_at));
                            });
                        }
                    });
            }

            ui.separator();
            let probing = matches!(node.status, NodeStatus::Probing(_) | NodeStatus::Queued);
            if ui
                .add_enabled(!probing, egui::Button::new("🔁 Re-probe this node"))
                .clicked()
            {
                action = DetailAction::Reprobe;
            }
        });

    action
}

/// Rough humanized elapsed time ("12m ago").
fn ago(at: std::time::SystemTime) -> String {
    let seconds = at.elapsed().map(|d| d.as_secs()).unwrap_or(0);
    match seconds {
        0..=59 => "just now".to_string(),
        60..=3_599 => format!("{}m ago", seconds / 60),
        3_600..=86_399 => format!("{}h ago", seconds / 3_600),
        _ => format!("{}d ago", seconds / 86_400),
    }
}

fn grade_section(ui: &mut egui::Ui, result: &ProbeResult) {
    let grade = &result.grade;
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(grade.letter.label())
                .size(36.0)
                .strong()
                .color(grade_color(grade.letter)),
        );
        ui.label(egui::RichText::new(format!("{:.1} / 100", grade.score)).size(18.0));
    });
    ui.add_space(4.0);

    dimension_bar(ui, "Completeness", Some(grade.completeness));
    dimension_bar(ui, "Throughput", Some(grade.throughput));
    dimension_bar(ui, "Reliability", Some(grade.reliability));
    dimension_bar(ui, "Latency", grade.latency);
    dimension_bar(ui, "Connectivity", grade.connectivity);
}

fn dimension_bar(ui: &mut egui::Ui, name: &str, score: Option<f64>) {
    ui.horizontal(|ui| {
        ui.add_sized([90.0, 16.0], egui::Label::new(name));
        match score {
            Some(score) => {
                let fraction = (score / 100.0).clamp(0.0, 1.0) as f32;
                ui.add(
                    egui::ProgressBar::new(fraction)
                        .text(format!("{score:.0}"))
                        .desired_height(14.0),
                );
            }
            None => {
                ui.weak("not measured");
            }
        }
    });
}

fn format_bytes(bytes: u64) -> String {
    match bytes {
        0..=1023 => format!("{bytes} B"),
        1024..=1048575 => format!("{:.1} KiB", bytes as f64 / 1024.0),
        _ => format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0)),
    }
}

fn metrics_section(ui: &mut egui::Ui, m: &ProbeMetrics) {
    egui::Grid::new("node_metrics")
        .num_columns(2)
        .striped(true)
        .show(ui, |ui| {
            ui.label("Headers");
            ui.label(format!("{} / {}", m.headers_synced, m.headers_target));
            ui.end_row();
            ui.label("Headers/s");
            ui.label(format!("{:.0}", m.headers_per_sec));
            ui.end_row();
            ui.label("Filter headers");
            ui.label(m.filter_headers_synced.to_string());
            ui.end_row();
            ui.label("Filters");
            ui.label(m.filters_synced.to_string());
            ui.end_row();
            ui.label("Filters/s");
            ui.label(format!("{:.0}", m.filters_per_sec));
            ui.end_row();
            ui.label("Advertised height");
            ui.label(
                m.advertised_height
                    .map(|h| h.to_string())
                    .unwrap_or_else(|| "—".into()),
            );
            ui.end_row();
            ui.label("Connect");
            ui.label(
                m.connect_time
                    .map(|d| format!("{} ms", d.as_millis()))
                    .unwrap_or_else(|| "—".into()),
            );
            ui.end_row();
            ui.label("Ping");
            ui.label(
                m.avg_ping
                    .map(|d| format!("{} ms", d.as_millis()))
                    .unwrap_or_else(|| "—".into()),
            );
            ui.end_row();
            ui.label("Bytes received");
            ui.label(format_bytes(m.bytes_received));
            ui.end_row();
            ui.label("Timeouts");
            ui.label(m.timeouts.to_string());
            ui.end_row();
            ui.label("Validation failures");
            if m.validation_failures > 0 {
                ui.colored_label(egui::Color32::RED, m.validation_failures.to_string());
            } else {
                ui.label("0");
            }
            ui.end_row();
            ui.label("Probe time");
            ui.label(format!("{:.1}s", m.total_time.as_secs_f32()));
            ui.end_row();
        });
}
