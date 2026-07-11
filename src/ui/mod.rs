//! egui frontend: node table, probe controls, summary strip, detail panel.
//!
//! The UI thread owns no network state. It drains [`AppEvent`]s from the
//! backend every frame and sends [`Command`]s when the user acts.

mod detail;
mod table;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::mpsc::Receiver;

use eframe::egui;
use tokio::sync::mpsc::UnboundedSender;

use crate::backend::{AppEvent, Command};
use crate::config::{AppConfig, SyncDepth};
use crate::export;
use crate::types::{HistoryEntry, LetterGrade, NodeKind, NodeRecord, NodeStatus};

pub(super) fn grade_color(letter: LetterGrade) -> egui::Color32 {
    match letter {
        LetterGrade::A => egui::Color32::from_rgb(0x2e, 0xcc, 0x71),
        LetterGrade::B => egui::Color32::from_rgb(0x9a, 0xcd, 0x32),
        LetterGrade::C => egui::Color32::from_rgb(0xf1, 0xc4, 0x0f),
        LetterGrade::D => egui::Color32::from_rgb(0xe6, 0x7e, 0x22),
        LetterGrade::F => egui::Color32::from_rgb(0xe7, 0x4c, 0x3c),
    }
}

const LETTERS: [LetterGrade; 5] = [
    LetterGrade::A,
    LetterGrade::B,
    LetterGrade::C,
    LetterGrade::D,
    LetterGrade::F,
];

fn letter_index(letter: LetterGrade) -> usize {
    match letter {
        LetterGrade::A => 0,
        LetterGrade::B => 1,
        LetterGrade::C => 2,
        LetterGrade::D => 3,
        LetterGrade::F => 4,
    }
}

/// How the UI edits sync depth: a block count plus a "full sync" switch.
struct DepthEdit {
    full: bool,
    blocks: u32,
}

impl DepthEdit {
    fn from(depth: SyncDepth) -> Self {
        match depth {
            SyncDepth::Full => DepthEdit {
                full: true,
                blocks: 1000,
            },
            SyncDepth::RecentBlocks(n) => DepthEdit {
                full: false,
                blocks: n,
            },
        }
    }

    fn depth(&self) -> SyncDepth {
        if self.full {
            SyncDepth::Full
        } else {
            SyncDepth::RecentBlocks(self.blocks)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusFilter {
    All,
    Graded,
    Active,
    Pending,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KindFilter {
    All,
    Regular,
    Evo,
}

/// Which rows are visible: status/kind/grade chips plus the search box.
struct RowFilter {
    status: StatusFilter,
    kind: KindFilter,
    /// One flag per letter grade (A..F); only restricts graded rows.
    letters: [bool; 5],
    search: String,
}

impl Default for RowFilter {
    fn default() -> Self {
        RowFilter {
            status: StatusFilter::All,
            kind: KindFilter::All,
            letters: [true; 5],
            search: String::new(),
        }
    }
}

impl RowFilter {
    fn matches(&self, node: &NodeRecord) -> bool {
        let status_ok = match self.status {
            StatusFilter::All => true,
            StatusFilter::Graded => matches!(node.status, NodeStatus::Done(_)),
            StatusFilter::Active => {
                matches!(node.status, NodeStatus::Probing(_) | NodeStatus::Queued)
            }
            StatusFilter::Pending => matches!(node.status, NodeStatus::Pending),
            StatusFilter::Failed => matches!(node.status, NodeStatus::Failed { .. }),
        };
        if !status_ok {
            return false;
        }

        let kind_ok = match self.kind {
            KindFilter::All => true,
            KindFilter::Regular => node.kind == NodeKind::Regular,
            KindFilter::Evo => node.kind == NodeKind::Evo,
        };
        if !kind_ok {
            return false;
        }

        if let NodeStatus::Done(result) = &node.status {
            if !self.letters[letter_index(result.grade.letter)] {
                return false;
            }
        }

        if !self.search.is_empty() {
            let needle = self.search.to_lowercase();
            let addr = node.address.to_string();
            if !addr.contains(&needle)
                && !node
                    .pro_tx_hash
                    .to_string()
                    .to_lowercase()
                    .contains(&needle)
            {
                return false;
            }
        }
        true
    }
}

/// Network-wide aggregates for the summary strip.
#[derive(Default)]
struct Summary {
    total: usize,
    graded: usize,
    active: usize,
    failed: usize,
    median_score: Option<f64>,
    letter_counts: [usize; 5],
}

impl Summary {
    fn compute(nodes: &BTreeMap<SocketAddr, NodeRecord>) -> Self {
        let mut summary = Summary {
            total: nodes.len(),
            ..Default::default()
        };
        let mut scores = Vec::new();
        for node in nodes.values() {
            match &node.status {
                NodeStatus::Done(result) => {
                    summary.graded += 1;
                    summary.letter_counts[letter_index(result.grade.letter)] += 1;
                    scores.push(result.grade.score);
                }
                NodeStatus::Probing(_) | NodeStatus::Queued => summary.active += 1,
                NodeStatus::Failed { .. } => summary.failed += 1,
                NodeStatus::Pending => {}
            }
        }
        if !scores.is_empty() {
            scores.sort_by(f64::total_cmp);
            summary.median_score = Some(scores[scores.len() / 2]);
        }
        summary
    }

    /// Failed / attempted: the headline "how much of the network is dark".
    fn unreachable_percent(&self) -> Option<f64> {
        let attempted = self.graded + self.failed;
        (attempted > 0).then(|| 100.0 * self.failed as f64 / attempted as f64)
    }
}

pub struct HealthApp {
    commands: UnboundedSender<Command>,
    events: Receiver<AppEvent>,
    config: AppConfig,

    nodes: BTreeMap<SocketAddr, NodeRecord>,
    tip_height: Option<u32>,
    discovering: bool,
    status_line: String,

    depth_edit: DepthEdit,
    filters_enabled: bool,
    concurrency: usize,
    filter: RowFilter,
    sort: table::Sort,
    selected: Option<SocketAddr>,
    /// "Clear results" was clicked once and awaits confirmation.
    clear_armed: bool,
}

impl HealthApp {
    pub fn new(
        _cc: &eframe::CreationContext<'_>,
        commands: UnboundedSender<Command>,
        events: Receiver<AppEvent>,
        config: AppConfig,
    ) -> Self {
        let depth_edit = DepthEdit::from(config.probe.depth);
        let filters_enabled = config.probe.enable_filters;
        let concurrency = config.probe.concurrency;
        HealthApp {
            commands,
            events,
            config,
            nodes: BTreeMap::new(),
            tip_height: None,
            discovering: false,
            status_line: "Refresh the masternode list to discover nodes.".to_string(),
            depth_edit,
            filters_enabled,
            concurrency,
            filter: RowFilter::default(),
            sort: table::Sort::default(),
            selected: None,
            clear_armed: false,
        }
    }

    fn drain_events(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            match event {
                AppEvent::DiscoveryStarted => {
                    self.discovering = true;
                    self.status_line = "Syncing masternode list…".to_string();
                }
                AppEvent::DiscoveryProgress { message } => {
                    self.status_line = message;
                }
                AppEvent::MasternodeList {
                    nodes,
                    tip_height,
                    unroutable,
                    duplicates,
                } => {
                    self.discovering = false;
                    self.tip_height = Some(tip_height);
                    let mut skipped = String::new();
                    if unroutable > 0 || duplicates > 0 {
                        skipped = format!(
                            " ({unroutable} unroutable, {duplicates} duplicate addresses skipped)"
                        );
                    }
                    self.status_line = format!(
                        "Discovered {} nodes at height {tip_height}{skipped}.",
                        nodes.len()
                    );
                    self.nodes = nodes.into_iter().map(|n| (n.address, n)).collect();
                    if let Some(selected) = self.selected {
                        if !self.nodes.contains_key(&selected) {
                            self.selected = None;
                        }
                    }
                }
                AppEvent::DiscoveryFailed(error) => {
                    self.discovering = false;
                    self.status_line = format!("Discovery failed: {error}");
                }
                AppEvent::Notice(message) => {
                    self.status_line = message;
                }
                AppEvent::ProbeQueued(address) => {
                    if let Some(node) = self.nodes.get_mut(&address) {
                        node.status = NodeStatus::Queued;
                    }
                }
                AppEvent::ProbeUpdate { address, snapshot } => {
                    if let Some(node) = self.nodes.get_mut(&address) {
                        node.status = NodeStatus::Probing(snapshot);
                    }
                }
                AppEvent::ProbeFinished { address, result } => {
                    if let Some(node) = self.nodes.get_mut(&address) {
                        // Mirror the history entry the backend persists.
                        node.history.push(HistoryEntry {
                            probed_at: result.probed_at,
                            score: result.grade.score,
                            letter: result.grade.letter,
                        });
                        node.status = NodeStatus::Done(*result);
                    }
                }
                AppEvent::ProbeFailed { address, error } => {
                    if let Some(node) = self.nodes.get_mut(&address) {
                        node.status = NodeStatus::Failed { error };
                    }
                }
                AppEvent::BackendError(error) => {
                    self.status_line = error;
                }
            }
        }
    }

    fn send(&self, command: Command) {
        // The backend outlives the UI; a send failure means shutdown is racing us.
        let _ = self.commands.send(command);
    }

    fn push_probe_config(&self) {
        let mut probe = self.config.probe.clone();
        probe.depth = self.depth_edit.depth();
        probe.enable_filters = self.filters_enabled;
        probe.concurrency = self.concurrency;
        self.send(Command::UpdateProbeConfig(probe));
    }

    /// Addresses of the rows currently visible, filtered and sorted.
    fn visible_rows(&self) -> Vec<SocketAddr> {
        let mut addresses: Vec<SocketAddr> = self
            .nodes
            .values()
            .filter(|n| self.filter.matches(n))
            .map(|n| n.address)
            .collect();
        table::sort_addresses(&mut addresses, &self.nodes, self.sort);
        addresses
    }

    fn controls(&mut self, ui: &mut egui::Ui, visible: &[SocketAddr]) {
        ui.horizontal_wrapped(|ui| {
            ui.label(format!("Network: {}", self.config.network));
            ui.separator();

            if ui
                .add_enabled(
                    !self.discovering,
                    egui::Button::new("🔄 Refresh masternode list"),
                )
                .clicked()
            {
                self.send(Command::RefreshMasternodeList);
            }

            ui.separator();
            let mut config_changed = false;
            config_changed |= ui
                .checkbox(&mut self.depth_edit.full, "Full sync")
                .changed();
            if !self.depth_edit.full {
                ui.label("Blocks per probe:");
                config_changed |= ui
                    .add(
                        egui::DragValue::new(&mut self.depth_edit.blocks)
                            .range(1..=2_500_000)
                            .speed(50),
                    )
                    .changed();
            }
            config_changed |= ui
                .checkbox(&mut self.filters_enabled, "Sync filters")
                .changed();
            ui.label("Concurrency:");
            config_changed |= ui
                .add(egui::DragValue::new(&mut self.concurrency).range(1..=64))
                .changed();
            if config_changed {
                self.push_probe_config();
            }

            ui.separator();
            if ui
                .add_enabled(!self.nodes.is_empty(), egui::Button::new("▶ Probe all"))
                .clicked()
            {
                self.send(Command::StartProbes { targets: None });
            }
            if ui
                .add_enabled(!visible.is_empty(), egui::Button::new("▶ Probe shown"))
                .on_hover_text("Probe only the rows matching the current filters")
                .clicked()
            {
                self.send(Command::StartProbes {
                    targets: Some(visible.to_vec()),
                });
            }
            if ui.button("⏹ Cancel").clicked() {
                self.send(Command::CancelProbes);
            }
        });
    }

    fn filter_bar(&mut self, ui: &mut egui::Ui, visible: &[SocketAddr]) {
        ui.horizontal_wrapped(|ui| {
            ui.label("Show:");
            for (value, label) in [
                (StatusFilter::All, "All"),
                (StatusFilter::Graded, "Graded"),
                (StatusFilter::Active, "Active"),
                (StatusFilter::Pending, "Pending"),
                (StatusFilter::Failed, "Failed"),
            ] {
                ui.selectable_value(&mut self.filter.status, value, label);
            }

            ui.separator();
            ui.label("Type:");
            for (value, label) in [
                (KindFilter::All, "All"),
                (KindFilter::Regular, "Regular"),
                (KindFilter::Evo, "Evo"),
            ] {
                ui.selectable_value(&mut self.filter.kind, value, label);
            }

            ui.separator();
            ui.label("Grades:");
            for letter in LETTERS {
                let index = letter_index(letter);
                ui.toggle_value(
                    &mut self.filter.letters[index],
                    egui::RichText::new(letter.label())
                        .color(grade_color(letter))
                        .strong(),
                );
            }

            ui.separator();
            ui.label("Search:");
            ui.add(
                egui::TextEdit::singleline(&mut self.filter.search)
                    .desired_width(160.0)
                    .hint_text("address or ProTx"),
            );

            ui.separator();
            let nodes = &self.nodes;
            let rows = || visible.iter().filter_map(|a| nodes.get(a));
            if ui
                .add_enabled(!visible.is_empty(), egui::Button::new("📋 Copy CSV"))
                .on_hover_text("Copy the visible rows as CSV")
                .clicked()
            {
                ui.ctx().copy_text(export::to_csv(rows()));
            }
            if ui
                .add_enabled(!visible.is_empty(), egui::Button::new("📋 Copy JSON"))
                .on_hover_text("Copy the visible rows as JSON")
                .clicked()
            {
                ui.ctx().copy_text(export::to_json(rows()));
            }

            ui.separator();
            if self.clear_armed {
                if ui
                    .button(egui::RichText::new("Confirm clear").color(egui::Color32::RED))
                    .on_hover_text("Really forget all grades and probe history?")
                    .clicked()
                {
                    self.send(Command::ClearResults);
                    self.clear_armed = false;
                }
                if ui.button("✕").on_hover_text("Keep the results").clicked() {
                    self.clear_armed = false;
                }
            } else if ui
                .button("🗑 Clear results")
                .on_hover_text(
                    "Forget all grades and probe history, including the persisted results file",
                )
                .clicked()
            {
                self.clear_armed = true;
            }
        });
    }

    fn summary_strip(&self, ui: &mut egui::Ui) {
        let summary = Summary::compute(&self.nodes);
        ui.horizontal_wrapped(|ui| {
            ui.strong(format!("{} nodes", summary.total));
            if let Some(tip) = self.tip_height {
                ui.label(format!("tip {tip}"));
            }
            ui.separator();
            ui.label(format!("{} graded", summary.graded));
            if summary.active > 0 {
                ui.label(format!("· {} active", summary.active));
            }
            if summary.failed > 0 {
                ui.colored_label(
                    egui::Color32::LIGHT_RED,
                    format!("· {} failed", summary.failed),
                );
            }
            if let Some(median) = summary.median_score {
                ui.separator();
                ui.label("median score:");
                ui.strong(format!("{median:.1}"));
            }
            if let Some(unreachable) = summary.unreachable_percent() {
                ui.label(format!("· {unreachable:.1}% unreachable"));
            }
            if summary.graded > 0 {
                ui.separator();
                for letter in LETTERS {
                    let count = summary.letter_counts[letter_index(letter)];
                    if count > 0 {
                        ui.colored_label(
                            grade_color(letter),
                            format!("{} {count}", letter.label()),
                        );
                    }
                }
            }
        });
    }

    fn status_bar(&self, ui: &mut egui::Ui, visible_count: usize) {
        ui.horizontal(|ui| {
            ui.label(format!(
                "{visible_count}/{} shown · depth: {}",
                self.nodes.len(),
                self.depth_edit.depth().label(),
            ));
            ui.separator();
            ui.label(&self.status_line);
        });
    }
}

impl eframe::App for HealthApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_events();
        // Keep polling for backend events while work may be in flight.
        ctx.request_repaint_after(std::time::Duration::from_millis(250));

        let visible = self.visible_rows();

        egui::TopBottomPanel::top("controls").show(ctx, |ui| {
            self.controls(ui, &visible);
            self.filter_bar(ui, &visible);
        });
        egui::TopBottomPanel::bottom("status").show(ctx, |ui| {
            self.status_bar(ui, visible.len());
        });
        if !self.nodes.is_empty() {
            egui::TopBottomPanel::top("summary").show(ctx, |ui| {
                self.summary_strip(ui);
            });
        }

        if let Some(address) = self.selected {
            match self.nodes.get(&address) {
                Some(node) => match detail::show(ctx, node) {
                    detail::DetailAction::Close => self.selected = None,
                    detail::DetailAction::Reprobe => {
                        self.send(Command::StartProbes {
                            targets: Some(vec![address]),
                        });
                    }
                    detail::DetailAction::None => {}
                },
                None => self.selected = None,
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            if self.nodes.is_empty() {
                ui.centered_and_justified(|ui| {
                    ui.label(if self.discovering {
                        "Syncing masternode list…"
                    } else {
                        "No nodes yet — press “Refresh masternode list”."
                    });
                });
            } else {
                let action = table::show(ui, &self.nodes, &visible, self.sort, self.selected);
                if let Some(sort) = action.sort_changed {
                    self.sort = sort;
                }
                if let Some(clicked) = action.clicked {
                    // Clicking the selected row again deselects it.
                    self.selected = (self.selected != Some(clicked)).then_some(clicked);
                }
            }
        });
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        let _ = self.commands.send(Command::Shutdown);
    }
}
