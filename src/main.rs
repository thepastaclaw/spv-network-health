//! spv-network-health: an egui dashboard that discovers every node on the
//! Dash network via the masternode list and grades each one by running a
//! bounded SPV sync against it.

mod backend;
mod config;
mod export;
mod grading;
mod types;
mod ui;

use clap::Parser;
use tracing_subscriber::EnvFilter;

fn main() -> eframe::Result {
    // dash-spv mirrors every sync/network event into tracing at info level;
    // with many concurrent probes that floods the console, so default it to
    // warn (RUST_LOG still overrides).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,dash_spv=warn")),
        )
        .init();

    let args = config::Args::parse();
    let app_config = match config::AppConfig::from_args(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };

    // UI -> backend commands; backend -> UI events.
    let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, event_rx) = std::sync::mpsc::channel();

    // All network work runs on a dedicated thread inside a tokio runtime; the
    // eframe event loop stays synchronous.
    let backend_config = app_config.clone();
    std::thread::Builder::new()
        .name("spv-health-backend".into())
        .spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("failed to start tokio runtime");
            runtime.block_on(backend::run(command_rx, event_tx, backend_config));
        })
        .expect("failed to spawn backend thread");

    let options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 820.0])
            .with_title("Dash SPV Network Health"),
        ..Default::default()
    };
    eframe::run_native(
        "Dash SPV Network Health",
        options,
        Box::new(move |cc| {
            Ok(Box::new(ui::HealthApp::new(
                cc, command_tx, event_rx, app_config,
            )))
        }),
    )
}
