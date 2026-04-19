//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use eframe::egui;
use sr3rs::ui::{MyApp, NativeBackend, IoBackend};
use clap::Parser;
use std::path::PathBuf;
use sr3rs::config::paths;

use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Override the default configuration directory
    #[arg(long = "configDir", alias = "config_dir")]
    config_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> eframe::Result<()> {
    let cli = Cli::parse();
    
    if let Some(config_dir) = cli.config_dir {
        paths::set_config_dir_override(config_dir);
    }

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1024.0, 768.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    
    let backend = Arc::new(NativeBackend);
    let config_infos = backend.load_configs().await;
    let positions = backend.load_positions().await;

    eframe::run_native(

        "sr3rs UI",
        native_options,
        Box::new(move |cc| {
            cc.egui_ctx.set_visuals(egui::Visuals::light());
            let mut app = MyApp::new(backend);
            app.build_graph(config_infos, positions);
            Box::new(app)
        }),
    )
}
