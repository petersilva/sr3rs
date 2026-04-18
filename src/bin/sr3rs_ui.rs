//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use eframe::egui;
use sr3rs::ui::{MyApp, NativeBackend, IoBackend};

use std::sync::Arc;

#[tokio::main]
async fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1024.0, 768.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    
    let backend = Arc::new(NativeBackend);
    let config_infos = backend.load_configs().await;
    
    eframe::run_native(
        "sr3rs UI",
        native_options,
        Box::new(move |_cc| {
            let mut app = MyApp::new(backend);
            app.build_graph(config_infos);
            Box::new(app)
        }),
    )
}
