//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

#[cfg(target_arch = "wasm32")]
fn main() {
    // Redirect `log` message to `console.log` and friends:
    eframe::WebLogger::init(log::LevelFilter::Debug).ok();

    wasm_bindgen_futures::spawn_local(async {
        use sr3rs::ui::{MyApp, WebBackend, IoBackend};
        use std::sync::Arc;
        
        let window = eframe::web_sys::window().expect("no global `window` exists");
        let document = window.document().expect("should have a document on window");
        let origin = document.location().unwrap().origin().unwrap();

        let backend = Arc::new(WebBackend {
            base_url: origin, 
        });
        
        let config_infos = backend.load_configs().await;
        let positions = backend.load_positions().await;

        eframe::WebRunner::new().start(
            "main_canvas",
            eframe::WebOptions::default(),
            Box::new(move |_cc| {
                let mut app = MyApp::new(backend);
                app.build_graph(config_infos, positions);
                Box::new(app)
            }),
        )
        .await
        .expect("failed to start eframe");
    });
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    println!("This binary is intended for Wasm only.");
}
