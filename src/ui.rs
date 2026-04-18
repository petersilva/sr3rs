//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use eframe::egui;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct ConfigInfo {
    pub name: String,
    pub component: String,
    pub exchange: String,
    pub post_exchanges: Vec<String>,
    pub file_path: String,
}

pub struct Node {
    pub info: ConfigInfo,
    pub pos: egui::Pos2,
    pub color: egui::Color32,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait IoBackend {
    async fn load_configs(&self) -> Vec<ConfigInfo>;
    async fn read_file(&self, path: &str) -> Result<String, String>;
    async fn write_file(&self, path: &str, content: &str) -> Result<(), String>;
}

#[cfg(not(target_arch = "wasm32"))]
pub struct NativeBackend;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl IoBackend for NativeBackend {
    async fn load_configs(&self) -> Vec<ConfigInfo> {
        let configs = crate::utils::resolve_patterns(vec![], "subscribe");
        let mut results = Vec::new();
        for config_file in configs {
            let component = crate::utils::detect_component(&config_file);
            let mut config = crate::Config::new();
            config.apply_component_defaults(&component);
            if config.load(&config_file).is_ok() && config.finalize().is_ok() {
                results.push(ConfigInfo {
                    name: config.configname.clone().unwrap_or_else(|| "unknown".to_string()),
                    component,
                    exchange: config.resolve_exchange(),
                    post_exchanges: config.resolve_post_exchanges(),
                    file_path: config_file,
                });
            }
        }
        results
    }

    async fn read_file(&self, path: &str) -> Result<String, String> {
        std::fs::read_to_string(path).map_err(|e| e.to_string())
    }

    async fn write_file(&self, path: &str, content: &str) -> Result<(), String> {
        std::fs::write(path, content).map_err(|e| e.to_string())
    }
}

pub struct WebBackend {
    pub base_url: String,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl IoBackend for WebBackend {
    async fn load_configs(&self) -> Vec<ConfigInfo> {
        let url = format!("{}/api/configs", self.base_url);
        log::info!("Fetching configs from {}", url);
        match reqwest::get(&url).await {
            Ok(resp) => {
                log::info!("Fetch status: {}", resp.status());
                match resp.json::<Vec<ConfigInfo>>().await {
                    Ok(configs) => {
                        log::info!("Successfully parsed {} configs", configs.len());
                        configs
                    },
                    Err(e) => {
                        log::error!("Failed to parse configs JSON: {}", e);
                        Vec::new()
                    }
                }
            },
            Err(e) => {
                log::error!("Fetch failed: {}", e);
                Vec::new()
            },
        }
    }

    async fn read_file(&self, path: &str) -> Result<String, String> {
        let url = format!("{}/api/read?path={}", self.base_url, urlencoding::encode(path));
        match reqwest::get(&url).await {
            Ok(resp) => resp.text().await.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn write_file(&self, path: &str, content: &str) -> Result<(), String> {
        let url = format!("{}/api/write?path={}", self.base_url, urlencoding::encode(path));
        let client = reqwest::Client::new();
        match client.post(&url).body(content.to_string()).send().await {
            Ok(resp) => if resp.status().is_success() { Ok(()) } else { Err(format!("Server error: {}", resp.status())) },
            Err(e) => Err(e.to_string()),
        }
    }
}

use std::sync::Arc;

pub struct MyApp {
    pub nodes: Vec<Node>,
    pub edges: Vec<(usize, usize)>,
    pub zoom: f32,
    pub offset: egui::Vec2,
    pub editing_path: Option<String>,
    pub editing_content: String,
    pub save_status: Option<String>,
    #[cfg(not(target_arch = "wasm32"))]
    pub backend: Arc<dyn IoBackend + Send + Sync>,
    #[cfg(target_arch = "wasm32")]
    pub backend: Arc<dyn IoBackend>,
    pub async_load_result: Arc<std::sync::Mutex<Option<Result<(String, String), String>>>>,
    pub async_save_result: Arc<std::sync::Mutex<Option<Result<String, String>>>>,
}

impl MyApp {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(backend: Arc<dyn IoBackend + Send + Sync>) -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            zoom: 1.0,
            offset: egui::Vec2::ZERO,
            editing_path: None,
            editing_content: String::new(),
            save_status: None,
            backend,
            async_load_result: Arc::new(std::sync::Mutex::new(None)),
            async_save_result: Arc::new(std::sync::Mutex::new(None)),
        }
    }
    
    #[cfg(target_arch = "wasm32")]
    pub fn new(backend: Arc<dyn IoBackend>) -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
            zoom: 1.0,
            offset: egui::Vec2::ZERO,
            editing_path: None,
            editing_content: String::new(),
            save_status: None,
            backend,
            async_load_result: Arc::new(std::sync::Mutex::new(None)),
            async_save_result: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    pub fn sync_configs(&mut self) {
        // This needs to be async or handled via a message channel.
        // For simplicity in this turn, we'll let the binary handle the initial load.
    }
    
    pub fn build_graph(&mut self, config_infos: Vec<ConfigInfo>) {
        self.nodes.clear();
        self.edges.clear();
        let mut exchange_to_nodes: HashMap<String, Vec<usize>> = HashMap::new();

        for (i, info) in config_infos.into_iter().enumerate() {
            let color = match info.component.as_str() {
                "subscribe" => egui::Color32::from_rgb(100, 200, 100),
                "sender" => egui::Color32::from_rgb(200, 100, 100),
                "post" | "cpost" => egui::Color32::from_rgb(100, 100, 200),
                "poll" => egui::Color32::from_rgb(200, 200, 100),
                _ => egui::Color32::LIGHT_GRAY,
            };

            exchange_to_nodes.entry(info.exchange.clone()).or_default().push(i);

            self.nodes.push(Node {
                info,
                pos: egui::pos2(100.0 + (i as f32 * 150.0) % 800.0, 100.0 + (i as f32 * 150.0 / 800.0).floor() * 150.0),
                color,
            });
        }

        for (idx, node) in self.nodes.iter().enumerate() {
            for post_ex in &node.info.post_exchanges {
                if let Some(target_indices) = exchange_to_nodes.get(post_ex) {
                    for &target_idx in target_indices {
                        self.edges.push((idx, target_idx));
                    }
                }
            }
        }
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if let Ok(mut guard) = self.async_load_result.lock() {
            if let Some(res) = guard.take() {
                match res {
                    Ok((path, content)) => {
                        self.editing_path = Some(path);
                        self.editing_content = content;
                        self.save_status = None;
                    }
                    Err(e) => {
                        self.save_status = Some(format!("Failed to read file: {}", e));
                    }
                }
            }
        }
        
        if let Ok(mut guard) = self.async_save_result.lock() {
            if let Some(res) = guard.take() {
                match res {
                    Ok(msg) => self.save_status = Some(msg),
                    Err(e) => self.save_status = Some(format!("Error saving: {}", e)),
                }
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("sr3rs Node Graph View");
                if ui.button("Reset View").clicked() {
                    self.zoom = 1.0;
                    self.offset = egui::Vec2::ZERO;
                }
                ui.label("Scroll to zoom, drag background to pan, drag nodes to move them. Click to edit.");
            });

            let (response, painter) = ui.allocate_painter(ui.available_size(), egui::Sense::click_and_drag());

            // Handle panning
            if response.dragged() && ctx.dragged_id().is_none() {
                 self.offset += response.drag_delta();
            }

            // Handle zooming
            let scroll_delta = ctx.input(|i| i.smooth_scroll_delta.y);
            if scroll_delta != 0.0 {
                let zoom_factor = (scroll_delta * 0.001).exp();
                self.zoom *= zoom_factor;
            }

            let to_screen = |pos: egui::Pos2| {
                (pos.to_vec2() * self.zoom + self.offset).to_pos2()
            };

            // Draw edges
            for &(start_idx, end_idx) in &self.edges {
                let start = to_screen(self.nodes[start_idx].pos);
                let end = to_screen(self.nodes[end_idx].pos);
                painter.line_segment([start, end], egui::Stroke::new(2.0 * self.zoom, egui::Color32::from_gray(200)));
                
                // Draw arrow head
                let dir = (end - start).normalized();
                let arrow_len = 10.0 * self.zoom;
                let side = egui::vec2(-dir.y, dir.x) * arrow_len * 0.5;
                painter.line_segment([end, end - dir * arrow_len + side], egui::Stroke::new(2.0 * self.zoom, egui::Color32::from_gray(200)));
                painter.line_segment([end, end - dir * arrow_len - side], egui::Stroke::new(2.0 * self.zoom, egui::Color32::from_gray(200)));
            }

            // Draw nodes
            for node in &mut self.nodes {
                let screen_pos = to_screen(node.pos);
                let radius = 40.0 * self.zoom;
                
                let node_id = ui.id().with(&node.info.name);
                let rect = egui::Rect::from_center_size(screen_pos, egui::vec2(radius * 2.0, radius * 2.0));
                let node_resp = ui.interact(rect, node_id, egui::Sense::click_and_drag());
                
                if node_resp.dragged() {
                    node.pos += node_resp.drag_delta() / self.zoom;
                }
                
                if node_resp.clicked() {
                    let path = node.info.file_path.clone();
                    let backend = self.backend.clone();
                    
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        if let Ok(content) = pollster::block_on(backend.read_file(&path)) {
                            self.editing_content = content;
                            self.editing_path = Some(path);
                            self.save_status = None;
                        }
                    }
                    
                    #[cfg(target_arch = "wasm32")]
                    {
                        self.save_status = Some("Loading...".to_string());
                        let loader = self.async_load_result.clone();
                        let ctx_clone = ctx.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let res = backend.read_file(&path).await;
                            if let Ok(mut guard) = loader.lock() {
                                *guard = Some(res.map(|c| (path, c)));
                            }
                            ctx_clone.request_repaint();
                        });
                    }
                }

                painter.circle_filled(screen_pos, radius, node.color);
                if node_resp.hovered() {
                    painter.circle_stroke(screen_pos, radius + 2.0, egui::Stroke::new(2.0, egui::Color32::WHITE));
                }

                painter.text(
                    screen_pos,
                    egui::Align2::CENTER_CENTER,
                    &node.info.name,
                    egui::FontId::proportional(14.0 * self.zoom),
                    egui::Color32::BLACK,
                );

                painter.text(
                    screen_pos + egui::vec2(0.0, 15.0 * self.zoom),
                    egui::Align2::CENTER_CENTER,
                    &node.info.component,
                    egui::FontId::proportional(10.0 * self.zoom),
                    egui::Color32::from_gray(50),
                );
                
                if node_resp.hovered() {
                    egui::show_tooltip(ctx, node_id.with("tooltip"), |ui| {
                        ui.label(format!("Component: {}", node.info.component));
                        ui.label(format!("Exchange: {}", node.info.exchange));
                        ui.label(format!("Posts to: {:?}", node.info.post_exchanges));
                    });
                }
            }
        });

        if let Some(path) = &self.editing_path.clone() {
            let mut is_open = true;
            egui::Window::new(format!("Editing: {}", path))
                .open(&mut is_open)
                .resizable(true)
                .default_size([600.0, 400.0])
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        if ui.button("Save").clicked() {
                            let path_str = path.clone();
                            let content_str = self.editing_content.clone();
                            let backend = self.backend.clone();
                            
                            #[cfg(not(target_arch = "wasm32"))]
                            {
                                if let Err(e) = pollster::block_on(backend.write_file(&path_str, &content_str)) {
                                    self.save_status = Some(format!("Error saving: {}", e));
                                } else {
                                    self.save_status = Some("Saved successfully!".to_string());
                                }
                            }
                            
                            #[cfg(target_arch = "wasm32")]
                            {
                                self.save_status = Some("Saving...".to_string());
                                let saver = self.async_save_result.clone();
                                let ctx_clone = ctx.clone();
                                wasm_bindgen_futures::spawn_local(async move {
                                    let res = backend.write_file(&path_str, &content_str).await;
                                    if let Ok(mut guard) = saver.lock() {
                                        *guard = Some(res.map(|_| "Saved successfully!".to_string()));
                                    }
                                    ctx_clone.request_repaint();
                                });
                            }
                        }
                        if ui.button("Close").clicked() {
                            self.editing_path = None;
                        }
                        if let Some(status) = &self.save_status {
                            ui.label(status);
                        }
                    });
                    ui.separator();
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        ui.add(egui::TextEdit::multiline(&mut self.editing_content)
                            .font(egui::TextStyle::Monospace)
                            .desired_width(f32::INFINITY)
                        );
                    });
                });
                
            if !is_open {
                self.editing_path = None;
            }
        }
    }
}
