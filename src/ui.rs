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
    pub subtopics: Vec<String>,
}

pub struct Node {
    pub info: ConfigInfo,
    pub pos: egui::Pos2,
    pub color: egui::Color32,
}

pub struct Edge {
    pub start_idx: usize,
    pub end_idx: usize,
    pub post_exchange: String,
    pub subtopics: Vec<String>,
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait IoBackend {
    async fn load_configs(&self) -> Vec<ConfigInfo>;
    async fn read_file(&self, path: &str) -> Result<String, String>;
    async fn write_file(&self, path: &str, content: &str) -> Result<(), String>;
    async fn load_positions(&self) -> HashMap<String, (f32, f32)>;
    async fn save_positions(&self, positions: HashMap<String, (f32, f32)>) -> Result<(), String>;
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
                    component: component.clone(),
                    exchange: config.resolve_exchange(),
                    post_exchanges: config.resolve_post_exchanges(),
                    file_path: config_file,
                    subtopics: {
                        let mut t: Vec<String> = config.subscriptions.iter().flat_map(|s| s.bindings.iter().map(|b| b.topic.clone())).collect();
                        if t.is_empty() {
                            if !config.topic_prefix.is_empty() {
                                let mut parts = config.topic_prefix.clone();
                                parts.push("#".to_string());
                                t.push(parts.join("."));
                            } else if !config.subtopics.is_empty() {
                                t.extend(config.subtopics.clone());
                            } else {
                                t.push("#".to_string());
                            }
                        }
                        
                        match component.as_str() {
                            "sender" | "post" | "cpost" | "cpump" => t.clear(),
                            _ => {}
                        }
                        t
                    },
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

    async fn load_positions(&self) -> HashMap<String, (f32, f32)> {
        let path = crate::config::paths::get_user_config_dir().join("ui").join("node_positions.json");
        if let Ok(data) = std::fs::read_to_string(&path) {
            serde_json::from_str(&data).unwrap_or_default()
        } else {
            HashMap::new()
        }
    }

    async fn save_positions(&self, positions: HashMap<String, (f32, f32)>) -> Result<(), String> {
        let dir = crate::config::paths::get_user_config_dir().join("ui");
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let path = dir.join("node_positions.json");
        let data = serde_json::to_string_pretty(&positions).map_err(|e| e.to_string())?;
        std::fs::write(path, data).map_err(|e| e.to_string())
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

    async fn load_positions(&self) -> HashMap<String, (f32, f32)> {
        let url = format!("{}/api/positions", self.base_url);
        match reqwest::get(&url).await {
            Ok(resp) => resp.json().await.unwrap_or_default(),
            Err(_) => HashMap::new(),
        }
    }

    async fn save_positions(&self, positions: HashMap<String, (f32, f32)>) -> Result<(), String> {
        let url = format!("{}/api/positions", self.base_url);
        let client = reqwest::Client::new();
        match client.post(&url).json(&positions).send().await {
            Ok(resp) => if resp.status().is_success() { Ok(()) } else { Err(format!("Server error: {}", resp.status())) },
            Err(e) => Err(e.to_string()),
        }
    }
}

use std::sync::Arc;

pub struct MyApp {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
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
    
    pub fn build_graph(&mut self, config_infos: Vec<ConfigInfo>, positions: HashMap<String, (f32, f32)>) {
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

            let pos = if let Some(&(x, y)) = positions.get(&info.name) {
                egui::pos2(x, y)
            } else {
                egui::pos2(100.0 + (i as f32 * 150.0) % 800.0, 100.0 + (i as f32 * 150.0 / 800.0).floor() * 150.0)
            };

            self.nodes.push(Node {
                info,
                pos,
                color,
            });
        }

        for (idx, node) in self.nodes.iter().enumerate() {
            for post_ex in &node.info.post_exchanges {
                if let Some(target_indices) = exchange_to_nodes.get(post_ex) {
                    for &target_idx in target_indices {
                        self.edges.push(Edge {
                            start_idx: idx,
                            end_idx: target_idx,
                            post_exchange: post_ex.clone(),
                            subtopics: self.nodes[target_idx].info.subtopics.clone(),
                        });
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

        let mut save_layout = false;

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

            // Draw edges (lines only)
            for edge in &self.edges {
                let start = to_screen(self.nodes[edge.start_idx].pos);
                let end = to_screen(self.nodes[edge.end_idx].pos);
                painter.line_segment([start, end], egui::Stroke::new(2.0 * self.zoom, egui::Color32::from_gray(200)));
            }

            // Draw nodes
            for node in &mut self.nodes {
                let screen_pos = to_screen(node.pos);
                
                let font_id_name = egui::FontId::proportional(14.0 * self.zoom);
                let font_id_comp = egui::FontId::proportional(10.0 * self.zoom);
                
                let name_galley = painter.layout_no_wrap(
                    node.info.name.clone(),
                    font_id_name.clone(),
                    egui::Color32::BLACK,
                );
                
                let comp_galley = painter.layout_no_wrap(
                    node.info.component.clone(),
                    font_id_comp.clone(),
                    egui::Color32::from_gray(50),
                );
                
                let padding = egui::vec2(15.0 * self.zoom, 10.0 * self.zoom);
                let text_width = name_galley.size().x.max(comp_galley.size().x);
                let text_height = name_galley.size().y + comp_galley.size().y + (5.0 * self.zoom);
                
                let rect_size = egui::vec2(text_width, text_height) + padding * 2.0;
                let rect = egui::Rect::from_center_size(screen_pos, rect_size);
                let rounding = 5.0 * self.zoom;
                
                let node_id = ui.id().with(&node.info.name);
                let node_resp = ui.interact(rect, node_id, egui::Sense::click_and_drag());
                
                if node_resp.dragged() {
                    node.pos += node_resp.drag_delta() / self.zoom;
                }
                
                if node_resp.drag_stopped() {
                    save_layout = true;
                }
                
                if node_resp.clicked() {
                    let _path = node.info.file_path.clone();
                    let _backend = self.backend.clone();
                    
                    #[cfg(not(target_arch = "wasm32"))]
                    {
                        if let Ok(content) = pollster::block_on(_backend.read_file(&_path)) {
                            self.editing_content = content;
                            self.editing_path = Some(_path);
                            self.save_status = None;
                        }
                    }
                    
                    #[cfg(target_arch = "wasm32")]
                    {
                        self.save_status = Some("Loading...".to_string());
                        let loader = self.async_load_result.clone();
                        let ctx_clone = ctx.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let res = _backend.read_file(&_path).await;
                            if let Ok(mut guard) = loader.lock() {
                                *guard = Some(res.map(|c| (_path, c)));
                            }
                            ctx_clone.request_repaint();
                        });
                    }
                }

                painter.rect_filled(rect, rounding, node.color);
                if node_resp.hovered() {
                    painter.rect_stroke(rect.expand(2.0 * self.zoom), rounding + 2.0 * self.zoom, egui::Stroke::new(2.0 * self.zoom, egui::Color32::WHITE));
                } else {
                    painter.rect_stroke(rect, rounding, egui::Stroke::new(1.0 * self.zoom, egui::Color32::from_gray(100)));
                }

                painter.text(
                    screen_pos - egui::vec2(0.0, comp_galley.size().y / 2.0 + (2.0 * self.zoom)),
                    egui::Align2::CENTER_CENTER,
                    &node.info.name,
                    font_id_name,
                    egui::Color32::BLACK,
                );

                painter.text(
                    screen_pos + egui::vec2(0.0, name_galley.size().y / 2.0 + (2.0 * self.zoom)),
                    egui::Align2::CENTER_CENTER,
                    &node.info.component,
                    font_id_comp,
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
            
            // Draw edge labels and arrows (on top of nodes)
            for edge in &self.edges {
                let start = to_screen(self.nodes[edge.start_idx].pos);
                let end = to_screen(self.nodes[edge.end_idx].pos);
                if start != end {
                    let dir = (end - start).normalized();
                    
                    // Draw labels
                    let label_font = egui::FontId::proportional(11.0 * self.zoom);
                    let up_offset = egui::vec2(0.0, -4.0 * self.zoom); // visually "above" the line on screen
                    
                    let (source_align, consumer_align) = if dir.x > 0.0 {
                        (egui::Align2::LEFT_BOTTOM, egui::Align2::RIGHT_BOTTOM)
                    } else {
                        (egui::Align2::RIGHT_BOTTOM, egui::Align2::LEFT_BOTTOM)
                    };
                    
                    // The label should start clearing the node bounding box.
                    // A radius of 75-80 is typically enough to clear the dynamic rectangle width.
                    let box_clearance = 75.0 * self.zoom;
                    
                    // Label at source: post_exchange
                    painter.text(
                        start + dir * box_clearance + up_offset,
                        source_align,
                        &edge.post_exchange,
                        label_font.clone(),
                        egui::Color32::BLACK,
                    );

                    // Label at consumer: subtopics
                    if !edge.subtopics.is_empty() {
                        let subtopics_text = edge.subtopics.join(", ");
                        painter.text(
                            end - dir * box_clearance + up_offset,
                            consumer_align,
                            subtopics_text,
                            label_font,
                            egui::Color32::BLACK,
                        );
                    }

                    // Draw arrow head (solid triangle) at 60% along the line
                    let tip = start + (end - start) * 0.6;
                    let arrow_len = 15.0 * self.zoom;
                    let base = tip - dir * arrow_len;
                    let side = egui::vec2(-dir.y, dir.x) * (7.0 * self.zoom);
                    
                    painter.add(egui::Shape::convex_polygon(
                        vec![tip, base + side, base - side],
                        egui::Color32::from_gray(150),
                        egui::Stroke::NONE,
                    ));
                }
            }
        });

        if save_layout {
            let mut positions = HashMap::new();
            for node in &self.nodes {
                positions.insert(node.info.name.clone(), (node.pos.x, node.pos.y));
            }
            let _backend = self.backend.clone();
            
            #[cfg(not(target_arch = "wasm32"))]
            {
                let _ = pollster::block_on(_backend.save_positions(positions));
            }
            
            #[cfg(target_arch = "wasm32")]
            {
                wasm_bindgen_futures::spawn_local(async move {
                    let _ = _backend.save_positions(positions).await;
                });
            }
        }

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
