//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use eframe::egui;
use sr3rs::Config;
use sr3rs::utils::{detect_component, resolve_patterns};
use std::collections::HashMap;

struct Node {
    name: String,
    component: String,
    exchange: String,
    post_exchanges: Vec<String>,
    pos: egui::Pos2,
    color: egui::Color32,
}

struct MyApp {
    nodes: Vec<Node>,
    edges: Vec<(usize, usize)>,
    zoom: f32,
    offset: egui::Vec2,
}

impl MyApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let configs = resolve_patterns(vec![], "subscribe");
        let mut nodes = Vec::new();
        let mut exchange_to_nodes: HashMap<String, Vec<usize>> = HashMap::new();

        let mut i = 0;
        for config_file in configs {
            let component = detect_component(&config_file);
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            if config.load(&config_file).is_ok() && config.finalize().is_ok() {
                let name = config.configname.clone().unwrap_or_else(|| "unknown".to_string());
                let exchange = config.resolve_exchange();
                let post_exchanges = config.resolve_post_exchanges();
                
                let color = match component.as_str() {
                    "subscribe" => egui::Color32::from_rgb(100, 200, 100),
                    "sender" => egui::Color32::from_rgb(200, 100, 100),
                    "post" | "cpost" => egui::Color32::from_rgb(100, 100, 200),
                    "poll" => egui::Color32::from_rgb(200, 200, 100),
                    _ => egui::Color32::LIGHT_GRAY,
                };

                nodes.push(Node {
                    name,
                    component,
                    exchange: exchange.clone(),
                    post_exchanges,
                    pos: egui::pos2(100.0 + (i as f32 * 150.0) % 800.0, 100.0 + (i as f32 * 150.0 / 800.0).floor() * 150.0),
                    color,
                });
                
                exchange_to_nodes.entry(exchange).or_default().push(i);
                i += 1;
            }
        }

        let mut edges = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            for post_ex in &node.post_exchanges {
                if let Some(target_indices) = exchange_to_nodes.get(post_ex) {
                    for &target_idx in target_indices {
                        edges.push((idx, target_idx));
                    }
                }
            }
        }

        Self {
            nodes,
            edges,
            zoom: 1.0,
            offset: egui::Vec2::ZERO,
        }
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("sr3rs Node Graph View");
                if ui.button("Reset View").clicked() {
                    self.zoom = 1.0;
                    self.offset = egui::Vec2::ZERO;
                }
                ui.label("Scroll to zoom, drag background to pan, drag nodes to move them.");
            });

            let (response, painter) = ui.allocate_painter(ui.available_size(), egui::Sense::drag());

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
                
                let node_id = ui.id().with(&node.name);
                let rect = egui::Rect::from_center_size(screen_pos, egui::vec2(radius * 2.0, radius * 2.0));
                let node_resp = ui.interact(rect, node_id, egui::Sense::drag());
                
                if node_resp.dragged() {
                    node.pos += node_resp.drag_delta() / self.zoom;
                }

                painter.circle_filled(screen_pos, radius, node.color);
                if node_resp.hovered() {
                    painter.circle_stroke(screen_pos, radius + 2.0, egui::Stroke::new(2.0, egui::Color32::WHITE));
                }

                painter.text(
                    screen_pos,
                    egui::Align2::CENTER_CENTER,
                    &node.name,
                    egui::FontId::proportional(14.0 * self.zoom),
                    egui::Color32::BLACK,
                );

                painter.text(
                    screen_pos + egui::vec2(0.0, 15.0 * self.zoom),
                    egui::Align2::CENTER_CENTER,
                    &node.component,
                    egui::FontId::proportional(10.0 * self.zoom),
                    egui::Color32::from_gray(50),
                );
                
                if node_resp.hovered() {
                    egui::show_tooltip(ctx, node_id.with("tooltip"), |ui| {
                        ui.label(format!("Component: {}", node.component));
                        ui.label(format!("Exchange: {}", node.exchange));
                        ui.label(format!("Posts to: {:?}", node.post_exchanges));
                    });
                }
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1024.0, 768.0])
            .with_drag_and_drop(true),
        ..Default::default()
    };
    eframe::run_native(
        "sr3rs UI",
        native_options,
        Box::new(|cc| Box::new(MyApp::new(cc))),
    )
}
