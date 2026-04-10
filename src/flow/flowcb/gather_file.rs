//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::flowcb::FlowCB;
use crate::flow::Worklist;
use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::SystemTime;
use notify::{Watcher, RecursiveMode, RecommendedWatcher, Event, EventKind};
use tokio::sync::mpsc;

pub struct GatherFilePlugin {
    pub name: String,
    pub config: Config,
    pub primed: bool,
    pub initial_scan_done: bool,
    pub queued_messages: Vec<Message>,
    pub watcher: Option<RecommendedWatcher>,
    pub rx: Option<mpsc::Receiver<anyhow::Result<PathBuf>>>,
}

impl GatherFilePlugin {
    pub fn new(config: &Config) -> Self {
        Self {
            name: "gather.file".to_string(),
            config: config.clone(),
            primed: false,
            initial_scan_done: false,
            queued_messages: Vec::new(),
            watcher: None,
            rx: None,
        }
    }

    pub fn walk(&self, dir: &Path) -> Vec<Message> {
        let mut messages = Vec::new();
        if dir.is_file() {
            if let Ok(msg) = self.make_message(dir) {
                messages.push(msg);
            }
            return messages;
        }

        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();

                if path.is_dir() {
                    if self.config.recursive {
                        messages.extend(self.walk(&path));
                    }
                } else {
                    if let Ok(msg) = self.make_message(&path) {
                        messages.push(msg);
                    }
                }
            }
        }
        messages
    }

    fn make_message(&self, path: &Path) -> anyhow::Result<Message> {
        // Skip hidden files/directories
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if file_name == "." || file_name == ".." {
            return Err(anyhow::anyhow!("Skipping . or .."));
        }

        let mut msg = Message::from_file(path, &self.config)?;

        // Check age
        if let Ok(metadata) = fs::metadata(path) {
            if let Ok(mtime) = metadata.modified() {
                if let Ok(duration) = SystemTime::now().duration_since(mtime) {
                    let age = duration.as_secs_f64();
                    if age < self.config.file_age_min {
                        return Err(anyhow::anyhow!("File too young"));
                    }
                    if self.config.file_age_max > 0.0 && age > self.config.file_age_max {
                        return Err(anyhow::anyhow!("File too old"));
                    }
                }
            }
        }

        // Calculate identity
        if let Some(mut id_obj) = crate::identity::factory(&self.config.identity_method) {
            if let Ok(_) = id_obj.update_file(path.to_str().unwrap_or("")) {
                msg.fields.insert("identity".to_string(), format!("{}:{}", self.config.identity_method, id_obj.value()));
            }
        }

        Ok(msg)
    }

    fn setup_watcher(&mut self) -> anyhow::Result<()> {
        if self.config.force_polling {
            ::log::info!("GatherFile: force_polling is True, using periodic scans.");
            return Ok(());
        }

        // If sleep < 0 (interactive post), don't start a watcher.
        if self.config.sleep < 0.0 {
            return Ok(());
        }

        let (tx, rx) = mpsc::channel(100);
        let tx_clone = tx.clone();

        let mut watcher = RecommendedWatcher::new(move |res: notify::Result<Event>| {
            match res {
                Ok(event) => {
                    // We care about creation and modification
                    match event.kind {
                        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Any => {
                            for path in event.paths {
                                let _ = tx_clone.blocking_send(Ok(path));
                            }
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    let _ = tx_clone.blocking_send(Err(anyhow::anyhow!("Watcher error: {}", e)));
                }
            }
        }, notify::Config::default())?;

        let watch_dir = self.config.directory.clone();
        let mode = if self.config.recursive { RecursiveMode::Recursive } else { RecursiveMode::NonRecursive };
        
        if watch_dir.exists() {
            watcher.watch(&watch_dir, mode)?;
            ::log::info!("GatherFile: kernel-level watcher active on {}", watch_dir.display());
            self.watcher = Some(watcher);
            self.rx = Some(rx);
        } else {
            ::log::warn!("GatherFile: directory {} does not exist, cannot start watcher.", watch_dir.display());
        }

        Ok(())
    }
}

#[async_trait]
impl FlowCB for GatherFilePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self) -> anyhow::Result<()> {
        self.queued_messages.clear();
        self.primed = false;
        self.initial_scan_done = false;
        self.setup_watcher()?;
        Ok(())
    }

    async fn gather(&mut self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let batch_size = self.config.batch as usize;

        // 1. Drain internal queue first
        if !self.queued_messages.is_empty() {
            let to_take = self.queued_messages.len().min(batch_size);
            let drained: Vec<_> = self.queued_messages.drain(0..to_take).collect();
            worklist.incoming.extend(drained);
            return Ok(());
        }

        // 2. Initial scan or polling scan
        if !self.initial_scan_done || (self.watcher.is_none() && !self.primed) {
             let mut messages = Vec::new();
             
             if !self.initial_scan_done && !self.config.post_paths.is_empty() {
                 for path_str in &self.config.post_paths {
                     let p = Path::new(path_str);
                     if p.exists() {
                        ::log::info!("GatherFile: scanning path from CLI: {}", path_str);
                        messages.extend(self.walk(p));
                     } else {
                        ::log::warn!("GatherFile: CLI path does not exist: {}", path_str);
                     }
                 }
                 ::log::info!("GatherFile: found {} files in CLI paths.", messages.len());
             } else {
                 let watch_dir = self.config.directory.clone();
                 if watch_dir.exists() {
                     messages = self.walk(&watch_dir);
                 }
             }

             if !self.initial_scan_done && !self.config.post_on_start && self.config.post_paths.is_empty() {
                 ::log::info!("GatherFile: initial scan done, ignoring {} existing files (post_on_start is False)", messages.len());
                 self.initial_scan_done = true;
                 self.primed = true;
                 return Ok(());
             }

             self.initial_scan_done = true;
             
             if !messages.is_empty() {
                if messages.len() > batch_size {
                    let (batch, rest) = messages.split_at(batch_size);
                    worklist.incoming.extend(batch.iter().cloned());
                    self.queued_messages.extend(rest.iter().cloned());
                } else {
                    worklist.incoming.extend(messages);
                }
                self.primed = true;
                return Ok(());
             }
        }

        // 3. Process kernel events if available
        if let Some(mut rx) = self.rx.take() {
            let mut count = 0;
            while count < batch_size {
                match rx.try_recv() {
                    Ok(Ok(path)) => {
                        if path.is_file() {
                            if let Ok(msg) = self.make_message(&path) {
                                worklist.incoming.push(msg);
                                count += 1;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        ::log::error!("GatherFile: watcher channel error: {}", e);
                        break;
                    }
                    Err(_) => break, // Empty
                }
            }
            self.rx = Some(rx);
            if count > 0 {
                return Ok(());
            }
        }

        // 4. Fallback/Reset for next polling cycle if no watcher
        if self.watcher.is_none() && self.primed {
            if self.config.sleep > 0.0 {
                self.primed = false;
            }
        }

        Ok(())
    }
}
