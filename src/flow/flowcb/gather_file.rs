//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::flowcb::FlowCB;
use crate::flow::Worklist;
use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::path::Path;
use std::fs;
use std::time::SystemTime;

pub struct GatherFilePlugin {
    pub name: String,
    pub config: Config,
    pub primed: bool,
    pub initial_scan_done: bool,
    pub queued_messages: Vec<Message>,
}

impl GatherFilePlugin {
    pub fn new(config: &Config) -> Self {
        Self {
            name: "gather.file".to_string(),
            config: config.clone(),
            primed: false,
            initial_scan_done: false,
            queued_messages: Vec::new(),
        }
    }

    pub fn walk(&self, dir: &Path) -> Vec<Message> {
        let mut messages = Vec::new();
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                
                // Skip hidden files/directories
                if file_name.starts_with('.') {
                    continue;
                }

                if path.is_dir() {
                    if self.config.recursive {
                        messages.extend(self.walk(&path));
                    }
                } else {
                    if let Ok(mut msg) = Message::from_file(&path, &self.config) {
                        // Calculate identity
                        if let Some(mut id_obj) = crate::identity::factory(&self.config.identity_method) {
                            if let Ok(_) = id_obj.update_file(path.to_str().unwrap_or("")) {
                                msg.fields.insert("identity".to_string(), format!("{}:{}", self.config.identity_method, id_obj.value()));
                            }
                        }

                        // Check age
                        if let Ok(metadata) = fs::metadata(&path) {
                            if let Ok(mtime) = metadata.modified() {
                                if let Ok(duration) = SystemTime::now().duration_since(mtime) {
                                    let age = duration.as_secs_f64();
                                    if age < self.config.file_age_min {
                                        continue;
                                    }
                                    if self.config.file_age_max > 0.0 && age > self.config.file_age_max {
                                        continue;
                                    }
                                }
                            }
                        }
                        messages.push(msg);
                    }
                }
            }
        }
        messages
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
        Ok(())
    }

    async fn gather(&mut self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let batch_size = self.config.batch as usize;

        if !self.queued_messages.is_empty() {
            let to_take = self.queued_messages.len().min(batch_size);
            let drained: Vec<_> = self.queued_messages.drain(0..to_take).collect();
            worklist.incoming.extend(drained);
            return Ok(());
        }

        if self.primed {
            // In a polling model, we might want to re-scan or wait.
            if self.config.sleep > 0.0 {
                self.primed = false;
            }
            return Ok(());
        }

        // Use 'directory' (which 'path' is aliased to)
        let pbd = self.config.post_base_dir.clone().unwrap_or_else(|| self.config.directory.clone());
        
        if !pbd.exists() {
             ::log::warn!("GatherFile: directory {} does not exist", pbd.display());
             self.primed = true;
             return Ok(());
        }

        let messages = self.walk(&pbd);

        // Handle post_on_start
        if !self.initial_scan_done && !self.config.post_on_start {
            ::log::info!("GatherFile: initial scan done, ignoring {} existing files (post_on_start is False)", messages.len());
            self.initial_scan_done = true;
            self.primed = true;
            // We return nothing, but since we are primed, we will scan again after 'sleep'
            // To make sure they don't get picked up next time, we'd need to record them in nodupe.
            // But if they haven't changed, nodupe will catch them anyway if we DO return them now but reject them.
            // For now, let's just not return them.
            return Ok(());
        }

        self.initial_scan_done = true;

        if messages.len() > batch_size {
            let (batch, rest) = messages.split_at(batch_size);
            worklist.incoming.extend(batch.iter().cloned());
            self.queued_messages.extend(rest.iter().cloned());
        } else {
            worklist.incoming.extend(messages);
        }

        self.primed = true;
        Ok(())
    }
}
