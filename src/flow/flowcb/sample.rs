//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::{FlowCB, Decision};
use async_trait::async_trait;
use std::collections::HashMap;

pub struct SamplePlugin {
    pub name: String,
    pub reject_pattern: String,
}

impl SamplePlugin {
    pub fn new(name: &str, reject_pattern: &str) -> Self {
        Self {
            name: name.to_string(),
            reject_pattern: reject_pattern.to_string(),
        }
    }
}

#[async_trait]
impl FlowCB for SamplePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self) -> anyhow::Result<()> {
        ::log::info!("SamplePlugin '{}' starting.", self.name);
        Ok(())
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        let mut cursor = wl.incoming_cursor();
        
        while let Some(msg) = cursor.next(wl) {
            if msg.rel_path.contains(&self.reject_pattern) {
                ::log::info!("SamplePlugin: rejecting {} because it contains '{}'", msg.rel_path, self.reject_pattern);
                cursor.step(wl, Decision::Reject);
            } else {
                // Modify the message
                msg.fields.insert("sample_plugin_seen".to_string(), "true".to_string());
                cursor.step(wl, Decision::Keep);
            }
        }
        
        Ok(())
    }

    async fn after_work(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        let mut cursor = wl.ok_cursor();
        
        while let Some(msg) = cursor.next(wl) {
            ::log::info!("SamplePlugin: processed successful transfer of {}", msg.rel_path);
            cursor.advance();
        }
        
        Ok(())
    }

    fn metrics_report(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("sample_metric".to_string(), "42".to_string());
        m
    }
}
