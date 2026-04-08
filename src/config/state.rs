//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use serde::{Serialize, Deserialize};
use std::fs;
use rand::Rng;
use crate::config::paths;
use crate::config::subscription::Subscription;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub rand4: String,
    pub rand8: String,
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
}

impl State {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        let r4: u16 = rng.gen_range(0..0xFFFF);
        let r8: u32 = rng.gen();
        Self {
            rand4: format!("{:04x}", r4),
            rand8: format!("{:08x}", r8),
            subscriptions: Vec::new(),
        }
    }

    pub fn load_or_create(component: &str, configname: &str) -> Self {
        let state_dir = paths::get_user_cache_dir()
            .join(component)
            .join(configname);
        
        let state_json_path = state_dir.join(".state.json");
        let subscriptions_path = state_dir.join("subscriptions.json");

        // 1. Try loading from our own full state file first
        if state_json_path.exists() {
            if let Ok(content) = fs::read_to_string(&state_json_path) {
                if let Ok(mut state) = serde_json::from_str::<State>(&content) {
                    // Deduplicate
                    let mut unique_subs: Vec<Subscription> = Vec::new();
                    for sub in state.subscriptions.drain(..) {
                        if !unique_subs.contains(&sub) {
                            unique_subs.push(sub);
                        }
                    }
                    state.subscriptions = unique_subs;
                    return state;
                }
            }
        }

        // 2. Fallback to Python-compatible subscriptions.json
        if subscriptions_path.exists() {
            if let Ok(content) = fs::read_to_string(&subscriptions_path) {
                if let Ok(subscriptions) = serde_json::from_str::<Vec<Subscription>>(&content) {
                    let mut state = Self::new();
                    
                    // Deduplicate
                    let mut unique_subs: Vec<Subscription> = Vec::new();
                    for sub in subscriptions {
                        if !unique_subs.contains(&sub) {
                            unique_subs.push(sub);
                        }
                    }
                    state.subscriptions = unique_subs;
                    
                    // Recover rand8 from the first subscription's queue name
                    if let Some(sub) = state.subscriptions.first() {
                        let parts: Vec<&str> = sub.queue.name.split('_').collect();
                        if let Some(last) = parts.last() {
                            if last.len() == 8 {
                                state.rand8 = last.to_string();
                            }
                        }
                    }
                    return state;
                }
            }
        }

        // Create new state if not found or invalid
        Self::new()
    }

    pub fn save(&self, component: &str, configname: &str) -> anyhow::Result<()> {
        let state_dir = paths::get_user_cache_dir()
            .join(component)
            .join(configname);
        
        fs::create_dir_all(&state_dir)?;
        
        // Save full state for ourselves
        let state_json_path = state_dir.join(".state.json");
        let state_content = serde_json::to_string_pretty(self)?;
        fs::write(state_json_path, state_content)?;

        // Save as a list of subscriptions for Python compatibility
        let subscriptions_path = state_dir.join("subscriptions.json");
        let subs_content = serde_json::to_string_pretty(&self.subscriptions)?;
        fs::write(subscriptions_path, subs_content)?;
        
        Ok(())
    }
}
