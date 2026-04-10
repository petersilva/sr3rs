//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::FlowCB;
use crate::Config;
use crate::message::Message;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use chrono::Utc;
use std::time::Instant;

pub struct LogPluginState {
    pub last_housekeeping: Instant,
    pub started: Instant,
    pub file_bytes: u64,
    pub lag_total: f64,
    pub lag_max: f64,
    pub msg_count: u64,
    pub reject_count: u64,
    pub transfer_count: u64,
}

pub struct LogPlugin {
    pub name: String,
    pub component: String,
    pub config_name: String,
    pub action_verb: String,
    pub log_events: HashSet<String>,
    pub log_message_dump: bool,
    pub state: Mutex<LogPluginState>,
}

impl LogPlugin {
    pub fn new(config: &Config) -> Self {
        let name = "log".to_string();
        let component = config.component.clone();
        let config_name = config.configname.clone().unwrap_or_else(|| "unknown".to_string());

        let action_verb = match component.as_str() {
            "sender" => "sent".to_string(),
            "subscribe" | "sarra" => "downloaded".to_string(),
            "post" | "poll" | "watch" => "noticed".to_string(),
            "flow" | "shovel" | "winnow" => format!("{}ed", component),
            _ => "done".to_string(),
        };

        let mut log_events = HashSet::new();
        if let Some(events_str) = config.options.get("logEvents") {
            for e in events_str.split(',') {
                log_events.insert(e.trim().to_string());
            }
        } else {
            // Defaults from Python implementation
            log_events.insert("after_accept".to_string());
            log_events.insert("on_housekeeping".to_string());
        }

        let log_message_dump = config.options.get("logMessageDump")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        let now = Instant::now();
        let state = LogPluginState {
            last_housekeeping: now,
            started: now,
            file_bytes: 0,
            lag_total: 0.0,
            lag_max: 0.0,
            msg_count: 0,
            reject_count: 0,
            transfer_count: 0,
        };

        Self {
            name,
            component,
            config_name,
            action_verb,
            log_events,
            log_message_dump,
            state: Mutex::new(state),
        }
    }

    fn reset(&self) {
        let mut state = self.state.lock().unwrap();
        state.last_housekeeping = Instant::now();
        state.file_bytes = 0;
        state.lag_total = 0.0;
        state.lag_max = 0.0;
        state.msg_count = 0;
        state.reject_count = 0;
        state.transfer_count = 0;
    }

    fn message_str(&self, msg: &Message) -> String {
        if self.log_message_dump {
            serde_json::to_string(msg).unwrap_or_else(|_| "err_dump".to_string())
        } else {
            format!("{}/{}", msg.base_url, msg.rel_path)
        }
    }

    fn message_accept_str(&self, msg: &Message) -> String {
        if self.log_message_dump {
            return self.message_str(msg);
        }

        let mut s = String::new();
        if let Some(exchange) = msg.fields.get("exchange") {
            s.push_str(&format!("exchange: {} ", exchange));
        }
        s.push_str(&format!("relPath: {} ", msg.rel_path));
        if let Some(size) = msg.fields.get("size") {
            s.push_str(&format!("size: {} ", size));
        }
        s
    }

    fn stats(&self) {
        let state = self.state.lock().unwrap();
        let tot = state.msg_count + state.reject_count;
        let how_long = state.last_housekeeping.elapsed().as_secs_f64();
        
        let apc = if tot > 0 { 100.0 * state.msg_count as f64 / tot as f64 } else { 0.0 };
        let rate = if how_long > 0.0 { state.msg_count as f64 / how_long } else { 0.0 };

        log::info!("started: {:.1}s ago, last_housekeeping: {:.1}s ago", 
            state.started.elapsed().as_secs_f64(), how_long);
        log::info!("messages received: {}, accepted: {}, rejected: {}   rate accepted: {:.1}% or {:.1} m/s",
            tot, state.msg_count, state.reject_count, apc, rate);
        log::info!("files transferred: {} bytes: {} rate: {:.1} B/s", 
            state.transfer_count, state.file_bytes, if how_long > 0.0 { state.file_bytes as f64 / how_long } else { 0.0 });
        if state.msg_count > 0 {
            log::info!("lag: average: {:.2}, maximum: {:.2}", state.lag_total / state.msg_count as f64, state.lag_max);
        }
    }
}

#[async_trait]
impl FlowCB for LogPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self) -> anyhow::Result<()> {
        if self.log_events.contains("on_start") {
            self.stats();
            log::info!("starting");
        }
        Ok(())
    }

    async fn on_stop(&mut self) -> anyhow::Result<()> {
        if self.log_events.contains("on_stop") {
            self.stats();
            log::info!("stopping");
        }
        Ok(())
    }

    async fn after_accept(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.reject_count += worklist.rejected.len() as u64;
        state.msg_count += worklist.incoming.len() as u64;
        let now = Utc::now();

        if self.log_events.contains("reject") {
            for msg in &worklist.rejected {
                log::info!("rejected: {}", self.message_accept_str(msg));
            }
        }

        for msg in &worklist.incoming {
            let lag = (now - msg.pub_time).num_milliseconds() as f64 / 1000.0;
            // Simplified retry check for now as Message doesn't have is_retry yet
            state.lag_total += lag;
            if lag > state.lag_max {
                state.lag_max = lag;
            }
            if self.log_events.contains("after_accept") {
                log::info!("accepted: (lag: {:.2} ) {}", lag, self.message_accept_str(msg));
            }
        }
        Ok(())
    }

    async fn after_work(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.transfer_count += worklist.ok.len() as u64;
        
        if self.log_events.contains("reject") {
            for msg in &worklist.rejected {
                log::info!("rejected: {}", self.message_str(msg));
            }
        }

        for msg in &worklist.ok {
            let size = msg.fields.get("size").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            state.file_bytes += size;

            if self.log_events.contains("after_work") {
                log::info!("{} ok: {} size: {}", self.action_verb, msg.rel_path, size);
            }
        }
        Ok(())
    }

    async fn on_housekeeping(&self) -> anyhow::Result<()> {
        if self.log_events.contains("on_housekeeping") {
            self.stats();
            log::debug!("housekeeping");
        }
        self.reset();
        Ok(())
    }

    fn metrics_report(&self) -> HashMap<String, String> {
        let state = self.state.lock().unwrap();
        let mut m = HashMap::new();
        m.insert("lagMax".to_string(), state.lag_max.to_string());
        m.insert("lagTotal".to_string(), state.lag_total.to_string());
        m.insert("msgCount".to_string(), state.msg_count.to_string());
        m.insert("rejectCount".to_string(), state.reject_count.to_string());
        m
    }
}
