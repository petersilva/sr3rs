//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::FlowCB;
use crate::Config;
use crate::config::is_true;
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
        // Defaults from Python implementation
        log_events.insert("after_accept".to_string());
        log_events.insert("after_work".to_string());
        log_events.insert("after_post".to_string());
        log_events.insert("on_housekeeping".to_string());

        if let Some(events_str) = config.options.get("logEvents") {
            for e in events_str.split(',') {
                let trimmed = e.trim();
                if let Some(item) = trimmed.strip_prefix('+') {
                    log_events.insert(item.trim().to_string());
                } else if let Some(item) = trimmed.strip_prefix('-') {
                    log_events.remove(item.trim());
                } else if !trimmed.is_empty() {
                    log_events.insert(trimmed.to_string());
                }
            }
        }

        let log_reject = config.log_reject
            || config.options.get("logReject").map(|v| is_true(v)).unwrap_or(false)
            || config.options.get("log_reject").map(|v| is_true(v)).unwrap_or(false);

        if log_reject {
            log_events.insert("reject".to_string());
        }

        let log_duplicates = config.options.get("logDuplicates").map(|v| is_true(v)).unwrap_or(false)
            || config.nodupe_ttl > 0
            || config.flow_callbacks.iter().any(|c| c == "nodupe" || c.starts_with("nodupe."));

        if log_duplicates {
            log_events.insert("nodupe".to_string());
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
                if let Some(ref rep) = msg.report {
                    log::info!("rejected: {} ({}: {})", self.message_accept_str(msg).trim_end(), rep.code, rep.message);
                } else if let Some(reject_reason) = msg.delete_on_post.get("reject") {
                    log::info!("rejected: {} ({})", self.message_accept_str(msg).trim_end(), reject_reason);
                } else {
                    log::info!("rejected: {}", self.message_accept_str(msg));
                }
            }
        } else if self.log_events.contains("nodupe") {
            for msg in &worklist.rejected {
                if let Some(ref rep) = msg.report {
                    if rep.code == 304 {
                        log::info!("{} rejected: {} {}", msg.get_id_str(), rep.code, rep.message);
                    }
                }
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
        state.reject_count += worklist.rejected.len() as u64;
        
        if self.log_events.contains("reject") {
            for msg in &worklist.rejected {
                if let Some(ref rep) = msg.report {
                    log::info!("rejected: {} ({}: {})", self.message_str(msg).trim_end(), rep.code, rep.message);
                } else if let Some(reject_reason) = msg.delete_on_post.get("reject") {
                    log::info!("rejected: {} ({})", self.message_str(msg).trim_end(), reject_reason);
                } else {
                    log::info!("rejected: {}", self.message_str(msg));
                }
            }
        } else if self.log_events.contains("nodupe") {
            for msg in &worklist.rejected {
                if let Some(ref rep) = msg.report {
                    if rep.code == 304 {
                        log::info!("{} rejected: {} {}", msg.get_id_str(), rep.code, rep.message);
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log_plugin_reject() {
        let mut config = Config::default();
        config.parse_string("log_reject on", "test.conf").unwrap();

        let plugin = LogPlugin::new(&config);
        let mut wl = Worklist::new();
        let mut msg = Message::new("http://localhost/", "rejected_file.txt");
        msg.set_report(304, "Not modified 1 (nodupe check)");
        wl.rejected.push(msg);

        plugin.after_accept(&mut wl).await.unwrap();

        let state = plugin.state.lock().unwrap();
        assert_eq!(state.reject_count, 1);
        assert_eq!(wl.rejected[0].report.as_ref().unwrap().code, 304);
        assert_eq!(wl.rejected[0].report.as_ref().unwrap().message, "Not modified 1 (nodupe check)");
    }

    #[tokio::test]
    async fn test_log_plugin_plus_prefix() {
        let mut config = Config::default();
        config.parse_string("logEvents +after_accept", "test.conf").unwrap();
        config.parse_string("log_reject on", "test.conf").unwrap();

        let plugin = LogPlugin::new(&config);
        assert!(plugin.log_events.contains("after_accept"));
        assert!(plugin.log_events.contains("on_housekeeping"));
        assert!(plugin.log_events.contains("reject"));
    }

    #[tokio::test]
    async fn test_log_plugin_nodupe_304() {
        let mut config = Config::default();
        config.nodupe_ttl = 1200;
        config.flow_callbacks.push("nodupe".to_string());

        let plugin = LogPlugin::new(&config);
        assert!(plugin.log_events.contains("nodupe"));

        let mut wl = Worklist::new();
        let mut msg = Message::new("http://localhost:8001/", "20261119/file.txt");
        msg.set_report(304, "Not modified 1 (nodupe check)");
        wl.rejected.push(msg);

        plugin.after_accept(&mut wl).await.unwrap();

        let state = plugin.state.lock().unwrap();
        assert_eq!(state.reject_count, 1);
        assert_eq!(wl.rejected[0].get_id_str(), "http://localhost:8001/20261119/file.txt");
    }
}

