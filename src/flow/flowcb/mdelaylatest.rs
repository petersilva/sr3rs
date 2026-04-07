//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::FlowCB;
use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::sync::Mutex;
use chrono::Utc;

struct MDelayLatestState {
    ok_delay: Vec<Message>,
    suppressions: u32,
    stop_requested: bool,
}

pub struct MDelayLatestPlugin {
    name: String,
    mdelay: f64,
    state: Mutex<MDelayLatestState>,
}

impl MDelayLatestPlugin {
    pub fn new(config: &Config) -> Self {
        // Parse mdelay from options, defaulting to 30.0 seconds
        let mdelay = config.options.get("mdelay")
            .and_then(|v| {
                // Handle optional suffixes like 's' using config's parser logic
                // But simple parse is fine if it's already just a number
                let v = v.trim_end_matches('s');
                v.parse::<f64>().ok()
            })
            .unwrap_or(30.0);

        ::log::info!("mdelay set to {}", mdelay);

        Self {
            name: "mdelaylatest".to_string(),
            mdelay,
            state: Mutex::new(MDelayLatestState {
                ok_delay: Vec::new(),
                suppressions: 0,
                stop_requested: false,
            }),
        }
    }
}

#[async_trait]
impl FlowCB for MDelayLatestPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let now = Utc::now();
        let mut new_incoming = Vec::new();

        // 1. Process new incoming messages
        for m1 in wl.incoming.drain(..) {
            let elapsed = (now - m1.pub_time).num_milliseconds() as f64 / 1000.0;

            // Search for an existing message in the delay queue with the same rel_path
            if let Some(pos) = state.ok_delay.iter().position(|m2| m1.rel_path == m2.rel_path) {
                let m2 = state.ok_delay.remove(pos);

                // If it's a critical ordered operation (like rename, rm, mkdir), process it immediately
                if m1.fields.contains_key("fileOp") || m2.fields.contains_key("fileOp") {
                    let op = m2.fields.get("fileOp").unwrap_or(&"unknown".to_string()).clone();
                    ::log::info!("critically ordered operation: {} {}", m2.rel_path, op);
                    new_incoming.push(m2);
                    state.ok_delay.push(m1);
                } else {
                    // Suppress the older intermediate message
                    ::log::info!("intermediate version suppressed: {}", m1.rel_path);
                    state.suppressions += 1;
                    wl.rejected.push(m2);
                    state.ok_delay.push(m1);
                }
            } else {
                // If it's a completely new message
                if elapsed < self.mdelay {
                    state.ok_delay.push(m1);
                } else {
                    new_incoming.push(m1);
                }
            }
        }

        // 2. Check messages in the delay list
        let now = Utc::now(); // Re-evaluate now in case loop took time
        let mut still_delayed = Vec::new();
        
        for m1 in state.ok_delay.drain(..) {
            let elapsed = (now - m1.pub_time).num_milliseconds() as f64 / 1000.0;
            if elapsed >= self.mdelay {
                // Delay expired, ready to be published/processed
                new_incoming.push(m1);
            } else {
                // Keep delaying
                still_delayed.push(m1);
            }
        }
        state.ok_delay = still_delayed;

        // 3. Drain if a stop was requested
        if state.stop_requested {
            new_incoming.extend(state.ok_delay.drain(..));
            ::log::info!("stop requested, ok_delay queue drained");
        }

        wl.incoming = new_incoming;

        Ok(())
    }

    async fn on_housekeeping(&self, _wl: &mut Worklist) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        ::log::info!(
            "suppressions={} currently delay queue length: {}",
            state.suppressions,
            state.ok_delay.len()
        );
        state.suppressions = 0;
        Ok(())
    }

    async fn please_stop(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.stop_requested = true;
    }
}
