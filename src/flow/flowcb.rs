//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

pub mod log;
pub mod sample;
pub mod pywrapper;
pub mod retry;
pub mod mdelaylatest;
pub mod nodupe;
pub mod gather_file;
pub mod scheduled;
pub mod poll;

use crate::message::Message;
use crate::flow::Worklist;
use crate::Config;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

pub fn get_plugin(name: &str, config: &Config) -> Option<Arc<Mutex<dyn FlowCB>>> {
    let name_lower = name.to_lowercase();
    let short_name = name_lower.split('.').last().unwrap_or(&name_lower);
    
    match short_name {
        "log" => Some(Arc::new(Mutex::new(log::LogPlugin::new(config)))),
        "sample" => Some(Arc::new(Mutex::new(sample::SamplePlugin::new("sample", "reject_me")))),
        "retry" => Some(Arc::new(Mutex::new(retry::RetryPlugin::new(config)))),
        "mdelaylatest" => Some(Arc::new(Mutex::new(mdelaylatest::MDelayLatestPlugin::new(config)))),
        "disk" | "nodupe" => Some(Arc::new(Mutex::new(nodupe::disk::DiskNoDupePlugin::new(config)))),
        "name_only" | "name" => Some(Arc::new(Mutex::new(nodupe::modifiers::NameOnlyPlugin::new()))),
        "path_only" => Some(Arc::new(Mutex::new(nodupe::modifiers::PathOnlyPlugin::new()))),
        "data_only" => Some(Arc::new(Mutex::new(nodupe::modifiers::DataOnlyPlugin::new()))),
        "file" => Some(Arc::new(Mutex::new(gather_file::GatherFilePlugin::new(config)))),
        "scheduled" => Some(Arc::new(Mutex::new(scheduled::ScheduledPlugin::new(config)))),
        "poll" => Some(Arc::new(Mutex::new(poll::PollPlugin::new(config)))),
        _ => {
            match pywrapper::PyWrapperPlugin::new(name, config) {
                Ok(plugin) => Some(Arc::new(Mutex::new(plugin))),
                Err(e) => {
                    ::log::error!("Failed to load native or Python plugin '{}': {}", name, e);
                    None
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Accept,
    Reject,
    Fail,
    Keep,
}

/// A cursor for iterating and manipulating the `incoming` list of a `Worklist`.
pub struct IncomingCursor {
    index: usize,
}

impl IncomingCursor {
    pub fn new() -> Self {
        Self { index: 0 }
    }

    /// Get a mutable reference to the current message.
    pub fn next<'a>(&mut self, wl: &'a mut Worklist) -> Option<&'a mut Message> {
        wl.incoming.get_mut(self.index)
    }

    /// Advance the cursor to the next message without moving the current one.
    pub fn advance(&mut self) {
        self.index += 1;
    }

    /// Move the current message to the `ok` list.
    pub fn accept(&mut self, wl: &mut Worklist) {
        if self.index < wl.incoming.len() {
            let msg = wl.incoming.swap_remove(self.index);
            wl.ok.push(msg);
        }
    }

    /// Move the current message to the `rejected` list.
    pub fn reject(&mut self, wl: &mut Worklist) {
        if self.index < wl.incoming.len() {
            let msg = wl.incoming.swap_remove(self.index);
            wl.rejected.push(msg);
        }
    }

    /// Move the current message to the `failed` list.
    pub fn fail(&mut self, wl: &mut Worklist) {
        if self.index < wl.incoming.len() {
            let msg = wl.incoming.swap_remove(self.index);
            wl.failed.push(msg);
        }
    }

    /// Take a decision on the current message and move it accordingly.
    pub fn step(&mut self, wl: &mut Worklist, decision: Decision) {
        match decision {
            Decision::Accept => self.accept(wl),
            Decision::Reject => self.reject(wl),
            Decision::Fail => self.fail(wl),
            Decision::Keep => self.advance(),
        }
    }
}

/// A cursor for iterating and manipulating the `ok` list of a `Worklist`.
pub struct OkCursor {
    index: usize,
}

impl OkCursor {
    pub fn new() -> Self {
        Self { index: 0 }
    }

    pub fn next<'a>(&mut self, wl: &'a mut Worklist) -> Option<&'a mut Message> {
        wl.ok.get_mut(self.index)
    }

    pub fn advance(&mut self) {
        self.index += 1;
    }

    /// Move the current message back to the `failed` list (e.g., if post-processing fails).
    pub fn mark_failed(&mut self, wl: &mut Worklist) {
        if self.index < wl.ok.len() {
            let msg = wl.ok.swap_remove(self.index);
            wl.failed.push(msg);
        }
    }

    pub fn keep_ok(&mut self) {
        self.advance();
    }
}

/// A cursor for iterating and manipulating the `failed` list of a `Worklist`.
pub struct FailedCursor {
    index: usize,
}

impl FailedCursor {
    pub fn new() -> Self {
        Self { index: 0 }
    }

    pub fn next<'a>(&mut self, wl: &'a mut Worklist) -> Option<&'a mut Message> {
        wl.failed.get_mut(self.index)
    }

    pub fn advance(&mut self) {
        self.index += 1;
    }

    /// Move the current message back to the `incoming` list (retry).
    pub fn retry(&mut self, wl: &mut Worklist) {
        if self.index < wl.failed.len() {
            let msg = wl.failed.swap_remove(self.index);
            wl.incoming.push(msg);
        }
    }

    pub fn keep_failed(&mut self) {
        self.advance();
    }
}

/// Flow Callback trait for implementing plugin customization to flows.
/// Similar to Sarracenia's Python FlowCB class.
#[async_trait]
pub trait FlowCB: Send + Sync {
    /// Unique name of the plugin instance.
    fn name(&self) -> &str;

    /// Called during component startup.
    async fn on_start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called during component shutdown.
    async fn on_stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called during component cleanup (e.g. `sr3rs cleanup`).
    async fn on_cleanup(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called after messages go through basic accept/reject masks.
    /// Use `worklist.incoming_cursor()` to iterate and filter.
    async fn after_accept(&self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called after the "work" phase (e.g., download or send) is complete.
    /// Operates on `worklist.ok` and `worklist.failed`.
    async fn after_work(&self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called before the "work" phase, after gather and after_accept.
    async fn after_gather(&self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called after the "post" phase.
    async fn after_post(&self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called periodically for maintenance tasks.
    async fn on_housekeeping(&self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        Ok(())
    }

    /// Called when a stop has been requested, allowing for graceful wrap-up.
    async fn please_stop(&mut self) {
    }

    /// Called during the gather phase.
    async fn gather(&mut self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        Ok(())
    }

    /// Metrics report for this plugin.
    fn metrics_report(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }
}
