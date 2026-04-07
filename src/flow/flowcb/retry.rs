//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::FlowCB;
use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::collections::{VecDeque, HashMap};
use std::sync::Mutex;
use std::path::PathBuf;

pub struct SimpleDiskQueue {
    path: PathBuf,
    queue: VecDeque<Message>,
}

impl SimpleDiskQueue {
    pub fn new(path: PathBuf) -> Self {
        let mut sq = Self { path, queue: VecDeque::new() };
        sq.load();
        sq
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn put(&mut self, msgs: &mut Vec<Message>) {
        self.queue.extend(msgs.drain(..));
        self.save();
    }

    pub fn get(&mut self, qty: usize) -> Vec<Message> {
        let count = std::cmp::min(qty, self.queue.len());
        let mut res = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(m) = self.queue.pop_front() {
                res.push(m);
            }
        }
        self.save();
        res
    }

    fn load(&mut self) {
        if let Ok(content) = std::fs::read_to_string(&self.path) {
            if let Ok(msgs) = serde_json::from_str::<Vec<Message>>(&content) {
                self.queue = msgs.into();
            }
        }
    }

    fn save(&self) {
        if let Some(p) = self.path.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        // Write to temp file then rename for atomic write
        let tmp_path = self.path.with_extension("tmp");
        if let Ok(content) = serde_json::to_string(&self.queue) {
            if std::fs::write(&tmp_path, content).is_ok() {
                let _ = std::fs::rename(tmp_path, &self.path);
            }
        }
    }
}

pub struct RetryPlugin {
    pub name: String,
    pub retry_refilter: bool,
    pub batch_size: usize,
    pub download_retry: Mutex<Option<SimpleDiskQueue>>,
    pub post_retry: Mutex<Option<SimpleDiskQueue>>,
    pub cache_dir: PathBuf,
}

impl RetryPlugin {
    pub fn new(config: &Config) -> Self {
        let retry_refilter = config.options.get("retry_refilter")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        let config_part = config.configname.clone().unwrap_or_else(|| "unknown".to_string());
        
        let cache_dir = crate::config::paths::get_user_cache_dir()
            .join(&config.component)
            .join(config_part);

        Self {
            name: "retry".to_string(),
            retry_refilter,
            batch_size: config.batch as usize,
            download_retry: Mutex::new(None),
            post_retry: Mutex::new(None),
            cache_dir,
        }
    }

    fn set_is_retry(msg: &mut Message) {
        let current = msg.fields.get("_isRetry")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);
        
        msg.fields.insert("_isRetry".to_string(), (current + 1).to_string());
        
        let mut delete_on_post = msg.fields.get("_deleteOnPost")
            .map(|v| v.clone())
            .unwrap_or_default();
        
        if !delete_on_post.contains("_isRetry") {
            if !delete_on_post.is_empty() {
                delete_on_post.push(',');
            }
            delete_on_post.push_str("_isRetry");
            msg.fields.insert("_deleteOnPost".to_string(), delete_on_post);
        }
    }
}

#[async_trait]
impl FlowCB for RetryPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self) -> anyhow::Result<()> {
        let dl_path = self.cache_dir.join("work_retry.json");
        let post_path = self.cache_dir.join("post_retry.json");

        *self.download_retry.lock().unwrap() = Some(SimpleDiskQueue::new(dl_path));
        *self.post_retry.lock().unwrap() = Some(SimpleDiskQueue::new(post_path));
        
        ::log::debug!("RetryPlugin on_start: initialized queues in {}", self.cache_dir.display());
        Ok(())
    }

    async fn on_stop(&mut self) -> anyhow::Result<()> {
        *self.download_retry.lock().unwrap() = None;
        *self.post_retry.lock().unwrap() = None;
        Ok(())
    }

    async fn after_gather(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        if !self.retry_refilter {
            return Ok(());
        }

        let mut dl_queue_guard = self.download_retry.lock().unwrap();
        if let Some(dl_queue) = dl_queue_guard.as_mut() {
            if dl_queue.len() == 0 {
                return Ok(());
            }

            let qty = (self.batch_size / 2).saturating_sub(wl.incoming.len());
            if qty > 0 {
                let mut msgs = dl_queue.get(qty);
                for m in &mut msgs {
                    Self::set_is_retry(m);
                }
                if !msgs.is_empty() {
                    wl.incoming.extend(msgs);
                }
            }
        }
        Ok(())
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        if self.retry_refilter {
            return Ok(());
        }

        let mut dl_queue_guard = self.download_retry.lock().unwrap();
        if let Some(dl_queue) = dl_queue_guard.as_mut() {
            if dl_queue.len() == 0 {
                return Ok(());
            }

            let qty = (self.batch_size / 2).saturating_sub(wl.incoming.len());
            if qty == 0 {
                ::log::info!("{} messages to process, too busy to retry", wl.incoming.len());
                return Ok(());
            }

            let mut msgs = dl_queue.get(qty);
            for m in &mut msgs {
                Self::set_is_retry(m);
            }
            if !msgs.is_empty() {
                wl.incoming.extend(msgs);
            }
        }
        Ok(())
    }

    async fn after_work(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        // Messages in wl.failed go into download_retry
        if !wl.failed.is_empty() {
            let mut dl_queue_guard = self.download_retry.lock().unwrap();
            if let Some(dl_queue) = dl_queue_guard.as_mut() {
                ::log::debug!("putting {} messages into download retry queue", wl.failed.len());
                dl_queue.put(&mut wl.failed);
            }
        }

        // Check if we can retry posting
        let mut post_queue_guard = self.post_retry.lock().unwrap();
        if let Some(post_queue) = post_queue_guard.as_mut() {
            if post_queue.len() == 0 {
                return Ok(());
            }

            let qty = if self.batch_size > 2 {
                (self.batch_size / 2).saturating_sub(wl.ok.len())
            } else if wl.ok.len() < self.batch_size {
                self.batch_size - wl.ok.len()
            } else {
                0
            };

            if qty == 0 {
                ::log::info!("{} messages to process, too busy to retry posting", wl.ok.len());
                return Ok(());
            }

            let msgs = post_queue.get(qty);
            if !msgs.is_empty() {
                ::log::debug!("loading from post retry: qty={} ... got: {}", qty, msgs.len());
                wl.ok.extend(msgs);
            }
        }
        Ok(())
    }

    async fn after_post(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        // Messages in wl.failed go into post_retry
        if !wl.failed.is_empty() {
            for m in &mut wl.failed {
                Self::set_is_retry(m);
            }
            
            let mut post_queue_guard = self.post_retry.lock().unwrap();
            if let Some(post_queue) = post_queue_guard.as_mut() {
                post_queue.put(&mut wl.failed);
            }
        }
        Ok(())
    }

    fn metrics_report(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        
        let dl_len = self.download_retry.lock().unwrap().as_ref().map(|q| q.len()).unwrap_or(0);
        let post_len = self.post_retry.lock().unwrap().as_ref().map(|q| q.len()).unwrap_or(0);
        
        m.insert("msgs_in_download_retry".to_string(), dl_len.to_string());
        m.insert("msgs_in_post_retry".to_string(), post_len.to_string());
        
        m
    }
}
