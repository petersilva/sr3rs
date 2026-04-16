
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::sync::Arc;
use std::path::PathBuf;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub mod subscribe;
pub mod sender;
pub mod log;
pub mod flowcb;
pub mod metrics;

use crate::flow::log::FlowLog;
use crate::flow::flowcb::{IncomingCursor, OkCursor, FailedCursor};
use crate::transfer::get_transfer;
use crate::flow::metrics::Metrics;
use crate::utils::redact_url;

#[derive(Debug, Default)]
pub struct Worklist {
    pub incoming: Vec<Message>,
    pub ok: Vec<Message>,
    pub rejected: Vec<Message>,
    pub failed: Vec<Message>,
    pub directories_ok: Vec<String>,
    pub gathered_from_mq: usize,
}

impl Worklist {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.incoming.clear();
        self.ok.clear();
        self.rejected.clear();
        self.failed.clear();
        self.directories_ok.clear();
        self.gathered_from_mq = 0;
    }

    pub fn incoming_cursor(&mut self) -> IncomingCursor {
        IncomingCursor::new()
    }

    pub fn ok_cursor(&mut self) -> OkCursor {
        OkCursor::new()
    }

    pub fn failed_cursor(&mut self) -> FailedCursor {
        FailedCursor::new()
    }
}

#[async_trait]
pub trait Flow: Send + Sync {
    fn config(&self) -> &Config;
    fn logger(&self) -> Arc<Mutex<FlowLog>>;
    fn metrics(&self) -> Arc<Mutex<Metrics>>;
    fn publishers(&self) -> Vec<Arc<Mutex<crate::flow::subscribe::MothPublisher>>> { Vec::new() }
    fn callbacks(&self) -> Vec<Arc<Mutex<dyn crate::flow::flowcb::FlowCB>>> { Vec::new() }
    
    async fn gather(&self, worklist: &mut Worklist) -> anyhow::Result<()>;
    
    async fn filter(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let config = self.config();
        let mut filtered_incoming = Vec::new();
        let mut rejected_count = 0;

        for m in worklist.incoming.drain(..) {
            let url_to_match = format!("{}{}", m.base_url, m.rel_path);
            let rel_path_to_match = m.rel_path.clone();
            let mut matched_filter = None;

            for mask in &config.masks {
                if mask.matches(&url_to_match) || mask.matches(&rel_path_to_match) {
                    matched_filter = Some(mask.clone());
                    break;
                }
            }

            if let Some(mask) = matched_filter {
                let mut msg = m;
                if mask.accepting {
                    msg.delete_on_post.insert("_dest_dir".to_string(), mask.directory.to_string_lossy().to_string());
                    msg.delete_on_post.insert("_mirror".to_string(), mask.mirror.to_string());
                    filtered_incoming.push(msg);
                } else {
                    worklist.rejected.push(msg);
                    rejected_count += 1;
                }
            } else if config.accept_unmatched {
                let mut msg = m;
                msg.delete_on_post.insert("_dest_dir".to_string(), config.directory.to_string_lossy().to_string());
                msg.delete_on_post.insert("_mirror".to_string(), config.mirror.to_string());
                filtered_incoming.push(msg);
            } else {
                worklist.rejected.push(m);
                rejected_count += 1;
            }
        }

        {
            let metrics_arc = self.metrics();
            let mut metrics = metrics_arc.lock().await;
            metrics.flow.msg_count_rejected += rejected_count;
        }

        let mut final_incoming = Vec::with_capacity(filtered_incoming.len());

        for mut msg in filtered_incoming { // this loop is updateFieldsAccepted from python...
            let mirror = msg.delete_on_post.get("_mirror").map(|s| s == "true").unwrap_or(false);
            let raw_dest_dir = msg.delete_on_post.get("_dest_dir").cloned().unwrap_or_else(|| ".".to_string());
            
            ::log::debug!(" raw_dest_dir: {:?} mirror: {:?}", raw_dest_dir, mirror);
            let mut vars = config.options.clone();
            for (k, v) in &msg.fields {
                vars.insert(k.clone(), v.clone());
            }

            let mut new_dir = crate::config::variable_expansion::expand_variables(&raw_dest_dir, &vars);
            ::log::debug!("2nd raw_dest_dir: {:?} mirror: {:?}", raw_dest_dir, mirror);

            let rel_path = msg.rel_path.clone();
            let parts: Vec<&str> = rel_path.split('/').collect();
            let filename = parts.last().unwrap_or(&"");

            if mirror && parts.len() > 1 {
                let dir_parts = format!("/{}",parts[..parts.len() - 1].join("/"));
                if !new_dir.is_empty() && new_dir != "." {
                    new_dir.push('/');
                    new_dir.push_str(&dir_parts);
                } else {
                    new_dir = dir_parts;
                }
            }
            msg.delete_on_post.insert("new_dir".to_string(), new_dir.clone());
            msg.delete_on_post.insert("new_file".to_string(), filename.to_string());


            ::log::debug!(" new_dir: {:?}, filename: {:?}", new_dir, filename);
            let mut new_full_path = if new_dir.is_empty() || new_dir == "." {
                 filename.to_string()
            } else {
                 format!("{}/{}", new_dir, filename)
            };
            ::log::debug!(" post_base_dir: {:?}, new_full_path: {:?}", config.post_base_dir, new_full_path);

             // Strip post_base_dir from the new_relPath if configured
            if let Some(ref pbd) = config.post_base_dir {
                let pbd_str = crate::config::variable_expansion::expand_variables(&pbd.to_string_lossy(), &vars);
                if pbd_str.len() > 1 && new_full_path.to_string().starts_with(&pbd_str) {
                    new_full_path = new_full_path[pbd_str.len()..].to_string();
                    if new_full_path.starts_with('/') {
                        new_full_path = new_full_path[1..].to_string();
                    }
                }
            }

            msg.delete_on_post.insert("new_relPath".to_string(), new_full_path);

            ::log::debug!("updatedFields for Accepted message : {:?} ", msg);
            final_incoming.push(msg);
        }

        worklist.incoming = final_incoming;
        
        for cb_mutex in self.callbacks() {
            let cb = cb_mutex.lock().await;
            cb.after_accept(worklist).await?;
        }

        {
            let logger_arc = self.logger();
            let mut logger = logger_arc.lock().await;
            logger.after_accept(config, worklist);
        }

        Ok(())
    }

    async fn accept(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { 
        ::log::warn!("AcCepting!? " );
        Ok(()) 
    }
    
    async fn work(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let config = self.config();
        if !config.download {
            let mut count = 0;
            for m in worklist.incoming.drain(..) {
                ::log::debug!("WORK: download disabled, skipping {}", m.rel_path);
                worklist.ok.push(m);
                count += 1;
            }
            {
                let metrics_arc = self.metrics();
                let mut metrics = metrics_arc.lock().await;
                metrics.flow.msg_count_out += count;
            }
            return Ok(());
        }

        let mut ok_count = 0;
        let mut failed_count = 0;
        let mut bytes_in = 0;

        for mut m in worklist.incoming.drain(..) {
            let scheme = match url::Url::parse(&m.base_url) {
                Ok(u) => u.scheme().to_string(),
                Err(_) => {
                    ::log::error!("WORK: invalid base_url: {}", redact_url(&m.base_url));
                    worklist.failed.push(m);
                    failed_count += 1;
                    continue;
                }
            };

            if let Some(transfer) = get_transfer(&scheme, config) {
                let dest_dir = m.delete_on_post.get("_dest_dir")
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|| config.directory.clone());
                
                let mirror = m.delete_on_post.get("_mirror")
                    .map(|s| s == "true")
                    .unwrap_or(config.mirror);

                let rel_path = m.rel_path.trim_start_matches('/');

                let local_file = if mirror {
                    dest_dir.join(rel_path)
                } else {
                    let filename = std::path::Path::new(rel_path)
                        .file_name()
                        .unwrap_or_else(|| std::ffi::OsStr::new("unknown"));
                    dest_dir.join(filename)
                };

                match transfer.get(&m, &local_file).await {
                    Ok(size) => {
                        ::log::info!("WORK: downloaded {} to {} ({} bytes)", m.rel_path, local_file.display(), size);
                        m.fields.insert("size".to_string(), size.to_string());
                        worklist.ok.push(m);
                        ok_count += 1;
                        bytes_in += size as u64;
                    }
                    Err(e) => {
                        ::log::error!("WORK: download failed for {}: {}", m.rel_path, e);
                        worklist.failed.push(m);
                        failed_count += 1;
                    }
                }
            } else {
                ::log::error!("WORK: unsupported protocol: {}", scheme);
                worklist.failed.push(m);
                failed_count += 1;
            }
        }

        {
            let metrics_arc = self.metrics();
            let mut metrics = metrics_arc.lock().await;
            metrics.flow.msg_count_out += ok_count;
            metrics.flow.msg_count_failed += failed_count;
            metrics.flow.byte_count_in += bytes_in;
        }

        Ok(())
    }

    fn message_adjust_post(&self, m: &mut Message, p: &crate::config::publisher::Publisher ) {

        ::log::warn!("message_adjust_post publisher p: {:?}", p );

        let new_full_path =  PathBuf::from(m.delete_on_post.get("new_dir").unwrap()).join(m.delete_on_post.get("new_file").unwrap() );

        ::log::warn!("message_adjust_post new_full_path: {:?}", new_full_path );
        ::log::warn!("message_adjust_post post_base_url: {:?}, post_base_dir: {:?}", p.base_url, p.base_dir );

       
        if let Some(base) = &p.base_dir {
            match new_full_path.strip_prefix(base) {
                Ok(new_rel_path) => {
                    ::log::debug!("Relative path: {}", new_rel_path.display());
                    m.rel_path = new_rel_path.to_string_lossy().into_owned();
                }
                Err(_) => {
                    ::log::warn!("Base is not a prefix of the full path");
                }
            }
        }
        
        // deferred: presence of m.delete_on_post.get("post_url") as an override.

        m.base_url = p.base_url.clone().unwrap();

        // deferred: convert to windows backslashes in path.
        ::log::warn!("message_adjust_post new_rel_path: {:?}", m.rel_path );
    }

    async fn post(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let publishers = self.publishers();
        if publishers.is_empty() {
            return Ok(());
        }

        let config = self.config();
        
        // Loopback protection for poll: only post if we didn't just ingest these from the queue.
        if config.component == "poll" && worklist.gathered_from_mq > 0 {
             ::log::debug!("POST: loopback protection active, skipping post of {} messages ingested from queue.", worklist.ok.len());
             return Ok(());
        }

        let mut next_ok = Vec::new();
        let mut failed_count = 0;
        let mut bytes_out = 0;

        for mut m in worklist.ok.drain(..) {

            ::log::warn!( "posting message: {:?}", m);
            let mut failed_indices = Vec::new();
            for (idx, pub_mutex) in publishers.iter().enumerate() {
                self.message_adjust_post( &mut m, &self.config().publishers[idx] );
                let mut p = pub_mutex.lock().await;
                if let Err(e) = p.publish_mut(&m).await {
                    ::log::error!("POST: failed to publish to {}: {}", redact_url(&p.broker_url), e);
                    failed_indices.push(idx);
                }
            }

            if failed_indices.is_empty() {
                // Approximate bytes out by calculating size field or payload
                if let Some(size_str) = m.fields.get("size") {
                    bytes_out += size_str.parse::<u64>().unwrap_or(0);
                }
                next_ok.push(m);
            } else {
                worklist.failed.push(m);
                failed_count += 1;
            }
        }
        worklist.ok = next_ok;

        {
            let metrics_arc = self.metrics();
            let mut metrics = metrics_arc.lock().await;
            metrics.flow.msg_count_failed += failed_count;
            metrics.flow.byte_count_out += bytes_out;
        }

        Ok(())
    }

    async fn ack(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { 
        Ok(())
    }

    fn message_adjust_work(&self, m: &mut Message) {

        if m.delete_on_post.contains_key("new_base_url") {

            m.delete_on_post.insert("old_base_url".to_string(), m.base_url.clone() );

            m.base_url = m.delete_on_post.get("new_base_url").clone().unwrap().to_string();  
            ::log::warn!("adjusting new_base_url: {:?}", m.base_url );
        }

        if m.delete_on_post.contains_key("new_rel_path") {
            // Deferred: check file name matches rel_path last element, update rel_path if need be.

            m.delete_on_post.insert("old_rel_path".to_string(), m.rel_path.clone() );

            m.rel_path = m.delete_on_post.get("new_rel_path").clone().unwrap().to_string();  
            ::log::warn!("adjusting new_rel_path: {:?}", m.rel_path );
        }

    }


    async fn run_once(&self, worklist: &mut Worklist) -> anyhow::Result<usize> {
        let config = self.config();
        let active = config.is_active();
        
        if !active {
            if config.component == "poll" {
                // In passive mode, POLL still gathers from the broker to warm its cache.
                ::log::debug!("RUN_ONCE: (passive poll) calling gather...");
                self.gather(worklist).await?;
                ::log::debug!("RUN_ONCE: (passive poll) gather returned.");
                // But we SKIP calling callbacks (which includes the Poll plugin itself)
            } else {
                // Other components just sleep when passive.
                return Ok(0);
            }
        } else {
            // Active mode: normal behavior.
            ::log::debug!("RUN_ONCE: (active) calling gather...");
            self.gather(worklist).await?;
            ::log::debug!("RUN_ONCE: (active) gather returned. worklist.incoming.len()={}", worklist.incoming.len());
            for cb_mutex in self.callbacks() {
                let mut cb = cb_mutex.lock().await;
                cb.gather(worklist).await?;
            }
        }

        let gathered_count = worklist.incoming.len();
        if gathered_count > 0 {
            let metrics_arc = self.metrics();
            let mut metrics = metrics_arc.lock().await;
            metrics.flow.msg_count_in += gathered_count as u64;
        }

        self.filter(worklist).await?;
        self.accept(worklist).await?;
        self.work(worklist).await?;

        // adjust message after action is done, but before 'after_work' so adjustment is possible.

        /* I think message_adjust_work does nothing useful.
        let mut next_ok = Vec::new();
        for mut m in worklist.ok.drain(..) {
            if config.publishers.len() <= 1 {
                self.message_adjust_work( &mut m);
                next_ok.push(m);
            } else {
                ::log::error!("multiple publisher feature deferred");
            }
        }
        worklist.ok = next_ok;
        */

        for cb_mutex in self.callbacks() {
            let cb = cb_mutex.lock().await;
            cb.after_work(worklist).await?;
        }

        {
            let logger_arc = self.logger();
            let mut logger = logger_arc.lock().await;
            logger.after_work(self.config(), worklist);
        }

        self.post(worklist).await?;
        self.ack(worklist).await?;
        
        // Count how many we actually processed in some way
        let processed = gathered_count + worklist.ok.len() + worklist.failed.len() + worklist.rejected.len();
        Ok(processed)
    }

    async fn run(&self) -> anyhow::Result<()> {
        let token = CancellationToken::new();
        self.run_with_shutdown(token).await
    }

    async fn run_with_shutdown(&self, token: CancellationToken) -> anyhow::Result<()> {
        let mut worklist = Worklist::new();
        let mut last_housekeeping = std::time::Instant::now();
        let housekeeping_interval = std::time::Duration::from_secs(self.config().housekeeping as u64);
        let mut total_messages: usize = 0;
        let message_count_max = self.config().message_count_max as usize;

        for cb_mutex in self.callbacks() {
            let mut cb = cb_mutex.lock().await;
            cb.on_start().await?;
        }

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    ::log::info!("Shutdown requested. Finalizing...");
                    self.housekeeping().await?;
                    for cb_mutex in self.callbacks() {
                        let mut cb = cb_mutex.lock().await;
                        cb.on_stop().await?;
                    }
                    self.shutdown().await?;
                    break;
                }
                res = self.run_once(&mut worklist) => {
                    let count = res?;
                    total_messages += count;
                    if message_count_max > 0 && total_messages >= message_count_max {
                        ::log::info!("{} messages processed >= messageCountMax {}. Shutting down...", total_messages, message_count_max);
                        self.housekeeping().await?;
                        for cb_mutex in self.callbacks() {
                            let mut cb = cb_mutex.lock().await;
                            cb.on_stop().await?;
                        }
                        self.shutdown().await?;
                        break;
                    }
                }
            }
            
            if last_housekeeping.elapsed() >= housekeeping_interval {
                self.housekeeping().await?;
                last_housekeeping = std::time::Instant::now();
            }

            if worklist.incoming.is_empty() && worklist.ok.is_empty() {
                tokio::select! {
                    _ = token.cancelled() => {
                        ::log::info!("Shutdown requested during sleep. Finalizing...");
                        self.housekeeping().await?;
                        for cb_mutex in self.callbacks() {
                            let mut cb = cb_mutex.lock().await;
                            cb.on_stop().await?;
                        }
                        self.shutdown().await?;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs_f64(self.config().sleep)) => {}
                }
            }
        }
        Ok(())
    }

    async fn housekeeping(&self) -> anyhow::Result<()> {
        {
            let logger_arc = self.logger();
            let mut logger = logger_arc.lock().await;
            logger.stats(self.config());
            logger.reset();
        }

        // Collect plugin metrics
        {
            let metrics_arc = self.metrics();
            let mut metrics = metrics_arc.lock().await;
            metrics.flow.last_housekeeping = chrono::Utc::now();
            
            // Get CPU times
            let ost = unsafe {
                let mut t: libc::tms = std::mem::zeroed();
                libc::times(&mut t);
                t
            };
            let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) as f64 };
            metrics.flow.cpu_time_user = ost.tms_utime as f64 / clk_tck;
            metrics.flow.cpu_time_system = ost.tms_stime as f64 / clk_tck;

            for cb_mutex in self.callbacks() {
                let cb = cb_mutex.lock().await;
                metrics.plugins.insert(cb.name().to_string(), cb.metrics_report());
            }

            // Save metrics to file
            if let Some(configname) = &self.config().configname {
                let component = crate::utils::detect_component_from_config(self.config());
                
                // Get instance number if we can... default to 1 for now.
                let instance = 1; // FIXME: get from config or env
                let metrics_file = crate::config::paths::get_metrics_filename(
                    self.config().host_dir.as_deref(),
                    &component,
                    Some(configname.as_str()),
                    instance,
                );
                
                if let Some(parent) = metrics_file.parent() {
                    if !parent.exists() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                }
                
                if let Ok(json) = serde_json::to_string_pretty(&*metrics) {
                    let _ = std::fs::write(metrics_file, json);
                }
            }
        }

        for cb_mutex in self.callbacks() {
            let cb = cb_mutex.lock().await;
            cb.on_housekeeping().await?;
        }

        Ok(())
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn declare(&mut self) -> anyhow::Result<()> {
        self.connect_exchanges().await?;
        self.connect_queues().await?;
        Ok(())
    }

    /// Declare exchanges used by this flow
    async fn connect_exchanges(&mut self) -> anyhow::Result<()>;

    /// Declare queues and bindings used by this flow
    async fn connect_queues(&mut self) -> anyhow::Result<()>;

    async fn cleanup(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct BaseFlow {
    pub config: Config,
    pub logger: Arc<Mutex<FlowLog>>,
    pub metrics: Arc<Mutex<Metrics>>,
    pub callbacks: Vec<Arc<Mutex<dyn crate::flow::flowcb::FlowCB>>>,
}

impl BaseFlow {
    pub fn new(config: Config) -> Self {
        let mut callbacks: Vec<Arc<Mutex<dyn crate::flow::flowcb::FlowCB>>> = Vec::new();
        
        for cb_name in &config.flow_callbacks {
            if let Some(plugin) = crate::flow::flowcb::get_plugin(cb_name, &config) {
                callbacks.push(plugin);
            } else {
                ::log::warn!("Plugin '{}' requested in config but not found or unsupported", cb_name);
            }
        }

        Self {
            config,
            logger: Arc::new(Mutex::new(FlowLog::new())),
            metrics: Arc::new(Mutex::new(Metrics::new())),
            callbacks,
        }
    }
}

#[async_trait]
impl Flow for BaseFlow {
    fn config(&self) -> &Config {
        &self.config
    }

    fn logger(&self) -> Arc<Mutex<FlowLog>> {
        self.logger.clone()
    }

    fn metrics(&self) -> Arc<Mutex<Metrics>> {
        self.metrics.clone()
    }

    fn callbacks(&self) -> Vec<Arc<Mutex<dyn crate::flow::flowcb::FlowCB>>> {
        self.callbacks.clone()
    }

    async fn gather(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn accept(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn ack(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn connect_exchanges(&mut self) -> anyhow::Result<()> { Ok(()) }
    async fn connect_queues(&mut self) -> anyhow::Result<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worklist_initialization() {
        let wl = Worklist::new();
        assert!(wl.incoming.is_empty());
        assert!(wl.ok.is_empty());
    }

    #[test]
    fn test_worklist_clear() {
        let mut wl = Worklist::new();
        wl.incoming.push(Message::new("url", "path"));
        wl.clear();
        assert!(wl.incoming.is_empty());
    }

    #[tokio::test]
    async fn test_flow_filter() {
        let mut config = Config::new();
        config.masks.push(crate::filter::Filter::new(".*accept.*", true, std::path::PathBuf::from("."), false).unwrap());
        config.masks.push(crate::filter::Filter::new(".*reject.*", false, std::path::PathBuf::from("."), false).unwrap());
        config.accept_unmatched = false;

        let flow = BaseFlow::new(config);
        let mut wl = Worklist::new();
        wl.incoming.push(Message::new("http://host/", "accept_me.txt"));
        wl.incoming.push(Message::new("http://host/", "reject_me.txt"));
        wl.incoming.push(Message::new("http://host/", "ignore_me.txt"));

        flow.filter(&mut wl).await.unwrap();

        assert_eq!(wl.incoming.len(), 1);
        assert_eq!(wl.incoming[0].rel_path, "accept_me.txt");
        assert_eq!(wl.rejected.len(), 2);
    }
 
    #[test] 
    fn test_message_adjust_post() {
       let mut m = crate::message::Message::new("file:/", "home/peter/Sarracenia/bugs/samples/data/20200105/WXO-DD/meteocode/atl/csv/2020-01-05T03-00-01Z_FPHX14_r10zf_CC.csv");
       m.delete_on_post.insert("new_dir".to_string(), "/home/peter/Sarracenia/bugs/samples/data/20200105/WXO-DD/meteocode/atl/csv".to_string() );
       m.delete_on_post.insert("new_file".to_string(), "2020-01-05T03-00-01Z_FPHX14_r10zf_CC.csv".to_string() );

       let config = Config::new();
       let mut publ = crate::config::publisher::Publisher::new(
           None, 
           vec!["xsarra".to_string()], 
           vec!["v03.post".to_string()], 
           "v03".to_string()
       );
       publ.base_url = Some("http://localhost:8090".to_string());
       publ.base_dir = Some(PathBuf::from("/home/peter/Sarracenia/bugs/samples/data"));

       let flow = BaseFlow::new(config);

       flow.message_adjust_post(&mut m, &publ);
       
       assert_eq!(m.base_url, "http://localhost:8090" );
       assert_eq!(m.rel_path, "20200105/WXO-DD/meteocode/atl/csv/2020-01-05T03-00-01Z_FPHX14_r10zf_CC.csv" );
    }
}
  
