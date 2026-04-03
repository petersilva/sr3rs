use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub mod subscribe;
pub mod log;

use crate::flow::log::FlowLog;
use crate::transfer::get_transfer;

#[derive(Debug, Default)]
pub struct Worklist {
    pub incoming: Vec<Message>,
    pub ok: Vec<Message>,
    pub rejected: Vec<Message>,
    pub failed: Vec<Message>,
    pub directories_ok: Vec<String>,
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
    }
}

#[async_trait]
pub trait Flow: Send + Sync {
    fn config(&self) -> &Config;
    fn logger(&self) -> Arc<Mutex<FlowLog>>;
    fn publishers(&self) -> Vec<Arc<Mutex<crate::flow::subscribe::AmqpPublisher>>> { Vec::new() }
    
    async fn gather(&self, worklist: &mut Worklist) -> anyhow::Result<()>;
    
    async fn filter(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let config = self.config();
        let mut filtered_incoming = Vec::new();

        for m in worklist.incoming.drain(..) {
            let url_to_match = format!("{}{}", m.base_url, m.rel_path);
            let mut matched_filter = None;

            for mask in &config.masks {
                if mask.matches(&url_to_match) {
                    matched_filter = Some(mask.clone());
                    break;
                }
            }

            if let Some(mask) = matched_filter {
                let mut msg = m;
                if mask.accepting {
                    msg.fields.insert("_dest_dir".to_string(), mask.directory.to_string_lossy().to_string());
                    msg.fields.insert("_mirror".to_string(), mask.mirror.to_string());
                    filtered_incoming.push(msg);
                } else {
                    worklist.rejected.push(msg);
                }
            } else if config.accept_unmatched {
                let mut msg = m;
                msg.fields.insert("_dest_dir".to_string(), config.directory.to_string_lossy().to_string());
                msg.fields.insert("_mirror".to_string(), config.mirror.to_string());
                filtered_incoming.push(msg);
            } else {
                worklist.rejected.push(m);
            }
        }

        worklist.incoming = filtered_incoming;
        
        {
            let logger_arc = self.logger();
            let mut logger = logger_arc.lock().await;
            logger.after_accept(config, worklist);
        }

        Ok(())
    }

    async fn accept(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    
    async fn work(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let config = self.config();
        if !config.download {
            for m in worklist.incoming.drain(..) {
                ::log::debug!("WORK: download disabled, skipping {}", m.rel_path);
                worklist.ok.push(m);
            }
            return Ok(());
        }

        for mut m in worklist.incoming.drain(..) {
            let scheme = match url::Url::parse(&m.base_url) {
                Ok(u) => u.scheme().to_string(),
                Err(_) => {
                    ::log::error!("WORK: invalid base_url: {}", m.base_url);
                    worklist.failed.push(m);
                    continue;
                }
            };

            if let Some(transfer) = get_transfer(&scheme, config) {
                let dest_dir = m.fields.get("_dest_dir")
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|| config.directory.clone());
                
                let mirror = m.fields.get("_mirror")
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
                    }
                    Err(e) => {
                        ::log::error!("WORK: download failed for {}: {}", m.rel_path, e);
                        worklist.failed.push(m);
                    }
                }
            } else {
                ::log::error!("WORK: unsupported protocol: {}", scheme);
                worklist.failed.push(m);
            }
        }
        Ok(())
    }

    async fn post(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let publishers = self.publishers();
        if publishers.is_empty() {
            return Ok(());
        }

        let mut next_ok = Vec::new();
        for m in worklist.ok.drain(..) {
            let mut failed_indices = Vec::new();
            for (idx, pub_mutex) in publishers.iter().enumerate() {
                let p = pub_mutex.lock().await;
                if let Err(e) = p.publish(&m).await {
                    ::log::error!("POST: failed to publish to {}: {}", p.broker_url, e);
                    failed_indices.push(idx);
                }
            }

            if failed_indices.is_empty() {
                next_ok.push(m);
            } else {
                worklist.failed.push(m);
            }
        }
        worklist.ok = next_ok;
        Ok(())
    }

    async fn ack(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { 
        Ok(())
    }

    async fn run_once(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.gather(worklist).await?;
        self.filter(worklist).await?;
        self.accept(worklist).await?;
        self.work(worklist).await?;
        {
            let logger_arc = self.logger();
            let mut logger = logger_arc.lock().await;
            logger.after_work(self.config(), worklist);
        }
        self.post(worklist).await?;
        self.ack(worklist).await?;
        Ok(())
    }

    async fn run(&self) -> anyhow::Result<()> {
        let token = CancellationToken::new();
        self.run_with_shutdown(token).await
    }

    async fn run_with_shutdown(&self, token: CancellationToken) -> anyhow::Result<()> {
        let mut worklist = Worklist::new();
        let mut last_housekeeping = std::time::Instant::now();
        let housekeeping_interval = std::time::Duration::from_secs(self.config().housekeeping as u64);

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    ::log::info!("Shutdown requested. Finalizing...");
                    self.housekeeping(&mut worklist).await?;
                    self.shutdown().await?;
                    break;
                }
                res = self.run_once(&mut worklist) => {
                    res?;
                }
            }
            
            if last_housekeeping.elapsed() >= housekeeping_interval {
                self.housekeeping(&mut worklist).await?;
                last_housekeeping = std::time::Instant::now();
            }

            if worklist.incoming.is_empty() && worklist.ok.is_empty() {
                tokio::select! {
                    _ = token.cancelled() => {
                        ::log::info!("Shutdown requested during sleep. Finalizing...");
                        self.housekeeping(&mut worklist).await?;
                        self.shutdown().await?;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs_f64(self.config().sleep)) => {}
                }
            }
        }
        Ok(())
    }

    async fn housekeeping(&self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        let logger_arc = self.logger();
        let mut logger = logger_arc.lock().await;
        logger.stats(self.config());
        logger.reset();
        Ok(())
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn declare(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct BaseFlow {
    pub config: Config,
    pub logger: Arc<Mutex<FlowLog>>,
}

impl BaseFlow {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            logger: Arc::new(Mutex::new(FlowLog::new())),
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

    async fn gather(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn accept(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn ack(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
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
}
