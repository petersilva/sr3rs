//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::{Flow, Worklist, BaseFlow, subscribe::{MothConsumer, MothPublisher}};
use crate::Config;
use crate::moth::MothFactory;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::transfer::get_transfer;

pub struct SenderFlow {
    pub base: BaseFlow,
    pub consumers: Vec<Arc<Mutex<MothConsumer>>>,
    pub publishers: Vec<Arc<Mutex<MothPublisher>>>,
    pub declaration_moths: Vec<Arc<Mutex<Box<dyn crate::moth::Moth>>>>,
}

impl SenderFlow {
    pub fn new(config: Config) -> Self {
        Self {
            base: BaseFlow::new(config),
            consumers: Vec::new(),
            publishers: Vec::new(),
            declaration_moths: Vec::new(),
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        self.connect_full(true, true).await
    }

    pub async fn connect_full(&mut self, declare: bool, consume: bool) -> anyhow::Result<()> {
        if declare {
            self.connect_exchanges().await?;
            self.connect_queues().await?;
        } else if consume {
            let subscriptions_count = self.base.config.subscriptions.len();
            for idx in 0..subscriptions_count {
                self.connect_subscription_full(idx, false, true).await?;
            }
        }

        if consume {
            let publishers_config = self.base.config.publishers.clone();
            for (idx, p_cfg) in publishers_config.into_iter().enumerate() {
                let cred = p_cfg.broker.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Publisher missing broker credentials"))?;
                
                let mut broker = crate::broker::Broker::parse(&cred.url.to_string())?;
                broker.user = Some(cred.url.username().to_string());
                broker.password = cred.url.password().map(String::from);

                log::info!("Connecting publisher to broker: {}", broker.url);
                let moth = MothFactory::new(&broker, false).await?;

                let mut options = serde_json::to_value(&self.base.config).unwrap_or(serde_json::json!({}));
                if let Some(obj) = options.as_object_mut() {
                    obj.insert("publisher_index".to_string(), serde_json::json!(idx));
                }

                self.publishers.push(Arc::new(Mutex::new(MothPublisher {
                    options,
                    moth,
                    broker_url: cred.url.to_string(),
                    exchanges: p_cfg.exchange.clone(),
                })));
            }
        }
        Ok(())
    }

    async fn connect_subscription_full(&mut self, idx: usize, declare: bool, consume: bool) -> anyhow::Result<()> {
        let sub = &self.base.config.subscriptions[idx];
        let cred = sub.broker.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Subscription {} missing broker credentials", idx))?;
        
        let mut broker = crate::broker::Broker::parse(&cred.url.to_string())?;
        broker.user = Some(cred.url.username().to_string());
        broker.password = cred.url.password().map(String::from);

        log::info!("Connecting to broker: {}", broker.url);
        
        let mut moth = MothFactory::new(&broker, true).await?;

        if declare {
            let topics: Vec<String> = sub.bindings.iter().map(|b| b.topic.clone()).collect();
            let exchange = sub.bindings.first().and_then(|b| b.exchange.as_deref()).unwrap_or("xpublic");
            
            moth.subscribe(&topics, exchange, &sub.queue.name).await?;
        }

        if consume {
            let new_consumer = Arc::new(Mutex::new(MothConsumer {
                moth,
                broker_url: cred.url.to_string(),
                subscription_idx: idx,
            }));

            if idx < self.consumers.len() {
                self.consumers[idx] = new_consumer;
            } else {
                self.consumers.push(new_consumer);
            }
        } else {
             self.declaration_moths.push(Arc::new(Mutex::new(moth)));
        }
        Ok(())
    }
}

#[async_trait]
impl Flow for SenderFlow {
    fn config(&self) -> &Config {
        &self.base.config
    }

    fn logger(&self) -> Arc<Mutex<crate::flow::log::FlowLog>> {
        self.base.logger.clone()
    }

    fn metrics(&self) -> Arc<Mutex<crate::flow::metrics::Metrics>> {
        self.base.metrics.clone()
    }

    fn publishers(&self) -> Vec<Arc<Mutex<MothPublisher>>> {
        self.publishers.clone()
    }

    async fn connect_exchanges(&mut self) -> anyhow::Result<()> {
        if let Some(broker_cfg) = &self.base.config.broker {
            log::info!("Connecting to broker for exchange declaration: {}", broker_cfg.url);
            let mut moth = MothFactory::new(broker_cfg, false).await?;
            let exchange = self.base.config.exchange.clone();
            
            log::info!("Declaring primary exchange: {}", exchange);
            moth.declare_exchange(&exchange, "topic").await?;
            self.declaration_moths.push(Arc::new(Mutex::new(moth)));
        }

        if let Some(broker_cfg) = &self.base.config.post_broker {
            log::info!("Connecting to post_broker for exchange declaration: {}", broker_cfg.url);
            let mut moth = MothFactory::new(broker_cfg, false).await?;
            let exchange = self.base.config.post_exchange.clone().unwrap_or_else(|| "xpublic".to_string());
            
            log::info!("Declaring post exchange: {}", exchange);
            moth.declare_exchange(&exchange, "topic").await?;
            self.declaration_moths.push(Arc::new(Mutex::new(moth)));
        }

        let publishers_config = self.base.config.publishers.clone();
        for p_cfg in publishers_config {
            let cred = p_cfg.broker.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Publisher missing broker credentials"))?;
            
            let mut broker = crate::broker::Broker::parse(&cred.url.to_string())?;
            broker.user = Some(cred.url.username().to_string());
            broker.password = cred.url.password().map(String::from);

            log::info!("Connecting publisher to broker for exchange declaration: {}", broker.url);
            let mut moth = MothFactory::new(&broker, false).await?;

            for exchange in &p_cfg.exchange {
                log::info!("Declaring publisher exchange: {}", exchange);
                moth.declare_exchange(exchange, "topic").await?;
            }
            self.declaration_moths.push(Arc::new(Mutex::new(moth)));
        }

        Ok(())
    }

    async fn connect_queues(&mut self) -> anyhow::Result<()> {
        let subscriptions_count = self.base.config.subscriptions.len();
        for idx in 0..subscriptions_count {
            self.connect_subscription_full(idx, true, false).await?;
        }
        Ok(())
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        log::info!("Cleaning up broker resources for {}", self.base.config.configname.as_deref().unwrap_or("unknown"));
        
        let mut deleted_queues = std::collections::HashSet::new();
        let mut last_log_time = std::time::Instant::now() - std::time::Duration::from_secs(11);

        // Group queues by broker for standalone cleanup
        let mut broker_map = std::collections::HashMap::new();
        for sub in &self.base.config.subscriptions {
            if let Some(cred) = &sub.broker {
                broker_map.entry(cred.url.to_string()).or_insert_with(Vec::new).push(sub.queue.name.clone());
            }
        }

        // 1. Use established moths if available
        for consumer_mutex in &self.consumers {
            let mut consumer = consumer_mutex.lock().await;
            for sub in &self.base.config.subscriptions {
                if !deleted_queues.contains(&sub.queue.name) {
                    if last_log_time.elapsed().as_secs() >= 10 {
                        log::info!("Deleting queue: {}", sub.queue.name);
                        last_log_time = std::time::Instant::now();
                    }
                    if let Ok(_) = consumer.moth.delete_queue(&sub.queue.name).await {
                        deleted_queues.insert(sub.queue.name.clone());
                    }
                }
            }
        }

        // 2. Try declaration moths
        for moth_mutex in &self.declaration_moths {
            let mut moth = moth_mutex.lock().await;
            for sub in &self.base.config.subscriptions {
                if !deleted_queues.contains(&sub.queue.name) {
                    if last_log_time.elapsed().as_secs() >= 10 {
                        log::info!("Deleting queue: {}", sub.queue.name);
                        last_log_time = std::time::Instant::now();
                    }
                    if let Ok(_) = moth.delete_queue(&sub.queue.name).await {
                        deleted_queues.insert(sub.queue.name.clone());
                    }
                }
            }
        }

        // 3. Standalone cleanup connections
        for (broker_url, queues) in broker_map {
            let broker = crate::broker::Broker::parse(&broker_url)?;
            log::info!("Connecting to broker for standalone queue deletion: {}", broker_url);
            if let Ok(mut moth) = MothFactory::new(&broker, false).await {
                for q_name in queues {
                    if !deleted_queues.contains(&q_name) {
                        if last_log_time.elapsed().as_secs() >= 10 {
                            log::info!("Deleting queue: {}", q_name);
                            last_log_time = std::time::Instant::now();
                        }
                        match moth.delete_queue(&q_name).await {
                            Ok(_) => {
                                deleted_queues.insert(q_name);
                            },
                            Err(e) => log::warn!("Failed to delete queue {}: {}", q_name, e),
                        }
                    }
                }
                let _ = moth.close().await;
            }
        }

        for cb_mutex in self.callbacks() {
            let mut cb = cb_mutex.lock().await;
            if let Err(e) = cb.on_cleanup().await {
                log::warn!("Cleanup failed for plugin {}: {}", cb.name(), e);
            }
        }

        Ok(())
    }

    async fn gather(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        // Gathering from consumers if we are an amqp sender
        let batch_size = self.config().batch as usize;
        let mut total_gathered = 0;

        for consumer_mutex in &self.consumers {
            let mut consumer = consumer_mutex.lock().await;
            
            let mut count = 0;
            while total_gathered < batch_size && count < (batch_size / self.consumers.len()).max(1) {
                match tokio::time::timeout(tokio::time::Duration::from_millis(500), consumer.moth.consume()).await {
                    Ok(Ok(Some(mut msg))) => {
                        msg.fields.insert("_consumer_idx".to_string(), consumer.subscription_idx.to_string());
                        worklist.incoming.push(msg);
                        count += 1;
                        total_gathered += 1;
                    }
                    Ok(Ok(None)) => break,
                    Ok(Err(e)) => {
                        log::error!("GATHER: error from {}: {}", consumer.broker_url, e);
                        break;
                    }
                    Err(_) => break, // Timeout
                }
            }
        }
        if total_gathered > 0 {
            worklist.gathered_from_mq += total_gathered;
        }
        Ok(())
    }

    async fn accept(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.accept(worklist).await
    }

    async fn work(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let config = self.config();
        
        // Sender only sends if download is True (which is default for sender)
        if !config.download {
            for m in worklist.incoming.drain(..) {
                worklist.ok.push(m);
            }
            return Ok(());
        }

        let send_to = if let Some(st) = &config.send_to {
            st
        } else {
            anyhow::bail!("sendTo must be set for sender component");
        };

        let scheme = match url::Url::parse(send_to) {
            Ok(u) => u.scheme().to_string(),
            Err(_) => {
                anyhow::bail!("invalid sendTo URL: {}", send_to);
            }
        };

        if let Some(transfer) = get_transfer(&scheme, config) {
            for mut m in worklist.incoming.drain(..) {
                // Determine local file to send
                // msg['relPath'] or similar
                let local_file = if !config.directory.as_os_str().is_empty() {
                    let dir_str = config.directory.to_string_lossy().into_owned();
                    // Treat config.directory as a base prefix that handles variable expansion properly
                    // e.g. "directory /" + "home/peter/..." -> "/home/peter/..."
                    // "directory ." + "home/peter/..." -> "./home/peter/..."
                    
                    let rel_path = m.rel_path.trim_start_matches('/');
                    
                    if dir_str == "." {
                        // If directory is ".", just use rel_path but prepend "./" 
                        // Wait, if it's ".", and we append "home/...", we get "./home/..." which fails 
                        // if the file is ACTUALLY at "/home/...".
                        // In Sarracenia, if `baseDir` is not set, it defaults to `/` + relPath.
                        std::path::Path::new("/").join(rel_path)
                    } else {
                        config.directory.join(rel_path)
                    }
                } else {
                    std::path::Path::new("/").join(m.rel_path.trim_start_matches('/'))
                };

                if !local_file.exists() {
                     log::error!("WORK: local file does not exist: {}", local_file.display());
                     worklist.failed.push(m);
                     continue;
                }

                // Remote location: new_dir + new_file
                let remote_dir = m.fields.get("new_dir").cloned().unwrap_or_else(|| ".".to_string());
                let remote_file_name = m.fields.get("new_file").cloned().unwrap_or_else(|| m.rel_path.clone());
                
                let remote_full_path = if remote_dir == "." || remote_dir.is_empty() {
                    remote_file_name
                } else {
                    format!("{}/{}", remote_dir.trim_end_matches('/'), remote_file_name.trim_start_matches('/'))
                };

                match transfer.put(&m, &local_file, &remote_full_path).await {
                    Ok(size) => {
                        log::info!("WORK: sent {} to {} ({} bytes)", local_file.display(), remote_full_path, size);
                        m.fields.insert("size".to_string(), size.to_string());
                        
                        // Update base_url for the outgoing message
                        if let Some(post_url) = &config.post_base_url {
                            m.base_url = post_url.clone();
                        } else if let Some(send_to) = &config.send_to {
                            m.base_url = send_to.clone();
                        }
                        
                        worklist.ok.push(m);
                    }
                    Err(e) => {
                        log::error!("WORK: send failed for {}: {}", local_file.display(), e);
                        worklist.failed.push(m);
                    }
                }
            }
        } else {
            anyhow::bail!("unsupported protocol for sender: {}", scheme);
        }
        Ok(())
    }

    async fn ack(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        // Reuse SubscribeFlow ack logic
        for consumer_mutex in &self.consumers {
            let mut consumer = consumer_mutex.lock().await;
            let idx_str = consumer.subscription_idx.to_string();

            for m in &worklist.ok {
                if m.fields.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(ack_id) = &m.ack_id {
                        let _ = consumer.moth.ack(ack_id).await;
                    }
                }
            }
            
            for m in &worklist.rejected {
                if m.fields.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(ack_id) = &m.ack_id {
                        let _ = consumer.moth.ack(ack_id).await;
                    }
                }
            }

            for m in &worklist.failed {
                if m.fields.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(ack_id) = &m.ack_id {
                        let _ = consumer.moth.nack(ack_id).await;
                    }
                }
            }
        }
        worklist.clear();
        Ok(())
    }

    async fn housekeeping(&self) -> anyhow::Result<()> {
        self.base.housekeeping().await
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        for consumer_mutex in &self.consumers {
            let mut consumer = consumer_mutex.lock().await;
            let _ = consumer.moth.close().await;
        }
        for pub_mutex in &self.publishers {
            let mut p = pub_mutex.lock().await;
            let _ = p.moth.close().await;
        }
        for moth_mutex in &self.declaration_moths {
            let mut moth = moth_mutex.lock().await;
            let _ = moth.close().await;
        }
        Ok(())
    }
}
