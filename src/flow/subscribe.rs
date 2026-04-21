//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::{Flow, Worklist, BaseFlow};
use crate::Config;
use crate::message::Message;
use crate::moth::{Moth, MothFactory};
use crate::utils::redact_url;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MothConsumer {
    pub moth: Box<dyn Moth>,
    pub broker_url: String,
    pub subscription_idx: usize,
}

pub struct MothPublisher {
    pub options: serde_json::Value,
    pub moth: Box<dyn Moth>,
    pub broker_url: String,
    pub exchanges: Vec<String>,
}

pub struct SubscribeFlow {
    pub base: BaseFlow,
    pub consumers: Vec<Arc<Mutex<MothConsumer>>>,
    pub publishers: Vec<Arc<Mutex<MothPublisher>>>,
    pub declaration_moths: Vec<Arc<Mutex<Box<dyn Moth>>>>,
}

impl SubscribeFlow {
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
        }
        
        if declare || consume {
            let subscriptions_count = self.base.config.subscriptions.len();
            for idx in 0..subscriptions_count {
                self.connect_subscription_full(idx, declare, consume).await?;
            }
        }

        // Always connect publishers if consuming or explicitly requested
        if consume {
            let publishers_config = self.base.config.publishers.clone();
            for (idx, p_cfg) in publishers_config.into_iter().enumerate() {
                let cred = p_cfg.broker.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Publisher missing broker credentials"))?;
                
                let mut broker = crate::broker::Broker::parse(&cred.url.to_string())?;
                broker.user = Some(cred.url.username().to_string());
                broker.password = cred.url.password().map(String::from);

                log::info!("Connecting publisher to broker: {}", broker.redacted());
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

        log::info!("Connecting to broker: {}", broker.redacted());
        
        let mut moth = MothFactory::new(&broker, true).await?;
        moth.set_consume_options(&sub.queue.name, self.base.config.prefetch as u16, sub.queue.expire);

        if declare {
            let topics: Vec<String> = sub.bindings.iter().map(|b| b.topic.clone()).collect();
            // In SR3, a subscription usually has one exchange in bindings, or we use a default
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
impl Flow for SubscribeFlow {
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

    fn callbacks(&self) -> Vec<Arc<Mutex<dyn crate::flow::flowcb::FlowCB>>> {
        self.base.callbacks.clone()
    }

    async fn connect_exchanges(&mut self) -> anyhow::Result<()> {
        let publishers_config = self.base.config.publishers.clone();
        for p_cfg in publishers_config {
            if !p_cfg.exchange_declare {
                continue;
            }

            let cred = p_cfg.broker.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Publisher missing broker credentials"))?;
            
            let mut broker = crate::broker::Broker::parse(&cred.url.to_string())?;
            broker.user = Some(cred.url.username().to_string());
            broker.password = cred.url.password().map(String::from);

            log::info!("Connecting publisher to broker for exchange declaration: {}", broker.redacted());
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
            log::info!("Connecting to broker for standalone queue deletion: {}", redact_url(&broker_url));
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
        let batch_size = self.config().batch as usize;
        let mut total_gathered = 0;

        for consumer_mutex in &self.consumers {
            let mut consumer = consumer_mutex.lock().await;
            
            let mut count = 0;
            log::debug!("GATHER: starting to gather from consumer idx {}", consumer.subscription_idx);
            while total_gathered < batch_size && count < (batch_size / self.consumers.len()).max(1) {
                // moth.consume() is async
                log::debug!("GATHER: waiting for message from moth.consume()...");
                match tokio::time::timeout(tokio::time::Duration::from_millis(500), consumer.moth.consume()).await {
                    Ok(Ok(Some(mut msg))) => {
                        log::debug!("GATHER: received message");
                        msg.delete_on_post.insert("_consumer_idx".to_string(), consumer.subscription_idx.to_string());
                        worklist.incoming.push(msg);
                        count += 1;
                        total_gathered += 1;
                    }
                    Ok(Ok(None)) => {
                        log::debug!("GATHER: received None");
                        break;
                    }
                    Ok(Err(e)) => {
                        log::error!("GATHER: error from {}: {}", redact_url(&consumer.broker_url), e);
                        break;
                    }
                    Err(_) => {
                        log::debug!("GATHER: timeout after 500ms");
                        break; // Timeout
                    }
                }
            }
        }

        if total_gathered > 0 {
            log::info!("GATHER: received {} messages.", total_gathered);
            worklist.gathered_from_mq += total_gathered;
        }

        Ok(())
    }

    async fn accept(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.accept(worklist).await
    }

    async fn work(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.work(worklist).await
    }

    async fn ack(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        for consumer_mutex in &self.consumers {
            let mut consumer = consumer_mutex.lock().await;
            let idx_str = consumer.subscription_idx.to_string();

            for m in &worklist.ok {
                if m.delete_on_post.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(ack_id) = &m.ack_id {
                        let _ = consumer.moth.ack(ack_id).await;
                    }
                }
            }
            
            for m in &worklist.rejected {
                if m.delete_on_post.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(ack_id) = &m.ack_id {
                        let _ = consumer.moth.ack(ack_id).await;
                    }
                }
            }

            for m in &worklist.failed {
                if m.delete_on_post.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(ack_id) = &m.ack_id {
                        let _ = consumer.moth.nack(ack_id).await;
                    }
                }
            }
        }

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

impl MothPublisher {
    pub async fn publish_mut(&mut self, msg: &Message) -> anyhow::Result<()> {
        for exchange in &self.exchanges {
            self.moth.publish(exchange, "", msg, &self.options).await?;
        }
        Ok(())
    }
}
