use crate::flow::{Flow, Worklist, BaseFlow};
use crate::Config;
use crate::message::Message;
use async_trait::async_trait;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties, Channel, Consumer, BasicProperties};
use std::sync::Arc;
use std::collections::HashSet;
use tokio::sync::Mutex;
use futures_util::StreamExt;

pub struct AmqpConsumer {
    pub channel: Channel,
    pub consumer: Consumer,
    pub broker_url: String,
    pub subscription_idx: usize,
}

pub struct AmqpPublisher {
    pub channel: Channel,
    pub broker_url: String,
    pub exchanges: Vec<String>,
}

impl AmqpPublisher {
    pub async fn publish(&self, msg: &Message) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(msg)?;
        
        for exchange in &self.exchanges {
            // Sarracenia v3 uses '.' as separator for AMQP
            let topic = msg.rel_path.replace('/', "."); 
            
            self.channel.basic_publish(
                exchange,
                &topic,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default().with_content_type("application/json".into()),
            ).await?;
        }
        Ok(())
    }
}

pub struct SubscribeFlow {
    pub base: BaseFlow,
    pub consumers: Vec<Arc<Mutex<AmqpConsumer>>>,
    pub publishers: Vec<Arc<Mutex<AmqpPublisher>>>,
    pub declaration_channels: Vec<Channel>,
}

impl SubscribeFlow {
    pub fn new(config: Config) -> Self {
        Self {
            base: BaseFlow::new(config),
            consumers: Vec::new(),
            publishers: Vec::new(),
            declaration_channels: Vec::new(),
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        self.connect_full(true, true).await
    }

    pub async fn connect_full(&mut self, declare: bool, consume: bool) -> anyhow::Result<()> {
        let subscriptions_count = self.base.config.subscriptions.len();
        if subscriptions_count > 0 {
            for idx in 0..subscriptions_count {
                self.connect_subscription_full(idx, declare, consume).await?;
            }
        } else if let Some(broker_cfg) = &self.base.config.broker {
            if declare {
                // If we have a broker but no subscriptions, at least ensure the exchange is declared
                let addr = broker_cfg.to_lapin_uri();
                let mut props = ConnectionProperties::default();
                let conn_name = format!("sr3rs-decl-{}-{}", self.base.config.component, self.base.config.configname.as_deref().unwrap_or("unknown"));
                props.client_properties.insert("connection_name".into(), lapin::types::AMQPValue::LongString(conn_name.into()));

                log::info!("Connecting to broker for declaration: {}", addr);
                let conn = Connection::connect(&addr, props).await?;
                let channel = conn.create_channel().await?;
                let exchange = self.base.config.exchange.clone();
                
                log::info!("Declaring primary exchange: {}", exchange);
                channel.exchange_declare(
                    &exchange,
                    lapin::ExchangeKind::Topic,
                    ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                ).await?;
                self.declaration_channels.push(channel);
            }
        }

        let publishers_config = self.base.config.publishers.clone();
        if publishers_config.is_empty() {
             if let Some(broker_cfg) = &self.base.config.post_broker {
                if declare {
                    let addr = broker_cfg.to_lapin_uri();
                    let mut props = ConnectionProperties::default();
                    let conn_name = format!("sr3rs-post-decl-{}-{}", self.base.config.component, self.base.config.configname.as_deref().unwrap_or("unknown"));
                    props.client_properties.insert("connection_name".into(), lapin::types::AMQPValue::LongString(conn_name.into()));

                    log::info!("Connecting to post_broker for declaration: {}", addr);
                    let conn = Connection::connect(&addr, props).await?;
                    let channel = conn.create_channel().await?;
                    let exchange = self.base.config.post_exchange.clone().unwrap_or_else(|| "xpublic".to_string());
                    
                    log::info!("Declaring post exchange: {}", exchange);
                    channel.exchange_declare(
                        &exchange,
                        lapin::ExchangeKind::Topic,
                        ExchangeDeclareOptions {
                            durable: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    ).await?;
                    self.declaration_channels.push(channel);
                }
             }
        }

        for p_cfg in publishers_config {
            let cred = p_cfg.broker.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Publisher missing broker credentials"))?;
            
            let mut broker = crate::broker::Broker::parse(&cred.url.to_string())?;
            broker.user = Some(cred.url.username().to_string());
            broker.password = cred.url.password().map(String::from);

            let addr = broker.to_lapin_uri();
            let mut props = ConnectionProperties::default();
            let conn_name = format!("sr3rs-pub-{}-{}", self.base.config.component, self.base.config.configname.as_deref().unwrap_or("unknown"));
            props.client_properties.insert("connection_name".into(), lapin::types::AMQPValue::LongString(conn_name.into()));

            log::info!("Connecting publisher to broker: {}", addr);
            let conn = Connection::connect(&addr, props).await?;
            let channel = conn.create_channel().await?;

            if declare {
                for exchange in &p_cfg.exchange {
                    channel.exchange_declare(
                        exchange,
                        lapin::ExchangeKind::Topic,
                        ExchangeDeclareOptions {
                            durable: p_cfg.durable,
                            auto_delete: p_cfg.auto_delete,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    ).await?;
                }
            }

            self.publishers.push(Arc::new(Mutex::new(AmqpPublisher {
                channel,
                broker_url: cred.url.to_string(),
                exchanges: p_cfg.exchange.clone(),
            })));
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

        let addr = broker.to_lapin_uri();
        
        let mut props = ConnectionProperties::default();
        let conn_name = format!("sr3rs-{}-{}", self.base.config.component, self.base.config.configname.as_deref().unwrap_or("unknown"));
        props.client_properties.insert("connection_name".into(), lapin::types::AMQPValue::LongString(conn_name.into()));

        log::info!("Connecting to broker: {}", addr);
        
        let mut retry_count = 0;
        let max_retries = 10;
        let conn = loop {
            match Connection::connect(&addr, props.clone()).await {
                Ok(c) => break c,
                Err(e) => {
                    retry_count += 1;
                    if retry_count > max_retries {
                        anyhow::bail!("Failed to connect to broker {} after {} attempts: {}", addr, max_retries, e);
                    }
                    let delay = std::cmp::min(2u64.pow(retry_count), 30);
                    log::warn!("Connection failed: {}. Retrying in {} seconds...", e, delay);
                    tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
                }
            }
        };

        let channel = conn.create_channel().await?;

        if declare {
            log::debug!("Declaring queue: {}", sub.queue.name);
            channel.queue_declare(
                &sub.queue.name,
                QueueDeclareOptions {
                    durable: sub.queue.durable,
                    auto_delete: sub.queue.auto_delete,
                    ..Default::default()
                },
                FieldTable::default(),
            ).await?;

            for binding in &sub.bindings {
                let exchange = binding.exchange.as_deref().unwrap_or("xpublic");
                log::info!("Binding queue {} to exchange {} with topic {}", sub.queue.name, exchange, binding.topic);
                channel.queue_bind(
                    &sub.queue.name,
                    exchange,
                    &binding.topic,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                ).await?;
            }
        }

        if consume {
            log::info!("Starting consumer on queue: {}", sub.queue.name);
            let consumer = channel.basic_consume(
                &sub.queue.name,
                "sr3rs_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            ).await?;

            let new_consumer = Arc::new(Mutex::new(AmqpConsumer {
                channel,
                consumer,
                broker_url: cred.url.to_string(),
                subscription_idx: idx,
            }));

            if idx < self.consumers.len() {
                self.consumers[idx] = new_consumer;
            } else {
                self.consumers.push(new_consumer);
            }
        } else {
             // For cleanup, we still need the channel to delete the queue
             // We can use a dummy consumer structure or just store the channel.
             // Let's store it as a consumer with a dummy consumer object if possible?
             // Actually lapin::Consumer is not easily dummy-able.
             // Let's just create a channel-only representation if needed, 
             // but for now, we can just not start the consumer and keep it.
             // Wait, AmqpConsumer HAS a consumer field.
             
             // If we are just here for cleanup, we might want to just store the channel.
             // Let's add the channel to declaration_channels if it's just for cleanup.
             self.declaration_channels.push(channel);
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

    fn publishers(&self) -> Vec<Arc<Mutex<AmqpPublisher>>> {
        self.publishers.clone()
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        log::info!("Cleaning up broker resources for {}", self.base.config.configname.as_deref().unwrap_or("unknown"));
        
        let mut deleted_queues = HashSet::new();
        let mut last_log_time = std::time::Instant::now() - std::time::Duration::from_secs(11);

        // If we have already established channels (e.g. via connect_full), use them.
        if !self.declaration_channels.is_empty() || !self.consumers.is_empty() {
            // Collect all unique queues to delete
            let queues_to_delete: Vec<String> = self.base.config.subscriptions.iter()
                .map(|s| s.queue.name.clone())
                .collect();

            // Use the first available channel to delete all queues. 
            // (Assuming they are on the same broker if we only have one set of channels, 
            // or we just try each channel for each queue, which is safe)
            for channel in &self.declaration_channels {
                for q_name in &queues_to_delete {
                    if !deleted_queues.contains(q_name) {
                        if last_log_time.elapsed().as_secs() >= 10 {
                            log::info!("Deleting queue: {}", q_name);
                            last_log_time = std::time::Instant::now();
                        }
                        if let Ok(_) = channel.queue_delete(q_name, QueueDeleteOptions::default()).await {
                            deleted_queues.insert(q_name.clone());
                        }
                    }
                }
            }
            for consumer_mutex in &self.consumers {
                let amqp = consumer_mutex.lock().await;
                for q_name in &queues_to_delete {
                    if !deleted_queues.contains(q_name) {
                        if last_log_time.elapsed().as_secs() >= 10 {
                            log::info!("Deleting queue: {}", q_name);
                            last_log_time = std::time::Instant::now();
                        }
                        if let Ok(_) = amqp.channel.queue_delete(q_name, QueueDeleteOptions::default()).await {
                            deleted_queues.insert(q_name.clone());
                        }
                    }
                }
            }
        } else {
            // Standalone cleanup connection: group by broker to avoid reconnecting many times
            let mut broker_map = std::collections::HashMap::new();
            for sub in &self.base.config.subscriptions {
                if let Some(cred) = &sub.broker {
                    broker_map.entry(cred.url.to_string()).or_insert_with(Vec::new).push(sub.queue.name.clone());
                }
            }

            for (broker_url, queues) in broker_map {
                let broker = crate::broker::Broker::parse(&broker_url)?;
                // We need to re-extract user/password if they were in the original cred but not the url-string
                // For now, assume url-string has everything needed or we lookup in credentials.
                // Re-parsing to get a clean URI.
                let addr = broker.to_lapin_uri();
                
                let mut props = ConnectionProperties::default();
                let conn_name = format!("sr3rs-cleanup-{}-{}", self.base.config.component, self.base.config.configname.as_deref().unwrap_or("unknown"));
                props.client_properties.insert("connection_name".into(), lapin::types::AMQPValue::LongString(conn_name.into()));

                log::info!("Connecting to broker for queue deletion: {}", broker_url);
                if let Ok(conn) = Connection::connect(&addr, props).await {
                    if let Ok(channel) = conn.create_channel().await {
                        for q_name in queues {
                            if !deleted_queues.contains(&q_name) {
                                if last_log_time.elapsed().as_secs() >= 10 {
                                    log::info!("Deleting queue: {}", q_name);
                                    last_log_time = std::time::Instant::now();
                                }
                                match channel.queue_delete(&q_name, QueueDeleteOptions::default()).await {
                                    Ok(_) => {
                                        deleted_queues.insert(q_name);
                                    },
                                    Err(e) => log::warn!("Failed to delete queue {}: {}", q_name, e),
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn gather(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let batch_size = self.config().batch as usize;
        let mut total_gathered = 0;

        for consumer_mutex in &self.consumers {
            let mut amqp = consumer_mutex.lock().await;
            
            if !amqp.channel.status().connected() {
                log::warn!("Channel for {} is disconnected. Reconnect should be triggered.", amqp.broker_url);
                continue;
            }

            let mut count = 0;
            while total_gathered < batch_size && count < (batch_size / self.consumers.len()).max(1) {
                let wait_time = if total_gathered == 0 { 500 } else { 50 };
                
                match tokio::time::timeout(tokio::time::Duration::from_millis(wait_time), amqp.consumer.next()).await {
                    Ok(Some(delivery)) => {
                        match delivery {
                            Ok(delivery) => {
                                let payload = String::from_utf8_lossy(&delivery.data);
                                log::debug!("GATHER: received raw payload from {}: {}", amqp.broker_url, payload);
                                
                                let mut parsed_msg = None;
                                if payload.starts_with('{') {
                                    match serde_json::from_str::<Message>(&payload) {
                                        Ok(msg) => parsed_msg = Some(msg),
                                        Err(e) => log::error!("GATHER: failed to parse v03 JSON message: {}. Payload: {}", e, payload),
                                    }
                                } else {
                                    let parts: Vec<&str> = payload.split_whitespace().collect();
                                    if parts.len() >= 3 {
                                        let pub_time = Message::parse_v02_time(parts[0]).unwrap_or_else(chrono::Utc::now);
                                        let base_url = parts[1].replace("%20", " ").replace("%23", "#");
                                        let rel_path = parts[2].to_string();
                                        let mut msg = Message::new(&base_url, &rel_path);
                                        msg.pub_time = pub_time;
                                        parsed_msg = Some(msg);
                                    } else {
                                        log::error!("GATHER: unknown message format from {}: {}", amqp.broker_url, payload);
                                    }
                                }

                                if let Some(mut msg) = parsed_msg {
                                    msg.ack_id = Some(delivery.delivery_tag);
                                    msg.fields.insert("_consumer_idx".to_string(), amqp.subscription_idx.to_string());
                                    worklist.incoming.push(msg);
                                }
                                count += 1;
                                total_gathered += 1;
                            }
                            Err(e) => {
                                log::error!("GATHER: delivery error from {}: {}", amqp.broker_url, e);
                                break;
                            }
                        }
                    }
                    _ => break,
                }
            }
        }

        if total_gathered > 0 {
            log::info!("GATHER: received {} messages.", total_gathered);
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
            let amqp = consumer_mutex.lock().await;
            if !amqp.channel.status().connected() { continue; }
            
            let idx_str = amqp.subscription_idx.to_string();

            for m in &worklist.ok {
                if m.fields.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(tag) = m.ack_id {
                        let _ = amqp.channel.basic_ack(tag, BasicAckOptions::default()).await;
                    }
                }
            }
            
            for m in &worklist.rejected {
                if m.fields.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(tag) = m.ack_id {
                        let _ = amqp.channel.basic_ack(tag, BasicAckOptions::default()).await;
                    }
                }
            }

            for m in &worklist.failed {
                if m.fields.get("_consumer_idx") == Some(&idx_str) {
                    if let Some(tag) = m.ack_id {
                        let _ = amqp.channel.basic_nack(tag, BasicNackOptions { requeue: true, ..Default::default() }).await;
                    }
                }
            }
        }

        worklist.clear();
        Ok(())
    }

    async fn housekeeping(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.housekeeping(worklist).await
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        log::info!("Shutting down SubscribeFlow: closing {} consumers and {} publishers.", self.consumers.len(), self.publishers.len());
        for consumer_mutex in &self.consumers {
            let amqp = consumer_mutex.lock().await;
            if amqp.channel.status().connected() {
                let _ = amqp.channel.close(200, "Normal shutdown").await;
            }
        }
        for pub_mutex in &self.publishers {
            let amqp = pub_mutex.lock().await;
            if amqp.channel.status().connected() {
                let _ = amqp.channel.close(200, "Normal shutdown").await;
            }
        }
        for channel in &self.declaration_channels {
            if channel.status().connected() {
                let _ = channel.close(200, "Normal shutdown").await;
            }
        }
        Ok(())
    }

    async fn declare(&self) -> anyhow::Result<()> {
        // connect() already does queue and exchange declarations.
        // We just need to ensure it's called.
        // Note: we might want to make connect() more granular if we want 
        // to declare without starting consumers, but for now this matches SR3 basics.
        log::info!("Declaring exchanges and queues for {}", self.base.config.configname.as_deref().unwrap_or("unknown"));
        Ok(())
    }
}
