use crate::flow::{Flow, Worklist, BaseFlow};
use crate::Config;
use crate::message::Message;
use async_trait::async_trait;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties, Channel, Consumer};
use std::sync::Arc;
use tokio::sync::Mutex;
use futures_util::StreamExt;

pub struct SubscribeFlow {
    pub base: BaseFlow,
    pub channel: Option<Channel>,
    pub consumer: Option<Arc<Mutex<Consumer>>>,
}

impl SubscribeFlow {
    pub fn new(config: Config) -> Self {
        Self {
            base: BaseFlow::new(config),
            channel: None,
            consumer: None,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let broker = self.base.config.broker.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No broker configured"))?;
        
        let addr = broker.url.to_string();
        let conn = Connection::connect(&addr, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        for sub in &self.base.config.subscriptions {
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
                channel.queue_bind(
                    &sub.queue.name,
                    exchange,
                    &binding.topic,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                ).await?;
            }

            if self.consumer.is_none() {
                let consumer = channel.basic_consume(
                    &sub.queue.name,
                    "sr3rs_consumer",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                ).await?;
                self.consumer = Some(Arc::new(Mutex::new(consumer)));
            }
        }

        self.channel = Some(channel);
        Ok(())
    }
}

#[async_trait]
impl Flow for SubscribeFlow {
    fn config(&self) -> &Config {
        &self.base.config
    }

    async fn gather(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        if let Some(consumer_mutex) = &self.consumer {
            let mut consumer = consumer_mutex.lock().await;
            
            let mut count = 0;
            let batch_size = self.config().batch as usize;
            
            while count < batch_size {
                match tokio::time::timeout(tokio::time::Duration::from_millis(100), consumer.next()).await {
                    Ok(Some(delivery)) => {
                        let delivery = delivery?;
                        let payload = String::from_utf8_lossy(&delivery.data);
                        if let Ok(mut msg) = serde_json::from_str::<Message>(&payload) {
                            msg.ack_id = Some(delivery.delivery_tag);
                            worklist.incoming.push(msg);
                        }
                        count += 1;
                    }
                    _ => break,
                }
            }
        }
        Ok(())
    }

    async fn accept(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.accept(worklist).await
    }

    async fn work(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.work(worklist).await
    }

    async fn post(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.post(worklist).await
    }

    async fn ack(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        if let Some(channel) = &self.channel {
            // ACK successfully processed messages
            for m in &worklist.ok {
                if let Some(tag) = m.ack_id {
                    channel.basic_ack(tag, BasicAckOptions::default()).await?;
                }
            }
            
            // ACK rejected messages (SR3 also acks these to remove from queue)
            for m in &worklist.rejected {
                if let Some(tag) = m.ack_id {
                    channel.basic_ack(tag, BasicAckOptions::default()).await?;
                }
            }

            // NACK failed messages so they return to queue or are retried
            for m in &worklist.failed {
                if let Some(tag) = m.ack_id {
                    channel.basic_nack(tag, BasicNackOptions { requeue: true, ..Default::default() }).await?;
                }
            }
        }

        worklist.clear();
        Ok(())
    }

    async fn housekeeping(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.base.housekeeping(worklist).await
    }
}
