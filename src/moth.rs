//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use async_trait::async_trait;
use crate::broker::Broker;
use crate::message::Message;
use anyhow::Result;

pub mod amqp091;
pub mod amqp1;
pub mod mqtt;

#[async_trait]
pub trait Moth: Send + Sync {
    /// Initialize consumer settings
    fn set_consume_options(&mut self, queue_name: &str, prefetch: u16, expire: Option<f64>);

    /// Subscribe to topics (for consumers)
    async fn subscribe(&mut self, topics: &[String], exchange: &str, queue_name: &str) -> Result<()>;
    
    /// Start consuming messages
    async fn start_consume(&mut self) -> Result<()>;

    /// Get next message
    async fn consume(&mut self) -> Result<Option<Message>>;
    
    /// Acknowledge a message
    async fn ack(&mut self, ack_id: &str) -> Result<()>;

    /// Negative-acknowledge a message (requeue)
    async fn nack(&mut self, ack_id: &str) -> Result<()>;
    
    /// Publish a message
    async fn publish(&mut self, exchange: &str, topic: &str, msg: &Message, options: &serde_json::Value) -> Result<()>;

    /// Declare an exchange (optional, implementation-specific)
    async fn declare_exchange(&mut self, exchange: &str, kind: &str) -> Result<()>;

    /// Delete a queue
    async fn delete_queue(&mut self, queue_name: &str) -> Result<()>;

    /// Close connection
    async fn close(&mut self) -> Result<()>;
}

pub struct MothFactory;

impl MothFactory {
    pub async fn new(broker: &Broker, is_subscriber: bool) -> Result<Box<dyn Moth>> {
        let scheme = broker.url.scheme();
        match scheme {
            "amqp" | "amqps" => {
                let m = amqp091::Amqp091::new(broker, is_subscriber).await?;
                Ok(Box::new(m))
            }
            "mqtt" | "mqtts" => {
                // Placeholder for MQTT
                Err(anyhow::anyhow!("MQTT protocol not yet implemented in sr3rs"))
            }
            "amqp1" | "amq1" => {
                // Placeholder for AMQP 1.0
                Err(anyhow::anyhow!("AMQP 1.0 protocol not yet implemented in sr3rs"))
            }
            _ => Err(anyhow::anyhow!("Unsupported protocol scheme: {}", scheme)),
        }
    }
}
