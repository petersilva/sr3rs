use async_trait::async_trait;
use crate::moth::Moth;
use crate::broker::Broker;
use crate::message::Message;
use anyhow::Result;
use lapin::{
    options::*, types::FieldTable, Connection, ConnectionProperties, Channel, Consumer, 
    ExchangeKind, BasicProperties
};
use futures_util::stream::StreamExt;

pub struct Amqp091 {
    connection: Connection,
    channel: Channel,
    consumer: Option<Consumer>,
}

impl Amqp091 {
    pub async fn new(broker: &Broker, _is_subscriber: bool) -> Result<Self> {
        let addr = broker.to_lapin_uri();
        let mut props = ConnectionProperties::default();
        
        let conn_name = format!("sr3rs-moth-{}", broker.user.as_deref().unwrap_or("unknown"));
        props.client_properties.insert(
            "connection_name".into(), 
            lapin::types::AMQPValue::LongString(conn_name.into())
        );

        log::debug!("MOTH: AMQP 0.9.1 connecting to {}", addr);
        let connection = Connection::connect(&addr, props).await?;
        let channel = connection.create_channel().await?;

        Ok(Self {
            connection,
            channel,
            consumer: None,
        })
    }
}

#[async_trait]
impl Moth for Amqp091 {
    async fn subscribe(&mut self, topics: &[String], exchange: &str, queue_name: &str) -> Result<()> {
        log::debug!("MOTH: AMQP 0.9.1 subscribing to topics {:?} on exchange {} with queue {}", topics, exchange, queue_name);
        
        self.declare_exchange(exchange, "topic").await?;

        self.channel.queue_declare(
            queue_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;

        for topic in topics {
            self.channel.queue_bind(
                queue_name,
                exchange,
                topic,
                QueueBindOptions::default(),
                FieldTable::default(),
            ).await?;
        }

        let consumer = self.channel.basic_consume(
            queue_name,
            "sr3rs-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ).await?;

        self.consumer = Some(consumer);
        Ok(())
    }

    async fn consume(&mut self) -> Result<Option<Message>> {
        let consumer = self.consumer.as_mut().ok_or_else(|| anyhow::anyhow!("Not subscribed"))?;
        
        match consumer.next().await {
            Some(Ok(delivery)) => {
                let payload = String::from_utf8_lossy(&delivery.data);
                let mut msg: Message = if payload.starts_with('{') {
                    serde_json::from_str(&payload)?
                } else {
                    // Fallback for v02 plain text
                    let parts: Vec<&str> = payload.split_whitespace().collect();
                    if parts.len() >= 3 {
                        let pub_time = Message::parse_v02_time(parts[0]).unwrap_or_else(chrono::Utc::now);
                        let base_url = parts[1].replace("%20", " ").replace("%23", "#");
                        let rel_path = parts[2].to_string();
                        let mut m = Message::new(&base_url, &rel_path);
                        m.pub_time = pub_time;
                        m
                    } else {
                        anyhow::bail!("Unknown message format: {}", payload);
                    }
                };
                
                msg.ack_id = Some(delivery.delivery_tag.to_string());
                Ok(Some(msg))
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => Ok(None),
        }
    }

    async fn ack(&mut self, ack_id: &str) -> Result<()> {
        let delivery_tag = ack_id.parse::<u64>()?;
        self.channel.basic_ack(delivery_tag, BasicAckOptions::default()).await?;
        Ok(())
    }

    async fn nack(&mut self, ack_id: &str) -> Result<()> {
        let delivery_tag = ack_id.parse::<u64>()?;
        self.channel.basic_nack(delivery_tag, BasicNackOptions { requeue: true, ..Default::default() }).await?;
        Ok(())
    }

    async fn publish(&mut self, exchange: &str, topic: &str, msg: &Message) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        self.channel.basic_publish(
            exchange,
            topic,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default()
                .with_delivery_mode(2) // Persistent
                .with_content_type("application/json".into()),
        ).await?;
        Ok(())
    }

    async fn declare_exchange(&mut self, exchange: &str, kind: &str) -> Result<()> {
        let amqp_kind = match kind {
            "topic" => ExchangeKind::Topic,
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            _ => ExchangeKind::Topic,
        };

        self.channel.exchange_declare(
            exchange,
            amqp_kind,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        ).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.connection.close(0, "Moth closed").await?;
        Ok(())
    }
}
