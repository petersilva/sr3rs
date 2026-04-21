//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

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
    queue_name: Option<String>,
    prefetch: u16,
    expire: Option<f64>,
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

        //println!("MOTH: AMQP 0.9.1 connecting to {}", addr);
        let connection = Connection::connect(&addr, props).await?;
        let channel = connection.create_channel().await?;

        Ok(Self {
            connection,
            channel,
            consumer: None,
            queue_name: None,
            prefetch: 10,
            expire: None,
        })
    }
}

#[async_trait]
impl Moth for Amqp091 {
    fn set_consume_options(&mut self, queue_name: &str, prefetch: u16, expire: Option<f64>) {
        self.queue_name = Some(queue_name.to_string());
        self.prefetch = prefetch;
        self.expire = expire;
    }

    async fn subscribe(&mut self, topics: &[String], exchange: &str, queue_name: &str) -> Result<()> {
        log::info!("MOTH: AMQP 0.9.1 subscribing to topics {:?} on exchange {} with queue {}", topics, exchange, queue_name);
        
        // DO NOT automatically declare exchange here.
        // Subscribers often have bind permissions but not declare permissions for public exchanges.

        let mut args = FieldTable::default();
        if let Some(expire) = self.expire {
            let expire_ms = (expire * 1000.0) as u32;
            if expire_ms > 0 {
                args.insert("x-expires".into(), lapin::types::AMQPValue::LongUInt(expire_ms));
            }
        }

        self.channel.queue_declare(
            queue_name,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            args,
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

        self.queue_name = Some(queue_name.to_string());
        Ok(())
    }

    async fn start_consume(&mut self) -> Result<()> {
        if self.consumer.is_some() {
            return Ok(());
        }

        let queue_name = self.queue_name.as_ref().ok_or_else(|| anyhow::anyhow!("No queue to consume from. Call subscribe first."))?;
        
        self.channel.basic_qos(self.prefetch, BasicQosOptions::default()).await?;
        
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
        if self.consumer.is_none() {
            self.start_consume().await?;
        }
        let consumer = self.consumer.as_mut().unwrap();
        
        match consumer.next().await {
            Some(Ok(delivery)) => {
                let payload = &delivery.data;
                
                // Convert lapin properties/headers to HashMap
                let mut headers = std::collections::HashMap::new();
                if let Some(amqp_headers) = delivery.properties.headers() {
                    for (k, v) in amqp_headers.inner() {
                        let val_str = match v {
                            lapin::types::AMQPValue::LongString(s) => s.to_string(),
                            lapin::types::AMQPValue::ShortString(s) => s.to_string(),
                            _ => format!("{:?}", v),
                        };
                        headers.insert(k.to_string(), val_str);
                    }
                }
                
                let content_type = delivery.properties.content_type().as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "".to_string());
                
                // Construct a fake empty JSON value to represent options right now
                // In a complete implementation, this would be passed from config/options
                let options = serde_json::json!({});
                
                let msg_opt = crate::postformat::import_any(payload, &headers, &content_type, &options);
                
                if let Some(mut msg) = msg_opt {
                    msg.ack_id = Some(delivery.delivery_tag.to_string());
                    // Attach routing key mapping similar to python
                    msg.fields.insert("exchange".to_string(), delivery.exchange.to_string());
                    Ok(Some(msg))
                } else {
                    log::error!("Failed to decode message, acknowledging and dropping.");
                    self.channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default()).await?;
                    Ok(None)
                }
            }
            Some(Err(e)) => Err(anyhow::Error::from(e)),
            None => {
                self.consumer = None;
                Ok(None)
            }
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

    async fn publish(&mut self, exchange: &str, topic: &str, msg: &Message, options: &serde_json::Value) -> Result<()> {
        let version = msg.delete_on_post.get("format").map(|s| s.as_str()).unwrap_or("v03");
        
        if let Some((raw_body, mut headers_map, content_type)) = crate::postformat::export_any(msg, version, options) {
            let derived_topic = headers_map.remove("topic").unwrap_or_else(|| topic.to_string());
            
            let mut headers = FieldTable::default();
            for (k, v) in headers_map {
                headers.insert(k.into(), lapin::types::AMQPValue::LongString(v.into()));
            }
            
            
            let safe_topic = derived_topic.replace("#", "%23").replace("*", "%22");
            
            log::info!("moth/amqp091 publish 1: topic {:?}  exchange: {:?}", safe_topic, exchange );
            log::info!("moth/amqp091 publish 2: headers {:?} ", headers );
            log::info!("moth/amqp091 publish 3: body {:?} ", raw_body );

            // Python's behavior replaces '*' and '#' in the routing_key (topic).
            self.channel.basic_publish(
                exchange,
                &safe_topic,
                BasicPublishOptions::default(),
                raw_body.as_bytes(),
                BasicProperties::default()
                    .with_delivery_mode(2) // Persistent
                    .with_content_type(content_type.into())
                    .with_headers(headers),
            ).await?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Failed to export message using format {}", version))
        }
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

    async fn delete_queue(&mut self, queue_name: &str) -> Result<()> {
        self.channel.queue_delete(queue_name, QueueDeleteOptions::default()).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.connection.close(0, "Moth closed").await?;
        Ok(())
    }
}
