//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use async_trait::async_trait;
use crate::moth::Moth;
use crate::message::Message;
use anyhow::Result;

pub struct Mqtt;

#[async_trait]
impl Moth for Mqtt {
    async fn subscribe(&mut self, _topics: &[String], _exchange: &str, _queue_name: &str) -> Result<()> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn start_consume(&mut self) -> Result<()> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn consume(&mut self) -> Result<Option<Message>> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn ack(&mut self, _ack_id: &str) -> Result<()> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn nack(&mut self, _ack_id: &str) -> Result<()> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn publish(&mut self, _exchange: &str, _topic: &str, _msg: &Message) -> Result<()> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn declare_exchange(&mut self, _exchange: &str, _kind: &str) -> Result<()> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn delete_queue(&mut self, _queue_name: &str) -> Result<()> {
        Err(anyhow::anyhow!("MQTT not implemented"))
    }
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
