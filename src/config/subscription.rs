use serde::{Serialize, Deserialize};
use crate::config::credentials::Credential;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub exchange: Option<String>,
    pub topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    pub template: String,
    pub auto_delete: bool,
    pub durable: bool,
    pub prefetch: u32,
    pub expire: Option<u32>,
    pub queue_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub broker: Option<Credential>,
    pub bindings: Vec<Binding>,
    pub queue: QueueConfig,
}

impl Subscription {
    pub fn new(
        broker: Option<Credential>,
        queue_name: String,
        queue_template: String,
        exchange: Option<String>,
        topic: String,
    ) -> Self {
        Self {
            broker: broker.clone(),
            bindings: vec![Binding { exchange, topic }],
            queue: QueueConfig {
                name: queue_name,
                template: queue_template,
                auto_delete: false,
                durable: true,
                prefetch: 10,
                expire: None,
                queue_type: None,
            },
        }
    }
}
