//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use serde::{Serialize, Deserialize};
use crate::config::credentials::Credential;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Binding {
    pub exchange: Option<String>,
    pub topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct QueueConfig {
    pub name: String,
    pub template: String,
    pub auto_delete: bool,
    pub durable: bool,
    pub prefetch: u32,
    pub expire: Option<f64>,
    pub queue_type: Option<String>,
    pub bind: bool,
    pub cleanup_needed: Option<bool>,
    pub declare: bool,
    pub mismatch: Vec<String>,
    pub tls_rigour: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub base_dir: Option<String>,
    pub bindings: Vec<Binding>,
    pub bindings_to_remove: Vec<Binding>,
    #[serde(with = "broker_serde")]
    pub broker: Option<Credential>,
    pub queue: QueueConfig,
}

mod broker_serde {
    use crate::config::credentials::Credential;
    use serde::{self, Deserialize, Serializer, Deserializer};
    use url::Url;

    pub fn serialize<S>(cred: &Option<Credential>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match cred {
            Some(c) => {
                let mut u = c.url.clone();
                let _ = u.set_password(None);
                serializer.serialize_str(u.as_str())
            },
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Credential>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = Option::<String>::deserialize(deserializer)?;
        match s {
            Some(url_str) => {
                let url = Url::parse(&url_str).map_err(serde::de::Error::custom)?;
                Ok(Some(Credential::new(url)))
            }
            None => Ok(None),
        }
    }
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
            base_dir: None,
            broker,
            bindings: vec![Binding { exchange, topic }],
            bindings_to_remove: Vec::new(),
            queue: QueueConfig {
                name: queue_name,
                template: queue_template,
                auto_delete: false,
                durable: true,
                prefetch: 10,
                expire: None,
                queue_type: None,
                bind: true,
                cleanup_needed: None,
                declare: true,
                mismatch: Vec::new(),
                tls_rigour: "normal".to_string(),
            },
        }
    }
}
