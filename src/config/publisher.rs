//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use crate::config::credentials::Credential;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Publisher {
    pub broker: Option<Credential>,
    pub exchange: Vec<String>,
    pub topic_prefix: Vec<String>,
    pub format: String,
    pub base_dir: Option<PathBuf>,
    pub base_url: Option<String>,
    pub auto_delete: bool,
    pub durable: bool,
    pub exchange_declare: bool,
    pub message_age_max: u32,
    pub persistent: bool,
    pub timeout: u32,
}

impl Publisher {
    pub fn new(
        broker: Option<Credential>,
        exchange: Vec<String>,
        topic_prefix: Vec<String>,
        format: String,
    ) -> Self {
        Self {
            broker,
            exchange,
            topic_prefix,
            format,
            base_dir: None,
            base_url: None,
            auto_delete: false,
            durable: true,
            exchange_declare: true,
            message_age_max: 0,
            persistent: true,
            timeout: 300,
        }
    }
}
