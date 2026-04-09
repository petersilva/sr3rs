//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FlowMetrics {
    pub start_time: DateTime<Utc>,
    pub last_housekeeping: DateTime<Utc>,
    pub msg_count_in: u64,
    pub msg_count_out: u64,
    pub msg_count_rejected: u64,
    pub msg_count_failed: u64,
    pub byte_count_in: u64,
    pub byte_count_out: u64,
    pub cpu_time_user: f64,
    pub cpu_time_system: f64,
}

impl Default for FlowMetrics {
    fn default() -> Self {
        Self {
            start_time: Utc::now(),
            last_housekeeping: Utc::now(),
            msg_count_in: 0,
            msg_count_out: 0,
            msg_count_rejected: 0,
            msg_count_failed: 0,
            byte_count_in: 0,
            byte_count_out: 0,
            cpu_time_user: 0.0,
            cpu_time_system: 0.0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Metrics {
    pub flow: FlowMetrics,
    pub plugins: HashMap<String, HashMap<String, String>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset_counts(&mut self) {
        self.flow.msg_count_in = 0;
        self.flow.msg_count_out = 0;
        self.flow.msg_count_rejected = 0;
        self.flow.msg_count_failed = 0;
        self.flow.byte_count_in = 0;
        self.flow.byte_count_out = 0;
    }
}
