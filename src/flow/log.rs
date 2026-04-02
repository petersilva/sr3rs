use crate::flow::Worklist;
use crate::Config;
use std::time::Instant;
use chrono::Utc;

pub struct FlowLog {
    pub start_time: Instant,
    pub last_housekeeping: Instant,
    pub msg_count: u64,
    pub reject_count: u64,
    pub transfer_count: u64,
    pub file_bytes: u64,
    pub lag_total: f64,
    pub lag_max: f64,
}

impl FlowLog {
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_housekeeping: now,
            msg_count: 0,
            reject_count: 0,
            transfer_count: 0,
            file_bytes: 0,
            lag_total: 0.0,
            lag_max: 0.0,
        }
    }

    pub fn reset(&mut self) {
        self.last_housekeeping = Instant::now();
        self.file_bytes = 0;
        self.lag_total = 0.0;
        self.lag_max = 0.0;
        self.msg_count = 0;
        self.reject_count = 0;
        self.transfer_count = 0;
    }

    pub fn after_accept(&mut self, _config: &Config, worklist: &Worklist) {
        self.reject_count += worklist.rejected.len() as u64;
        self.msg_count += worklist.incoming.len() as u64;
        
        let now = Utc::now();

        for m in &worklist.rejected {
            log::debug!("rejected: {}/{}", m.base_url, m.rel_path);
        }

        for m in &worklist.incoming {
            let lag = (now - m.pub_time).num_milliseconds() as f64 / 1000.0;
            self.lag_total += lag;
            if lag > self.lag_max {
                self.lag_max = lag;
            }
            log::info!("accepted: (lag: {:.2}s) {}/{}", lag, m.base_url, m.rel_path);
        }
    }

    pub fn after_work(&mut self, _config: &Config, worklist: &Worklist) {
        self.transfer_count += worklist.ok.len() as u64;
        for m in &worklist.ok {
            if let Some(size_str) = m.fields.get("size") {
                if let Ok(size) = size_str.parse::<u64>() {
                    self.file_bytes += size;
                }
            }
            log::info!("worked ok: {}/{}", m.base_url, m.rel_path);
        }
    }

    pub fn stats(&self, config: &Config) {
        let tot = self.msg_count + self.reject_count;
        let how_long = self.last_housekeeping.elapsed().as_secs_f64();
        
        let apc = if tot > 0 { 100.0 * self.msg_count as f64 / tot as f64 } else { 0.0 };
        let rate = if how_long > 0.0 { self.msg_count as f64 / how_long } else { 0.0 };

        log::info!("--- Statistics ---");
        log::info!("Component: {}, Config: {:?}", config.component, config.configname);
        log::info!("Messages received: {}, accepted: {}, rejected: {}", tot, self.msg_count, self.reject_count);
        log::info!("Rate accepted: {:.1}% or {:.1} m/s", apc, rate);
        log::info!("Files transferred: {}, bytes: {}", self.transfer_count, self.file_bytes);
        if self.msg_count > 0 {
            log::info!("Lag: average: {:.2}s, maximum: {:.2}s", self.lag_total / self.msg_count as f64, self.lag_max);
        }
        log::info!("------------------");
    }
}
