//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::flowcb::FlowCB;
use crate::flow::Worklist;
use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use chrono::{DateTime, Utc, Timelike, Duration};
use std::collections::HashMap;

pub struct ScheduledPlugin {
    pub name: String,
    pub config: Config,
    pub appointments: Vec<DateTime<Utc>>,
    pub next_gather_time: DateTime<Utc>,
    pub last_gather_time: Option<DateTime<Utc>>,
    pub hours: Vec<u32>,
    pub minutes: Vec<u32>,
    pub sched_times: Vec<(u32, u32)>,
}

impl ScheduledPlugin {
    pub fn new(config: &Config) -> Self {
        let mut hours = Vec::new();
        for h_str in &config.scheduled_hour {
            for part in h_str.split(',') {
                if let Ok(h) = part.trim().parse::<u32>() {
                    hours.push(h);
                }
            }
        }
        if hours.is_empty() {
            hours = (0..24).collect();
        }

        let mut minutes = Vec::new();
        for m_str in &config.scheduled_minute {
            for part in m_str.split(',') {
                if let Ok(m) = part.trim().parse::<u32>() {
                    minutes.push(m);
                }
            }
        }
        if minutes.is_empty() {
            minutes = vec![0];
        }

        let mut sched_times = Vec::new();
        for t_str in &config.scheduled_time {
            for part in t_str.split(',') {
                let parts: Vec<&str> = part.split(':').collect();
                if parts.len() == 2 {
                    if let (Ok(h), Ok(m)) = (parts[0].trim().parse::<u32>(), parts[1].trim().parse::<u32>()) {
                        sched_times.push((h, m));
                    }
                }
            }
        }

        let mut plugin = Self {
            name: "scheduled".to_string(),
            config: config.clone(),
            appointments: Vec::new(),
            next_gather_time: Utc::now(), // Placeholder
            last_gather_time: None,
            hours,
            minutes,
            sched_times,
        };

        let now = Utc::now();
        plugin.update_appointments(now);
        plugin.calc_next_gather_time(now);
        
        plugin
    }

    fn update_appointments(&mut self, when: DateTime<Utc>) {
        self.appointments.clear();
        
        // Hour/Minute combinations
        for &h in &self.hours {
            for &m in &self.minutes {
                if let Some(appointment) = when.with_hour(h).and_then(|t| t.with_minute(m)).and_then(|t| t.with_second(0)).and_then(|t| t.with_nanosecond(0)) {
                    if appointment >= when {
                        self.appointments.push(appointment);
                    }
                }
            }
        }

        // Specific times
        for &(h, m) in &self.sched_times {
            if let Some(appointment) = when.with_hour(h).and_then(|t| t.with_minute(m)).and_then(|t| t.with_second(0)).and_then(|t| t.with_nanosecond(0)) {
                if appointment >= when {
                    if !self.appointments.contains(&appointment) {
                        self.appointments.push(appointment);
                    }
                }
            }
        }

        self.appointments.sort();
        ::log::info!("Scheduled: appointments for {}: {:?}", when, self.appointments);
    }

    fn calc_next_gather_time(&mut self, last_gather: DateTime<Utc>) {
        // If we are at a scheduled time, remove it
        self.appointments.retain(|&t| t > last_gather);

        if self.config.scheduled_interval > 0 {
            self.next_gather_time = last_gather + Duration::seconds(self.config.scheduled_interval as i64);
            ::log::debug!("Scheduled: next gather in {}s, scheduled for {}", self.config.scheduled_interval, self.next_gather_time);
        } else if !self.hours.is_empty() || !self.sched_times.is_empty() {
            if self.appointments.is_empty() {
                // Done for today, move to tomorrow
                let tomorrow = last_gather + Duration::days(1);
                let midnight = tomorrow.with_hour(0).unwrap().with_minute(0).unwrap().with_second(0).unwrap().with_nanosecond(0).unwrap();
                self.update_appointments(midnight);
            }
            
            if let Some(&next) = self.appointments.first() {
                self.next_gather_time = next;
                ::log::debug!("Scheduled: next gather scheduled for {} from appointments", self.next_gather_time);
            } else {
                // Fallback if update_appointments failed to produce anything (should not happen with default hours)
                self.next_gather_time = last_gather + Duration::seconds(300);
            }
        } else {
            self.next_gather_time = last_gather + Duration::seconds(300);
            ::log::debug!("Scheduled: default wait 300s, scheduled for {}", self.next_gather_time);
        }
    }

    fn ready_to_gather(&mut self) -> bool {
        let now = Utc::now();
        if now >= self.next_gather_time {
            let late = (now - self.next_gather_time).num_seconds();
            ::log::info!("Scheduled: yes, now >= {} ({}s late)", self.next_gather_time, late);
            self.calc_next_gather_time(now);
            self.last_gather_time = Some(now);
            true
        } else {
            ::log::debug!("Scheduled: no, next gather scheduled for {}", self.next_gather_time);
            false
        }
    }
}

#[async_trait]
impl FlowCB for ScheduledPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn gather(&mut self, worklist: &mut Worklist) -> anyhow::Result<()> {
        if self.ready_to_gather() {
            for rel_path in &self.config.post_paths {
                match Message::from_file(std::path::Path::new(rel_path), &self.config, std::path::Path::new(rel_path)) {
                    Ok(msg) => worklist.incoming.push(msg),
                    Err(e) => ::log::error!("Scheduled: failed to build message for {}: {}", rel_path.to_str().unwrap(), e),
                }
            }
        }
        Ok(())
    }

    async fn on_housekeeping(&self) -> anyhow::Result<()> {
        ::log::info!("Scheduled: next gather scheduled for {}", self.next_gather_time);
        if !self.appointments.is_empty() {
            ::log::info!("Scheduled: {} appointments remaining for today", self.appointments.len());
        }
        Ok(())
    }

    fn metrics_report(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("next_gather_time".to_string(), self.next_gather_time.to_rfc3339());
        if let Some(last) = self.last_gather_time {
            m.insert("last_gather_time".to_string(), last.to_rfc3339());
        }
        m.insert("appointments_remaining".to_string(), self.appointments.len().to_string());
        m
    }
}
