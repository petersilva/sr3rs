//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::FlowCB;
use crate::Config;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use percent_encoding::{utf8_percent_encode, percent_decode_str, NON_ALPHANUMERIC};

struct NoDupeState {
    cache_dict: HashMap<String, HashMap<String, f64>>,
    cache_file: PathBuf,
    fp: Option<BufWriter<File>>,
    count: usize,
    last_time: f64,
    last_count: usize,
    nodupe_ttl: f64,
}

pub struct DiskNoDupePlugin {
    name: String,
    state: Mutex<NoDupeState>,
    file_age_min: f64,
    file_age_max: f64,
}

impl DiskNoDupePlugin {
    pub fn new(config: &Config) -> Self {
        let config_part = config.configname.clone().unwrap_or_else(|| "unknown".to_string());
        
        let cache_file = crate::config::paths::get_user_cache_dir()
            .join(&config.component)
            .join(&config_part)
            .join(format!("recent_files_001.cache"));

        let file_age_min = config.file_age_min;
        let file_age_max = config.file_age_max;
        let nodupe_ttl = config.nodupe_ttl as f64;

        Self {
            name: "nodupe.disk".to_string(),
            file_age_min,
            file_age_max,
            state: Mutex::new(NoDupeState {
                cache_dict: HashMap::new(),
                cache_file,
                fp: None,
                count: 0,
                last_time: Utc::now().timestamp_millis() as f64 / 1000.0,
                last_count: 0,
                nodupe_ttl,
            }),
        }
    }

    fn now_flt() -> f64 {
        Utc::now().timestamp_millis() as f64 / 1000.0
    }

    fn not_in_cache(state: &mut NoDupeState, key: &str, relpath: &str, now: f64) -> bool {
        let qpath = utf8_percent_encode(relpath, NON_ALPHANUMERIC).to_string();

        if !state.cache_dict.contains_key(key) {
            ::log::debug!("adding entry to NoDupe cache");
            let mut kdict = HashMap::new();
            kdict.insert(relpath.to_string(), now);
            state.cache_dict.insert(key.to_string(), kdict);
            
            if let Some(fp) = &mut state.fp {
                let _ = writeln!(fp, "{} {:.6} {}", key, now, qpath);
            }
            state.count += 1;
            return true;
        }

        let kdict = state.cache_dict.get_mut(key).unwrap();
        let present = if let Some(t) = kdict.get(relpath) {
            *t + state.nodupe_ttl >= now
        } else {
            false
        };

        kdict.insert(relpath.to_string(), now);

        if let Some(fp) = &mut state.fp {
            let _ = writeln!(fp, "{} {:.6} {}", key, now, qpath);
        }
        state.count += 1;

        if present {
            ::log::debug!("updated time of old NoDupe entry: relpath={}", relpath);
            false
        } else {
            ::log::debug!("added relpath={}", relpath);
            true
        }
    }

    fn load(state: &mut NoDupeState, now: f64) {
        state.cache_dict.clear();
        state.count = 0;

        if !state.cache_file.exists() {
            if let Some(p) = state.cache_file.parent() {
                let _ = std::fs::create_dir_all(p);
            }
            if let Ok(f) = File::create(&state.cache_file) {
                let _ = f.sync_all();
            }
        }

        if let Ok(file) = File::open(&state.cache_file) {
            let reader = BufReader::new(file);
            for (lineno, line) in reader.lines().enumerate() {
                if let Ok(line) = line {
                    let words: Vec<&str> = line.split_whitespace().collect();
                    if words.len() >= 3 {
                        let key = words[0];
                        if let Ok(ctime) = words[1].parse::<f64>() {
                            let qpath = words[2];
                            let path = percent_decode_str(qpath).decode_utf8_lossy().to_string();

                            let ttl = now - ctime;
                            if ttl > state.nodupe_ttl {
                                continue;
                            }

                            let kdict = state.cache_dict.entry(key.to_string()).or_insert_with(HashMap::new);
                            if !kdict.contains_key(&path) {
                                state.count += 1;
                            }
                            kdict.insert(path, ctime);
                        }
                    }
                } else {
                    ::log::error!("load corrupted: lineno={}, cache_file={}", lineno + 1, state.cache_file.display());
                }
            }
        }

        if let Ok(file) = OpenOptions::new().read(true).write(true).append(true).open(&state.cache_file) {
            state.fp = Some(BufWriter::new(file));
        }
    }

    fn save(state: &mut NoDupeState, now: f64) {
        if let Some(mut fp) = state.fp.take() {
            let _ = fp.flush();
        }
        let _ = std::fs::remove_file(&state.cache_file);

        if let Ok(file) = File::create(&state.cache_file) {
            state.fp = Some(BufWriter::new(file));
            Self::clean(state, now, true);
            if let Some(fp) = &mut state.fp {
                let _ = fp.flush();
            }
        }
    }

    fn clean(state: &mut NoDupeState, now: f64, persist: bool) {
        let mut new_dict: HashMap<String, HashMap<String, f64>> = HashMap::new();
        state.count = 0;

        for (key, kdict) in &state.cache_dict {
            let mut ndict = HashMap::new();
            for (value, t) in kdict {
                let ttl = now - *t;
                if ttl > state.nodupe_ttl {
                    continue;
                }

                let parts: Vec<&str> = value.split('*').collect();
                let path = parts[0];
                let qpath = utf8_percent_encode(path, NON_ALPHANUMERIC).to_string();

                ndict.insert(value.clone(), *t);
                state.count += 1;

                if persist {
                    if let Some(fp) = &mut state.fp {
                        let _ = writeln!(fp, "{} {:.6} {}", key, t, qpath);
                    }
                }
            }
            if !ndict.is_empty() {
                new_dict.insert(key.clone(), ndict);
            }
        }
        state.cache_dict = new_dict;
    }
}

#[async_trait]
impl FlowCB for DiskNoDupePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn on_start(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let now = Self::now_flt();
        Self::load(&mut state, now);
        Ok(())
    }

    async fn on_stop(&mut self) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let now = Self::now_flt();
        Self::save(&mut state, now);
        Ok(())
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let now = Self::now_flt();

        let min_mtime = if self.file_age_max > 0.0 { now - self.file_age_max } else { 0.0 };
        let max_mtime = if self.file_age_min > 0.0 { now - self.file_age_min } else { now + 100.0 };

        let mut new_incoming = Vec::new();

        for mut m in wl.incoming.drain(..) {
            if let Some(mtime_str) = m.fields.get("mtime") {
                if let Ok(mtime) = mtime_str.parse::<f64>() {
                    if mtime < min_mtime {
                        m.fields.insert("_deleteOnPost".to_string(), "reject".to_string());
                        m.fields.insert("reject".to_string(), format!("{} too old (nodupe check)", mtime));
                        wl.rejected.push(m);
                        continue;
                    } else if mtime > max_mtime {
                        m.fields.insert("_deleteOnPost".to_string(), "reject".to_string());
                        m.fields.insert("reject".to_string(), format!("{} too new (nodupe check)", mtime));
                        wl.rejected.push(m);
                        continue;
                    }
                }
            }

            let is_retry = m.fields.contains_key("_isRetry");
            let key = super::derive_key(&m);
            let path = if let Some(p) = m.fields.get("_nodupe_override_path") {
                p.clone()
            } else {
                m.rel_path.trim_start_matches('/').to_string()
            };

            m.fields.insert("_noDupe_key".to_string(), key.clone());
            m.fields.insert("_noDupe_path".to_string(), path.clone());
            let mut dop = m.fields.get("_deleteOnPost").cloned().unwrap_or_default();
            if !dop.is_empty() { dop.push(','); }
            dop.push_str("_noDupe_key,_noDupe_path");
            m.fields.insert("_deleteOnPost".to_string(), dop);

            if is_retry || Self::not_in_cache(&mut state, &key, &path, now) {
                new_incoming.push(m);
            } else {
                m.fields.insert("_deleteOnPost".to_string(), "reject".to_string());
                m.fields.insert("reject".to_string(), "not modified 1 (nodupe check)".to_string());
                wl.rejected.push(m);
            }
        }

        if let Some(fp) = &mut state.fp {
            let _ = fp.flush();
        }

        wl.incoming = new_incoming;
        Ok(())
    }

    async fn on_housekeeping(&self, _wl: &mut Worklist) -> anyhow::Result<()> {
        let mut state = self.state.lock().unwrap();
        let now = Self::now_flt();
        
        let count = state.count;
        Self::save(&mut state, now);
        
        let new_count = state.count;
        if new_count > 0 {
            ::log::info!(
                "was {}, but since {:.2} sec, increased up to {}, now saved {} entries",
                state.last_count, now - state.last_time, count, new_count
            );
        }

        state.last_time = now;
        state.last_count = new_count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::message::Message;
    use crate::flow::Worklist;

    fn make_message() -> Message {
        let mut m = Message::new("https://NotARealURL", "ThisIsAPath/To/A/File.txt");
        m.fields.insert("mtime".to_string(), "1700000000.0".to_string());
        m
    }

    #[tokio::test]
    async fn test_disk_nodupe_after_accept() {
        let tmp = tempdir().unwrap();
        let mut config = Config::default();
        config.component = "sarra".to_string();
        config.configname = Some("test".to_string());
        config.nodupe_ttl = 3600;

        let now = DiskNoDupePlugin::now_flt();
        let mtime = (now - 100.0).to_string();

        // Override cache file for testing
        let mut plugin = DiskNoDupePlugin::new(&config);
        {
            let mut state = plugin.state.lock().unwrap();
            state.cache_file = tmp.path().join("recent_files_001.cache");
        }

        plugin.on_start().await.unwrap();

        let mut wl = Worklist::new();
        let mut m1 = Message::new("https://NotARealURL", "ThisIsAPath/To/A/File.txt");
        m1.fields.insert("mtime".to_string(), mtime.clone());
        wl.incoming.push(m1.clone());
        wl.incoming.push(m1.clone());
        wl.incoming.push(m1.clone());

        plugin.after_accept(&mut wl).await.unwrap();

        assert_eq!(wl.incoming.len(), 1);
        assert_eq!(wl.rejected.len(), 2);

        // Test restart persistence
        plugin.on_stop().await.unwrap();
        
        let mut plugin2 = DiskNoDupePlugin::new(&config);
        {
            let mut state = plugin2.state.lock().unwrap();
            state.cache_file = tmp.path().join("recent_files_001.cache");
        }
        plugin2.on_start().await.unwrap();

        let mut wl2 = Worklist::new();
        wl2.incoming.push(m1);
        plugin2.after_accept(&mut wl2).await.unwrap();

        assert_eq!(wl2.incoming.len(), 0);
        assert_eq!(wl2.rejected.len(), 1);
    }

    #[tokio::test]
    async fn test_disk_nodupe_file_ages() {
        let tmp = tempdir().unwrap();
        let mut config = Config::default();
        config.file_age_min = 10.0;
        config.file_age_max = 3600.0;
        config.nodupe_ttl = 3600;

        let now = DiskNoDupePlugin::now_flt();
        
        let mut plugin = DiskNoDupePlugin::new(&config);
        {
            let mut state = plugin.state.lock().unwrap();
            state.cache_file = tmp.path().join("file_ages.cache");
        }
        plugin.on_start().await.unwrap();

        let mut wl = Worklist::new();
        
        // Too old
        let mut m_old = make_message();
        m_old.fields.insert("mtime".to_string(), (now - 4000.0).to_string());
        
        // Too new
        let mut m_new = make_message();
        m_new.fields.insert("mtime".to_string(), (now - 5.0).to_string());
        
        // Just right
        let mut m_ok = make_message();
        m_ok.rel_path = "ok.txt".to_string();
        m_ok.fields.insert("mtime".to_string(), (now - 100.0).to_string());

        wl.incoming.push(m_old);
        wl.incoming.push(m_new);
        wl.incoming.push(m_ok);

        plugin.after_accept(&mut wl).await.unwrap();

        assert_eq!(wl.incoming.len(), 1);
        assert_eq!(wl.rejected.len(), 2);
        
        assert!(wl.rejected[0].fields.get("reject").unwrap().contains("too old"));
        assert!(wl.rejected[1].fields.get("reject").unwrap().contains("too new"));
    }

    #[tokio::test]
    async fn test_disk_nodupe_housekeeping_expiry() {
        let tmp = tempdir().unwrap();
        let mut config = Config::default();
        config.component = "sarra".to_string();
        config.configname = Some("test_hk".to_string());
        config.nodupe_ttl = 1; // 1 second TTL

        let mut plugin = DiskNoDupePlugin::new(&config);
        {
            let mut state = plugin.state.lock().unwrap();
            state.cache_file = tmp.path().join("hk.cache");
        }
        plugin.on_start().await.unwrap();

        let m1 = make_message();

        // 1. Initial accept should pass
        let mut wl1 = Worklist::new();
        wl1.incoming.push(m1.clone());
        plugin.after_accept(&mut wl1).await.unwrap();
        assert_eq!(wl1.incoming.len(), 1);

        // 2. Immediate second accept should be rejected (cached)
        let mut wl2 = Worklist::new();
        wl2.incoming.push(m1.clone());
        plugin.after_accept(&mut wl2).await.unwrap();
        assert_eq!(wl2.incoming.len(), 0);
        assert_eq!(wl2.rejected.len(), 1);

        // 3. Wait for TTL to expire
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        // 4. Run housekeeping (should clean expired cache)
        let mut hk_wl = Worklist::new();
        plugin.on_housekeeping(&mut hk_wl).await.unwrap();

        // 5. Try accepting again, it should pass now
        let mut wl3 = Worklist::new();
        wl3.incoming.push(m1.clone());
        plugin.after_accept(&mut wl3).await.unwrap();
        assert_eq!(wl3.incoming.len(), 1);
    }
}

