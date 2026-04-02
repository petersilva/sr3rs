use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod subscribe;

#[derive(Debug, Default)]
pub struct Worklist {
    pub incoming: Vec<Message>,
    pub ok: Vec<Message>,
    pub rejected: Vec<Message>,
    pub failed: Vec<Message>,
    pub directories_ok: Vec<String>,
}

impl Worklist {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.incoming.clear();
        self.ok.clear();
        self.rejected.clear();
        self.failed.clear();
        self.directories_ok.clear();
    }
}

#[async_trait]
pub trait Flow: Send + Sync {
    fn config(&self) -> &Config;
    
    async fn gather(&self, worklist: &mut Worklist) -> anyhow::Result<()>;
    async fn filter(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let config = self.config();
        let mut filtered_incoming = Vec::new();

        for m in worklist.incoming.drain(..) {
            let url_to_match = format!("{}{}", m.base_url, m.rel_path);
            let mut matched = false;
            let mut accepted = config.accept_unmatched;

            for mask in &config.masks {
                if mask.matches(&url_to_match) {
                    matched = true;
                    accepted = mask.accepting;
                    break;
                }
            }

            if matched {
                if accepted {
                    filtered_incoming.push(m);
                } else {
                    worklist.rejected.push(m);
                }
            } else if config.accept_unmatched {
                filtered_incoming.push(m);
            } else {
                worklist.rejected.push(m);
            }
        }

        worklist.incoming = filtered_incoming;
        Ok(())
    }
    async fn accept(&self, worklist: &mut Worklist) -> anyhow::Result<()>;
    async fn work(&self, worklist: &mut Worklist) -> anyhow::Result<()>;
    async fn post(&self, worklist: &mut Worklist) -> anyhow::Result<()>;
    async fn ack(&self, worklist: &mut Worklist) -> anyhow::Result<()>;

    async fn run_once(&self, worklist: &mut Worklist) -> anyhow::Result<()> {
        self.gather(worklist).await?;
        self.filter(worklist).await?;
        self.accept(worklist).await?;
        self.work(worklist).await?;
        self.post(worklist).await?;
        self.ack(worklist).await?;
        Ok(())
    }

    async fn run(&self) -> anyhow::Result<()> {
        let mut worklist = Worklist::new();
        let mut last_housekeeping = std::time::Instant::now();
        let housekeeping_interval = std::time::Duration::from_secs(self.config().housekeeping as u64);

        loop {
            self.run_once(&mut worklist).await?;
            
            if last_housekeeping.elapsed() >= housekeeping_interval {
                self.housekeeping(&mut worklist).await?;
                last_housekeeping = std::time::Instant::now();
            }

            if worklist.incoming.is_empty() && worklist.ok.is_empty() {
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(self.config().sleep)).await;
            }
            
            // In a real implementation we'd check for stop signals here
        }
    }

    async fn housekeeping(&self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct BaseFlow {
    pub config: Config,
    pub worklist: Arc<Mutex<Worklist>>,
}

impl BaseFlow {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            worklist: Arc::new(Mutex::new(Worklist::new())),
        }
    }
}

#[async_trait]
impl Flow for BaseFlow {
    fn config(&self) -> &Config {
        &self.config
    }

    async fn gather(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn accept(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn work(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn post(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn ack(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
    async fn housekeeping(&self, _worklist: &mut Worklist) -> anyhow::Result<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worklist_initialization() {
        let wl = Worklist::new();
        assert!(wl.incoming.is_empty());
        assert!(wl.ok.is_empty());
    }

    #[test]
    fn test_worklist_clear() {
        let mut wl = Worklist::new();
        wl.incoming.push(Message::new("url", "path"));
        wl.clear();
        assert!(wl.incoming.is_empty());
    }

    #[tokio::test]
    async fn test_flow_filter() {
        let mut config = Config::new();
        config.masks.push(crate::filter::Filter::new(".*accept.*", true).unwrap());
        config.masks.push(crate::filter::Filter::new(".*reject.*", false).unwrap());
        config.accept_unmatched = false;

        let flow = BaseFlow::new(config);
        let mut wl = Worklist::new();
        wl.incoming.push(Message::new("http://host/", "accept_me.txt"));
        wl.incoming.push(Message::new("http://host/", "reject_me.txt"));
        wl.incoming.push(Message::new("http://host/", "ignore_me.txt"));

        flow.filter(&mut wl).await.unwrap();

        assert_eq!(wl.incoming.len(), 1);
        assert_eq!(wl.incoming[0].rel_path, "accept_me.txt");
        assert_eq!(wl.rejected.len(), 2);
    }
}
