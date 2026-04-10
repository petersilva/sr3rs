//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::flowcb::FlowCB;
use crate::flow::Worklist;
use crate::Config;
use async_trait::async_trait;

pub struct PollPlugin {
    pub name: String,
    pub config: Config,
}

impl PollPlugin {
    pub fn new(config: &Config) -> Self {
        Self {
            name: "poll".to_string(),
            config: config.clone(),
        }
    }
}

#[async_trait]
impl FlowCB for PollPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn gather(&mut self, _worklist: &mut Worklist) -> anyhow::Result<()> {
        if let Some(poll_url) = &self.config.poll_url {
            ::log::info!("POLL: Native polling for {} is a stub. Use Python plugins for full functionality.", poll_url);
        } else {
            ::log::warn!("POLL: No pollUrl configured.");
        }
        Ok(())
    }
}
