//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::Worklist;
use crate::flow::flowcb::FlowCB;
use async_trait::async_trait;

pub struct NameOnlyPlugin {
    name: String,
}

impl NameOnlyPlugin {
    pub fn new() -> Self {
        Self { name: "nodupe.name_only".to_string() }
    }
}

#[async_trait]
impl FlowCB for NameOnlyPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        for m in &mut wl.incoming {
            let parts: Vec<&str> = m.rel_path.split('/').collect();
            let file_name = parts.last().copied().unwrap_or(m.rel_path.as_str());

            m.delete_on_post.insert("_nodupe_override_path".to_string(), file_name.to_string());
            m.delete_on_post.insert("_nodupe_override_key".to_string(), file_name.to_string());
            
        }
        Ok(())
    }
}

pub struct PathOnlyPlugin {
    name: String,
}

impl PathOnlyPlugin {
    pub fn new() -> Self {
        Self { name: "nodupe.path_only".to_string() }
    }
}

#[async_trait]
impl FlowCB for PathOnlyPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        for m in &mut wl.incoming {
            m.delete_on_post.insert("_nodupe_override_key".to_string(), m.rel_path.clone());
        }
        Ok(())
    }
}

pub struct DataOnlyPlugin {
    name: String,
}

impl DataOnlyPlugin {
    pub fn new() -> Self {
        Self { name: "nodupe.data_only".to_string() }
    }
}

#[async_trait]
impl FlowCB for DataOnlyPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn after_accept(&self, wl: &mut Worklist) -> anyhow::Result<()> {
        for m in &mut wl.incoming {
            m.delete_on_post.insert("_nodupe_override_path".to_string(), "data".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use crate::flow::Worklist;

    fn make_message() -> Message {
        let mut m = Message::new("https://NotARealURL", "ThisIsAPath/To/A/File.txt");
        m.fields.insert("pubTime".to_string(), "20180118151049.356378078".to_string());
        m.fields.insert("mtime".to_string(), "20180118151048".to_string());
        m.fields.insert("size".to_string(), "69".to_string());
        m
    }

    #[tokio::test]
    async fn test_name_only_after_accept() {
        let plugin = NameOnlyPlugin::new();
        let mut wl = Worklist::new();
        
        let mut m1 = make_message();
        m1.delete_on_post.insert("_nodupe_override_path".to_string(), "existing".to_string());
        
        let m2 = make_message();
        
        wl.incoming.push(m1);
        wl.incoming.push(m2);

        plugin.after_accept(&mut wl).await.unwrap();

        assert_eq!(wl.incoming.len(), 2);
        assert_eq!(wl.incoming[0].delete_on_post.get("_nodupe_override_key").unwrap(), "File.txt");
        assert_eq!(wl.incoming[0].delete_on_post.get("_nodupe_override_path").unwrap(), "File.txt");
        
        assert_eq!(wl.incoming[1].delete_on_post.get("_nodupe_override_key").unwrap(), "File.txt");
        assert_eq!(wl.incoming[1].delete_on_post.get("_nodupe_override_path").unwrap(), "File.txt");
        
    }

    #[tokio::test]
    async fn test_path_only_after_accept() {
        let plugin = PathOnlyPlugin::new();
        let mut wl = Worklist::new();
        
        let mut m1 = make_message();
        m1.delete_on_post.insert("_nodupe_override_key".to_string(), "existing".to_string());
        
        let m2 = make_message();
        
        wl.incoming.push(m1);
        wl.incoming.push(m2);

        plugin.after_accept(&mut wl).await.unwrap();

        assert_eq!(wl.incoming.len(), 2);
        assert_eq!(wl.incoming[0].delete_on_post.get("_nodupe_override_key").unwrap(), "ThisIsAPath/To/A/File.txt");
        assert_eq!(wl.incoming[1].delete_on_post.get("_nodupe_override_key").unwrap(), "ThisIsAPath/To/A/File.txt");
    }

    #[tokio::test]
    async fn test_data_only_after_accept() {
        let plugin = DataOnlyPlugin::new();
        let mut wl = Worklist::new();
        
        let mut m1 = make_message();
        m1.delete_on_post.insert("_nodupe_override_path".to_string(), "existing".to_string());
        
        let m2 = make_message();
        
        wl.incoming.push(m1);
        wl.incoming.push(m2);

        plugin.after_accept(&mut wl).await.unwrap();

        assert_eq!(wl.incoming.len(), 2);
        assert_eq!(wl.incoming[0].delete_on_post.get("_nodupe_override_path").unwrap(), "data");
        assert_eq!(wl.incoming[1].delete_on_post.get("_nodupe_override_path").unwrap(), "data");
    }
}
