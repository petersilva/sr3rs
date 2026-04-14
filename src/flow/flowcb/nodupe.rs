//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

pub mod disk;
pub mod modifiers;

use crate::message::Message;

pub fn derive_key(msg: &Message) -> String {
    // 1st priority: use the key from nodupe_override in the msg
    if let Some(key) = msg.fields.get("_nodupe_override_key") {
        return key.clone();
    }

    // 2nd: derive from fileOp if fileOp is link or is a non-remove directory op

    // 3rd: use identity (checksum) if available (cod = calculate on download, i.e. no checksum yet)
    if msg.identity.is_empty() {
       if msg.file_operation.is_empty() {
           // pass
       } else if msg.file_operation.get("link").is_some() {
           return format!("link,{}", msg.file_operation.get("link").unwrap().to_string() );
       } else if msg.file_operation.get("directory").is_some() {
           return msg.rel_path.clone();
       }
    } else { // regular file should have identity and checksum.

       if msg.identity.get("method").is_some() && msg.identity.get("method").unwrap() != "cod" {
           return format!("{},{}", msg.identity.get("method").unwrap().to_string(), msg.identity.get("value").unwrap().to_string());
       }
    }

    // 4th: use relPath and time (and size, if known)
    let t = if let Some(mtime) = msg.fields.get("mtime") {
        mtime.clone()
    } else {
        msg.pub_time.to_rfc3339() // pubTime is a DateTime<Utc>, need its string rep
    };

    let path = if let Some(p) = msg.fields.get("_nodupe_override_path") {
        p.clone()
    } else {
        msg.rel_path.clone()
    };

    if let Some(size) = msg.fields.get("size") {
        format!("{},{},{}", path, t, size)
    } else {
        format!("{},{}", path, t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use chrono::{Utc, TimeZone};

    fn make_message() -> Message {
        let mut m = Message::new("https://NotARealURL", "ThisIsAPath/To/A/File.txt");
        m.pub_time = Utc.with_ymd_and_hms(2018, 1, 18, 15, 10, 49).unwrap();
        m.fields.insert("mtime".to_string(), "20180118151048".to_string());
        m
    }

    #[test]
    fn test_derive_key() {
        let mut msg = make_message();
        
        // Default: path,mtime
        assert_eq!(derive_key(&msg), "ThisIsAPath/To/A/File.txt,20180118151048");

        // With size
        msg.fields.insert("size".to_string(), "28234".to_string());
        assert_eq!(derive_key(&msg), "ThisIsAPath/To/A/File.txt,20180118151048,28234");

        // Identity override
        msg.identity.insert("method".to_string(), "sha512".to_string());
        msg.identity.insert("value".to_string(), "C/HbD77eLraAo".to_string());
        assert_eq!(derive_key(&msg), "sha512,C/HbD77eLraAo");

        // cod (calculate on download) should fallback to path,mtime,size
        msg.identity.insert("method".to_string(), "cod".to_string());
        msg.identity.insert("value".to_string(), "md5".to_string());
        assert_eq!(derive_key(&msg), "ThisIsAPath/To/A/File.txt,20180118151048,28234");

        // nodupe_override key
        msg.fields.insert("_nodupe_override_key".to_string(), "SomeKeyValue".to_string());
        assert_eq!(derive_key(&msg), "SomeKeyValue");

        // fileOp link
        let mut msg2 = make_message();
        msg2.file_operation.insert("link".to_string(), "SomeLinkTarget".to_string());
        assert_eq!(derive_key(&msg2), "link,SomeLinkTarget");

        // fileOp directory
        let mut msg3 = make_message();
        msg3.file_operation.insert("directory".to_string(), "".to_string());
        assert_eq!(derive_key(&msg3), "ThisIsAPath/To/A/File.txt");
    }
}

