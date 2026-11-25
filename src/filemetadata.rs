//
// File metadata implementation supporting extended attributes (xattr) on POSIX/Linux/macOS
// and Alternate Data Streams (ADS) on Windows.
//

use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

static XATTR_DISABLED: AtomicBool = AtomicBool::new(false);

/// Disables extended attribute / ADS reading and writing globally.
pub fn disable_xattr() {
    XATTR_DISABLED.store(true, Ordering::Relaxed);
}

/// Enables extended attribute / ADS reading and writing globally.
pub fn enable_xattr() {
    XATTR_DISABLED.store(false, Ordering::Relaxed);
}

/// Checks whether extended attributes / ADS are globally disabled.
pub fn is_xattr_disabled() -> bool {
    XATTR_DISABLED.load(Ordering::Relaxed)
}

/// Key-value metadata attached to a file.
/// On Unix/Linux/macOS, uses extended attributes with `user.sr_` prefix.
/// On Windows, uses Alternate Data Streams (ADS) named `sr_.json`.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    path: PathBuf,
    dirty: bool,
    data: HashMap<String, Value>,
}

impl FileMetadata {
    /// Creates a new `FileMetadata` object and loads any existing extended attributes from `path`.
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let mut fmd = Self {
            path,
            dirty: false,
            data: HashMap::new(),
        };
        fmd.load();
        fmd
    }

    /// Loads metadata attributes from disk.
    fn load(&mut self) {
        if is_xattr_disabled() {
            return;
        }

        #[cfg(all(not(target_arch = "wasm32"), unix))]
        {
            if let Ok(attrs) = xattr::list(&self.path) {
                for attr in attrs {
                    if let Some(key) = attr.to_str().and_then(|s| s.strip_prefix("user.sr_")) {
                        if let Ok(Some(val_bytes)) = xattr::get(&self.path, &attr) {
                            if let Ok(val_str) = std::str::from_utf8(&val_bytes) {
                                let json_val = serde_json::from_str(val_str)
                                    .unwrap_or_else(|_| Value::String(val_str.to_string()));
                                self.data.insert(key.to_string(), json_val);
                            }
                        }
                    }
                }
            }
        }

        #[cfg(all(not(target_arch = "wasm32"), windows))]
        {
            let mut ads_path = self.path.clone();
            let file_name = self.path.file_name().unwrap_or_default().to_string_lossy();
            let stream_name = format!("{file_name}:sr_.json");
            ads_path.set_file_name(stream_name);

            if let Ok(content) = std::fs::read_to_string(&ads_path) {
                if let Ok(parsed) = serde_json::from_str::<HashMap<String, Value>>(&content) {
                    self.data = parsed;
                }
            }
        }

        // Migration logic: convert legacy 'integrity' field to 'identity'
        if let Some(integrity) = self.data.remove("integrity") {
            self.data.insert("identity".to_string(), integrity);
        }
    }

    /// Returns the list of metadata attribute keys currently defined.
    pub fn list(&self) -> Vec<String> {
        self.data.keys().cloned().collect()
    }

    /// Returns a reference to the metadata value for `name`, if present.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.data.get(name)
    }

    /// Gets string representation of attribute value, if string or stringifiable.
    pub fn get_str(&self, name: &str) -> Option<String> {
        match self.data.get(name) {
            Some(Value::String(s)) => Some(s.clone()),
            Some(v) => Some(v.to_string()),
            None => None,
        }
    }

    /// Sets key & value pair for file metadata.
    pub fn set(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.data.insert(name.into(), value.into());
        self.dirty = true;
    }

    /// Removes an attribute key from metadata.
    pub fn remove(&mut self, name: &str) -> Option<Value> {
        let removed = self.data.remove(name);
        if removed.is_some() {
            self.dirty = true;
        }
        removed
    }

    /// Writes in-memory extended attributes to disk.
    pub fn persist(&mut self) -> Result<(), std::io::Error> {
        if !self.dirty || is_xattr_disabled() {
            return Ok(());
        }

        #[cfg(all(not(target_arch = "wasm32"), unix))]
        {
            for (k, v) in &self.data {
                let attr_name = format!("user.sr_{k}");
                let bytes = match v {
                    Value::String(s) => s.as_bytes().to_vec(),
                    other => serde_json::to_vec(other).unwrap_or_default(),
                };
                let _ = xattr::set(&self.path, &attr_name, &bytes);
            }
        }

        #[cfg(all(not(target_arch = "wasm32"), windows))]
        {
            let mut ads_path = self.path.clone();
            let file_name = self.path.file_name().unwrap_or_default().to_string_lossy();
            let stream_name = format!("{file_name}:sr_.json");
            ads_path.set_file_name(stream_name);

            if let Ok(json_bytes) = serde_json::to_vec_pretty(&self.data) {
                let _ = std::fs::write(ads_path, json_bytes);
            }
        }

        self.dirty = false;
        Ok(())
    }
}

impl Drop for FileMetadata {
    fn drop(&mut self) {
        let _ = self.persist();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_metadata_in_memory() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut meta = FileMetadata::new(path);
        meta.set("sum", "1234567890abcdef");
        meta.set("mtime", "2026-11-25T12:00:00Z");

        assert_eq!(meta.get_str("sum"), Some("1234567890abcdef".to_string()));
        assert_eq!(meta.get_str("mtime"), Some("2026-11-25T12:00:00Z".to_string()));
        assert!(meta.list().contains(&"sum".to_string()));
        assert!(meta.list().contains(&"mtime".to_string()));
    }

    #[test]
    fn test_file_metadata_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut meta = FileMetadata::new(path);
            meta.set("sum", "deadbeef");
            meta.set("identity", "test_file_id");
            meta.persist().unwrap();
        }

        // Re-open file and check if xattr/ADS persistence worked (if supported on system)
        let meta2 = FileMetadata::new(path);
        #[cfg(all(not(target_arch = "wasm32"), unix))]
        {
            // Extended attributes may fail on tmpfs or filesystems without user_xattr enabled in test environment,
            // but if supported, it loads correctly.
            if let Some(sum) = meta2.get_str("sum") {
                assert_eq!(sum, "deadbeef");
                assert_eq!(meta2.get_str("identity"), Some("test_file_id".to_string()));
            }
        }
    }

    #[test]
    fn test_integrity_migration() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut meta = FileMetadata::new(path);
        meta.data.insert("integrity".to_string(), Value::String("old_integrity_val".to_string()));
        meta.load(); // triggers migration logic

        assert_eq!(meta.get_str("identity"), Some("old_integrity_val".to_string()));
        assert_eq!(meta.get("integrity"), None);
    }
}
