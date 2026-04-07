use std::io::{self, Read};
use std::fs::File;

pub mod md5;
pub mod sha512;
pub mod random;
pub mod arbitrary;

pub trait Identity: Send + Sync {
    fn registered_as(&self) -> &'static str;
    fn get_method(&self) -> String;
    fn set_path(&mut self, path: &str);
    fn update(&mut self, chunk: &[u8]);
    fn value(&self) -> String;

    fn update_file(&mut self, path: &str) -> io::Result<()> {
        self.set_path(path);
        let mut f = File::open(path)?;
        let mut buffer = [0u8; 1024 * 1024];
        loop {
            let n = f.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            self.update(&buffer[..n]);
        }
        Ok(())
    }
}

pub fn factory(method: &str) -> Option<Box<dyn Identity>> {
    match method.to_lowercase().as_str() {
        "md5" => Some(Box::new(md5::Md5::new())),
        "sha512" => Some(Box::new(sha512::Sha512::new())),
        "random" => Some(Box::new(random::Random::new())),
        "arbitrary" => Some(Box::new(arbitrary::Arbitrary::new())),
        _ => None,
    }
}

pub const BINARY_METHODS: &[&str] = &["sha512", "md5", "unknown"];
pub const KNOWN_METHODS: &[&str] = &["md5", "sha512", "random", "arbitrary"];

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_factory() {
        let identity = factory("foobar");
        assert!(identity.is_none());

        let identity = factory("sha512").unwrap();
        assert_eq!(identity.registered_as(), "s");
    }

    #[test]
    fn test_get_method() {
        let identity = factory("sha512").unwrap();
        assert_eq!(identity.get_method(), "sha512");
    }

    #[test]
    fn test_update_file() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "randomstring").unwrap();
        
        let mut identity = factory("sha512").unwrap();
        let result = identity.update_file(file.path().to_str().unwrap());
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_property_value() {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "randomstring").unwrap();
        
        let mut identity = factory("sha512").unwrap();
        identity.update_file(file.path().to_str().unwrap()).unwrap();

        assert_eq!(
            identity.value(),
            "kkUPxxKfR72my8noS5yekWcdFnmIJSvDJIvtSF7uTyvnhtm0saERCXReIcNDAk2B7gET3o+tQY3gTbd36ynoDA=="
        );
    }
}
