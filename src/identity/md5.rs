use md5::{Context};
use base64::{Engine as _, engine::general_purpose};
use super::Identity;

pub struct Md5 {
    context: Context,
}

impl Md5 {
    pub fn new() -> Self {
        Self {
            context: Context::new(),
        }
    }
}

impl Identity for Md5 {
    fn registered_as(&self) -> &'static str {
        "d"
    }

    fn get_method(&self) -> String {
        "md5".to_string()
    }

    fn set_path(&mut self, _path: &str) {
        self.context = Context::new();
    }

    fn update(&mut self, chunk: &[u8]) {
        self.context.consume(chunk);
    }

    #[allow(deprecated)]
    fn value(&self) -> String {
        let digest = self.context.clone().compute();
        general_purpose::STANDARD.encode(digest.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registered_as() {
        let hash = Md5::new();
        assert_eq!(hash.registered_as(), "d");
    }

    #[test]
    fn test_set_path() {
        let mut hash = Md5::new();
        hash.set_path("dummy_path.txt");
        
        // The MD5 digest of an empty string
        assert_eq!(hash.value(), "1B2M2Y8AsgTpgAmY7PhCfg==");
    }

    #[test]
    fn test_update() {
        let mut hash = Md5::new();
        hash.set_path("dummy_path.txt");
        
        hash.update(b"randomstring");
        assert_eq!(hash.value(), "tpDC1B4RAL5h8WAs1C1OFg==");

        hash.update(b"randombytes");
        assert_eq!(hash.value(), "+sILUpRAJFq9hB7p8kx1xA==");
    }
}
