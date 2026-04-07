use sha2::{Sha512 as Sha512Hasher, Digest};
use base64::{Engine as _, engine::general_purpose};
use super::Identity;

pub struct Sha512 {
    hasher: Sha512Hasher,
}

impl Sha512 {
    pub fn new() -> Self {
        Self {
            hasher: Sha512Hasher::new(),
        }
    }
}

impl Identity for Sha512 {
    fn registered_as(&self) -> &'static str {
        "s"
    }

    fn get_method(&self) -> String {
        "sha512".to_string()
    }

    fn set_path(&mut self, _path: &str) {
        self.hasher = Sha512Hasher::new();
    }

    fn update(&mut self, chunk: &[u8]) {
        self.hasher.update(chunk);
    }

    fn value(&self) -> String {
        let digest = self.hasher.clone().finalize();
        general_purpose::STANDARD.encode(digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registered_as() {
        let hash = Sha512::new();
        assert_eq!(hash.registered_as(), "s");
    }

    #[test]
    fn test_set_path() {
        let mut hash = Sha512::new();
        hash.set_path("dummy_path.txt");
        
        // When initialized or reset, the digest should be the sha512 of empty string
        assert_eq!(
            hash.value(),
            "z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg=="
        );
    }

    #[test]
    fn test_update() {
        let mut hash = Sha512::new();
        hash.set_path("dummy_path.txt");
        
        hash.update(b"randomstring");
        assert_eq!(
            hash.value(),
            "kkUPxxKfR72my8noS5yekWcdFnmIJSvDJIvtSF7uTyvnhtm0saERCXReIcNDAk2B7gET3o+tQY3gTbd36ynoDA=="
        );

        hash.update(b"randombytes");
        assert_eq!(
            hash.value(),
            "pPhNwHi6/lnnslx41G9BZ/5bEwpE+GbPTf9+6Rj7j76UeO7wT0c+Dlc2VioFI9Fy66G0pCszFkB/8cfrBFBRRw=="
        );
    }
}
