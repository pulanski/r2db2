use bcrypt::{hash, verify, DEFAULT_COST};

pub(crate) struct PasswordAuthenticator {
    // TODO: Use a more secure data structure and persist to storage layer
    user_password_hash: std::collections::HashMap<String, String>,
}

impl PasswordAuthenticator {
    pub(crate) fn new() -> Self {
        // Initialize with some default user credentials
        let mut user_password_hash = std::collections::HashMap::new();
        user_password_hash.insert("test".to_string(), hash("test", DEFAULT_COST).unwrap());
        Self { user_password_hash }
    }

    pub(crate) fn authenticate(&self, username: &str, password: &str) -> bool {
        if let Some(hash) = self.user_password_hash.get(username) {
            verify(password, hash).unwrap_or(false)
        } else {
            false
        }
    }
}
