use jsonwebtoken::{
    decode, encode, errors::Result as JWTResult, Algorithm, DecodingKey, EncodingKey, Header,
    Validation,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

pub(crate) struct TokenAuthenticator {
    secret: String,
}

impl TokenAuthenticator {
    pub(crate) fn new(secret: String) -> Self {
        Self { secret }
    }

    pub(crate) fn authenticate(&self, token: &str) -> JWTResult<bool> {
        let validation = Validation::new(Algorithm::HS256);
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_ref()),
            &validation,
        )?;

        // TODO: check the token's validity (e.g., expiration)

        Ok(true)
    }
}
