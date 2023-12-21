use anyhow::Result;
use getset::{Getters, Setters};
use thiserror::Error;
use typed_builder::TypedBuilder;

#[derive(Debug, Error, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GenericKeyError {
    #[error("Tuple size exceeds key size")]
    TupleSizeExceedsKeySize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Getters, Setters, TypedBuilder)]
pub struct GenericKey<const KEY_SIZE: usize> {
    data: [u8; KEY_SIZE],
}

impl<const KEY_SIZE: usize> GenericKey<KEY_SIZE> {
    pub fn new() -> Self {
        Self {
            data: [0; KEY_SIZE],
        }
    }

    pub fn set_from_key(&mut self, tuple: &[u8]) -> Result<()> {
        if tuple.len() > KEY_SIZE {
            Err(GenericKeyError::TupleSizeExceedsKeySize.into())
        } else {
            self.data[..tuple.len()].copy_from_slice(tuple);
            Ok(())
        }
    }
}
