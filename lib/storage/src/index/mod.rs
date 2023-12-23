use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("Index creation failed: {0}")]
    CreationError(String),

    #[error("Index not found: {0}")]
    NotFoundError(String),
}

pub mod b_plus_tree;
pub mod generic_key;
pub mod index;
pub mod iterator;
pub mod manager;
pub mod stats;
