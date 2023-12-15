//! # Encoding Subsystem
//!
//! The encoding subsystem is responsible for converting SQL data types into binary format. Ensures
//! that data is serialized in a consistent, efficient, and portable manner.
//!
//! ## Key Component
//!
//! ### `Encodable` Trait
//!
//! The `Encodable` trait is the core of the encoding subsystem. It defines a single method, `encode`, which
//! takes a reference to the implementing type and returns a `Result` containing a `Vec<u8>` on success or an
//! `EncodingError` on failure.
//!
//! ```rust
//! use common::traits::encode::EncodingError;
//!
//! trait Encodable {
//!    fn encode(&self) -> Result<Vec<u8>, EncodingError>;
//! }
//! ```
//!
//! Any type that needs to be encoded into binary format should implement this trait. The `encode` method
//! should handle the conversion of the type into a byte vector.

use anyhow::Result;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EncodingError {
    #[error("Invalid data type")]
    InvalidDataType,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Date/time error: {0}")]
    DateTimeError(#[from] chrono::format::ParseError),
    // ...
}

pub trait Encodable {
    fn encode(&self) -> Result<Vec<u8>, EncodingError>;
}
