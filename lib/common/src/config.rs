//! Various configuration parameters for the dbms.

#![allow(dead_code)]

use config::{Config, ConfigBuilder, ConfigError, Environment, File, FileFormat};
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use shrinkwraprs::Shrinkwrap;
use std::env;
use std::{fmt, time::Duration};
use thiserror::Error;
use typed_builder::TypedBuilder;
use url::Url;

/// The size of a page in bytes (4 KiB). Pages are a fixed-size block of data and
/// are the unit of data transfer between disk and memory.
pub const PAGE_SIZE: usize = 4096;

/// The size of the buffer pool (in frames). Specifies the number of pages that can be held in
/// memory at any given time. The buffer pool is the primary mechanism for storing pages in memory.
pub const BUFFER_POOL_SIZE: usize = 10;

/// The maximum number of concurrent transactions. Sets an upper limit on the number of transactions
/// that can be processed concurrently by the DBMS. This is used to initialize the scheduler.
/// Transactions beyond this limit will be blocked until a transaction completes.
pub const MAX_TRANSACTIONS: usize = 10;

/// Unique identifier for a frame. Frames are identified by a monotonically increasing integer
/// and are the unit of storage in the buffer pool.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct FrameId(pub u32);

impl FrameId {
    pub fn new(frame_id: u32) -> Self {
        Self(frame_id)
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl From<FrameId> for u32 {
    fn from(frame_id: FrameId) -> Self {
        frame_id.0
    }
}

impl From<i32> for FrameId {
    fn from(frame_id: i32) -> Self {
        if frame_id < 0 {
            panic!("FrameId out of range")
        }

        Self(frame_id as u32)
    }
}

impl From<usize> for FrameId {
    fn from(frame_id: usize) -> Self {
        if frame_id > u32::MAX as usize {
            panic!("FrameId out of range")
        }

        Self(frame_id as u32)
    }
}

impl From<u32> for FrameId {
    fn from(frame_id: u32) -> Self {
        Self(frame_id)
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FrameId({})", self.0)
    }
}

/// Unique identifier for a page. Pages are identified by a tuple of (file_id, page_number).
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct PageId(pub u32);

impl PageId {
    pub fn new(page_id: u32) -> Self {
        Self(page_id)
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl From<i64> for PageId {
    fn from(page_id: i64) -> Self {
        if page_id < 0 {
            panic!("PageId out of range")
        }

        Self(page_id as u32)
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}

impl From<PageId> for u32 {
    fn from(page_id: PageId) -> Self {
        page_id.0
    }
}

impl From<u32> for PageId {
    fn from(page_id: u32) -> Self {
        Self(page_id)
    }
}

impl From<i32> for PageId {
    fn from(page_id: i32) -> Self {
        if page_id < 0 {
            panic!("PageId out of range")
        }

        Self(page_id as u32)
    }
}

impl From<usize> for PageId {
    fn from(page_id: usize) -> Self {
        if page_id > u32::MAX as usize {
            panic!("PageId out of valid range. Got {}", page_id)
        }

        Self(page_id as u32)
    }
}

/// Offset of a page within a file. Pages are stored sequentially within a file.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct PageOffset(usize);

/// Unique identifier for a transaction. Transactions are identified by a monotonically increasing
/// integer.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct TxnId(u32);

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnId({})", self.0)
    }
}

/// Timestamp of a transaction. Timestamps are used to determine the relative ordering of
/// transactions and are used to implement concurrency control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Timestamp(u64);

impl Default for Timestamp {
    fn default() -> Self {
        Self(u64::default())
    }
}

#[derive(Debug, Error, Clone)]
pub enum DbConfigError {
    #[error("Invalid configuration")]
    InvalidConfig,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Getters, Setters, TypedBuilder,
)]
#[getset(get = "pub", set = "pub")]
pub struct DbConfig {
    // TODO: add functionality to parse/validate connection string and extract parameters
    // as well as generate connection string from parameters
    // pub connection_string: String,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    pub max_pool_size: usize,
    pub connection_timeout: Duration,
    pub command_timeout: Duration,
    // TODO: Other configuration options like SSL, logging preferences, etc.
}

impl DbConfig {
    // Load configuration from a given file and the environment
    pub fn load_from_file_and_env(file_path: &str) -> Result<Self, DbConfigError> {
        // TODO: more fine-grained error handling (e.g. file not found, invalid file format, invalid env vars, etc.)
        let builder = ConfigBuilder::<config::builder::DefaultState>::default() // Explicitly use the synchronous version
            .set_default("host", "localhost")
            .map_err(|_| DbConfigError::InvalidConfig)?
            // Set some defaults, can be overridden by file or env
            .set_default("port", "2345")
            .map_err(|_| DbConfigError::InvalidConfig)?
            .add_source(File::new(file_path, FileFormat::Toml).required(false)) // File is optional
            .add_source(Environment::with_prefix("APP").separator("__")) // Environment variables
            .build()
            .map_err(|_| DbConfigError::InvalidConfig)?;

        builder
            .try_deserialize::<DbConfig>()
            .map_err(|_| DbConfigError::InvalidConfig)
    }

    // Parses a connection string and returns a DbConfig
    pub fn from_connection_string(conn_str: &str) -> Result<Self, DbConfigError> {
        // TODO: more fine-grained error handling (e.g. invalid connection string, invalid parameters, etc.)
        let url = Url::parse(conn_str).map_err(|_| DbConfigError::InvalidConfig)?;
        let user = url.username();
        let password = url.password().unwrap_or("");
        let host = url.host_str().unwrap_or("");
        let port = url.port().unwrap_or(2345); // Default PostgreSQL port
        let path_segments = url
            .path_segments()
            .map(|c| c.collect::<Vec<_>>())
            .unwrap_or_default();
        let database = path_segments.get(0).unwrap_or(&"").to_string();

        Ok(DbConfig {
            host: host.to_string(),
            port,
            database,
            user: user.to_string(),
            password: password.to_string(),
            max_pool_size: 10,                        // TODO: define default values
            connection_timeout: Duration::new(30, 0), // TODO: define default values
            command_timeout: Duration::new(60, 0),    // TODO: define default values
        })
    }

    // Generates a connection string from the DbConfig
    pub fn to_connection_string(&self) -> String {
        format!(
            "r2db2://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::io::Write;

    #[test]
    #[ignore = "Not yet implemented"]
    fn load_valid_config_from_file() {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"
        host = "localhost"
        port = 2345
        database = "test_db"
        user = "test_user"
        password = "test_pass"
        max_pool_size = 10
        connection_timeout_secs = 30
        command_timeout_secs = 60
    "#
        )
        .unwrap();
        let config_path = temp_file.path().to_str().unwrap();

        // Load the configuration
        let config_result = DbConfig::load_from_file_and_env(config_path);

        // Check if the configuration was loaded successfully
        assert!(config_result.is_ok());

        // Check the configuration values against the expected values
        if let Ok(config) = config_result {
            assert_eq!(config.host, "localhost");
            assert_eq!(config.port, 2345);
            assert_eq!(config.database, "test_db");
            assert_eq!(config.user, "test_user");
            assert_eq!(config.password, "test_pass");
            assert_eq!(config.max_pool_size, 10);
            assert_eq!(config.connection_timeout, Duration::new(30, 0));
            assert_eq!(config.command_timeout, Duration::new(60, 0));
        }
    }

    #[test]
    #[ignore = "Not yet implemented"]
    fn override_config_with_env_vars() {
        // Set environment variables to override
        env::set_var("APP__HOST", "env_host");
        env::set_var("APP__PORT", "5433");

        // Create a temporary config file
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"
            host = "localhost"
            port = 2345
        "#
        )
        .unwrap();
        let config_path = temp_file.path().to_str().unwrap();

        // Load the configuration
        let config = DbConfig::load_from_file_and_env(config_path).unwrap();

        // Environment variables should override file config
        assert_eq!(config.host, "env_host");
        assert_eq!(config.port, 5433);

        // Clean up environment variables
        env::remove_var("APP__HOST");
        env::remove_var("APP__PORT");
    }

    // ... other test cases
}

#[cfg(test)]
mod connection_string_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parse_valid_connection_string() {
        let conn_str = "r2db2://test_user:test_pass@localhost:2345/test_db";
        let config_result = DbConfig::from_connection_string(conn_str);

        assert!(config_result.is_ok());
        let config = config_result.unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 2345);
        assert_eq!(config.database, "test_db");
        assert_eq!(config.user, "test_user");
        assert_eq!(config.password, "test_pass");
    }

    #[test]
    fn serialize_to_connection_string() {
        let config = DbConfig {
            host: "localhost".to_string(),
            port: 2345,
            database: "test_db".to_string(),
            user: "test_user".to_string(),
            password: "test_pass".to_string(),
            max_pool_size: 10,
            connection_timeout: Duration::new(30, 0),
            command_timeout: Duration::new(60, 0),
        };

        let conn_str = config.to_connection_string();
        assert_eq!(
            conn_str,
            "r2db2://test_user:test_pass@localhost:2345/test_db"
        );
    }

    #[test]
    fn handle_invalid_connection_string() {
        let conn_str = "not_a_valid_connection_string";
        let config_result = DbConfig::from_connection_string(conn_str);
        assert!(config_result.is_err());
    }
}
