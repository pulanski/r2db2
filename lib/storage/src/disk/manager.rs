#[allow(unused_imports)]
use crate::disk::setup_dm;
use anyhow::Result;
use common::PAGE_SIZE;
use parking_lot::RwLock;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, error, info, instrument};

#[derive(Error, Debug)]
pub enum DiskManagerError {
    #[error("I/O error occurred: {0}")]
    IoError(String),

    #[error("Failed to perform async I/O operation")]
    AsyncIoError(#[from] tokio::io::Error),

    #[error("Page exceeds maximum allowed size of 4KB")]
    PageSizeError,
    // TODO: future other error types ...
    // TODO: more semantic error types (e.g. PageNotFound, etc.)
    // read/write errors
}

/// DiskManager handles disk-based storage operations.
/// It provides synchronous and asynchronous methods to read and write pages of data.
///
/// # Key Features
/// - Page-Based I/O: Operates at the granularity of pages.
/// - Async I/O Support: Incorporates async I/O operations using Tokio.
/// - Logging: Facilitates logging of operations using the `tracing` crate.
/// - Atomic Counters: Maintains counters for flushes and writes.
///
/// # Usage Scenarios
/// Ideal for high-throughput and low-latency disk access
#[derive(Debug)]
pub struct DiskManager {
    // Synchronous file handle for the database.
    db_io: Arc<RwLock<File>>,
    // Synchronous file handle for the log.
    log_io: Arc<RwLock<File>>,
    // File path for the database.
    db_file: String,
    // File path for the log.
    log_file: String,
    // Counter for the number of flushes to disk (used for statistics)
    num_flushes: AtomicU32,
    // Counter for the number of writes to disk (used for statistics)
    num_writes: AtomicU32,
}

impl DiskManager {
    pub fn new(db_file: &str) -> Result<Self> {
        let log_file = format!("{}.log", db_file);
        info!(
            "Initializing storage manager for `{}` with log file `{}`",
            db_file, log_file
        );

        if db_file.is_empty() {
            return Err(DiskManagerError::IoError(
                "Database file path cannot be empty".to_string(),
            )
            .into());
        }

        if !std::path::Path::new(db_file).exists() {
            debug!(
                "Database file {} does not exist. Creating a new database file",
                db_file
            );
        } else {
            debug!(
                "Database file {} exists. Opening the existing database file",
                db_file
            );
        }

        let db_io = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file)?;
        let log_io = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&log_file)?;

        Ok(Self {
            db_io: Arc::new(RwLock::new(db_io)),
            log_io: Arc::new(RwLock::new(log_io)),
            db_file: db_file.to_string(),
            log_file,
            num_flushes: AtomicU32::new(0),
            num_writes: AtomicU32::new(0),
        })
    }

    #[instrument(skip(self))]
    pub fn shut_down(&self) -> Result<()> {
        debug!(
            "[DiskManager::shut_down] Shutting down storage manager for {}",
            self.db_file
        );
        self.db_io.write().flush()?;
        self.log_io.write().flush()?;
        Ok(())
    }

    pub fn num_pages(&self) -> u32 {
        let db_io = self.db_io.read();
        let metadata = db_io.metadata().expect("Failed to read metadata");
        let file_size = metadata.len();
        debug!(
            "[DiskManager::num_pages] File size for {} is {} bytes",
            self.db_file, file_size
        );
        eprintln!(
            "[DiskManager::num_pages] File size for {} is {} bytes",
            self.db_file, file_size
        );

        // Round up to the nearest page
        ((file_size + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64) as u32
    }

    #[instrument(skip(self))]
    pub fn write_page(&self, page_id: u32, page_data: &[u8]) -> Result<()> {
        debug!(
            "[DiskManager::write_page] Writing page {} with {} bytes",
            page_id,
            page_data.len()
        );

        let mut db_io = self.db_io.write();
        db_io
            .seek(SeekFrom::Start((page_id as u64) * PAGE_SIZE as u64))
            .map_err(|e| {
                error!("Failed to seek to page {}: {}", page_id, e);
                e
            })?;

        db_io.write_all(page_data).map_err(|e| {
            error!("Failed to write page {}: {}", page_id, e);
            e
        })?;
        db_io.flush().map_err(|e| {
            error!("Failed to flush page {}: {}", page_id, e);
            e
        })?;
        info!("Page {} written successfully", page_id);

        self.num_flushes.fetch_add(1, Ordering::SeqCst);
        self.num_writes.fetch_add(1, Ordering::SeqCst);

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn write_page_async(&self, page_id: u32, page_data: &[u8]) -> Result<()> {
        if page_data.len() > PAGE_SIZE {
            return Err(DiskManagerError::PageSizeError.into());
        }

        debug!(
            "[DiskManager::write_page_async] Writing page {} (async) with {} bytes",
            page_id,
            page_data.len()
        );

        // If data itself is less than PAGE_SIZE, we need to pad it with zeros
        let mut page_data = page_data.to_vec();
        if page_data.len() < PAGE_SIZE {
            page_data.resize(PAGE_SIZE, 0);
        }

        let mut db_io = AsyncFile::options()
            .write(true)
            .create(true)
            .open(&self.db_file)
            .await
            .map_err(|e| {
                error!("Failed to open db file {}: {}", self.db_file, e);
                e
            })?;

        db_io
            .seek(SeekFrom::Start((page_id as u64) * PAGE_SIZE as u64))
            .await
            .map_err(|e| {
                error!("Failed to seek to page {}: {}", page_id, e);
                e
            })?;
        db_io.write_all(page_data.as_slice()).await.map_err(|e| {
            error!("Failed to write page {}: {}", page_id, e);
            e
        })?;
        db_io.flush().await?; // Explicitly flush the data to disk

        info!("Page {} written successfully (async)", page_id);
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn read_page(&self, page_id: u32, page_data: &mut [u8]) -> Result<()> {
        debug!(
            "[DiskManager::read_page] Reading page {} with {} bytes",
            page_id,
            page_data.len()
        );
        let mut db_io = File::options()
            .read(true)
            .open(&self.db_file)
            .map_err(|e| {
                error!("Failed to open db file {}: {}", self.db_file, e);
                e
            })?;

        db_io
            .seek(SeekFrom::Start((page_id as u64) * PAGE_SIZE as u64))
            .map_err(|e| {
                error!("Failed to seek to page {}: {}", page_id, e);
                e
            })?;
        let read_size = db_io.read(page_data).map_err(|e| {
            error!("Failed to read page {}: {}", page_id, e);
            e
        })?;

        if read_size < page_data.len() {
            page_data[read_size..].fill(0); // Fill the rest of the buffer with zeros
        }
        info!("Page {} read successfully", page_id);

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn read_page_async(&self, page_id: u32, page_data: &mut [u8]) -> Result<()> {
        debug!(
            "[DiskManager::read_page_async] Reading page {} (async) with {} bytes",
            page_id,
            page_data.len()
        );

        let mut db_io = AsyncFile::open(&self.db_file).await.map_err(|e| {
            error!("Failed to open db file {}: {}", self.db_file, e);
            e
        })?;

        db_io
            .seek(SeekFrom::Start((page_id as u64) * PAGE_SIZE as u64))
            .await
            .map_err(|e| {
                error!("Failed to seek to page {}: {}", page_id, e);
                e
            })?;

        db_io.read_exact(page_data).await.map_err(|e| {
            error!("Failed to read page {}: {}", page_id, e);
            e
        })?;

        info!("Page {} read successfully (async)", page_id);
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn write_data(&self, page_id: u32, data: &[u8]) -> anyhow::Result<()> {
        if data.len() > PAGE_SIZE {
            return Err(DiskManagerError::PageSizeError.into());
        }

        let mut page_data = vec![0; PAGE_SIZE];
        page_data[..data.len()].copy_from_slice(data);
        self.write_page(page_id, &page_data)?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn write_data_async(&self, page_id: u32, data: &[u8]) -> anyhow::Result<()> {
        if data.len() > PAGE_SIZE {
            return Err(DiskManagerError::PageSizeError.into());
        }

        let mut page_data = vec![0; PAGE_SIZE];
        page_data[..data.len()].copy_from_slice(data);
        self.write_page_async(page_id, &page_data).await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn read_data(&self, page_id: u32) -> anyhow::Result<Vec<u8>> {
        let mut page_data = vec![0; PAGE_SIZE];
        self.read_page(page_id, &mut page_data)?;
        Ok(page_data)
    }

    #[instrument(skip(self))]
    pub async fn read_data_async(&self, page_id: u32) -> anyhow::Result<Vec<u8>> {
        let mut page_data = vec![0; PAGE_SIZE];
        self.read_page_async(page_id, &mut page_data).await?;
        Ok(page_data)
    }

    #[instrument(skip(self))]
    pub fn write_log(&self, log_data: &[u8]) -> Result<()> {
        let mut log_io = self.log_io.write();

        info!("Writing log ({} bytes)", log_data.len());
        log_io.write_all(log_data).map_err(|e| {
            error!("Failed to write log: {}", e);
            e
        })?;
        log_io.flush().map_err(|e| {
            error!("Failed to flush log: {}", e);
            e
        })?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub fn read_log(&self, offset: u64, log_data: &mut [u8]) -> Result<()> {
        let mut log_io = File::options()
            .read(true)
            .open(&self.log_file)
            .map_err(|e| {
                error!("Failed to open log file {}: {}", self.log_file, e);
                e
            })?;

        info!("Reading log at offset {}", offset);
        log_io.seek(SeekFrom::Start(offset)).map_err(|e| {
            error!("Failed to seek to offset {}: {}", offset, e);
            e
        })?;

        let read_size = log_io.read(log_data).map_err(|e| {
            error!("Failed to read log: {}", e);
            e
        })?;

        if read_size < log_data.len() {
            log_data[read_size..].fill(0); // Fill the rest of the buffer with zeros
        }

        info!("Log read successfully");

        Ok(())
    }
}

#[cfg(test)]
mod single_thread_tests {
    use super::*;

    #[test]
    fn read_write_page_test() {
        let (dm, _temp_dir) = setup_dm();
        let mut buf = [0u8; PAGE_SIZE];
        let mut data = [0u8; PAGE_SIZE];
        data[..14].copy_from_slice(b"A test string.");

        // Tolerate empty read
        let _ = dm.read_page(0, &mut buf).expect("Failed to read page");

        dm.write_page(0, &data).expect("Failed to write page");
        dm.read_page(0, &mut buf).expect("Failed to read page");
        assert_eq!(buf, data);

        buf.fill(0);
        dm.write_page(5, &data).expect("Failed to write page");
        dm.read_page(5, &mut buf).expect("Failed to read page");
        assert_eq!(buf, data);
    }

    #[test]
    fn read_write_log_test() {
        let (dm, _temp_dir) = setup_dm();
        let mut buf = [0u8; PAGE_SIZE];
        let mut data = [0u8; PAGE_SIZE];
        let log_string = b"A log string.";
        data[..log_string.len()].copy_from_slice(log_string);

        dm.write_log(&data[..log_string.len()]).unwrap();
        dm.read_log(0, &mut buf).unwrap();
        assert_eq!(buf[..log_string.len()], data[..log_string.len()]);
    }

    #[test]
    fn throw_bad_file_test() {
        let result = DiskManager::new("dev/null\\/foo/bar/baz/test.db");
        assert!(result.is_err(), "Expected an error for bad file path");
    }
}

#[cfg(test)]
mod concurrent_tests {
    use super::*;
    use rand::{distributions::Alphanumeric, Rng};
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    const NUM_THREADS: usize = 10;
    const NUM_OPS: usize = 100;

    fn random_data(size: usize) -> Vec<u8> {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .collect()
    }

    #[test]
    fn concurrent_read_write_test() {
        let (dm, _temp_dir) = setup_dm();
        let barrier = Arc::new(Barrier::new(NUM_THREADS));

        let mut handles = vec![];

        for _ in 0..NUM_THREADS {
            let dm_clone = Arc::clone(&dm);
            let barrier_clone = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier_clone.wait();
                for _ in 0..NUM_OPS {
                    let data = random_data(PAGE_SIZE);
                    let page_id = rand::random::<u32>();
                    dm_clone.write_page(page_id, &data).unwrap();
                    let mut buf = [0u8; PAGE_SIZE];
                    dm_clone.read_page(page_id, &mut buf).unwrap();
                    assert_eq!(&buf[..], &data[..PAGE_SIZE]);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn async_read_write_page_test() {
        let (dm, _temp_dir) = setup_dm();
        let data = vec![1u8; PAGE_SIZE];
        let page_id: u32 = 0;

        dm.write_page_async(page_id, &data)
            .await
            .expect("Failed to write page async");
        let mut buf = vec![0u8; PAGE_SIZE];
        dm.read_page_async(page_id, &mut buf)
            .await
            .expect("Failed to read page async");
        assert_eq!(buf, data, "Async read data does not match written data");
    }
}

#[cfg(test)]
mod high_level_api_tests {
    use super::*;

    #[test]
    fn test_write_and_read_data() {
        let (dm, _temp_dir) = setup_dm();

        let data = b"Hello, DiskManager!".to_vec();
        dm.write_data(0, &data).unwrap();

        let read_data = dm.read_data(0).unwrap();
        assert_eq!(read_data[..data.len()], data[..]);
    }

    #[tokio::test]
    async fn test_write_and_read_data_async() {
        let (dm, _temp_dir) = setup_dm();

        let data = b"Hello, Async DiskManager!".to_vec();
        dm.write_data_async(0, &data).await.unwrap();

        let read_data = dm.read_data_async(0).await.unwrap();
        assert_eq!(read_data[..data.len()], data[..]);
    }
}
