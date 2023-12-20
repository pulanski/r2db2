//! # Buffer Pool Manager
//!
//! Buffer Pool Manager for a database system, providing efficient management of database pages in memory.
//! The Buffer Pool Manager plays a critical role in database performance, offering a layer between the
//! physical disk storage and the database operations, thereby minimizing disk I/O operations.
//!
//! ## Architecture
//!
//! ```plaintext
//! +--------------------------------------------------+
//! |        BufferPoolManager (size = N frames)       |
//! |  +--------------------------------------------+  |
//! |  |               Page Table                   |  |
//! |  |  +----------+  +---------+  +-----------+  |  |
//! |  |  | PageId i |->| Frame 1 |  | Page Data |  |  |
//! |  |  +----------+  +---------+  +-----------+  |  |
//! |  |  | PageId j |->| Frame 2 |  | Page Data |  |  |
//! |  |  +----------+  +---------+  +-----------+  |  |
//! |  |  |   ...    |  |   ...   |  |    ...    |  |  |
//! |  |  +----------+  +---------+  +-----------+  |  |
//! |  |  | PageId z |->| Frame N |  | Page Data |  |  |
//! |  |  +----------+  +---------+  +-----------+  |  |
//! |  +--------------------------------------------+  |
//! |                                                  |
//! |  +--------------------------------------------+  |
//! |  |         Replacement Policy (LRU)           |  |
//! |  |  +------------+  +------------+  +-------+ |  |
//! |  |  | Frame 1    |  | Frame 2    |  |  ...  | |  |
//! |  |  +------------+  +------------+  +-------+ |  |
//! |  +--------------------------------------------+  |
//! |                                                  |
//! |  +--------------------------------------------+  |
//! |  |              Disk Scheduler                |  |
//! |  |  +-----------+  +-----------+  +---------+ |  |
//! |  |  | Read/Write|  | Read/Write|  |   ...   | |  |
//! |  |  +-----------+  +-----------+  +---------+ |  |
//! |  +--------------------------------------------+  |
//! +--------------------------------------------------+
//! ```
//!
//! ## Data Flow (High-Level)
//!
//! 1. Fetch Page:
//!    - Check PageTable for PageId.
//!    - If not in buffer pool, read from disk (Disk Scheduler) and allocate a frame.
//!
//! 2. New Page:
//!    - Allocate a frame from the free list.
//!    - If the free list is empty, use LRU policy to evict and write a page to disk if dirty.
//!
//! 3. Write Data:
//!    - Write data to the page in the buffer pool.
//!    - Mark page as dirty.
//!
//! 4. Eviction:
//!    - Based on LRU policy, select a frame to evict.
//!    - If the page is dirty, write it to disk (Disk Scheduler) before eviction.
//!
//! ## Functionality
//!
//! The Buffer Pool Manager optimizes database operations by keeping frequently accessed pages in memory.
//! Pages are loaded into the buffer pool from disk on demand and are written back to the disk only when
//! necessary. This reduces disk I/O, which is often considered to be the _"high pole in the tent"_ traditionally
//! limiting database performance.
//!
//! ## Design Considerations
//!
//! - **Concurrency Control**: The buffer pool manager is designed to be thread-safe, allowing for concurrent
//!  access to the buffer pool from multiple workers (e.g., threads, coroutines, etc.) and transactions.
//!
//! - **Flexibility**: The architecture allows for different replacement policies and disk schedulers to be
//!   plugged in, making it adaptable to various performance and usage patterns (e.g., OLTP vs OLAP workloads).
//!
//! - **Scalability**: Designed to handle a large number of pages and concurrent operations, scaling with the
//!   needs of the database system.
//!
//! ## Examples
//!
//! ```no_run
//! use buffer::{BufferPoolManager, ReplacementPolicy};
//! use storage::disk::DiskManager;
//! use std::sync::Arc;
//!
//! let disk_manager = Arc::new(DiskManager::new("path/to/dbfile").expect("Failed to start disk manager"));
//! let buffer_pool_manager = BufferPoolManager::new(ReplacementPolicy::LRU, disk_manager);
//!
//! // Operations such as creating new pages, fetching pages, and writing data can be performed
//! // on the buffer_pool_manager instance.
//!
//! // More examples are demonstrated in the tests below ...
//! ```
//!
//! ## Further Development
//!
//! Future enhancements could include more sophisticated replacement policies, better integration with
//! transaction management for ensuring data consistency, and advanced performance tuning options.
#![allow(dead_code, unused_variables, unused_imports)]

use crate::{
    replacer::{self, ReplacementPolicy},
    LRUReplacer,
};
use anyhow::Result;
use common::{FrameId, PageId, BUFFER_POOL_SIZE, PAGE_SIZE};
use dashmap::DashMap;
use getset::{Getters, Setters};
use parking_lot::RwLock;
use rand::RngCore;
use std::{fmt, sync::Arc};
use storage::{
    disk::{setup_dm, DiskManager, DiskScheduler, WriteStrategy},
    page::Page,
};
use thiserror::Error;
use tracing::{debug, error, info, instrument, trace, warn};
use typed_builder::TypedBuilder;

#[derive(Error, Debug)]
pub enum BufferPoolError {
    #[error("Buffer pool is full. No frames available for eviction.")]
    PoolFull,
    #[error("Page not found in buffer pool")]
    PageNotFound,
    #[error("Write to disk failed")]
    DiskWriteFailed,
    #[error("Data access error: {0}")]
    DataAccessError(String),
    // ...
}

/// The `BufferPoolManager` manages a buffer pool for pages in a database system.
///
/// This manager handles operations such as creating new pages, fetching pages from disk,
/// writing pages to disk, and managing the eviction of pages based on a replacement policy.
/// It uses a `DashMap` for concurrent access to the page table and a `RwLock` to manage
/// the buffer pool frames.
///
/// # Examples
///
/// Basic usage:
///
/// ```ignore
/// use buffer::{BufferPoolManager, ReplacementPolicy};
/// use storage::DiskManager;
/// use std::sync::Arc;
///
/// let disk_manager = Arc::new(DiskManager::new("path/to/dbfile").expect("Failed to start disk manager"));
/// let buffer_pool_manager = BufferPoolManager::new(ReplacementPolicy::LRU, disk_manager);
///
/// // Create a new page
/// let (page_id, page) = buffer_pool_manager.new_page().await.expect("Failed to create new page");
///
/// // Read and write data to a page
/// buffer_pool_manager.write_data(page_id, &data).await.expect("Failed to write data");
/// let data = buffer_pool_manager.read_data(page_id).await.expect("Failed to read data");
/// ```
#[derive(Debug, Getters, Setters, TypedBuilder)]
pub struct BufferPoolManager {
    /// Page table for keeping track of buffer pool pages (page_id -> frame_id)
    page_table: DashMap<PageId, FrameId>,
    /// Disk scheduler for reading/writing pages to disk
    disk_scheduler: Arc<DiskScheduler>,
    /// Replacer for keeping track of unpinned pages
    replacer: replacer::LRUReplacer,
    /// List of free frames
    free_list: Vec<FrameId>,
    /// Next page id to be allocated
    #[getset(get = "pub")]
    next_page_id: Arc<PageId>,
    /// Array of buffer pool frames/pages
    #[getset(get = "pub")]
    pool: Arc<RwLock<Vec<Page>>>,
    /// Replacement policy for keeping track of unpinned pages
    #[getset(get = "pub", set = "pub")]
    policy: ReplacementPolicy,
}

impl BufferPoolManager {
    /// Constructs a new [`BufferPoolManager`] with a given replacement policy and disk manager.
    /// Initializes the page table, free list, and buffer pool frames.
    ///
    /// # Arguments
    ///
    /// * `policy`: The replacement policy to use for page eviction.
    /// * `disk_manager`: Shared reference to the disk manager for I/O operations.
    ///
    /// # Usage
    ///
    /// ```no_run,ignore
    /// use buffer::{BufferPoolManager, ReplacementPolicy};
    /// use storage::DiskManager;
    /// use std::sync::Arc;
    ///
    /// let disk_manager = Arc::new(DiskManager::new("path/to/dbfile"));
    /// let buffer_pool_manager = BufferPoolManager::new(ReplacementPolicy::LRU, disk_manager);
    /// ```
    #[instrument(level = "trace")]
    pub fn new(policy: ReplacementPolicy, disk_manager: Arc<DiskManager>) -> Self {
        let disk_scheduler = DiskScheduler::new(disk_manager);
        let free_list = (0..BUFFER_POOL_SIZE)
            .map(FrameId::from)
            .collect::<Vec<FrameId>>();
        let replacer = replacer::LRUReplacer::new(BUFFER_POOL_SIZE);

        assert_eq!(free_list.len(), BUFFER_POOL_SIZE);

        eprintln!("Free list: {:?}", free_list);
        eprintln!("Replacer: {}", replacer);

        Self {
            page_table: DashMap::new(),
            policy,
            disk_scheduler,
            free_list,
            replacer,
            next_page_id: Arc::new(PageId::from(0)),
            pool: Arc::new(RwLock::new(vec![Page::default(); BUFFER_POOL_SIZE])),
        }
    }

    pub fn new_with_size(
        policy: ReplacementPolicy,
        disk_manager: Arc<DiskManager>,
        size: usize,
    ) -> Self {
        let disk_scheduler = DiskScheduler::new(disk_manager);
        // make sure buffer pool size is a power of 2 for bit masking (at least 1 frame)
        let size = size.next_power_of_two().max(1).min(BUFFER_POOL_SIZE);
        debug!("Initializing buffer pool with size {}", size);

        let free_list = (0..size).map(FrameId::from).collect::<Vec<FrameId>>();
        let pool = vec![Page::default(); size];

        assert!(pool.iter().all(|page| page.is_empty()), "Pool is not empty");
        assert_eq!(free_list.len(), size, "Free list is not correct size");

        Self {
            page_table: DashMap::new(),
            policy,
            disk_scheduler,
            free_list,
            replacer: LRUReplacer::new(size),
            next_page_id: Arc::new(PageId::from(0)),
            pool: Arc::new(RwLock::new(pool)),
        }
    }

    /// Creates a new page in the buffer pool. If necessary, evicts an existing page.
    ///
    /// This method allocates a new frame from the free list or evicts a page using the
    /// replacement policy if the free list is empty. It then creates a new page with
    /// default data and increments its pin count.
    ///
    /// # Returns
    ///
    /// Returns a tuple containing the [`PageId`] of the new page and the [`Page`] itself,
    /// or an error if the buffer pool is full and no page can be evicted.
    ///
    /// # Example
    ///
    /// ```no_run,ignore
    /// use buffer::{BufferPoolManager, ReplacementPolicy};
    /// use storage::DiskManager;
    /// use std::sync::Arc;
    ///
    /// let disk_manager = Arc::new(DiskManager::new("path/to/dbfile").expect("Failed to start disk manager"));
    /// let mut buffer_pool_manager = BufferPoolManager::new(ReplacementPolicy::LRU, disk_manager);
    ///
    /// let (page_id, page) = buffer_pool_manager.new_page().await.expect("Failed to create new page");
    /// ```
    pub async fn new_page(&mut self) -> Result<(PageId, Page)> {
        eprintln!("Attempting to create new page");
        let page_id = *self.next_page_id;
        let frame_id = self.allocate_frame().await?;

        let mut page = Page::new(page_id, vec![0; PAGE_SIZE])?;
        page.increment_pin_count()?;

        self.update_pool_state_on_new_page(page_id, frame_id, page.clone());
        eprintln!("Buffer pool state: {}", self);

        Ok((page_id, page))
    }

    async fn allocate_frame(&mut self) -> Result<FrameId, BufferPoolError> {
        if let Some(frame_id) = self.free_list.pop() {
            // Frame available in the free list
            Ok(frame_id)
        } else {
            // Attempt to evict a page if free list is empty
            self.evict_page().await
        }
    }

    fn update_pool_state_on_new_page(&mut self, page_id: PageId, frame_id: FrameId, page: Page) {
        self.page_table.insert(page_id, frame_id);
        let mut pool = self.pool.write();
        pool[frame_id.0 as usize] = page;
        self.replacer.record_access(frame_id);
        self.next_page_id = Arc::new(PageId::from(page_id.0 + 1));
    }

    /// Evicts a page from the buffer pool based on the replacement policy.
    ///
    /// This method is called when the buffer pool needs space for new pages. It uses the `replacer`
    /// to determine which page to evict. If the selected page is dirty, it writes the page to disk
    /// before eviction. This ensures that no data is lost from memory.
    ///
    /// # Errors
    ///
    /// Returns `BufferPoolError::PoolFull` if all pages are pinned and cannot be evicted.
    ///
    /// # Example
    ///
    /// ```no_run,ignore
    /// use buffer::{BufferPoolManager, ReplacementPolicy};
    /// use storage::DiskManager;
    /// use std::sync::Arc;
    ///
    /// let disk_manager = Arc::new(DiskManager::new("path/to/dbfile").expect("Failed to start disk manager"));
    /// let mut buffer_pool_manager = BufferPoolManager::new(ReplacementPolicy::LRU, disk_manager);
    ///
    /// // Operations such as creating new pages, fetching pages, and writing data can be performed
    /// // ...
    ///
    /// let frame_id = buffer_pool_manager.evict_page().await.expect("Failed to evict page");
    /// ```
    async fn evict_page(&mut self) -> Result<FrameId, BufferPoolError> {
        eprintln!("Attempting to evict a page");
        if let Some(frame_id) = self.replacer.evict() {
            let evicted_page = self.pool.write()[frame_id.0 as usize].clone();
            if evicted_page.is_dirty() {
                self.write_page_to_disk(&evicted_page).await?;
            }

            self.page_table.remove(&evicted_page.id());
            self.free_list.push(frame_id);
            Ok(frame_id)
        } else {
            // No page could be evicted (possibly all pages are pinned)
            eprintln!("Failed to evict any page: All pages might be pinned");
            Err(BufferPoolError::PoolFull)
        }
    }

    async fn write_page_to_disk(&self, page: &Page) -> Result<(), BufferPoolError> {
        let data = page.data().to_vec();
        self.disk_scheduler
            .schedule_write(page.id(), data, WriteStrategy::Immediate)
            .await
            .map_err(|_| BufferPoolError::DiskWriteFailed)
    }

    fn increment_pin_and_return_page(&mut self, frame_id: FrameId) -> Result<Page> {
        let mut pool = self.pool.write();
        let page = &mut pool[frame_id.0 as usize];
        page.increment_pin_count()?;
        self.replacer.record_access(frame_id);
        Ok(page.clone())
    }

    async fn load_page_from_disk(&mut self, page_id: PageId) -> Result<Option<Page>> {
        if self.replacer.size() == BUFFER_POOL_SIZE {
            warn!("All pages are pinned, unable to fetch new page.");
            return Ok(None);
        }

        match self.disk_scheduler.schedule_read(page_id.0).await {
            Ok(data) => self.allocate_and_load_page(page_id, data).await,
            Err(e) => {
                error!("Failed to load page {} from disk: {}", page_id, e);
                Ok(None)
            }
        }
    }

    async fn allocate_and_load_page(
        &mut self,
        page_id: PageId,
        data: Vec<u8>,
    ) -> Result<Option<Page>> {
        let frame_id = self.allocate_frame().await?;
        let mut new_page = Page::new(page_id, data)
            .map_err(|e| BufferPoolError::DataAccessError(e.to_string()))?;

        new_page.increment_pin_count()?;
        self.update_pool_state_on_new_page(page_id, frame_id, new_page.clone());
        Ok(Some(new_page))
    }

    /// Fetches a page with the specified `page_id` from the buffer pool. If the page is not already resident
    /// within DRAM and in the buffer pool, it is consequently loaded from disk, which might involve evicting
    /// another page if the buffer pool is full.
    ///
    /// # Returns
    ///
    /// Returns `Option<Page>` which is `Some(Page)` if the page is successfully fetched or loaded, or `None` if
    /// the page cannot be fetched because the buffer pool is full and all pages are pinned.
    ///
    /// # Errors
    ///
    /// Returns `BufferPoolError` in case of failures in reading from disk or internal buffer pool errors.
    ///
    /// # Example
    ///
    /// ```no_run,ignore
    /// use common::PageId;
    /// use buffer::{BufferPoolManager, ReplacementPolicy};
    /// use storage::disk::DiskManager;
    /// use std::sync::Arc;
    ///
    /// let disk_manager = Arc::new(DiskManager::new("path/to/dbfile"));
    /// let buffer_pool_manager = BufferPoolManager::new(ReplacementPolicy::LRU, disk_manager);
    /// let page_id = PageId::from(0);
    ///
    /// let maybe_page = buffer_pool_manager.fetch_page(page_id).await.expect("Failed to fetch page");
    /// ```
    #[instrument(skip(self), level = "info")]
    pub async fn fetch_page(&mut self, page_id: PageId) -> Result<Option<Page>> {
        if let Some(frame_id) = self
            .page_table
            .get(&page_id)
            .map(|frame_ref| *frame_ref.value())
        {
            let page = self.increment_pin_and_return_page(frame_id)?;
            Ok(Some(page))
        } else {
            self.load_page_from_disk(page_id).await
        }
    }

    #[instrument(skip(self))]
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        eprintln!("Unpinning page: {:?}", page_id);
        eprintln!("Buffer pool state: {}", self);
        let frame_id = if let Some(frame_id) = self.page_table.get(&page_id) {
            frame_id.value().clone()
        } else {
            eprintln!(
                "Failed to unpin page {:?}: not found in buffer pool",
                page_id
            );
            return Err(BufferPoolError::PageNotFound.into());
        };

        let page = &mut self.pool.write()[frame_id.0 as usize];
        page.set_dirty(is_dirty);
        page.decrement_pin_count();
        eprintln!("Unpinned page {}, pin count: {}", page_id, page.pin_count());

        if page.pin_count() == 0 {
            self.replacer.set_evictable(frame_id, true);
            eprintln!("Page {} is now evictable", page_id);
        }

        eprintln!("Buffer pool state: {}", self);
        Ok(())
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn flush_page(&self, page_id: PageId) -> Result<(), BufferPoolError> {
        let frame_id = self
            .find_frame(page_id)
            .ok_or(BufferPoolError::PageNotFound)?;

        let page = self.pool.read()[frame_id.0 as usize].clone();
        if page.is_dirty() {
            self.write_page_to_disk(&page).await?;
            info!("Flushed page {} to disk", page_id);
        }

        Ok(())
    }

    pub fn find_frame(&self, page_id: PageId) -> Option<FrameId> {
        self.page_table
            .get(&page_id)
            .map(|frame_ref| *frame_ref.value())
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn delete_page(&mut self, page_id: PageId) -> Result<()> {
        let frame_id = self
            .find_frame(page_id)
            .ok_or(BufferPoolError::PageNotFound)?;

        if self.pool.read()[frame_id.0 as usize].is_dirty() {
            self.flush_page(page_id).await?;
        }

        self.page_table.remove(&page_id);
        self.free_list.push(frame_id);
        Ok(())
    }

    #[instrument(skip(self), level = "info")]
    pub async fn flush_all_pages(&self) -> Result<(), BufferPoolError> {
        trace!("Flushing all pages");
        let pool = self.pool.read();
        let dirty_pages: Vec<_> = pool.iter().filter(|page| page.is_dirty()).collect();

        if dirty_pages.is_empty() {
            trace!("No dirty pages to flush");
            return Ok(());
        }

        let batch: Vec<_> = dirty_pages
            .into_iter()
            .map(|page| (page.id(), page.data().to_vec()))
            .collect();

        self.disk_scheduler.batch_write(batch).await.map_err(|_| {
            error!("Failed to flush all pages");
            BufferPoolError::DiskWriteFailed
        })?;

        trace!("All dirty pages flushed");
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn reset(&mut self) -> Result<()> {
        self.flush_all_pages().await?;
        self.page_table.clear();
        self.free_list = (0..BUFFER_POOL_SIZE).map(FrameId::from).collect();
        self.replacer = replacer::LRUReplacer::new(BUFFER_POOL_SIZE);
        self.next_page_id = Arc::new(PageId::from(0));
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn write_data(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = &mut self.pool.write()[frame_id.value().0 as usize];
            page.write_data(data);
            page.set_dirty(true);
            Ok(())
        } else {
            error!(
                "Failed to write data to page {}: not found in buffer pool",
                page_id
            );
            Err(BufferPoolError::PageNotFound.into())
        }
    }

    #[instrument(skip(self))]
    pub async fn read_data(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = &mut self.pool.write()[frame_id.value().0 as usize];
            let data = page.read_data();
            Ok(data)
        } else {
            error!(
                "Failed to read data from page {}: not found in buffer pool",
                page_id
            );
            Err(BufferPoolError::PageNotFound.into())
        }
    }
}

impl fmt::Display for BufferPoolManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BufferPoolManager (size: {})\n", BUFFER_POOL_SIZE)?;
        write!(f, "Free list: {:?}\n", self.free_list)?;
        write!(f, "Replacer: {}\n", self.replacer)?;
        write!(f, "Page table:\n")?;
        for (page_id, frame_id) in self
            .page_table
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
        {
            write!(f, " {:?} -> {:?}\n", frame_id, page_id)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod buffer_pool_manager_tests {
    use super::*;
    use anyhow::Error;

    #[tokio::test]
    async fn test_fetch_page() {
        // Setup
        let (dm, _temp_dir) = setup_dm();
        let mut bpm = BufferPoolManager::new(ReplacementPolicy::LRU, dm);

        // Create a new page
        let (page_id, page) = bpm.new_page().await.unwrap();

        assert_eq!(page_id, PageId::from(0));
        assert_eq!(page.data().to_vec(), vec![0; PAGE_SIZE]);
        assert_eq!(page.pin_count(), 1);

        // Fetch the page
        let fetched_page = bpm
            .fetch_page(page_id)
            .await
            .expect("Failed to fetch page")
            .expect("Page not found");

        assert_eq!(fetched_page.id(), page_id);
        assert_eq!(fetched_page.data(), page.data());
        assert_eq!(fetched_page.pin_count(), 2); // 1 from new_page() and 1 from fetch_page()
        assert!(!fetched_page.is_dirty());
    }

    #[tokio::test]
    async fn test_fetch_and_flush_page() {
        let (dm, _temp_dir) = setup_dm();
        let mut bpm = BufferPoolManager::new(ReplacementPolicy::LRU, dm);

        // Create new pages until the buffer pool is full
        for i in 0..BUFFER_POOL_SIZE {
            let (page_id, page) = bpm.new_page().await.expect("Failed to create new page");
            assert_eq!(page_id, PageId::from(i));
            assert_eq!(page.data().to_vec(), vec![0; PAGE_SIZE]);
        }

        // Now, creating new pages should fail as the buffer pool is full
        for _ in 0..BUFFER_POOL_SIZE {
            let result = bpm.new_page().await;
            match result {
                Err(err) => {
                    if let Some(BufferPoolError::PoolFull) = err.downcast_ref::<BufferPoolError>() {
                        // Correctly matched PoolFull error
                    } else {
                        panic!("Expected BufferPoolError::PoolFull, found {:?}", err);
                    }
                }
                _ => panic!("Expected an error, but got {:?}", result),
            }
        }

        // Unpinning pages {0, 1, 2, 3, 4}
        for i in 0..5 {
            bpm.unpin_page(PageId::from(i), true).unwrap();
            bpm.flush_page(PageId::from(i))
                .await
                .expect("Failed to flush page");

            // Assert page is unpinned
            let frame = bpm
                .page_table
                .get(&PageId::from(i))
                .expect("Page not found in page table")
                .value()
                .clone();
            // let page = &bpm.pool[frame.0 as usize];
            let page = &bpm.pool.read()[frame.0 as usize];
            assert_eq!(page.pin_count(), 0);
        }

        // Now, creating new pages should be successful as previous pages are unpinned
        for i in 5..10 {
            bpm.new_page()
                .await
                .expect(&format!("Failed to create new page {}", i));
        }

        // Unpin pages to prevent future fetching
        for i in 5..10 {
            bpm.unpin_page(PageId::from(i), true).unwrap();

            // Assert page is unpinned
            let frame = bpm
                .page_table
                .get(&PageId::from(i))
                .expect("Page not found in page table")
                .value()
                .clone();
            // let page = &bpm.pool[frame.0 as usize];
            let page = &bpm.pool.read()[frame.0 as usize];
            assert_eq!(page.pin_count(), 0);
        }
    }

    #[tokio::test]
    async fn test_sample() {
        let (dm, _temp_dir) = setup_dm();
        let mut bpm = BufferPoolManager::new(ReplacementPolicy::LRU, dm);
        let buffer_pool_size = 10usize;

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        eprintln!(
            "\nScenario: The buffer pool is empty. We should be able to create a new page.\n"
        );
        let (page_id0, page0) = bpm.new_page().await.expect("Failed to create new page");
        assert_eq!(page_id0, PageId::from(0));

        // Scenario: Once we have a page, we should be able to read and write content.
        eprintln!(
            "\nScenario: Once we have a page, we should be able to read and write content.\n"
        );
        bpm.write_data(page_id0, "Hello".as_bytes())
            .await
            .expect("Failed to write data");
        let fetched_data = bpm.read_data(page_id0).await.expect("Failed to read data");
        let expected_data = "Hello".as_bytes();

        // Assert the fetched data matches the expected data
        assert_eq!(
            fetched_data[..5],
            expected_data[..5],
            "Read data does not match expected data"
        );

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        eprintln!(
            "\nScenario: We should be able to create new pages until we fill up the buffer pool.\n"
        );
        for i in 1..buffer_pool_size {
            let (page_id, page) = bpm
                .new_page()
                .await
                .expect(&format!("Failed to create new page {}", i));
            assert_eq!(page_id, PageId::from(i));
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        eprintln!("\nScenario: Once the buffer pool is full, we should not be able to create any new pages.\n");
        // TODO: Fix bug in here, where when we try to create a new page and the buffer
        // pool is full, we get a BufferPoolError::PoolFull error, but we're updating
        // the state of the lrureplacer, which is incorrect.
        for _ in 0..buffer_pool_size {
            let result = bpm.new_page().await;
            match result {
                Err(err) => {
                    if let Some(BufferPoolError::PoolFull) = err.downcast_ref::<BufferPoolError>() {
                        // Correctly matched PoolFull error
                    } else {
                        panic!("Expected BufferPoolError::PoolFull, found {:?}", err);
                    }
                }
                _ => panic!("Expected an error, but got {:?}", result),
            }
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
        // there would still be one buffer page left for reading page 0.
        eprintln!("\nScenario: Unpinning pages [0, 1, 2, 3, 4] and pinning another 4 new pages, there would still be one buffer page left for reading page 0.\n");
        eprintln!("Buffer pool state: {}", bpm);
        for i in 0..5 {
            bpm.unpin_page(PageId::from(i), true)
                .expect("Failed to unpin page");
        }
        for _ in 0..4 {
            assert!(bpm.new_page().await.is_ok());
        }
        assert!(bpm.fetch_page(PageId::from(0)).await.is_ok());

        // Scenario: We should be able to fetch the data we wrote a while ago.
        let mut fetched_page0 = bpm
            .fetch_page(PageId::from(0))
            .await
            .expect("Failed to fetch page")
            .unwrap();
        assert_eq!(fetched_page0.read_data()[..5], expected_data[..5]);

        // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
        // now be pinned. Fetching page 0 again should fail.
        bpm.unpin_page(PageId::from(0), true).unwrap();
        assert!(bpm.new_page().await.is_ok());
        // assert!(bpm.fetch_page(PageId::from(0)).await.is_err()); // TODO: Fix this (currently fails)
    }
}

#[cfg(test)]
mod batch_write_tests {
    use super::*;

    #[tokio::test]
    async fn test_flush_all_pages_empty_pool() {
        let bpm = setup_bpm();
        assert!(bpm.flush_all_pages().await.is_ok());

        // TODO: Add support for logging operations w/ log manager
        // Verify that no write operations were logged
        // let log = bpm.disk_scheduler.write_log.read().await;
        // assert!(log.is_empty());
    }

    #[tokio::test]
    #[ignore = "Not yet implemented"]
    async fn test_flush_all_pages_with_dirty_pages() {
        let bpm = setup_bpm();
        // Simulate dirty pages
        // ...

        // assert!(bpm.flush_all_pages().await.is_ok());
        // // Verify that correct write operations were logged
        // let log = bpm.disk_scheduler.write_log.lock().await;
        // assert_eq!(log.len() /* expected number of writes */,);
        // // Additional assertions to verify the content of writes
    }
}

#[cfg(test)]
mod buffer_pool_partitioning_tests {
    use super::*;

    #[tokio::test]
    async fn test_page_marking_hot_and_cold() {
        let bpm = setup_bpm();

        // Mark certain pages as hot and others as cold
        // ...

        // Verify correct partitioning
        // assert!(bpm.hot_pages.read().await.contains(&/* some page id */));
        // assert!(bpm.cold_pages.read().await.contains(&/* some page id */));
    }
}

pub fn setup_bpm() -> BufferPoolManager {
    let (dm, _temp_dir) = setup_dm();
    BufferPoolManager::new(ReplacementPolicy::LRU, dm)
}
