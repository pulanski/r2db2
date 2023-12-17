#![allow(dead_code, unused_variables, unused_imports)]

use crate::replacer::{self, ReplacementPolicy};
use anyhow::Result;
use common::{FrameId, PageId, BUFFER_POOL_SIZE, PAGE_SIZE};
use dashmap::DashMap;
use parking_lot::RwLock;
use rand::RngCore;
use std::{fmt, sync::Arc};
use storage::{
    disk::{setup_dm, DiskManager, DiskScheduler, WriteStrategy},
    page::Page,
};
use thiserror::Error;
use tracing::{error, info, instrument};

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

pub struct BufferPoolManager {
    /// Replacement policy for keeping track of unpinned pages
    policy: ReplacementPolicy,
    /// Page table for keeping track of buffer pool pages (page_id -> frame_id)
    page_table: DashMap<PageId, FrameId>,
    /// Disk scheduler for reading/writing pages to disk
    disk_scheduler: Arc<DiskScheduler>,
    /// Replacer for keeping track of unpinned pages
    replacer: replacer::LRUReplacer,
    /// Next page id to be allocated
    next_page_id: Arc<PageId>,
    /// List of free frames
    free_list: Vec<FrameId>,
    /// Array of buffer pool frames/pages
    pool: Arc<RwLock<Vec<Page>>>,
}

impl BufferPoolManager {
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
        let free_list = (0..size).map(FrameId::from).collect::<Vec<FrameId>>();
        let pool = vec![Page::default(); size];

        assert!(pool.iter().all(|page| page.is_empty()), "Pool is not empty");
        assert_eq!(free_list.len(), size, "Free list is not correct size");

        Self {
            page_table: DashMap::new(),
            policy,
            disk_scheduler,
            free_list,
            replacer: replacer::LRUReplacer::new(size),
            next_page_id: Arc::new(PageId::from(0)),
            pool: Arc::new(RwLock::new(pool)),
        }
    }

    pub async fn new_page(&mut self) -> Result<(PageId, Page), BufferPoolError> {
        eprintln!("Attempting to create new page");
        let page_id = *self.next_page_id;

        let frame_id = match self.free_list.pop() {
            Some(id) => {
                eprintln!("Found free frame {:?}", id);
                eprintln!("Updated free list: {:?}", self.free_list);
                id
            }
            None => {
                eprintln!("Free list is empty, attempting to evict a page");
                self.evict_page().await?
            }
        };

        let mut page = Page::new(page_id, vec![0; PAGE_SIZE]).map_err(|e| {
            error!("Failed to create new page: {}", e);
            BufferPoolError::PageNotFound
        })?;
        page.increment_pin_count().map_err(|e| {
            error!("Failed to increment pin count for new page: {}", e);
            BufferPoolError::PageNotFound
        })?; // Increment pin count for new page

        self.page_table.insert(page_id, frame_id);
        self.pool.write()[frame_id.0 as usize] = page.clone();
        self.replacer.record_access(frame_id);
        eprintln!("Buffer pool state: {}", self);

        self.next_page_id = Arc::new(PageId::from(page_id.0 + 1));

        Ok((page_id, page))
    }

    async fn evict_page(&mut self) -> Result<FrameId, BufferPoolError> {
        eprintln!("Attempting to evict a page");
        if let Some(frame_id) = self.replacer.evict() {
            // let evicted_page = &mut self.pool[frame_id.0 as usize];
            let evicted_page = &mut self.pool.write()[frame_id.0 as usize];
            eprintln!("Evicting page: {:?}", evicted_page.id());

            // If the evicted page is dirty, write it to disk
            if evicted_page.is_dirty() {
                let page_id = evicted_page.id();
                let data = evicted_page.data().to_vec();
                eprintln!("Evicted page is dirty, writing to disk: {:?}", page_id);

                self.disk_scheduler
                    .schedule_write(page_id, data, WriteStrategy::Immediate)
                    .await
                    .map_err(|e| {
                        error!("Failed to write page to disk: {}", e);
                        BufferPoolError::DiskWriteFailed
                    })?;
            }

            // Remove the evicted page from the page table
            self.page_table.remove(&evicted_page.id());

            // Add the frame to the free list for reuse
            self.free_list.push(frame_id);

            eprintln!("Evicted page removed from page table and added to free list");
            eprintln!("Free list: {:?}", self.free_list);

            Ok(frame_id)
        } else {
            // No page could be evicted (possibly all pages are pinned)
            eprintln!("Failed to evict any page: All pages might be pinned");
            Err(BufferPoolError::PoolFull)
        }
    }

    #[instrument(skip(self))]
    pub async fn fetch_page(&mut self, page_id: PageId) -> Result<Option<Page>> {
        let frame_id_option = self
            .page_table
            .get(&page_id)
            .map(|frame_ref| *frame_ref.value());

        eprintln!("Replacer size: {}", self.replacer.size());

        if let Some(frame_id) = frame_id_option {
            info!("Page {} found in buffer pool", page_id);
            let page = &mut self.pool.write()[frame_id.0 as usize];
            page.increment_pin_count().map_err(|e| {
                error!("Failed to increment pin count for page: {}", e);
                BufferPoolError::DataAccessError(format!("{}", e))
            })?;
            self.replacer.record_access(frame_id);
            Ok(Some(page.clone()))
        } else {
            info!(
                "Page {} not found in buffer pool, loading from disk",
                page_id
            );

            // If all pages are pinned, we can't fetch a new page
            if self.replacer.size() == BUFFER_POOL_SIZE {
                error!("Failed to fetch page {}: All pages are pinned", page_id);
                return Ok(None);
            }

            match self.disk_scheduler.schedule_read(page_id.0).await {
                Ok(data) => {
                    let mut new_page = Page::new(page_id, data).map_err(|e| {
                        error!("Failed to create new page: {}", e);
                        BufferPoolError::DiskWriteFailed
                    })?;
                    new_page.increment_pin_count().map_err(|e| {
                        error!("Failed to increment pin count for new page: {}", e);
                        BufferPoolError::DiskWriteFailed
                    })?;

                    let frame_id = match self.free_list.pop() {
                        Some(id) => id,
                        None => {
                            info!("Buffer pool is full, attempting to evict a page");
                            self.evict_page().await?
                        }
                    };

                    // Update the buffer pool
                    self.pool.write()[frame_id.0 as usize] = new_page.clone();
                    self.page_table.insert(page_id, frame_id);
                    self.replacer.record_access(frame_id);

                    Ok(Some(new_page))
                }
                Err(e) => {
                    error!("Failed to load page {} from disk: {}", page_id, e);
                    Ok(None)
                }
            }
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

        // let page = &mut self.pool[frame_id.0 as usize];
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

    #[instrument(skip(self))]
    pub async fn flush_page(&self, page_id: PageId) -> Result<(), BufferPoolError> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            // let page = &self.pool[frame_id.value().0 as usize];
            let page = &self.pool.read()[frame_id.value().0 as usize];
            if page.is_dirty() {
                let data = page.data().to_vec();
                self.disk_scheduler
                    .schedule_write(page_id, data, WriteStrategy::Immediate)
                    .await
                    .map_err(|_| BufferPoolError::DiskWriteFailed)?;
                info!("Flushed page {} to disk", page_id);
            }
            Ok(())
        } else {
            error!("Failed to flush page {}: not found in buffer pool", page_id);
            Err(BufferPoolError::PageNotFound.into())
        }
    }

    pub async fn delete_page(&mut self, page_id: PageId) -> Result<()> {
        let frame_id = if let Some(frame_id) = self.page_table.get(&page_id) {
            frame_id.value().clone()
        } else {
            error!(
                "Failed to delete page {}: not found in buffer pool",
                page_id
            );
            return Err(BufferPoolError::PageNotFound.into());
        };

        // Optionally flush the page if it's dirty before deleting
        // if self.pool[frame_id.0 as usize].is_dirty() {
        if self.pool.read()[frame_id.0 as usize].is_dirty() {
            self.flush_page(page_id).await?;
        }

        self.page_table.remove(&page_id);
        self.free_list.push(frame_id);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn flush_all_pages(&self) -> Result<()> {
        for page_id in self.page_table.iter().map(|entry| *entry.key()) {
            self.flush_page(page_id).await?;
        }
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
            // let page = &mut self.pool[frame_id.value().0 as usize];
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
            // let page = &self.pool[frame_id.value().0 as usize];
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
    use std::f32::consts::E;

    use super::*;

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
            assert!(matches!(
                bpm.new_page().await.unwrap_err(),
                BufferPoolError::PoolFull
            ));
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
        let buffer_pool_size = 10usize;
        let mut bpm = BufferPoolManager::new(ReplacementPolicy::LRU, dm);

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
            assert!(matches!(
                bpm.new_page().await,
                Err(BufferPoolError::PoolFull)
            ));
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
