#![allow(dead_code, unused_variables, unused_imports)]

use crate::replacer::{self, ReplacementPolicy};
use anyhow::Result;
use common::{FrameId, PageId, BUFFER_POOL_SIZE, PAGE_SIZE};
use dashmap::DashMap;
use rand::RngCore;
use std::sync::Arc;
use storage::{
    disk::{setup_dm, DiskManager, DiskScheduler, WriteStrategy},
    page::Page,
};
use thiserror::Error;
use tracing::{error, info, instrument};

#[derive(Error, Debug)]
pub enum BufferPoolError {
    #[error("Buffer pool is full")]
    PoolFull,
    #[error("Page not found in buffer pool")]
    PageNotFound,
    // TODO: More errors here...
}

pub struct BufferPoolManager {
    /// Replacement policy for keeping track of unpinned pages
    policy: ReplacementPolicy,
    /// Page table for keeping track of buffer pool pages (page_id -> frame_id)
    page_table: DashMap<PageId, FrameId>,
    /// Disk scheduler for reading/writing pages to disk
    disk_scheduler: Arc<DiskScheduler>,
    /// Replacer for keeping track of unpinned pages
    replacer: replacer::LRUReplacer, // TODO: replace with a trait object (Box<dyn Replacer>, pattern match on policy for impl)
    /// Next page id to be allocated
    next_page_id: Arc<PageId>,
    /// List of free frames
    free_list: Vec<FrameId>,
    /// Array of buffer pool frames/pages
    pool: Vec<Page>,
}

impl BufferPoolManager {
    // TODO: optional param for buffer pool size
    pub fn new(policy: ReplacementPolicy, disk_manager: Arc<DiskManager>) -> Self {
        let disk_scheduler = DiskScheduler::new(disk_manager);
        let free_list = (0..BUFFER_POOL_SIZE)
            .map(FrameId::from)
            .collect::<Vec<FrameId>>();

        Self {
            page_table: DashMap::new(),
            policy,
            disk_scheduler,
            free_list,
            replacer: replacer::LRUReplacer::new(BUFFER_POOL_SIZE),
            next_page_id: Arc::new(PageId::from(0)),
            pool: vec![Page::default(); BUFFER_POOL_SIZE],
        }
    }

    #[instrument(skip(self))]
    pub fn fetch_page(&self, page_id: PageId) -> Result<Option<Page>> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            info!("Page {} found in buffer pool", page_id);
            // Return the page from the buffer pool
            return Ok(Some(self.pool[frame_id.value().0 as usize].clone()));
        }

        info!(
            "Page {} not found in buffer pool, loading from disk",
            page_id
        );
        // Load page from disk using DiskScheduler
        // ...

        error!("Failed to fetch page {}", page_id);
        Ok(None)
    }

    #[instrument(skip(self))]
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) -> Result<()> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = &mut self.pool[frame_id.value().0 as usize];
            page.set_dirty(is_dirty);
            page.decrement_pin_count();
            info!("Unpinned page {}", page_id);
            return Ok(());
        }

        error!("Failed to unpin page {}: not found in buffer pool", page_id);
        Err(BufferPoolError::PageNotFound.into())
    }

    #[instrument(skip(self))]
    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let page = &self.pool[frame_id.value().0 as usize];
            if page.is_dirty() {
                // Write the page to disk
                // ...
                info!("Flushed page {} to disk", page_id);
            }
            return Ok(());
        }

        error!("Failed to flush page {}: not found in buffer pool", page_id);
        Err(BufferPoolError::PageNotFound.into())
    }

    pub fn new_page(&mut self) -> Result<(PageId, Page), BufferPoolError> {
        let page_id = *self.next_page_id;

        // Check if the free list is empty
        let frame_id = match self.free_list.pop() {
            Some(id) => id,
            None => return Err(BufferPoolError::PoolFull),
        };

        // Initialize a new page
        let page = Page::new(page_id, vec![0; PAGE_SIZE]);

        // Update the page table and buffer pool
        self.page_table.insert(page_id, frame_id);
        self.pool[frame_id.0 as usize] = page.clone();

        // Increment next page id
        self.next_page_id = Arc::new(PageId::from(page_id.0 + 1));

        Ok((page_id, page))
    }

    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        todo!("Impl delete_page()")
    }

    #[instrument(skip(self))]
    pub fn flush_all_pages(&self) -> Result<()> {
        for page_id in self.page_table.iter().map(|entry| *entry.key()) {
            self.flush_page(page_id)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_and_flush_page() {
        let (dm, _temp_dir) = setup_dm();
        let mut bpm = BufferPoolManager::new(ReplacementPolicy::LRU, dm);
        let (page_id, page) = bpm.new_page().expect("Failed to create new page");

        // Scenario: The buffer pool is empty. We should be able to create a new page.
        assert_eq!(page_id, PageId::from(0));
        assert_eq!(page.data().to_vec(), vec![0; PAGE_SIZE]);

        // Generate random (non-zero) data and write it to disk (via the disk scheduler)
        let mut data = vec![0; PAGE_SIZE];
        for (i, b) in data.iter_mut().enumerate() {
            *b = i as u8;
        }

        // Scenario: Once we have a page, we should be able to read and write content.
        bpm.disk_scheduler
            .schedule_write(page_id, data.clone(), WriteStrategy::Immediate)
            .await
            .expect("Failed to schedule write");

        // Assert data is written to disk...
        let page = bpm
            .disk_scheduler
            .schedule_read(page_id.0)
            .await
            .expect("Failed to schedule read");

        assert_eq!(page.to_vec(), data);

        // Scenario: We should be able to create new pages until we fill up the buffer
        // pool.
        for i in 1..BUFFER_POOL_SIZE {
            let (page_id, page) = bpm.new_page().expect("Failed to create new page");
            assert_eq!(page_id, PageId::from(i));
            assert_eq!(page.data().to_vec(), vec![0; PAGE_SIZE]);
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any
        // new pages.
        for _ in 0..BUFFER_POOL_SIZE {
            assert!(matches!(
                bpm.new_page().unwrap_err(),
                BufferPoolError::PoolFull
            ));
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to
        // create 5 new pages
        for i in 0..5 {
            bpm.unpin_page(PageId::from(i), true)
                .expect("Failed to unpin page");
            bpm.flush_page(PageId::from(i))
                .expect("Failed to flush page");
        }

        // TODO: Fix this test
        // for i in 0..5 {
        //     let (page_id, page) = bpm.new_page().expect("Failed to create new page");
        //     assert_eq!(page.data().to_vec(), vec![0; PAGE_SIZE]);
        //     // Unpin the page here to allow future fetching
        //     bpm.unpin_page(page_id, true).expect("Failed to unpin page");
        // }
    }
}
