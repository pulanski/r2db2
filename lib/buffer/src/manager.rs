#![allow(dead_code, unused_variables)]

use anyhow::Result;
use common::PageId;
use dashmap::DashMap;
use std::collections::VecDeque;
use storage::page::Page;

use crate::replacer::ReplacementPolicy;

pub struct BufferPoolManager {
    pool: DashMap<PageId, Page>,
    policy: ReplacementPolicy,
    lru_queue: VecDeque<u32>, // LRU Queue
}

impl BufferPoolManager {
    pub fn new(policy: ReplacementPolicy) -> Self {
        Self {
            pool: DashMap::new(),
            policy,
            lru_queue: VecDeque::new(),
        }
    }

    pub fn fetch_page(&self, page_id: PageId) -> Result<Option<Page>> {
        todo!("Impl fetch_page()")
    }

    pub fn flush_page(&self, page_id: PageId) -> Result<()> {
        if let Some(mut page) = self.pool.get_mut(&page_id) {
            if page.is_dirty() {
                // TODO: Flush to disk
                page.set_is_dirty(false);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "TODO: Remove ignore once BufferPoolManager::fetch_page() is implemented"]
    fn test_fetch_and_flush_page() {
        let manager = BufferPoolManager::new(ReplacementPolicy::LRU);
        let page_id = PageId::from(1);
        let data = vec![1, 2, 3, 4, 5];

        // Simulate adding a page
        manager.pool.insert(
            page_id,
            Page::builder()
                .id(page_id)
                .data(data.clone())
                .is_dirty(true)
                .pin_count(0)
                .build(),
        );

        // Fetch the page
        let fetched_page = manager.fetch_page(page_id).unwrap();
        assert!(fetched_page.is_some());
        assert_eq!(fetched_page.unwrap().data(), data.as_slice());

        // Flush the page
        manager.flush_page(page_id).unwrap();

        // Verify the page is no longer dirty
        let page = manager.pool.get(&page_id).unwrap();
        assert!(!page.is_dirty());
    }

    // More tests...
}
