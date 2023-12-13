use std::num::NonZeroUsize;

use common::FrameId;

use lru::LruCache;
use tracing::{error, info, warn};

pub struct LRUReplacer {
    cache: LruCache<FrameId, bool>, // Stores whether a frame is evictable or not
}

impl LRUReplacer {
    pub fn new(capacity: usize) -> Self {
        info!("Creating a new LRUReplacer with capacity {}", capacity);
        LRUReplacer {
            cache: LruCache::new(NonZeroUsize::new(capacity).expect("Capacity must be non-zero")),
        }
    }

    pub fn record_access(&mut self, frame_id: FrameId) {
        if self.cache.contains(&frame_id) {
            info!("Accessing existing frame {:?}", frame_id);
        } else {
            info!("Adding new frame {:?}", frame_id);
        }
        self.cache.put(frame_id, true); // all frames are evictable by default
    }

    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        if let Some(_) = self.cache.get(&frame_id) {
            // Remove and reinsert the frame to update its position in the LRU order
            self.cache.pop(&frame_id);
            self.cache.put(frame_id, evictable);
            eprintln!(
                "Updated evictability and LRU position for frame {:?} to {}",
                frame_id,
                if evictable {
                    "evictable"
                } else {
                    "non-evictable"
                }
            );
            eprintln!("LRU order: {:?}", self.cache.iter().collect::<Vec<_>>());
        } else {
            warn!(
                "Attempted to set evictability for non-existent frame {:?}",
                frame_id
            );
        }
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        while let Some((frame_id, evictable)) = self.cache.pop_lru() {
            if evictable {
                info!("Evicting frame {:?}", frame_id);
                return Some(frame_id);
            } else {
                warn!("Skipping non-evictable frame {:?}", frame_id);
                // TODO: optionally reinsert the frame at this point
            }
        }
        error!("No evictable frames available for eviction");
        None
    }

    pub fn size(&self) -> usize {
        self.cache.len()
    }
}

#[cfg(test)]
mod lru_replacer_tests {
    use super::*;

    #[test]
    fn test_empty_replacer() {
        let mut replacer = LRUReplacer::new(3);
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_single_element() {
        let mut replacer = LRUReplacer::new(1);
        replacer.record_access(FrameId(1));
        assert_eq!(replacer.size(), 1);
        assert_eq!(replacer.evict(), Some(FrameId(1)));
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_eviction_order() {
        let mut replacer = LRUReplacer::new(3);
        replacer.record_access(FrameId(1));
        replacer.record_access(FrameId(2));
        replacer.record_access(FrameId(3));
        assert_eq!(replacer.evict(), Some(FrameId(1)));
        assert_eq!(replacer.evict(), Some(FrameId(2)));
        assert_eq!(replacer.evict(), Some(FrameId(3)));
    }

    #[test]
    fn test_non_evictable_frames() {
        let mut replacer = LRUReplacer::new(3);
        replacer.record_access(FrameId(1));
        replacer.record_access(FrameId(2));
        replacer.record_access(FrameId(3));
        replacer.set_evictable(FrameId(1), false);
        replacer.set_evictable(FrameId(3), false);
        assert_eq!(replacer.evict(), Some(FrameId(2)));
        assert_eq!(replacer.evict(), None); // Frames 1 and 3 are non-evictable
    }

    #[test]
    fn test_capacity_handling() {
        let mut replacer = LRUReplacer::new(2);
        replacer.record_access(FrameId(1));
        replacer.record_access(FrameId(2));
        replacer.record_access(FrameId(3)); // This should evict FrameId(1)
        assert_eq!(replacer.evict(), Some(FrameId(2)));
        assert_eq!(replacer.evict(), Some(FrameId(3)));
    }

    #[test]
    fn test_repeated_access() {
        let mut replacer = LRUReplacer::new(3);
        replacer.record_access(FrameId(1));
        replacer.record_access(FrameId(2));
        replacer.record_access(FrameId(1));
        replacer.record_access(FrameId(3));
        assert_eq!(replacer.evict(), Some(FrameId(2)));
        assert_eq!(replacer.evict(), Some(FrameId(1)));
    }

    #[test]
    fn test_over_capacity() {
        let mut replacer = LRUReplacer::new(2);
        replacer.record_access(FrameId(1));
        replacer.record_access(FrameId(2));
        replacer.record_access(FrameId(3));
        assert_eq!(replacer.size(), 2); // Should not exceed capacity
    }

    #[test]
    #[ignore = "TODO: Fix this test"]
    fn test_reset_evictability() {
        let mut replacer = LRUReplacer::new(2);
        replacer.record_access(FrameId(1));
        replacer.record_access(FrameId(2));
        replacer.set_evictable(FrameId(1), false);
        replacer.set_evictable(FrameId(1), true);
        assert_eq!(replacer.evict(), Some(FrameId(1)));
    }
}
