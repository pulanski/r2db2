//! # MRU (Most Recently Used) Cache Replacer
//!
//! `MRUReplacer` is an implementation of the cache replacement policy based on
//! the most recently used strategy. It evicts the cache entries that are most recently
//! accessed, which is the opposite of the LRU strategy. This strategy is used in scenarios
//! where the most recently used items are less likely to be needed again soon.

use crate::ReplacerStats;
use common::FrameId;
use parking_lot::RwLock;
use std::{collections::HashMap, fmt, sync::Arc, time::Instant};
use tracing::{debug, info};
use typed_builder::TypedBuilder;

/// Represents an entry in the MRU cache, used for ordering in the eviction logic.
/// Each entry tracks the frame ID and the timestamp of the last access.
#[derive(Debug, Eq, PartialEq, TypedBuilder)]
struct MRUEntry {
    frame_id: FrameId,
    last_access: Instant,
}

/// `MRUReplacer` implements a cache replacement policy based on the
/// Most Recently Used (MRU) strategy.
///
/// The replacer manages cache entries by tracking their most recent access time.
/// Entries that are most recently used are evicted first when the cache reaches capacity.
#[derive(Debug, TypedBuilder)]
pub struct MRUReplacer {
    // Stores frame access times.
    cache: Arc<RwLock<HashMap<FrameId, Instant>>>,
    // Statistical data for cache operations.
    stats: ReplacerStats,
}

impl MRUReplacer {
    /// Constructs a new `MRUReplacer`.
    pub fn new() -> Self {
        info!("Initializing MRU Replacer");
        MRUReplacer::builder()
            .cache(Arc::new(RwLock::new(HashMap::new())))
            .stats(ReplacerStats::new())
            .build()
    }

    /// Records access to a cache frame, updating its last access time.
    pub fn record_access(&mut self, frame_id: FrameId) {
        let mut cache = self.cache.write();
        cache.insert(frame_id, Instant::now());
        debug!(frame_id = ?frame_id, "Recorded access in MRU Replacer");
    }

    /// Evicts the most recently used frame from the cache.
    /// Returns `Some(frame_id)` if a frame is evicted, or `None` if no frame can be evicted.
    pub fn evict(&mut self) -> Option<FrameId> {
        let mut cache = self.cache.write();
        if let Some((&frame_id, &_most_recent)) = cache.iter().max_by_key(|&(_, &time)| time) {
            cache.remove(&frame_id);
            debug!(frame_id = ?frame_id, "Evicted frame from MRU Replacer");
            return Some(frame_id);
        }

        None
    }

    /// Returns the number of frames in the replacer.
    pub fn size(&self) -> usize {
        self.cache.read().len()
    }

    /// Adds multiple frames to the replacer and marks them as evictable.
    pub fn bulk_add(&mut self, frame_ids: Vec<FrameId>, evictable: bool) {
        {
            let mut cache = self.cache.write();
            for frame_id in &frame_ids {
                cache.insert(*frame_id, Instant::now());
                debug!("Bulk added frame {:?} to MRU Replacer", frame_id);
            }
        } // Dropping `cache` here to end the immutable borrow

        if evictable {
            // If evictable, update the priority of these frames
            for frame_id in frame_ids {
                self.record_access(frame_id);
            }
        }
    }

    /// Evicts a specified number of frames from the cache.
    pub fn bulk_evict(&mut self, num_frames: usize) -> Vec<FrameId> {
        let mut evicted_frames = Vec::new();
        for _ in 0..num_frames {
            if let Some(evicted) = self.evict() {
                evicted_frames.push(evicted);
            } else {
                break; // No more frames to evict
            }
        }
        debug!(
            "Bulk evicted {} frames from MRU Replacer",
            evicted_frames.len()
        );
        evicted_frames
    }
}

impl fmt::Display for MRUReplacer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cache = self.cache.read();
        write!(f, "MRUReplacer (size: {})\n", cache.len())?;
        for (frame_id, last_access) in cache.iter() {
            writeln!(
                f,
                "Frame ID: {:?}, Last Accessed: {:?}",
                frame_id, last_access
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_replacer() {
        let replacer = MRUReplacer::new();
        assert_eq!(replacer.size(), 0, "New replacer should be empty");
    }

    #[test]
    fn test_record_access() {
        let mut replacer = MRUReplacer::new();
        let frame_id = FrameId::new(1);
        replacer.record_access(frame_id);
        assert_eq!(replacer.size(), 1, "Replacer should have one frame");
    }

    #[test]
    fn test_evict_most_recently_used() {
        let mut replacer = MRUReplacer::new();
        replacer.record_access(FrameId::new(1));
        thread::sleep(Duration::from_millis(10)); // Ensuring a time difference
        replacer.record_access(FrameId::new(2));
        let evicted = replacer.evict().expect("Should evict a frame");
        assert_eq!(
            evicted,
            FrameId::new(2),
            "Should evict the most recently used frame"
        );
    }

    #[test]
    fn test_bulk_add_and_evict() {
        let mut replacer = MRUReplacer::new();
        let frame_ids = vec![FrameId::new(1), FrameId::new(2)];
        replacer.bulk_add(frame_ids.clone(), true);

        assert_eq!(replacer.size(), 2, "Should have two frames after bulk add");
        let evicted_frames = replacer.bulk_evict(1);
        assert_eq!(evicted_frames.len(), 1, "Should evict one frame");
        assert!(frame_ids.contains(&evicted_frames[0]));
    }

    #[test]
    fn test_display() {
        let mut replacer = MRUReplacer::new();
        replacer.record_access(FrameId::new(1));
        let display_string = format!("{}", replacer);
        assert!(display_string.contains("Frame ID: FrameId(1), Last Accessed:"));
    }

    // Additional test cases...
}
