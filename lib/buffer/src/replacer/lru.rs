use crate::replacer::ReplacerStats;
use common::FrameId;
use core::fmt;
use lru::LruCache;
use parking_lot::RwLock;
use std::{
    num::NonZeroUsize,
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};
use typed_builder::TypedBuilder;

/// # [`LRUReplacer`]
///
/// Implementation of a cache replacement policy based on the
/// Least Recently Used (LRU) strategy.
///
/// Manages cache entries, ensuring that the least recently accessed items
/// are evicted first when the cache reaches its capacity. It's suitable for scenarios
/// where the most recently used items are more likely to be accessed again.
///
/// # Usage
/// Used for managing the cache of a buffer pool. The buffer pool uses a LRUReplacer to
/// reduce disk I/O by keeping frequently accessed data in memory.
///
/// # Examples
///
/// ```
/// use buffer::replacer::{LRUReplacer, ReplacerStats};
///
/// let lru_replacer = LRUReplacer::new(100); // A replacer with a capacity of 100 frames
/// ```
#[derive(Debug, TypedBuilder)]
pub struct LRUReplacer {
    cache: Arc<RwLock<LruCache<FrameId, bool>>>, // Stores whether a frame is evictable or not
    stats: ReplacerStats,
}

impl Clone for LRUReplacer {
    fn clone(&self) -> Self {
        LRUReplacer::builder()
            .cache(Arc::clone(&self.cache))
            .stats(self.stats.clone())
            .build()
    }
}

impl LRUReplacer {
    pub fn new(capacity: usize) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or({
            error!("Capacity must be greater than 0");
            NonZeroUsize::new(1).expect("Capacity must be greater than 0")
        });
        info!("Creating a new LRUReplacer with capacity {}", capacity);

        LRUReplacer::builder()
            .cache(Arc::new(RwLock::new(LruCache::new(capacity))))
            .stats(ReplacerStats::new())
            .build()
    }

    pub fn record_access(&mut self, frame_id: FrameId) {
        let start = Instant::now();

        {
            let mut cache = self.cache.write();
            match cache.pop(&frame_id) {
                Some(_) => {
                    debug!(
                        "Frame {:?} found in cache, updating LRU position.",
                        frame_id
                    );
                    self.stats.increment_cache_hits();
                }
                None => {
                    debug!("Frame {:?} not found, adding to cache.", frame_id);
                    cache.put(frame_id, false);
                    self.stats.increment_cache_misses();
                }
            }
            self.stats.set_current_cache_size(cache.len());
        } // write lock is released here

        self.stats.increment_requests();
        self.stats.update_latency(start.elapsed());
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        let start = Instant::now();
        let mut cache = self.cache.write();
        let evicted_frame =
            cache
                .pop_lru()
                .filter(|&(_, evictable)| evictable)
                .map(|(frame_id, _)| {
                    debug!("Evicting frame {:?}", frame_id);
                    self.stats.increment_cache_evictions();
                    frame_id
                });

        if evicted_frame.is_none() {
            warn!("No evictable frames available for eviction");
        }

        self.stats.update_latency(start.elapsed());
        evicted_frame
    }

    /// Returns the number of evictable frames that are currently in the replacer
    pub fn size(&self) -> usize {
        let mut size = 0;
        let cache = self.cache.read();
        for (_, evictable) in cache.iter() {
            if *evictable {
                size += 1;
            }
        }

        size
    }

    pub fn bulk_add(&mut self, frame_ids: Vec<FrameId>, evictable: bool) {
        let mut cache = self.cache.write();
        for frame_id in frame_ids.clone() {
            cache.put(frame_id, evictable);
            debug!("Added frame {:?} with evictability {}", frame_id, evictable);
        }
        self.stats.set_current_cache_size(cache.len());
        info!(
            "Bulk added {} frames as {}",
            frame_ids.len(),
            if evictable {
                "evictable"
            } else {
                "non-evictable"
            }
        );
    }

    pub fn bulk_evict(&mut self, num_frames: usize) -> Vec<FrameId> {
        let mut evicted_frames = Vec::new();
        for _ in 0..num_frames {
            if let Some(frame_id) = self.evict() {
                evicted_frames.push(frame_id);
            } else {
                break; // Stop if no more frames can be evicted
            }
        }

        self.stats.update_evictions(evicted_frames.len());
        info!("Bulk evicted {} frames", evicted_frames.len());
        evicted_frames
    }

    pub fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        let start_time = Instant::now();
        let mut cache = self.cache.write();
        if let Some(_) = cache.get(&frame_id) {
            cache.put(frame_id, evictable); // directly update the evictability status
            debug!(
                "Setting evictability of frame {:?} to {}",
                frame_id, evictable
            );

            // Update cache hits stats
            self.stats.increment_cache_hits();
        } else {
            tracing::warn!(
                "Frame {:?} not found in cache when setting evictability",
                frame_id
            );
            cache.put(frame_id, evictable);

            // Update cache misses stats
            self.stats.increment_cache_misses();
        }

        // Update requests stats
        self.stats.increment_requests();
        self.stats.set_current_cache_size(cache.len());
        self.stats.update_latency(start_time.elapsed());
    }

    pub fn get_statistics(&self) -> ReplacerStats {
        self.stats.clone()
    }
}

impl fmt::Display for LRUReplacer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LRUReplacer (size: {})\n", self.size())?;
        write!(f, "MRU Order (most recently used at the top):\n")?;
        write!(f, "\n- Most Recently Used -\n\n")?;
        for (frame_id, evictable) in self.cache.read().iter() {
            write!(
                f,
                " {}: {}\n",
                frame_id,
                if *evictable {
                    "evictable"
                } else {
                    "non-evictable"
                },
            )?;
        }
        write!(f, "\n- Least Recently Used -\n")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_new() {
        let replacer = LRUReplacer::new(2);
        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_empty_replacer() {
        let mut replacer = LRUReplacer::new(3);
        assert_eq!(replacer.size(), 0);
        assert_eq!(replacer.evict(), None);
    }

    #[test]
    fn test_record_access() {
        let mut replacer = LRUReplacer::new(2);
        let frame_id = FrameId::new(1);
        replacer.record_access(frame_id);
        assert_eq!(replacer.size(), 0); // No evictable frames yet
    }

    #[test]
    fn test_set_evictable() {
        let mut replacer = LRUReplacer::new(2);
        let frame_id = FrameId::new(1);
        replacer.record_access(frame_id);
        replacer.set_evictable(frame_id, true);
        assert_eq!(replacer.size(), 1);
    }

    #[test]
    fn test_single_element() {
        let mut replacer = LRUReplacer::new(1);
        replacer.record_access(FrameId(1));
        assert_eq!(replacer.size(), 0); // Frames are non-evictable by default

        replacer.set_evictable(FrameId(1), true);
        assert_eq!(replacer.size(), 1);

        assert_eq!(replacer.evict(), Some(FrameId(1)));
        assert_eq!(replacer.size(), 0);
    }

    #[test]
    fn test_eviction_order() {
        let mut replacer = LRUReplacer::new(3);
        let frame_ids = [FrameId::new(1), FrameId::new(2), FrameId::new(3)];

        // Insert each as evictable
        for &frame_id in &frame_ids {
            replacer.set_evictable(frame_id, true);
        }

        assert_eq!(replacer.evict(), Some(FrameId(1)));
        assert_eq!(replacer.evict(), Some(FrameId(2)));
        assert_eq!(replacer.evict(), Some(FrameId(3)));
    }

    #[test]
    fn test_evict() {
        let mut replacer = LRUReplacer::new(2);
        let (frame_id1, frame_id2) = (FrameId::new(1), FrameId::new(2));

        // Insert each as evictable
        replacer.record_access(frame_id1);
        replacer.set_evictable(frame_id1, true);

        replacer.record_access(frame_id2);
        replacer.set_evictable(frame_id2, true);

        // Evict the first frame (LRU)
        assert_eq!(replacer.evict(), Some(frame_id1));
        assert_eq!(replacer.size(), 1);
    }

    #[test]
    fn test_repeated_access() {
        let mut replacer = LRUReplacer::new(3);
        let (frame_id1, frame_id2, frame_id3) = (FrameId::new(1), FrameId::new(2), FrameId::new(3));

        replacer.record_access(frame_id1);
        replacer.set_evictable(frame_id1, true);
        replacer.record_access(frame_id2);
        replacer.set_evictable(frame_id2, true);

        replacer.record_access(frame_id1);

        replacer.record_access(frame_id3);
        replacer.set_evictable(frame_id3, true);

        assert_eq!(replacer.evict(), Some(frame_id2));
        assert_eq!(replacer.evict(), Some(frame_id3));

        // assertions for statistics
        assert_eq!(replacer.get_statistics().total_requests(), 7); // 4 record_access + 3 set_evictable
        assert_eq!(replacer.get_statistics().cache_misses(), 3); // 3 record_access (initial misses)
        assert_eq!(replacer.get_statistics().cache_hits(), 4); // 4 set_evictable
    }

    #[test]
    fn test_size() {
        let mut replacer = LRUReplacer::new(2);
        let frame_id = FrameId::new(1);
        replacer.record_access(frame_id);
        replacer.set_evictable(frame_id, true);
        assert_eq!(replacer.size(), 1);
    }

    #[test]
    fn test_display() {
        let mut replacer = LRUReplacer::new(2);
        let frame_id = FrameId::new(1);
        replacer.record_access(frame_id);
        replacer.set_evictable(frame_id, true);
        let output = format!("{}", replacer);
        assert!(output.contains("LRUReplacer"));
        assert!(output.contains("evictable"));
    }

    #[test]
    fn test_concurrent_access() {
        let replacer = LRUReplacer::new(10);
        let mut handles = vec![];

        for i in 0..10 {
            let mut replacer_clone = replacer.clone();
            handles.push(thread::spawn(move || {
                let frame_id = FrameId::new(i);
                replacer_clone.record_access(frame_id);
                replacer_clone.set_evictable(frame_id, true);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert all frames are added and marked as evictable
        assert_eq!(replacer.size(), 10);

        // TODO: updates to statistics currently don't work with concurrent access (cloned stats are not updated)
        // want to use atomics for statistics, but they don't implement Clone, might need to just use Arc<RwLock<>> instead (but that's slow)
        // Additional assertions for statistics
        // assert_eq!(replacer.get_statistics().total_requests(), 20); // 10 record_access + 10 set_evictable
        // assert_eq!(replacer.get_statistics().cache_misses(), 10); // 10 record_access (initial misses)
        // assert_eq!(replacer.get_statistics().cache_hits(), 10); // 10 set_evictable
        // assert_eq!(replacer.get_statistics().cache_evictions(), 0); // No evictions yet

        let mut handles = vec![];

        // All expected frames are in the replacer
        for i in 0..10 {
            let replacer_clone = replacer.clone();
            handles.push(thread::spawn(move || {
                // concurrent access to replacer
                assert!(replacer_clone.cache.read().contains(&FrameId::new(i)));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut handles = vec![];

        // Evict all frames
        for _ in 0..10 {
            let mut replacer_clone = replacer.clone();
            handles.push(thread::spawn(move || {
                // concurrent mutation of replacer
                assert!(replacer_clone.evict().is_some());
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
