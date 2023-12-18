use crate::ReplacerStats;
use common::FrameId;
use getset::{Getters, Setters};
use parking_lot::RwLock;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    fmt,
    sync::Arc,
    time::Instant,
};
use tracing::{debug, info, warn};
use typed_builder::TypedBuilder;

/// Represents an entry in LFU cache, used for ordering in the priority queue.
#[derive(Debug, PartialEq, Eq, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
struct LFUEntry {
    frame_id: FrameId,
    frequency: usize,
    last_access: Instant,
}

impl Ord for LFUEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.frequency
            .cmp(&other.frequency)
            .then_with(|| self.last_access.cmp(&other.last_access))
    }
}

impl PartialOrd for LFUEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// `LFUReplacer` implements a cache replacement policy based on the
/// Least Frequently Used (LFU) strategy.
///
/// Manages cache entries by tracking their access frequency and ensuring
/// that items with the lowest frequency are evicted first when the cache reaches capacity.
#[derive(Debug, TypedBuilder)]
pub struct LFUReplacer {
    cache: Arc<RwLock<HashMap<FrameId, usize>>>, // Stores frame access frequencies
    priority_queue: BinaryHeap<Reverse<LFUEntry>>, // Min-heap based on frequency
    stats: ReplacerStats,
}

impl LFUReplacer {
    pub fn new() -> Self {
        info!("Initializing LFU Replacer");
        LFUReplacer::builder()
            .cache(Arc::new(RwLock::new(HashMap::new())))
            .priority_queue(BinaryHeap::new())
            .stats(ReplacerStats::new())
            .build()
    }

    pub fn record_access(&mut self, frame_id: FrameId) {
        let mut cache = self.cache.write();
        let frequency = cache.entry(frame_id).or_insert(0);
        *frequency += 1;

        let now = Instant::now();
        self.priority_queue.push(Reverse(LFUEntry {
            frame_id,
            frequency: *frequency,
            last_access: now,
        }));

        debug!(frame_id = ?frame_id, frequency = *frequency, "Recorded access in LFU Replacer");
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        while let Some(Reverse(entry)) = self.priority_queue.pop() {
            let mut cache = self.cache.write();
            if let Some(&current_frequency) = cache.get(&entry.frame_id) {
                if current_frequency == entry.frequency {
                    cache.remove(&entry.frame_id);
                    debug!(frame_id = ?entry.frame_id, "Evicted frame from LFU Replacer");
                    return Some(entry.frame_id);
                }
            }
        }

        warn!("No frame evicted from LFU Replacer: all frames are in use");
        None
    }

    /// Returns the number of evictable frames that are currently in the replacer.
    pub fn size(&self) -> usize {
        self.cache.read().len()
    }

    /// Adds multiple frames to the replacer, marking them as either evictable or non-evictable.
    pub fn bulk_add(&mut self, frame_ids: Vec<FrameId>, evictable: bool) {
        for frame_id in frame_ids.clone() {
            // Record the access for each frame
            self.record_access(frame_id);
            if evictable {
                // If the frame is evictable, adjust its status in the cache
                self.set_evictable(frame_id);
            }
        }
        debug!("Bulk added {} frames to LFU Replacer", frame_ids.len());
    }

    /// Evicts a specified number of frames from the cache, based on their usage frequency.
    pub fn bulk_evict(&mut self, num_frames: usize) -> Vec<FrameId> {
        let mut evicted_frames = Vec::with_capacity(num_frames);
        for _ in 0..num_frames {
            if let Some(frame_id) = self.evict() {
                evicted_frames.push(frame_id);
            } else {
                break; // No more frames can be evicted
            }
        }
        info!(
            "Bulk evicted {} frames from LFU Replacer",
            evicted_frames.len()
        );
        evicted_frames
    }

    /// Marks a frame as evictable in the cache.
    fn set_evictable(&mut self, frame_id: FrameId) {
        let mut cache = self.cache.write();
        cache.insert(frame_id, 1); // Set a default frequency for new evictable frames
    }

    pub fn get_statistics(&self) -> ReplacerStats {
        self.stats.clone()
    }
}

impl fmt::Display for LFUReplacer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cache = self.cache.read();
        write!(f, "LFUReplacer (size: {})\n", cache.len())?;
        for (frame_id, &frequency) in cache.iter() {
            writeln!(f, "Frame ID: {:?}, Frequency: {}", frame_id, frequency)?;
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
        let replacer = LFUReplacer::new();
        assert_eq!(replacer.size(), 0, "New replacer should be empty");
    }

    #[test]
    fn test_record_access() {
        let mut replacer = LFUReplacer::new();
        let frame_id = FrameId::new(1);
        replacer.record_access(frame_id);

        assert_eq!(
            replacer.size(),
            1,
            "Replacer should have one frame after access"
        );
        assert!(
            replacer.cache.read().contains_key(&frame_id),
            "Frame should be in the cache"
        );
    }

    #[test]
    fn test_evict_least_frequently_used() {
        let mut replacer = LFUReplacer::new();
        replacer.record_access(FrameId::new(1)); // Frequency: 1
        thread::sleep(Duration::from_millis(10)); // Ensuring a time difference
        replacer.record_access(FrameId::new(2)); // Frequency: 1
        replacer.record_access(FrameId::new(2)); // Frequency: 2

        let evicted = replacer.evict().expect("Should evict a frame");
        assert_eq!(
            evicted,
            FrameId::new(1),
            "Should evict the least frequently used frame"
        );
    }

    #[test]
    fn test_bulk_add_and_evict() {
        let mut replacer = LFUReplacer::new();
        let frame_ids = vec![FrameId::new(1), FrameId::new(2), FrameId::new(3)];
        replacer.bulk_add(frame_ids.clone(), true);

        assert_eq!(
            replacer.size(),
            3,
            "Should have three frames after bulk add"
        );

        let evicted_frames = replacer.bulk_evict(2);
        assert_eq!(evicted_frames.len(), 2, "Should evict two frames");
        assert!(frame_ids.contains(&evicted_frames[0]));
        assert!(frame_ids.contains(&evicted_frames[1]));
    }

    #[test]
    fn test_display() {
        let mut replacer = LFUReplacer::new();
        replacer.record_access(FrameId::new(1));
        replacer.record_access(FrameId::new(2));
        replacer.record_access(FrameId::new(1));

        let display_string = format!("{}", replacer);
        assert!(display_string.contains("Frame ID: FrameId(1), Frequency: 2"));
        assert!(display_string.contains("Frame ID: FrameId(2), Frequency: 1"));
    }

    #[test]
    fn test_concurrent_access() {
        let replacer = Arc::new(RwLock::new(LFUReplacer::new()));
        let mut handles = vec![];

        for i in 0..10 {
            let replacer_clone = Arc::clone(&replacer);
            handles.push(thread::spawn(move || {
                replacer_clone.write().record_access(FrameId::new(i));
            }));
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        assert_eq!(
            replacer.read().size(),
            10,
            "Should have 10 frames after concurrent access"
        );
    }

    // TODO: Add additional tests for edge cases, error conditions, etc.
}
