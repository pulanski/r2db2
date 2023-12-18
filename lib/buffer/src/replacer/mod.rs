use std::{
    fmt,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use common::FrameId;

mod lfu;
mod lru;
mod lru_k;
mod mru;

pub use lfu::LFUReplacer;
pub use lru::LRUReplacer;
pub use mru::MRUReplacer;
use parking_lot::RwLock;
use typed_builder::TypedBuilder;

/// Policy for cache replacement
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReplacementPolicy {
    /// Least Recently Used
    LRU,
    /// Most Recently Used
    MRU,
    /// Least Frequently Used
    LFU,
    /// LRU-K (Least Recently Used K)
    LRUK,
}

/// `ReplacerStats` holds statistical data for cache operations within an LRU Replacer.
///
/// Tracks various statistics such as cache hits, misses, evictions,
/// total requests, latency, and current cache size. These statistics are utilized within other
/// layers of the database to make decisions about how to optimize the cache as well as
/// for monitoring and debugging purposes.
#[derive(Debug, TypedBuilder)]
pub struct ReplacerStats {
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    cache_evictions: AtomicUsize,
    total_requests: AtomicUsize,
    total_latency: Arc<RwLock<Duration>>,
    current_cache_size: AtomicUsize,
}

impl ReplacerStats {
    pub fn new() -> Self {
        ReplacerStats::builder()
            .cache_hits(AtomicUsize::new(0))
            .cache_misses(AtomicUsize::new(0))
            .cache_evictions(AtomicUsize::new(0))
            .total_requests(AtomicUsize::new(0))
            .total_latency(Arc::new(RwLock::new(Duration::new(0, 0))))
            .current_cache_size(AtomicUsize::new(0))
            .build()
    }

    pub fn cache_misses(&self) -> usize {
        self.cache_misses.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_cache_misses(&mut self, cache_misses: usize) {
        self.cache_misses
            .store(cache_misses, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_current_cache_size(&mut self, current_cache_size: usize) {
        self.current_cache_size
            .store(current_cache_size, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn cache_hits(&self) -> usize {
        self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn total_requests(&self) -> usize {
        self.total_requests
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn cache_evictions(&self) -> usize {
        self.cache_evictions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn cache_hit_rate(&self) -> f64 {
        if self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)
            + self.cache_misses.load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            0.0
        } else {
            self.cache_hits.load(std::sync::atomic::Ordering::Relaxed) as f64
                / (self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)
                    + self.cache_misses.load(std::sync::atomic::Ordering::Relaxed))
                    as f64
        }
    }

    pub fn cache_miss_rate(&self) -> f64 {
        if self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)
            + self.cache_misses.load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            0.0
        } else {
            self.cache_misses.load(std::sync::atomic::Ordering::Relaxed) as f64
                / (self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)
                    + self.cache_misses.load(std::sync::atomic::Ordering::Relaxed))
                    as f64
        }
    }

    pub fn cache_eviction_rate(&self) -> f64 {
        if self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)
            + self.cache_misses.load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            0.0
        } else {
            self.cache_evictions
                .load(std::sync::atomic::Ordering::Relaxed) as f64
                / (self.cache_hits.load(std::sync::atomic::Ordering::Relaxed)
                    + self.cache_misses.load(std::sync::atomic::Ordering::Relaxed))
                    as f64
        }
    }

    pub fn update(&mut self, other: &ReplacerStats) {
        self.cache_hits.fetch_add(
            other.cache_hits.load(std::sync::atomic::Ordering::Relaxed),
            std::sync::atomic::Ordering::Relaxed,
        );
        self.cache_misses.fetch_add(
            other
                .cache_misses
                .load(std::sync::atomic::Ordering::Relaxed),
            std::sync::atomic::Ordering::Relaxed,
        );
        self.cache_evictions.fetch_add(
            other
                .cache_evictions
                .load(std::sync::atomic::Ordering::Relaxed),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn reset(&mut self) {
        self.cache_hits
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.cache_misses
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.cache_evictions
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn update_latency(&mut self, latency: Duration) {
        let mut total_latency = self.total_latency.write();
        *total_latency += latency;
    }

    pub fn update_cache_size(&mut self, size: usize) {
        self.current_cache_size
            .store(size, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn update_requests(&mut self, requests: usize) {
        self.total_requests
            .fetch_add(requests, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn update_evictions(&mut self, evictions: usize) {
        self.cache_evictions
            .fetch_add(evictions, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn average_latency(&self) -> f64 {
        if self
            .total_requests
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
        {
            0.0
        } else {
            self.total_latency.read().as_micros() as f64
                / self
                    .total_requests
                    .load(std::sync::atomic::Ordering::Relaxed) as f64
        }
    }

    pub fn throughput(&self) -> f64 {
        // throughput is measured per second
        if self
            .total_latency
            .read()
            .as_secs_f64()
            .eq(&std::f64::EPSILON)
        {
            0.0
        } else {
            self.total_requests
                .load(std::sync::atomic::Ordering::Relaxed) as f64
                / self.total_latency.read().as_secs_f64()
        }
    }

    pub fn increment_cache_hits(&mut self) {
        self.cache_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_cache_misses(&mut self) {
        self.cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_cache_evictions(&mut self) {
        self.cache_evictions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn increment_requests(&mut self) {
        self.total_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Clone for ReplacerStats {
    fn clone(&self) -> Self {
        let cache_hits =
            AtomicUsize::new(self.cache_hits.load(std::sync::atomic::Ordering::Relaxed));
        let cache_misses =
            AtomicUsize::new(self.cache_misses.load(std::sync::atomic::Ordering::Relaxed));
        let cache_evictions = AtomicUsize::new(
            self.cache_evictions
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        let total_requests = AtomicUsize::new(
            self.total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        let total_latency = Arc::new(RwLock::new(self.total_latency.read().clone()));
        let current_cache_size = AtomicUsize::new(
            self.current_cache_size
                .load(std::sync::atomic::Ordering::Relaxed),
        );

        ReplacerStats::builder()
            .cache_hits(cache_hits)
            .cache_misses(cache_misses)
            .cache_evictions(cache_evictions)
            .total_requests(total_requests)
            .total_latency(total_latency)
            .current_cache_size(current_cache_size)
            .build()
    }
}

impl fmt::Display for ReplacerStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReplacerStats\n")?;
        write!(f, "Cache hit rate: {:.2}\n", self.cache_hit_rate())?;
        write!(f, "Cache miss rate: {:.2}\n", self.cache_miss_rate())?;
        write!(
            f,
            "Cache eviction rate: {:.2}\n",
            self.cache_eviction_rate()
        )?;
        Ok(())
    }
}

pub trait Replacer {
    /// Remove the victim frame as defined by the replacement policy.
    /// Returns `Option<FrameId>`
    /// `Some(frame_id)` if a victim frame was found, `None` otherwise.
    fn victim(&mut self) -> Option<FrameId>;

    /// Pins a frame, indicating that it should not be victimized until it is unpinned.
    fn pin(&mut self, frame_id: FrameId);

    /// Unpins a frame, indicating that it can now be victimized.
    fn unpin(&mut self, frame_id: FrameId);

    /// Returns the number of elements in the replacer that can be victimized.
    fn size(&self) -> usize;
}

// trait Replacer {
//     fn record_access(&mut self, frame_id: FrameId);
//     fn evict(&mut self) -> Option<FrameId>;
//     // Other common methods...
// }
