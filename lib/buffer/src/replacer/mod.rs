use common::FrameId;

mod lru;
mod lru_k;

pub use lru::LRUReplacer;

/// Policy for cache replacement
pub enum ReplacementPolicy {
    LRU,
    MRU,
    LFU,
    LRUK,
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
