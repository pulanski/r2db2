//! Various configuration parameters for the dbms.

use std::fmt;

use serde::{Deserialize, Serialize};
use shrinkwraprs::Shrinkwrap;

/// The size of a page in bytes (4 KiB). Pages are a fixed-size block of data and
/// are the unit of data transfer between disk and memory.
pub const PAGE_SIZE: usize = 4096;

/// The size of the buffer pool (in frames). Specifies the number of pages that can be held in
/// memory at any given time. The buffer pool is the primary mechanism for storing pages in memory.
pub const BUFFER_POOL_SIZE: usize = 10;

/// The maximum number of concurrent transactions. Sets an upper limit on the number of transactions
/// that can be processed concurrently by the DBMS. This is used to initialize the scheduler.
/// Transactions beyond this limit will be blocked until a transaction completes.
pub const MAX_TRANSACTIONS: usize = 10;

/// Unique identifier for a frame. Frames are identified by a monotonically increasing integer
/// and are the unit of storage in the buffer pool.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct FrameId(pub u32);

impl From<FrameId> for u32 {
    fn from(frame_id: FrameId) -> Self {
        frame_id.0
    }
}

impl From<i32> for FrameId {
    fn from(frame_id: i32) -> Self {
        if frame_id < 0 {
            panic!("FrameId out of range")
        }

        Self(frame_id as u32)
    }
}

impl From<usize> for FrameId {
    fn from(frame_id: usize) -> Self {
        if frame_id > u32::MAX as usize {
            panic!("FrameId out of range")
        }

        Self(frame_id as u32)
    }
}

impl From<u32> for FrameId {
    fn from(frame_id: u32) -> Self {
        Self(frame_id)
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FrameId({})", self.0)
    }
}

/// Unique identifier for a page. Pages are identified by a tuple of (file_id, page_number).
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct PageId(pub u32);

impl From<i64> for PageId {
    fn from(page_id: i64) -> Self {
        if page_id < 0 {
            panic!("PageId out of range")
        }

        Self(page_id as u32)
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}

impl From<PageId> for u32 {
    fn from(page_id: PageId) -> Self {
        page_id.0
    }
}

impl From<u32> for PageId {
    fn from(page_id: u32) -> Self {
        Self(page_id)
    }
}

impl From<i32> for PageId {
    fn from(page_id: i32) -> Self {
        if page_id < 0 {
            panic!("PageId out of range")
        }

        Self(page_id as u32)
    }
}

impl From<usize> for PageId {
    fn from(page_id: usize) -> Self {
        if page_id > u32::MAX as usize {
            panic!("PageId out of range")
        }

        Self(page_id as u32)
    }
}

/// Offset of a page within a file. Pages are stored sequentially within a file.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct PageOffset(usize);

/// Unique identifier for a transaction. Transactions are identified by a monotonically increasing
/// integer.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Shrinkwrap,
)]
pub struct TransactionId(u32);
