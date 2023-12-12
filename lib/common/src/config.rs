//! Various configuration parameters for the dbms.

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
pub struct FrameId(u32);

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
pub struct PageId(u32);

// impl From

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
