use anyhow::Result;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use typed_builder::TypedBuilder;

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum IndexPageKind {
    #[default]
    InvalidIndexPage = 0,
    LeafPage,
    InternalPage,
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum BTreePageError {
    #[error("Invalid page kind. Page kind must be either LeafPage or InternalPage")]
    InvalidPageKind,

    #[error("Invalid page size. Page size must be within the range of 1 to 4096")]
    InvalidPageSize,

    #[error(
        "Invalid log sequence number. Log sequence number must be within the range of 1 to 2^64"
    )]
    InvalidLogSequenceNumber,

    #[error("Key not found")]
    KeyNotFound,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Getters,
    Setters,
    TypedBuilder,
    Serialize,
    Deserialize,
)]
#[getset(get = "pub", set = "pub")]
pub struct BPlusTreePageHeader {
    page_kind: IndexPageKind,
    #[getset(skip)]
    size: usize,
    max_size: usize,
    parent_page_id: i32,
    page_id: i32,
    #[getset(skip)]
    lsn: i32, // Log sequence number
}

impl BPlusTreePageHeader {
    pub fn new(page_kind: IndexPageKind, max_size: usize) -> Self {
        BPlusTreePageHeader::builder()
            .page_kind(page_kind)
            .size(0)
            .max_size(max_size)
            .parent_page_id(-1)
            .page_id(-1)
            .lsn(0)
            .build()
    }

    pub fn is_leaf_page(&self) -> bool {
        self.page_kind == IndexPageKind::LeafPage
    }

    pub fn set_page_type(&mut self, page_type: IndexPageKind) {
        self.page_kind = page_type;
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn set_size(&mut self, size: usize) -> Result<()> {
        if size > self.max_size {
            Err(BTreePageError::InvalidPageSize.into())
        } else {
            self.size = size;
            Ok(())
        }
    }

    pub fn increase_size(&mut self, size: usize) -> Result<()> {
        if self.size + size > self.max_size {
            Err(BTreePageError::InvalidPageSize.into())
        } else {
            self.size += size;
            Ok(())
        }
    }

    pub fn set_lsn(&mut self, lsn: i32) -> Result<()> {
        if lsn < 0 {
            Err(BTreePageError::InvalidLogSequenceNumber.into())
        } else {
            self.lsn = lsn;
            Ok(())
        }
    }
}
