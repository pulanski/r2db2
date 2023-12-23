#![allow(dead_code, unused_variables)]

use crate::tuple::Tuple;

use super::{
    index::{Index, IndexMetadataRef},
    iterator::IndexIterator,
};
use anyhow::Result;
use getset::{Getters, Setters};
// use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

// Initial skeleton for B+ Tree and Hash Indexes

// B+ Tree Index
#[derive(Debug, Clone, Getters, Setters, TypedBuilder)] // TODO: Want to add serde support, need to figure out what that looks like w/ Arc
pub struct BPlusTreeIndex {
    metadata: IndexMetadataRef,
    // ... other B+ Tree specific fields
}

impl BPlusTreeIndex {
    /// Returns an iterator for a range query.
    pub fn range_query(&self, start_key: &Tuple, end_key: &Tuple) -> Box<dyn IndexIterator> {
        // Implementation for range query...
        todo!()
    }
}

impl Index for BPlusTreeIndex {
    fn insert_entry(&self, key: &crate::tuple::Tuple, rid: common::rid::RID) -> Result<()> {
        todo!()
    }

    fn delete_entry(&self, key: &crate::tuple::Tuple, rid: common::rid::RID) -> Result<()> {
        todo!()
    }

    fn scan_key(&self, key: &crate::tuple::Tuple) -> Result<Vec<common::rid::RID>> {
        todo!()
    }
    // Implement the trait methods for B+ Tree...
}

// Hash Index
pub struct HashIndex {
    metadata: IndexMetadataRef,
    // ... other Hash Index specific fields
}

impl Index for HashIndex {
    fn insert_entry(&self, key: &crate::tuple::Tuple, rid: common::rid::RID) -> Result<()> {
        todo!()
    }

    fn delete_entry(&self, key: &crate::tuple::Tuple, rid: common::rid::RID) -> Result<()> {
        todo!()
    }

    fn scan_key(&self, key: &crate::tuple::Tuple) -> Result<Vec<common::rid::RID>> {
        todo!()
    }
    // Implement the trait methods for Hash Index...
}
