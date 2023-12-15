#![allow(dead_code)]

use crate::PageId;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    hash::{Hash, Hasher},
};
use typed_builder::TypedBuilder;

/// Represents a Record Identifier (RID) in a database.
///
/// `RID` is used to uniquely identify a record within a database. It consists of a page ID and a slot number.
/// The page ID represents the page in the database where the record is stored, and the slot number
/// represents the record's position within that page.
///
/// # Examples
///
/// Basic usage:
///
/// ```rust,no_run
/// use crate::common::config::PageId;
/// use crate::common::rid::RID;
///
/// let rid = RID::new(PageId::from(1), 5);
/// assert_eq!(rid.to_string(), "RecordId(1, 5)");
/// ```
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Getters,
    Setters,
    TypedBuilder,
)]
#[getset(get = "pub", set = "pub")]
pub struct RID {
    pub page_id: Option<PageId>,
    pub slot_num: u32,
}

impl RID {
    pub fn new(page_id: PageId, slot_num: u32) -> Self {
        Self {
            page_id: Some(page_id),
            slot_num,
        }
    }

    /// Creates a `RID` from a 64-bit integer representation.
    /// The upper 32 bits represent the page ID and the lower 32 bits represent the slot number.
    ///
    /// # Arguments
    ///
    /// * `rid` - A 64-bit integer representing the `RID`.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::rid::RID;
    /// use common::PageId;
    ///
    /// let rid = RID::from_i64(0x00000001_00000005);
    /// assert_eq!(rid.page_id(), &Some(PageId::from(1)));
    /// assert_eq!(rid.slot_num(), &5);
    /// ```
    pub fn from_i64(rid: i64) -> Self {
        Self::from(rid)
    }

    pub fn get(&self) -> Option<i64> {
        match self.page_id {
            Some(page_id) => Some(((page_id.0 as i64) << 32) | self.slot_num as i64),
            None => None,
        }
    }

    pub fn to_string(&self) -> String {
        format!("RecordId({:?}, {})", self.page_id, self.slot_num)
    }
}

impl fmt::Display for RID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let page_id = match self.page_id {
            Some(page_id) => page_id.to_string(),
            None => "".to_string(),
        };
        write!(f, "RecordId({}, {})", page_id, self.slot_num)
    }
}

impl PartialEq for RID {
    fn eq(&self, other: &Self) -> bool {
        self.page_id == other.page_id && self.slot_num == other.slot_num
    }
}

impl From<i64> for RID {
    fn from(rid: i64) -> Self {
        Self {
            page_id: Some(PageId((rid >> 32) as u32)),
            slot_num: rid as u32,
        }
    }
}

impl Eq for RID {}

impl Hash for RID {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get().hash(state);
    }
}

#[cfg(test)]
mod rid_tests {
    use super::*;

    #[test]
    fn test_new_rid() {
        let page_id = PageId::from(1);
        let slot_num = 5;
        let rid = RID::new(page_id, slot_num);
        assert_eq!(rid.page_id, Some(page_id));
        assert_eq!(rid.slot_num, slot_num);
    }

    #[test]
    fn test_from_int64() {
        let rid_int = 0x12345678_9abcdef;
        let rid = RID::from_i64(rid_int);
        assert_eq!(rid.page_id, Some(PageId::from(rid_int >> 32)));
        // assert_eq!(rid.page_id, Some((rid_int >> 32) as u32));
        assert_eq!(rid.slot_num, rid_int as u32);

        let rid = RID::from_i64(0x00000001_00000005);
        assert_eq!(rid.page_id, Some(PageId::from(1)));
        assert_eq!(rid.slot_num, 5);
    }

    #[test]
    fn test_get() {
        let page_id = PageId::from(1);
        let slot_num = 5;
        let rid = RID::new(page_id, slot_num);
        let rid_int = (page_id.0 as i64) << 32 | slot_num as i64;
        assert_eq!(rid.get(), Some(rid_int));
    }
}
