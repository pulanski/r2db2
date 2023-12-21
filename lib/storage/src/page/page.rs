use std::time::Instant;

use common::{PageId, PAGE_SIZE};
use getset::{CopyGetters, Getters, Setters};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, trace, warn};
use typed_builder::TypedBuilder;

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Page not found")]
    NotFound,

    #[error("Data access error: {0}")]
    DataAccessError(String),

    #[error("Pin count overflow for page {0}")]
    PinCountOverflow(PageId),
}

/// Represents a memory page in the system.
///
/// [`Page`] is designed to efficiently store and manage data for memory pages.
/// It tracks access and modification, supports pinning, and provides methods
/// for data manipulation. It is the primary unit of storage in the buffer pool
/// and is used as the unit of data transfer between disk and memory and
/// the basis for all data structures in the system (e.g., tables, indexes, etc.).
///
/// # Examples
///
/// ```
/// use common::PageId;
/// use storage::Page;
///
/// let mut page = Page::new(PageId::new(1), vec![1, 2, 3]).expect("Failed to create page");
///
/// page.write_data(&[4, 5, 6]); // Overwrites existing data
/// assert_eq!(page.read_data(), vec![4, 5, 6]); // Read newly written data
/// ```
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Getters,
    CopyGetters,
    Setters,
    TypedBuilder,
)]
pub struct Page {
    #[getset(get_copy = "pub")]
    id: PageId,
    #[getset(get = "pub")]
    data: Vec<u8>,
    #[getset(get_copy = "pub")]
    is_dirty: bool,
    #[getset(get_copy = "pub", set = "pub")]
    pin_count: u32,
    #[getset(get_copy = "pub")]
    #[serde(skip)]
    last_accessed: Option<Instant>,
    #[getset(get_copy = "pub")]
    access_count: u64,
}

impl Page {
    /// Creates a new `Page` with the specified `id` and `data`.
    ///
    /// Initializes a new page with given data, setting `is_dirty`, `pin_count`,
    /// `last_accessed`, and `access_count` to their default values.
    pub fn new(id: PageId, data: Vec<u8>) -> Result<Self, PageError> {
        debug!("Creating new page {} with {} bytes", id, data.len());

        if data.is_empty() {
            Err(PageError::DataAccessError("Empty data provided".into()))?
        }

        Ok(Page::builder()
            .id(id)
            .data(data)
            .is_dirty(false)
            .pin_count(0)
            .last_accessed(None)
            .access_count(0)
            .build())
    }

    /// Marks the page as dirty or clean.
    ///
    /// A dirty page indicates that it has been modified and may need to be
    /// written back to storage. A clean page indicates that it has not been
    /// modified and does not need to be written back to storage.
    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    /// Increments the pin count of the page.
    ///
    /// Pinning a page prevents it from being moved or removed, typically for
    /// the duration of a read or write operation.
    ///
    /// See [`decrement_pin_count`] for more details.
    ///
    /// # Errors
    ///
    /// Returns `PageError::PinCountOverflow` if the pin count reaches its maximum value.
    pub fn increment_pin_count(&mut self) -> Result<(), PageError> {
        if self.pin_count == u32::MAX {
            error!("Page {} pin count overflow", self.id);
            return Err(PageError::PinCountOverflow(self.id));
        }

        debug!("Incrementing pin count for page {}", self.id);
        self.pin_count += 1;

        Ok(())
    }

    pub fn decrement_pin_count(&mut self) {
        if self.pin_count == 0 {
            warn!("Page {} pin count is already 0", self.id);
            return;
        }

        debug!("Decrementing pin count for page {}", self.id);
        self.pin_count -= 1;
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty() || self.data.iter().all(|&b| b == 0)
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.data.as_mut_slice()
    }

    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.data.resize(new_len, value);
    }

    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    pub fn truncate(&mut self, new_len: usize) {
        self.data.truncate(new_len);
    }

    pub fn write_data(&mut self, data: &[u8]) {
        self.update_access_stats();
        self.data.clear();
        self.data.extend_from_slice(data);
    }

    pub fn read_data(&mut self) -> Vec<u8> {
        self.update_access_stats();
        self.data.clone()
    }

    pub fn update_access_stats(&mut self) {
        self.last_accessed = Some(Instant::now());
        self.access_count += 1;

        trace!(
            "Updating access stats for page {} to {:?}",
            self.id,
            self.get_access_stats()
        );
    }

    /// Retrieves access statistics for the page.
    ///
    /// Returns the last time the page was accessed and the total number of
    /// times it has been accessed.
    pub fn get_access_stats(&self) -> (Option<Instant>, u64) {
        (self.last_accessed, self.access_count)
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            id: PageId::default(),
            data: vec![0; PAGE_SIZE],
            is_dirty: false,
            pin_count: 0,
            last_accessed: None,
            access_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let data = vec![1, 2, 3];
        let page = Page::new(PageId::new(1), data.clone()).expect("Failed to create page");

        assert_eq!(page.id(), PageId::new(1));
        assert_eq!(page.data(), &data);
        assert!(!page.is_dirty());
        assert_eq!(page.pin_count(), 0);
        assert_eq!(page.last_accessed(), None);
        assert_eq!(page.access_count(), 0);
    }

    #[test]
    fn test_write_and_read_data() {
        let mut page = Page::default();
        let data = vec![4, 5, 6];

        page.write_data(&data);
        assert_eq!(page.read_data(), data);
        assert!(page.last_accessed().is_some());
        assert_eq!(page.access_count(), 2); // One for write, one for read
    }

    #[test]
    fn test_page_access_stats() {
        let mut page = Page::default();
        let data = vec![4, 5, 6];

        let before = Instant::now();
        page.write_data(&data);
        assert!(page.get_access_stats().0 >= Some(before));
        assert_eq!(page.get_access_stats().1, 1); // One for write
    }

    #[test]
    fn test_set_dirty() {
        let mut page = Page::default();
        assert!(!page.is_dirty());
        page.set_dirty(true);
        assert!(page.is_dirty());
    }

    #[test]
    fn test_pin_count() {
        let mut page = Page::default();
        assert_eq!(page.pin_count(), 0);
        page.increment_pin_count()
            .expect("Failed to increment pin count");
        assert_eq!(page.pin_count(), 1);
        page.decrement_pin_count();
        assert_eq!(page.pin_count(), 0);
    }

    #[test]
    fn test_decrement_pin_count_at_zero() {
        let mut page = Page::default();
        page.decrement_pin_count();
        assert_eq!(page.pin_count(), 0); // Should not go below 0
    }

    #[test]
    fn test_page_data_methods() {
        let mut page = Page::default();
        assert!(page.is_empty());
        assert_eq!(page.len(), PAGE_SIZE);
        assert_eq!(page.capacity(), PAGE_SIZE);

        let data = vec![1, 2, 3];
        page.write_data(&data);
        assert_eq!(page.as_slice(), data.as_slice());

        let data_mut = page.as_mut_slice();
        data_mut[0] = 4;
        assert_eq!(page.read_data(), vec![4, 2, 3]);

        page.resize(5, 0);
        assert_eq!(page.read_data(), vec![4, 2, 3, 0, 0]);

        page.reserve(10);
        assert!(page.capacity() >= 15);

        page.truncate(3);
        assert_eq!(page.read_data(), vec![4, 2, 3]);
    }

    #[test]
    fn test_clear() {
        let mut page = Page::default();
        let data = vec![1, 2, 3];
        page.write_data(&data);
        page.clear();
        assert!(page.is_empty());
    }
}
