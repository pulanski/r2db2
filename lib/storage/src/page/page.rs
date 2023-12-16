use common::{PageId, PAGE_SIZE};
use getset::{CopyGetters, Getters, Setters};
use serde::{Deserialize, Serialize};
use tracing::warn;
use typed_builder::TypedBuilder;

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
}

impl Page {
    pub fn new(id: PageId, data: Vec<u8>) -> Self {
        Self {
            id,
            data,
            is_dirty: false,
            pin_count: 0,
        }
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    pub fn increment_pin_count(&mut self) {
        if self.pin_count == u32::MAX {
            warn!("Page {} pin count is already at max", self.id);
            return;
        }

        self.pin_count += 1;
    }

    pub fn decrement_pin_count(&mut self) {
        if self.pin_count == 0 {
            warn!("Page {} pin count is already 0", self.id);
            return;
        }
        self.pin_count -= 1;
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
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
        self.data.clear();
        self.data.extend_from_slice(data);
    }

    pub fn read_data(&self) -> Vec<u8> {
        self.data.clone()
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            id: PageId::default(),
            data: vec![0; PAGE_SIZE],
            is_dirty: false,
            pin_count: 0,
        }
    }
}
