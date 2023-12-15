#![allow(dead_code)]

use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
enum SlottedPageError {
    #[error("not enough space to store the record")]
    InsufficientSpace,
    #[error("record not found")]
    RecordNotFound,
    #[error("page split required")]
    PageSplitRequired,
    // ...
}

/// Header of the slotted page, which keeps track of the record slots and the free space.
#[derive(Debug, Clone)]
struct SlottedPageHeader {
    free_space_offset: usize,
    slots: HashMap<usize, RecordSlot>, // Maps slot index to the record slot
}

/// Represents a slot in the slotted page, which could either point to a fixed or variable length record.
#[derive(Debug, Clone)]
struct RecordSlot {
    offset: usize,
    length: usize,
}

/// A `SlottedPage` represents a single page in a slotted page storage system.
/// It is designed to store both fixed and variable-length records.
///
/// ## Data layout:
///
/// ```ignore
/// | Header | ...Free Space... | ...Records... |
/// ```
/// The header contains metadata such as the offset to the beginning of the free space
/// and a map of record slots. Records are stored at the end of the page, growing
/// towards the beginning, while free space is managed from the beginning of the page.
///
/// ## Example:
///
/// ```rust
/// use storage::slotted_page::SlottedPage;
///
/// let mut page = SlottedPage::new(1024);
/// let record = b"Example record";
/// let slot_index = page.add_record(record);
/// let retrieved_record = page.get_record(slot_index).unwrap();
///
/// println!("Retrieved record: {:?}", std::str::from_utf8(retrieved_record).unwrap());
/// ```
#[derive(Debug, Clone)]
pub struct SlottedPage {
    header: SlottedPageHeader,
    data: Vec<u8>, // A vector of bytes to store record data
}

impl SlottedPage {
    /// Initializes a new slotted page with a given size.
    pub fn new(page_size: usize) -> Self {
        Self {
            header: SlottedPageHeader {
                free_space_offset: 0,
                slots: HashMap::new(),
            },
            data: vec![0; page_size],
        }
    }

    /// Adds a new record to the slotted page, returning the slot index.
    pub fn add_record(&mut self, record: &[u8]) -> usize {
        let slot_index = self.header.slots.len();
        let offset = self.header.free_space_offset;
        let length = record.len();

        // Check if there is enough space to store the record.
        if offset + length > self.data.len() {
            panic!("Not enough space to store the record");
        }

        // Store the record data.
        self.data[offset..offset + length].copy_from_slice(record);
        // Update the header.
        self.header.free_space_offset += length;
        self.header
            .slots
            .insert(slot_index, RecordSlot { offset, length });

        slot_index
    }

    /// Retrieves a record from the slotted page by its slot index.
    pub fn get_record(&self, slot_index: usize) -> Option<&[u8]> {
        self.header
            .slots
            .get(&slot_index)
            .map(|slot| &self.data[slot.offset..slot.offset + slot.length])
    }

    // Additional methods like delete_record, update_record, etc. could be implemented.
}

// fn usage() {
//     // Example usage:
//     let mut page = SlottedPage::new(1024); // Initialize a new slotted page of size 1024 bytes.
//     let record = b"Example record";
//     let slot_index = page.add_record(record); // Add a record and get its slot index.
//     let retrieved_record = page.get_record(slot_index).unwrap(); // Retrieve the record by its slot index.

//     println!(
//         "Retrieved record: {:?}",
//         std::str::from_utf8(retrieved_record).unwrap()
//     );
// }
