use common::PageId;
use parking_lot::RwLock;

const HTABLE_HEADER_PAGE_METADATA_SIZE: usize = std::mem::size_of::<u32>();
const HTABLE_HEADER_MAX_DEPTH: usize = 9;
const HTABLE_HEADER_ARRAY_SIZE: usize = 1 << HTABLE_HEADER_MAX_DEPTH;
const HTABLE_DIRECTORY_MAX_DEPTH: usize = 9;
const HTABLE_DIRECTORY_ARRAY_SIZE: usize = 1 << HTABLE_DIRECTORY_MAX_DEPTH;

// Assuming MappingType is defined somewhere
#[derive(Clone, Copy)]
struct MappingType {
    // Your key-value pair structure
}

#[derive(Debug, Default, Clone)]
struct HeaderPage {
    directory_page_ids: Vec<PageId>,
    max_depth: u32,
}

impl HeaderPage {
    fn new(max_depth: u32) -> Self {
        // Initialize the header page with default values.
        todo!()
    }

    // ... other methods ...
}

struct DirectoryPage {
    max_depth: u32,
    global_depth: u32,
    local_depths: Vec<u8>,
    bucket_page_ids: Vec<PageId>,
}

impl DirectoryPage {
    fn new(max_depth: u32) -> Self {
        // Initialize the directory page with default values.
        todo!()
    }

    // ... other methods ...
}

#[derive(Debug, Default, Clone)]
struct BucketPage<KeyType, ValueType> {
    size: u32,
    max_size: u32,
    entries: Vec<(KeyType, ValueType)>,
}

impl<KeyType, ValueType> BucketPage<KeyType, ValueType> {
    fn new(max_size: u32) -> Self {
        // Initialize the bucket page with default values.
        todo!()
    }

    fn init(&mut self, max_size: u32) {
        // Initialize the bucket page with default values.
        todo!()
    }

    fn is_full(&self) -> bool {
        // Check if the bucket page is full.
        todo!()
    }

    fn is_empty(&self) -> bool {
        // Check if the bucket page is empty.
        todo!()
    }

    fn insert(&mut self, key: &KeyType, value: &ValueType) -> bool {
        // Insert a (key, value) pair into the bucket page.
        todo!()
    }

    fn lookup(&self, key: &KeyType) -> Option<&ValueType> {
        // Lookup a key in the bucket page.
        todo!()
    }

    fn remove(&mut self, key: &KeyType) -> bool {
        // Remove a key from the bucket page.
        todo!()
    }

    // ... other methods ...
}

// We would also need a struct to represent the entire extendible hash table,
// which would contain instances of the header, directory, and bucket pages.
struct ExtendibleHashTable<KeyType, ValueType> {
    header_page: RwLock<HeaderPage>,
    directory_pages: RwLock<Vec<DirectoryPage>>,
    bucket_pages: RwLock<Vec<BucketPage<KeyType, ValueType>>>,
}

impl<KeyType, ValueType> ExtendibleHashTable<KeyType, ValueType> {
    pub fn new() -> Self {
        // Initialize the extendible hash table
        todo!()
    }

    // ... other methods for insertion, deletion, search, etc. ...
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    // Helper function to simulate `BufferPoolManager`
    fn create_page<T>() -> Arc<Mutex<T>>
    where
        T: Default,
    {
        Arc::new(Mutex::new(T::default()))
    }

    #[test]
    #[ignore = "Not yet implemented"]
    fn bucket_page_sample_test() {
        let bucket_page = create_page::<BucketPage<i64, i64>>();

        // Simulate the `Init` function, set max_size to 10.
        {
            let mut bucket_page = bucket_page.lock().unwrap();
            bucket_page.init(10);
        }

        // Insert a few (key, value) pairs.
        for i in 0..10 {
            let mut bucket_page = bucket_page.lock().unwrap();
            assert!(bucket_page.insert(&i, &(i * 100)));
        }

        // Check if the bucket page is full.
        {
            let bucket_page = bucket_page.lock().unwrap();
            assert!(bucket_page.is_full());
        }

        // Check for the inserted pairs.
        for i in 0..10 {
            let bucket_page = bucket_page.lock().unwrap();
            assert_eq!(bucket_page.lookup(&i), Some(&(i * 100)));
        }

        // Remove a few pairs.
        for i in 0..10 {
            if i % 2 == 1 {
                let mut bucket_page = bucket_page.lock().unwrap();
                assert!(bucket_page.remove(&i));
            }
        }

        // Check that removed pairs are no longer present.
        for i in 0..10 {
            let bucket_page = bucket_page.lock().unwrap();
            if i % 2 == 1 {
                assert_eq!(bucket_page.lookup(&i), None);
            } else {
                assert_eq!(bucket_page.lookup(&i), Some(&(i * 100)));
            }
        }

        // Finally, check if the bucket page is empty.
        {
            let bucket_page = bucket_page.lock().unwrap();
            assert!(bucket_page.is_empty());
        }
    }

    #[test]
    fn header_directory_page_sample_test() {
        // Test setup for `HeaderPage` and `DirectoryPage`.
    }

    // TODO: Additional tests here to cover more scenarios.
}
