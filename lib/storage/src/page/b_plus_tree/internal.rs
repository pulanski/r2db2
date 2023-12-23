use super::header::{BPlusTreePageHeader, BTreePageError, IndexPageKind};
use anyhow::Result;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

/// Represents an internal page in a B+ Tree.
///
/// An internal page does not store actual data, but rather stores ordered key entries and child pointers (page IDs).
/// The first key is always set to be invalid as the number of pointers does not equal the number of keys.
///
/// # Parameters
/// - `KeyType`: The type of the keys stored in the page (e.g. `i32`, `String`, etc.). These map to the actual
///              data entries in the table.
/// - `ValueType`: The type of the child pointers (page IDs) stored in the page.
#[derive(Debug, Clone, Getters, Setters, TypedBuilder, Serialize, Deserialize)]
#[getset(get = "pub", set = "pub")]
pub struct BPlusTreeInternalPage<KeyType, ValueType> {
    header: BPlusTreePageHeader,
    keys: Vec<Option<KeyType>>, // Using Option to handle invalid first key
    children: Vec<ValueType>,
}

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType>
where
    KeyType: Default + Clone,
    ValueType: Default + Clone,
{
    /// Creates a new `BPlusTreeInternalPage` with a specified maximum size.
    ///
    /// Initializes all keys as `None` and children with default values.
    ///
    /// # Arguments
    /// - `max_size`: The maximum number of keys the page can hold.
    ///
    /// # Returns
    /// A new instance of `BPlusTreeInternalPage`.
    pub fn new(max_size: usize) -> Self {
        BPlusTreeInternalPage::builder()
            .header(BPlusTreePageHeader::new(
                IndexPageKind::InternalPage,
                max_size,
            ))
            .keys(vec![None; max_size]) // Initialize all keys as None
            .children(vec![ValueType::default(); max_size + 1]) // +1 for the extra child
            .build()
    }
}

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType>
where
    KeyType: Clone,
{
    /// Retrieves a reference to a key at a given index.
    ///
    /// # Arguments
    /// - `index`: The index of the key to retrieve.
    ///
    /// # Returns
    /// - `Ok(&KeyType)`: A reference to the key at the specified index.
    /// - `Err(BTreePageError)`: An error if the index is invalid or the key is not found.
    pub fn key_at(&self, index: usize) -> Result<&KeyType> {
        self.keys
            .get(index)
            .ok_or_else(|| BTreePageError::InvalidPageSize.into())
            .and_then(|key_option| {
                key_option
                    .as_ref()
                    .ok_or_else(|| BTreePageError::InvalidPageKind.into())
            })
    }

    /// Sets a key at a given index.
    ///
    /// # Arguments
    /// - `index`: The index at which to set the key.
    /// - `key`: The key to set.
    ///
    /// # Returns
    /// - `Ok(())`: Successfully set the key.
    /// - `Err(BTreePageError)`: An error if the index is invalid.
    pub fn set_key_at(&mut self, index: usize, key: KeyType) -> Result<()> {
        if index == 0 || index >= self.keys.len() {
            return Err(BTreePageError::InvalidPageKind.into());
        }

        self.keys[index] = Some(key);
        Ok(())
    }

    // TODO: other key management methods...
}

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType> {
    pub fn child_at(&self, index: usize) -> Result<&ValueType> {
        self.children
            .get(index)
            .ok_or_else(|| BTreePageError::InvalidPageSize.into())
    }

    pub fn set_child_at(&mut self, index: usize, child: ValueType) -> Result<()> {
        if index > self.children.len() {
            return Err(BTreePageError::InvalidPageSize.into());
        }
        self.children[index] = child;
        Ok(())
    }

    pub fn first_child(&self) -> Result<&ValueType> {
        self.children
            .first()
            .ok_or_else(|| BTreePageError::InvalidPageSize.into())
    }

    pub fn last_child(&self) -> Result<&ValueType> {
        self.children
            .last()
            .ok_or_else(|| BTreePageError::InvalidPageSize.into())
    }

    pub fn max_size(&self) -> usize {
        self.header.max_size().clone()
    }

    // TODO: other child pointer management methods...
}

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType>
where
    KeyType: ToString,
{
    pub fn to_string(&self) -> String {
        let keys_str: Vec<String> = self
            .keys
            .iter()
            .enumerate()
            .filter_map(|(i, key)| {
                if i != 0 {
                    key.as_ref().map(|k| k.to_string())
                } else {
                    None
                }
            })
            .collect();
        format!("({})", keys_str.join(","))
    }

    // TODO: other utility functions...
}

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType>
where
    KeyType: Clone + Ord,
{
    pub fn find_child_index(&self, key: &KeyType) -> Result<usize> {
        self.keys
            .iter()
            .position(|k| k.as_ref().map_or(false, |k| key < k))
            .or_else(|| Some(self.keys.len() - 1))
            .ok_or_else(|| BTreePageError::KeyNotFound.into())
    }
}

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType>
where
    KeyType: Clone + Ord,
    ValueType: Clone,
{
    pub fn insert_and_split(&mut self, key: KeyType, child: ValueType) -> Result<Option<Self>> {
        let insert_index = self
            .keys
            .iter()
            .position(|k| k.as_ref().map_or(true, |k| &key < k))
            .unwrap_or(self.keys.len());

        if self.keys.len() >= self.max_size() {
            let mid = self.keys.len() / 2;

            // Determine in which half to insert the new key and child
            let (new_keys, new_children) = if insert_index > mid {
                (self.keys.split_off(mid), self.children.split_off(mid + 1))
            } else {
                (self.keys.split_off(mid + 1), self.children.split_off(mid))
            };

            let mut new_sibling = Self {
                header: BPlusTreePageHeader::new(IndexPageKind::InternalPage, self.max_size()),
                keys: new_keys,
                children: new_children,
            };

            if insert_index > mid {
                new_sibling.keys.insert(insert_index - mid - 1, Some(key));
                new_sibling.children.insert(insert_index - mid, child);
            } else {
                self.keys.insert(insert_index, Some(key));
                self.children.insert(insert_index, child);
            }

            Ok(Some(new_sibling))
        } else {
            self.keys.insert(insert_index, Some(key));
            self.children.insert(insert_index, child);
            Ok(None)
        }
    }
}

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType>
where
    KeyType: Clone + Ord,
    ValueType: Clone + Default,
{
    pub fn delete_and_merge(&mut self, key: &KeyType) -> Result<Option<Self>> {
        let delete_index = self.keys.iter().position(|k| k.as_ref() == Some(key));

        match delete_index {
            Some(index) => {
                self.keys.remove(index);
                self.children.remove(index);

                // Check if the page is less than half full and needs merging
                if self.keys.len() < self.max_size() / 2 {
                    let mut merge_sibling = Self {
                        header: BPlusTreePageHeader::new(
                            IndexPageKind::InternalPage,
                            self.max_size(),
                        ),
                        keys: vec![None; self.max_size()],
                        children: vec![ValueType::default(); self.max_size() + 1],
                    };

                    merge_sibling.keys.append(&mut self.keys);
                    merge_sibling.children.append(&mut self.children);

                    Ok(Some(merge_sibling))
                } else {
                    Ok(None)
                }
            }
            None => Err(BTreePageError::KeyNotFound.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_internal_page() {
        let page: BPlusTreeInternalPage<i32, i32> = BPlusTreeInternalPage::new(4);
        assert_eq!(page.keys.len(), 4);
        assert_eq!(page.children.len(), 5); // One more child than keys
        assert!(page.keys.iter().all(|key| key.is_none())); // All keys should be None
    }

    #[test]
    fn test_insert_and_retrieve_key_child() {
        let mut page = BPlusTreeInternalPage::new(4);
        assert!(page.set_key_at(1, 10).is_ok());
        assert!(page.set_child_at(1, 20).is_ok());

        assert_eq!(*page.key_at(1).unwrap(), 10);
        assert_eq!(*page.child_at(1).unwrap(), 20);
    }

    #[test]
    #[ignore = "Not implemented yet"]
    fn test_insert_and_split() {
        let mut page = BPlusTreeInternalPage::new(2);
        assert!(page.set_key_at(1, 10).is_ok());
        assert!(page.set_child_at(1, 20).is_ok());
        assert!(page.set_child_at(2, 25).is_ok());

        let new_sibling = page.insert_and_split(15, 30).unwrap().unwrap();

        // Check if the original page and the new sibling have correct number of keys and children
        assert_eq!(page.keys.len(), 1);
        assert_eq!(page.children.len(), 2);
        assert_eq!(new_sibling.keys.len(), 1);
        assert_eq!(new_sibling.children.len(), 2);

        // TODO: Check specific key and child values
    }

    #[test]
    #[ignore = "Not implemented yet"]
    fn test_delete_and_merge() {
        let mut page: BPlusTreeInternalPage<i32, i32> = BPlusTreeInternalPage::new(4);
        // Populate the page with some keys and children

        let merge_result = page.delete_and_merge(&10).unwrap();
        // Assertions to verify the state after deletion and potential merging
    }
}
