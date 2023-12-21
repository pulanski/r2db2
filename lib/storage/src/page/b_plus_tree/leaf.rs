use super::header::{BPlusTreePageHeader, BTreePageError, IndexPageKind};
use common::rid::RID;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use std::fmt;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Getters, Setters, TypedBuilder, Serialize, Deserialize)]
#[getset(get = "pub", set = "pub")]
pub struct BPlusTreeLeafPage<KeyType> {
    header: BPlusTreePageHeader,
    next_page_id: i32, // ID of the next leaf page
    keys: Vec<KeyType>,
    rids: Vec<RID>, // Record IDs associated with the keys
}

impl<KeyType> BPlusTreeLeafPage<KeyType> {
    /// Creates a new `BPlusTreeLeafPage` with a specified maximum size.
    ///
    /// # Arguments
    /// - `max_size`: The maximum number of (key-RID) pairs the page can hold.
    ///
    /// # Returns
    /// A new instance of `BPlusTreeLeafPage`.
    pub fn new(max_size: usize) -> Self {
        Self {
            header: BPlusTreePageHeader::new(IndexPageKind::LeafPage, max_size),
            next_page_id: -1,
            keys: Vec::with_capacity(max_size),
            rids: Vec::with_capacity(max_size),
        }
    }

    pub fn max_size(&self) -> usize {
        self.header.max_size().clone()
    }
}

impl<KeyType> BPlusTreeLeafPage<KeyType>
where
    KeyType: Clone,
{
    /// Gets a reference to the key at a given index.
    pub fn key_at(&self, index: usize) -> Option<&KeyType> {
        self.keys.get(index)
    }

    // Additional methods for managing keys and record IDs...
}

impl<KeyType> fmt::Display for BPlusTreeLeafPage<KeyType>
where
    KeyType: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys_str = self
            .keys
            .iter()
            .map(|k| k.to_string())
            .collect::<Vec<String>>()
            .join(",");
        write!(f, "({})", keys_str)
    }
}

impl<KeyType> BPlusTreeLeafPage<KeyType>
where
    KeyType: Clone + Ord,
{
    /// Inserts a key and its corresponding record ID into the leaf page.
    pub fn insert(&mut self, key: KeyType, rid: RID) -> Result<(), BTreePageError> {
        if self.keys.len() >= self.max_size() {
            return Err(BTreePageError::InvalidPageSize);
        }

        let position = self
            .keys
            .iter()
            .position(|k| key < *k)
            .unwrap_or(self.keys.len());
        self.keys.insert(position, key);
        self.rids.insert(position, rid);
        Ok(())
    }

    /// Deletes a key and its corresponding record ID from the leaf page.
    pub fn delete(&mut self, key: &KeyType) -> Result<(), BTreePageError> {
        if let Some(position) = self.keys.iter().position(|k| k == key) {
            self.keys.remove(position);
            self.rids.remove(position);
            Ok(())
        } else {
            Err(BTreePageError::KeyNotFound)
        }
    }

    /// Finds the index of a key in the leaf page.
    pub fn find_key_index(&self, key: &KeyType) -> Result<usize, BTreePageError> {
        self.keys
            .iter()
            .position(|k| k == key)
            .ok_or(BTreePageError::KeyNotFound)
    }

    /// Splits the leaf page and returns a new leaf page containing the second half of the keys and RIDs.
    pub fn split(&mut self) -> Self {
        let mid = self.keys.len() / 2;
        let new_keys = self.keys.split_off(mid);
        let new_rids = self.rids.split_off(mid);

        BPlusTreeLeafPage::builder()
            .header(BPlusTreePageHeader::new(
                IndexPageKind::LeafPage,
                self.max_size(),
            ))
            .next_page_id(-1) // To be updated
            .keys(new_keys)
            .rids(new_rids)
            .build()
    }

    /// Merges the current leaf page with the given leaf page.
    pub fn merge(&mut self, other: &mut Self) {
        self.keys.append(&mut other.keys);
        self.rids.append(&mut other.rids);
    }

    /// Returns a range of keys and their corresponding RIDs within the given bounds.
    pub fn range_search(&self, start: &KeyType, end: &KeyType) -> Vec<(&KeyType, &RID)> {
        self.keys
            .iter()
            .zip(self.rids.iter())
            .filter(|(key, _)| key >= &start && key <= &end)
            .collect()
    }

    // TODO: Additional methods...
}

impl<'a, KeyType> IntoIterator for &'a BPlusTreeLeafPage<KeyType> {
    type Item = (&'a KeyType, &'a RID);
    type IntoIter = std::iter::Zip<std::slice::Iter<'a, KeyType>, std::slice::Iter<'a, RID>>;

    fn into_iter(self) -> Self::IntoIter {
        self.keys.iter().zip(self.rids.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::PageId;

    #[test]
    fn test_new_leaf_page() {
        let leaf_page: BPlusTreeLeafPage<i32> = BPlusTreeLeafPage::new(4);
        assert_eq!(leaf_page.keys().len(), 0);
        assert_eq!(leaf_page.rids().len(), 0);
        assert_eq!(leaf_page.max_size(), 4);
    }

    #[test]
    fn test_insert_and_find_key() {
        let mut leaf_page = BPlusTreeLeafPage::new(4);
        let rid = RID::builder()
            .page_id(Some(PageId::from(1)))
            .slot_num(1)
            .build();

        assert!(leaf_page.insert(10, rid).is_ok());
        assert_eq!(*leaf_page.key_at(0).unwrap(), 10);
        assert_eq!(leaf_page.find_key_index(&10).unwrap(), 0);
    }

    #[test]
    fn test_delete_key() {
        let mut leaf_page = BPlusTreeLeafPage::new(4);
        let rid = RID::builder()
            .page_id(Some(PageId::from(1)))
            .slot_num(1)
            .build();

        leaf_page.insert(10, rid).unwrap();
        assert!(leaf_page.delete(&10).is_ok());
        assert!(leaf_page.find_key_index(&10).is_err());
    }

    #[test]
    fn test_split_leaf_page() {
        let mut leaf_page = BPlusTreeLeafPage::new(2);
        leaf_page
            .insert(
                10,
                RID::builder()
                    .page_id(Some(PageId::from(1)))
                    .slot_num(1)
                    .build(),
            )
            .unwrap();
        leaf_page
            .insert(
                20,
                RID::builder()
                    .page_id(Some(PageId::from(2)))
                    .slot_num(2)
                    .build(),
            )
            .unwrap();
        let new_leaf = leaf_page.split();
        assert_eq!(leaf_page.keys().len(), 1);
        assert_eq!(new_leaf.keys().len(), 1);
    }

    #[test]
    fn test_merge_leaf_pages() {
        let mut leaf_page1 = BPlusTreeLeafPage::new(4);
        let mut leaf_page2 = BPlusTreeLeafPage::new(4);

        leaf_page1
            .insert(
                10,
                RID::builder()
                    .page_id(Some(PageId::from(1)))
                    .slot_num(1)
                    .build(),
            )
            .unwrap();
        leaf_page1
            .insert(
                20,
                RID::builder()
                    .page_id(Some(PageId::from(2)))
                    .slot_num(2)
                    .build(),
            )
            .unwrap();
        leaf_page2
            .insert(
                30,
                RID::builder()
                    .page_id(Some(PageId::from(3)))
                    .slot_num(3)
                    .build(),
            )
            .unwrap();
        leaf_page2
            .insert(
                40,
                RID::builder()
                    .page_id(Some(PageId::from(4)))
                    .slot_num(4)
                    .build(),
            )
            .unwrap();

        leaf_page1.merge(&mut leaf_page2);

        assert_eq!(leaf_page1.keys().len(), 4);
        assert_eq!(leaf_page1.key_at(2).unwrap(), &30);
        assert_eq!(leaf_page1.key_at(3).unwrap(), &40);
    }

    #[test]
    fn test_iteration() {
        let mut leaf_page = BPlusTreeLeafPage::new(4);
        for i in (10..=40).step_by(10) {
            leaf_page
                .insert(
                    i,
                    RID::builder()
                        .page_id(Some(PageId::from(i)))
                        .slot_num(i)
                        .build(),
                )
                .unwrap();
        }

        let mut iter = leaf_page.into_iter();
        assert_eq!(
            iter.next(),
            Some((
                &10,
                &RID::builder()
                    .page_id(Some(PageId::from(10)))
                    .slot_num(10)
                    .build()
            ))
        );
        assert_eq!(
            iter.next(),
            Some((
                &20,
                &RID::builder()
                    .page_id(Some(PageId::from(20)))
                    .slot_num(20)
                    .build()
            ))
        );
        assert_eq!(
            iter.next(),
            Some((
                &30,
                &RID::builder()
                    .page_id(Some(PageId::from(30)))
                    .slot_num(30)
                    .build()
            ))
        );
        assert_eq!(
            iter.next(),
            Some((
                &40,
                &RID::builder()
                    .page_id(Some(PageId::from(40)))
                    .slot_num(40)
                    .build()
            ))
        );
        assert_eq!(iter.next(), None);
    }
}
