use common::rid::RID;

pub trait IndexIterator {
    /// Advances the iterator to the next element.
    fn next(&mut self) -> Option<RID>;

    /// Checks if the iterator has more elements.
    fn has_next(&self) -> bool;
}

// pub struct BTreeIndexIterator {
//     // Implementation-specific fields...
// }

// impl IndexIterator for BTreeIndexIterator {
//     // Implement the methods...
// }
