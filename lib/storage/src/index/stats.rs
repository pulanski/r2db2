pub trait IndexStatistics {
    /// Returns the number of entries in the index.
    fn entry_count(&self) -> usize;

    /// Returns the size of the index in bytes.
    fn size_in_bytes(&self) -> usize;

    /// Returns the number of pages in the index.
    /// This is the number of pages that are allocated to the index.
    fn page_count(&self) -> usize;

    /// Returns the number of pages that are currently in memory.
    /// This is the number of pages that are currently pinned.
    fn pinned_page_count(&self) -> usize;
}
