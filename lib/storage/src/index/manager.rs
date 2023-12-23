use super::index::{Index, IndexMetadataRef};
use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;

/// Manages all indexes in the database.
///
/// This is a singleton object that is shared across the database.
///
/// TODO: This is a stub implementation. We need to implement this
///      as we add support for indexes.
///
/// TODO: This is not thread-safe. We need to add support for
///      concurrent access to the manager.
///
/// TODO: We need to add support for persisting indexes to disk.
///
/// TODO: We need to add support for loading indexes from disk.
///
/// TODO: We need to add support for transactions.
///
/// TODO: We need to add support for concurrency control.
///
/// TODO: We need to add support for recovery.
///
/// TODO: We need to add support for logging.
///
/// # Examples
///
/// ```rust,ignore // TODO: remove me
/// use anyhow::Result;
/// use catalog::schema::Schema;
/// use common::ids::TableId;
/// use common::storage::StorageEngine;
/// use index::manager::IndexManager;
/// use index::metadata::IndexMetadata;
/// use index::IndexType;
/// use std::sync::Arc;
/// use tuple::Tuple;
///
/// let tuple_schema = Arc::new(Schema::new(
///    vec![
///           Column::new_fixed("id", DataTypeKind::Integer).unwrap(),
///           Column::new_varlen_with_offset("name", DataTypeKind::VarChar, 255, 4).unwrap(),
///           Column::new_fixed_with_offset("age", DataTypeKind::Integer, 259).unwrap(),
///       ])
/// );
///    let index_metadata = IndexMetadata::new(
///        "my_index".to_string(),
///        "my_table".to_string(),
///        tuple_schema,
///        vec![0, 1],
///        true,
///    )?;
///
///    let mut index_manager = IndexManager::new();
///    index_manager.create_index(index_metadata, IndexType::BTree)?;
///
///    let index = index_manager.get_index("my_index").ok_or(IndexError::NotFoundError("my_index".to_string()))?;
///    index.insert_entry(&tuple, rid)?;
/// ```
pub struct IndexManager {
    indexes: Arc<DashMap<String, Box<dyn Index>>>,
}

impl IndexManager {
    /// Creates and registers a new index.
    pub fn create_index(
        &mut self,
        metadata: IndexMetadataRef,
        // index_type: IndexType,
    ) -> Result<()> {
        // Create index based on type and metadata...
        // Register the index in the manager...
        todo!()
    }

    /// Drops an index.
    pub fn drop_index(&mut self, index_name: &str) -> Result<()> {
        // Drop the index from the manager...
        todo!()
    }

    /// Retrieves an index by name.
    pub fn get_index(&self, index_name: &str) -> Option<&dyn Index> {
        // Return the index if it exists...
        todo!()
    }
}
