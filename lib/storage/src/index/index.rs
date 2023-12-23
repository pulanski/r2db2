use crate::table::tuple::Tuple;
use anyhow::Result;
use catalog::schema::{Schema, SchemaRef};
use common::rid::RID;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// A reference-counted reference to an `IndexMetadata` instance.
pub type IndexMetadataRef = Arc<IndexMetadata>;

/// Holds metadata of an index object.
#[derive(Debug, Clone, Getters, Setters, TypedBuilder, Serialize, Deserialize)]
pub struct IndexMetadata {
    name: String,
    table_name: String,
    key_attrs: Vec<u32>,
    #[serde(skip)]
    key_schema: SchemaRef,
    is_primary_key: bool,
}

impl IndexMetadata {
    /// Constructs a new `IndexMetadata` instance.
    pub fn new(
        name: String,
        table_name: String,
        tuple_schema: SchemaRef,
        key_attrs: Vec<u32>,
        is_primary_key: bool,
    ) -> Result<Self> {
        let key_schema = Schema::copy_schema(&tuple_schema, key_attrs.clone())?;

        Ok(IndexMetadata::builder()
            .name(name)
            .table_name(table_name)
            .key_attrs(key_attrs)
            .key_schema(Arc::new(key_schema))
            .is_primary_key(is_primary_key)
            .build())
    }
}

pub trait Index {
    /// Inserts an entry into the index.
    fn insert_entry(&self, key: &Tuple, rid: RID) -> Result<()>;

    /// Deletes an entry from the index.
    fn delete_entry(&self, key: &Tuple, rid: RID) -> Result<()>;

    /// Searches the index with the provided key.
    fn scan_key(&self, key: &Tuple) -> Result<Vec<RID>>;

    // TODO: Additional methods as needed...

    // TODO: support transactions
    // fn insert_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> Result<()>;
    // fn delete_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> Result<()>;
    // fn scan_key(&self, key: &Tuple, transaction: &Transaction) -> Result<Vec<RID>, String>;
}
