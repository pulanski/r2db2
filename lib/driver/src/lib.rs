use anyhow::Result;
use buffer::{BufferPoolManager, ReplacementPolicy};
use catalog::Database;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use storage::disk::DiskManager;
use tracing::info;
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
pub struct Driver {
    databases: Arc<DashMap<String, Arc<Database>>>,
    buffer_pool_manager: Arc<BufferPoolManager>,
    disk_manager: Arc<DiskManager>,
}

impl Driver {
    // Create a new driver
    pub fn new() -> Result<Self> {
        let disk_manager = Arc::new(DiskManager::new("test.db")?);
        let buffer_pool_manager = Arc::new(BufferPoolManager::new_with_size(
            ReplacementPolicy::LRU,
            disk_manager.clone(),
            10,
        ));

        Ok(Driver::builder()
            .databases(Arc::new(DashMap::new()))
            .buffer_pool_manager(buffer_pool_manager)
            .disk_manager(disk_manager)
            .build())
    }

    // Add a database
    pub fn add_database(&self, name: String, database: Arc<Database>) {
        todo!("Add a database")
    }

    // Get a database
    pub fn get_database(&self, name: &str) -> Option<Arc<Database>> {
        todo!("Get a database")
    }

    // Remove a database
    pub fn remove_database(&self, name: &str) {
        todo!("Remove a database")
    }
}
