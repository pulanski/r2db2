use dashmap::DashMap;
use std::sync::Arc;

pub mod column;
pub mod schema;

pub use column::*;

// #[derive(Debug)]
// pub struct Database {
//     tables: Arc<DashMap<String, Arc<Table>>>,
// }

#[derive(Debug)]
pub struct Table {
    name: String,
    schema: Schema,
    indexes: Vec<Index>,
}

#[derive(Debug)]
pub struct Schema {
    columns: Vec<Column>,
}

#[derive(Debug)]
pub struct Index {
    // TODO: Implement Index
    name: String,
    columns: Vec<Column>,
}
