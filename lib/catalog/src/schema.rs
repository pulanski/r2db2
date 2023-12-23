#![allow(dead_code)]

use std::sync::Arc;

use crate::{Column, ColumnLength};
use anyhow::Result;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, trace};
use typed_builder::TypedBuilder;

/// A reference-counted [`Schema`] handle that can be shared across threads.
pub type SchemaRef = Arc<Schema>;

/// [`Schema`] represents the structure of a dataset in a database, defining the
/// organization, types, and properties of the data.
///
/// Primarily focuses on handling the schema for database tables,
/// including column details and characteristics of the data they hold.
/// It also provides efficient data access and manipulation methods.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Getters,
    Setters,
    TypedBuilder,
    Serialize,
    Deserialize,
)]
#[getset(get = "pub", set = "pub")]
pub struct Schema {
    length: u32,
    columns: Vec<Column>,
    tuple_is_inlined: bool,
    uninlined_columns: Vec<u32>,
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Column index out of bounds")]
    ColumnIndexOutOfBounds,
    #[error("Column name not found")]
    ColumnNameNotFound,
    #[error("Column type mismatch")]
    ColumnTypeMismatch,
    #[error("Column length mismatch")]
    ColumnLengthMismatch,
}

impl Schema {
    /// Constructs a new [`Schema`] from a given set of columns.
    /// Computes the total length and identifies uninlined columns,
    /// setting up the initial state of the schema.
    pub fn new(columns: Vec<Column>) -> Self {
        let mut length = 0;
        let mut tuple_is_inlined = true;
        let mut uninlined_columns = Vec::new();

        for (index, column) in columns.iter().enumerate() {
            if !column.is_inlined() {
                tuple_is_inlined = false;
                uninlined_columns.push(index as u32);
            }

            length += match column.length() {
                ColumnLength::Fixed(len) => len,
                ColumnLength::Variable(len) => len,
            };
        }

        info!("Schema created with length: {}", length);
        Self {
            length,
            columns,
            tuple_is_inlined,
            uninlined_columns,
        }
    }

    /// Creates a copy of the schema based on specified attributes.
    /// This method allows for selective schema replication, providing flexibility
    /// and efficiency in schema management.
    pub fn copy_schema(schema: &Schema, attrs: Vec<u32>) -> Result<Self> {
        let mut cols = Vec::with_capacity(attrs.len());
        for i in attrs {
            match schema.columns.get(i as usize) {
                Some(column) => cols.push(column.clone()),
                None => {
                    error!("Column index {} is out of bounds", i);
                    return Err(SchemaError::ColumnIndexOutOfBounds.into());
                }
            }
        }

        trace!("Schema copied with {} columns", cols.len());
        Ok(Self::new(cols))
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_column(&self, col_idx: usize) -> Result<&Column> {
        self.columns
            .get(col_idx)
            .ok_or(SchemaError::ColumnIndexOutOfBounds.into())
    }

    pub fn get_col_idx(&self, col_name: &str) -> Result<usize> {
        let mut col_idx = 0;
        for col in &self.columns {
            if col.column_name() == col_name {
                return Ok(col_idx);
            }
            col_idx += 1;
        }

        Err(SchemaError::ColumnNameNotFound.into())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Schema::builder()
            .length(0)
            .columns(Vec::new())
            .tuple_is_inlined(true)
            .uninlined_columns(Vec::new())
            .build()
    }
}

impl std::fmt::Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut schema_str = String::new();
        for col in &self.columns {
            schema_str.push_str(&format!("{} ", col));
        }
        write!(f, "{}", schema_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Column;
    use pretty_assertions_sorted::assert_eq;
    use ty::DataTypeKind;

    #[test]
    fn test_schema_new() {
        // Create a vector of columns
        let columns = vec![
            Column::new_fixed("id", DataTypeKind::Integer).unwrap(),
            Column::new_varlen_with_offset("name", DataTypeKind::VarChar, 255, 4).unwrap(),
            Column::new_fixed_with_offset("age", DataTypeKind::Integer, 259).unwrap(),
        ];
        let schema = Schema::new(columns);
        let expected_length = 263;
        let expected_columns_len = 3;

        // Assertions to validate the schema's state
        assert_eq!(schema.length(), &expected_length);
        assert_eq!(schema.columns().len(), expected_columns_len);
    }

    #[test]
    fn test_schema_copy_valid() {
        let schema = Schema::new(vec![
            Column::new_fixed("id", DataTypeKind::Integer).unwrap(),
            Column::new_varlen_with_offset("name", DataTypeKind::VarChar, 255, 4).unwrap(),
            Column::new_fixed_with_offset("age", DataTypeKind::Integer, 259).unwrap(),
        ]);
        let copied_schema = Schema::copy_schema(&schema, vec![0, 1]).unwrap();

        // Validate the copied schema
        assert_eq!(copied_schema.columns().len(), 2);
        assert_eq!(*copied_schema.length(), 259);
        assert_eq!(copied_schema.columns()[0].column_name(), "id");
        assert_eq!(copied_schema.columns()[1].column_name(), "name");
    }

    #[test]
    fn test_schema_copy_invalid_index() {
        let schema = Schema::new(vec![/* ... */]);
        let result = Schema::copy_schema(&schema, vec![100]);

        // Expect an error
        assert!(result.is_err());
    }

    #[test]
    fn test_get_column_valid_index() {
        let schema = Schema::new(vec![
            Column::new_fixed("id", DataTypeKind::Integer).unwrap(),
            Column::new_varlen_with_offset("name", DataTypeKind::VarChar, 255, 4).unwrap(),
            Column::new_fixed_with_offset("age", DataTypeKind::Integer, 259).unwrap(),
        ]);
        let col0 = schema.get_column(0).unwrap();

        assert_eq!(col0.column_name(), "id");
        assert_eq!(col0.column_type(), &DataTypeKind::Integer);
        assert_eq!(col0.length(), &ColumnLength::Fixed(4));

        let col1 = schema.get_column(1).unwrap();

        assert_eq!(col1.column_name(), "name");
        assert!(matches!(col1.column_type(), DataTypeKind::VarChar));
        assert_eq!(col1.length(), &ColumnLength::Variable(255));

        let col2 = schema.get_column(2).unwrap();

        assert_eq!(col2.column_name(), "age");
        assert_eq!(col2.column_type(), &DataTypeKind::Integer);
        assert_eq!(col2.length(), &ColumnLength::Fixed(4));
    }

    #[test]
    fn test_get_column_invalid_index() {
        let schema = Schema::new(vec![/* ... */]);
        let result = schema.get_column(100);

        // Expect an error
        assert!(result.is_err());
    }
}
