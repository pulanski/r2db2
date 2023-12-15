//! # Column Representation
//!
//! This module provides a representation of a database column in a table schema.
//! It defines the `Column` that encapsulates the details of a column
//! such as its name, data type, length, and offset.
//!
//! ## Features
//!
//! - Supports both fixed-length and variable-length data types.
//! - Comprehensive error handling with `ColumnError`.
//!
//! ## Usage
//!
//! To create a new fixed-length column:
//!
//! ```
//! use catalog::Column;
//! use ty::DataTypeKind;
//!
//! let fixed_column = Column::new_fixed("id", DataTypeKind::Integer).expect("Failed to create a new fixed-length column.");
//! ```
//!
//! To create a new variable-length column:
//!
//! ```
//! use catalog::Column;
//! use ty::DataTypeKind;
//!
//! let varlen_column = Column::new_varlen("name", DataTypeKind::VarChar, 255).expect("Failed to create a new variable-length column.");
//! ```

use anyhow::Result;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use tracing::warn;
use ty::DataTypeKind;
use typed_builder::TypedBuilder;

#[derive(Error, Debug)]
pub enum ColumnError {
    #[error("Invalid type for this operation")]
    InvalidType,
    #[error("Invalid length for this operation")]
    InvalidLength,
    // ...
}

/// Represents the length type of a column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ColumnLength {
    Fixed(u32),
    Variable(u32),
}

/// Represents a column in a database table.
///
/// A `Column` is characterized by its name, data type, length, and an offset in the table.
/// The length can be fixed or variable, depending on the data type.
///
/// ```ignore
/// +--------------+--------------+--------------+--------------+
/// | column_name  | column_type  | length       | column_offset|
/// +--------------+--------------+--------------+--------------+
/// | id           | integer      | 4            | 0            | (fixed)
/// +--------------+--------------+--------------+--------------+
/// | name         | varchar      | 255          | 4            | (variable)
/// +--------------+--------------+--------------+--------------+
/// ```
///
/// ## Usage
///
/// To create a new `Column`, you can use the builder pattern:
///
/// ```rust
/// use catalog::Column;
/// use ty::DataTypeKind;
/// use catalog::ColumnLength;
///
/// let column = Column::builder()
///     .column_name("name".to_string())
///     .column_type(DataTypeKind::VarChar)
///     .length(ColumnLength::Variable(255))
///     .column_offset(4)
///     .build();
/// ```
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    TypedBuilder,
    Getters,
    Setters,
)]
#[getset(get = "pub")]
pub struct Column {
    column_name: String,
    column_type: DataTypeKind,
    length: ColumnLength,
    column_offset: u32,
}

impl Column {
    /// Creates a new fixed-length column.
    pub fn new_fixed(column_name: &str, column_type: DataTypeKind) -> Result<Self, ColumnError> {
        let length = match &column_type {
            DataTypeKind::SmallInt
            | DataTypeKind::Integer
            | DataTypeKind::BigInt
            | DataTypeKind::Boolean => 4,
            DataTypeKind::Float | DataTypeKind::DoublePrecision => 8,
            DataTypeKind::DateTime => 8,
            _ => {
                warn!("Invalid type for this operation. Expected a fixed-length type (e.g., integer, float, etc.), but found: {:?}", column_type);
                return Err(ColumnError::InvalidType);
            }
        };

        Ok(Column::builder()
            .column_name(column_name.to_string())
            .column_type(column_type)
            .length(ColumnLength::Fixed(length))
            .column_offset(0)
            .build())
    }

    /// Creates a new fixed-length column with an offset.
    /// This is useful when creating a column in a table with multiple columns.
    /// The offset is the sum of the lengths of the previous columns.
    pub fn new_fixed_with_offset(
        column_name: &str,
        column_type: DataTypeKind,
        column_offset: u32,
    ) -> Result<Self, ColumnError> {
        let length = match &column_type {
            DataTypeKind::SmallInt
            | DataTypeKind::Integer
            | DataTypeKind::BigInt
            | DataTypeKind::Boolean => 4,
            DataTypeKind::Float | DataTypeKind::DoublePrecision => 8,
            DataTypeKind::DateTime => 8,
            _ => {
                warn!("Invalid type for this operation. Expected a fixed-length type (e.g., integer, float, etc.), but found: {:?}", column_type);
                return Err(ColumnError::InvalidType);
            }
        };

        Ok(Column::builder()
            .column_name(column_name.to_string())
            .column_type(column_type)
            .length(ColumnLength::Fixed(length))
            .column_offset(column_offset)
            .build())
    }

    /// Creates a new variable-length column.
    pub fn new_varlen(
        column_name: &str,
        column_type: DataTypeKind,
        length: u32,
    ) -> Result<Self, ColumnError> {
        if length == 0 {
            warn!(
                "Invalid length for this operation. Expected a positive integer, but found: {:?}",
                length
            );
            return Err(ColumnError::InvalidLength);
        }

        if column_type != DataTypeKind::VarChar {
            warn!("Invalid type for this operation. Expected a variable-length type (e.g., varchar), but found: {:?}", column_type);
            return Err(ColumnError::InvalidType);
        }

        Ok(Column::builder()
            .column_name(column_name.to_string())
            .column_type(column_type)
            .length(ColumnLength::Variable(length))
            .column_offset(0)
            .build())
    }

    /// Creates a new variable-length column with an offset.
    /// This is useful when creating a column in a table with multiple columns.
    /// The offset is the sum of the lengths of the previous columns.
    pub fn new_varlen_with_offset(
        column_name: &str,
        column_type: DataTypeKind,
        length: u32,
        column_offset: u32,
    ) -> Result<Self, ColumnError> {
        if length == 0 {
            warn!(
                "Invalid length for this operation. Expected a positive integer, but found: {:?}",
                length
            );
            return Err(ColumnError::InvalidLength);
        }

        if column_type != DataTypeKind::VarChar {
            warn!("Invalid type for this operation. Expected a variable-length type (e.g., varchar), but found: {:?}", column_type);
            return Err(ColumnError::InvalidType);
        }

        Ok(Column::builder()
            .column_name(column_name.to_string())
            .column_type(column_type)
            .length(ColumnLength::Variable(length))
            .column_offset(column_offset)
            .build())
    }

    /// Returns `true` iff the column is fixed-length, `false` otherwise.
    pub fn is_inlined(&self) -> bool {
        match self.length {
            ColumnLength::Fixed(_) => true,
            ColumnLength::Variable(_) => false,
        }
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Column: {}, Type: {:?}",
            self.column_name, self.column_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_fixed_column() {
        let column = Column::new_fixed("id", DataTypeKind::Integer).unwrap();
        assert_eq!(column.column_name(), "id");
        assert_eq!(column.column_type(), &DataTypeKind::Integer);
        assert_eq!(column.length(), &ColumnLength::Fixed(4));
    }

    #[test]
    fn test_create_varchar_column() {
        let column = Column::new_varlen("name", DataTypeKind::VarChar, 255).unwrap();
        assert_eq!(column.column_name(), "name");
        assert!(matches!(column.column_type(), DataTypeKind::VarChar));
        assert_eq!(column.length(), &ColumnLength::Variable(255));
    }

    #[test]
    fn test_invalid_varchar_column() {
        let column = Column::new_varlen("name", DataTypeKind::VarChar, 0);
        assert!(column.is_err());
    }

    #[test]
    fn test_create_fixed_column_with_offset() {
        let column = Column::new_fixed_with_offset("id", DataTypeKind::Integer, 4).unwrap();
        assert_eq!(column.column_name(), "id");
        assert_eq!(column.column_type(), &DataTypeKind::Integer);
        assert_eq!(column.length(), &ColumnLength::Fixed(4));
        assert_eq!(column.column_offset(), &4);
    }

    #[test]
    fn test_create_varchar_column_with_offset() {
        let column = Column::new_varlen_with_offset("name", DataTypeKind::VarChar, 255, 4).unwrap();
        assert_eq!(column.column_name(), "name");
        assert!(matches!(column.column_type(), DataTypeKind::VarChar));
        assert_eq!(column.length(), &ColumnLength::Variable(255));
        assert_eq!(column.column_offset(), &4);
    }

    #[test]
    fn test_invalid_fixed_column() {
        let column = Column::new_fixed("id", DataTypeKind::VarChar);
        assert!(column.is_err());
    }

    #[test]
    fn test_invalid_fixed_column_with_offset() {
        let column = Column::new_fixed_with_offset("id", DataTypeKind::VarChar, 4);
        assert!(column.is_err());
    }

    #[test]
    fn test_invalid_varchar_column_with_offset() {
        let column = Column::new_varlen_with_offset("name", DataTypeKind::VarChar, 0, 4);
        assert!(column.is_err());
    }
}
