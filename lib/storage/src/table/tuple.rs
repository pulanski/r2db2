#![allow(dead_code)]

use catalog::{schema::Schema, ColumnLength};
use common::rid::RID;
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ty::value::Value;
use typed_builder::TypedBuilder;

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
struct TupleMeta {
    ts: i64,
    is_deleted: bool,
}

impl TupleMeta {
    // TODO: impl methods and functionality
}

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("Invalid operation")]
    InvalidOperation,
    // ...
}

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
struct Tuple {
    rid: RID,
    data: Vec<u8>,
}

impl Tuple {
    pub fn empty() -> Self {
        Self {
            rid: RID::default(),
            data: Vec::new(),
        }
    }

    pub fn new(rid: RID, data: Vec<u8>) -> Self {
        Self { rid, data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn get_data_ptr(&self, schema: &Schema, column_idx: usize) -> Result<&[u8], TupleError> {
        let column = &schema.columns()[column_idx];

        if column.is_inlined() {
            let start = *column.column_offset() as usize;
            let end = start
                + match column.length() {
                    ColumnLength::Fixed(len) => *len as usize,
                    ColumnLength::Variable(_) => {
                        return Err(TupleError::InvalidOperation);
                    }
                };
            self.data
                .get(start..end)
                .ok_or(TupleError::InvalidOperation)
        } else {
            let offset_pos = *column.column_offset() as usize;
            let offset =
                u32::from_ne_bytes(self.data[offset_pos..offset_pos + 4].try_into().unwrap())
                    as usize;
            Ok(&self.data[offset..])
        }
    }

    pub fn get_value(&self, schema: &Schema, column_idx: usize) -> Result<Value, TupleError> {
        if column_idx >= schema.columns().len() {
            return Err(TupleError::InvalidOperation);
        }

        let column = &schema.columns()[column_idx];
        let data_ptr = self.get_data_ptr(schema, column_idx)?;

        Value::deserialize_from_type(data_ptr, column.column_type().clone())
            .map_err(|_| TupleError::InvalidOperation)
    }

    pub fn key_from_tuple(
        &self,
        schema: &Schema,
        key_schema: &Schema,
        key_attrs: &[u32],
    ) -> Result<Self, TupleError> {
        let mut values = Vec::new();

        for &idx in key_attrs {
            let value = self.get_value(schema, idx as usize)?;
            values.push(value);
        }

        Self::from_values(values, key_schema)
    }

    pub fn from_values(values: Vec<Value>, schema: &Schema) -> Result<Self, TupleError> {
        let mut data = Vec::new();
        let mut offset = 0;

        for (idx, value) in values.iter().enumerate() {
            let column = &schema.columns()[idx];
            let column_offset = *column.column_offset() as usize;

            if column.is_inlined() {
                let start = offset;
                let end = start
                    + match column.length() {
                        ColumnLength::Fixed(len) => *len as usize,
                        ColumnLength::Variable(_) => {
                            return Err(TupleError::InvalidOperation);
                        }
                    };
                data.extend_from_slice(
                    &value
                        .serialize_to()
                        .map_err(|_| TupleError::InvalidOperation)?[start..end],
                );
            } else {
                let offset_pos = *column.column_offset() as usize;
                let offset =
                    u32::from_ne_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap())
                        as usize;
                data.extend_from_slice(
                    &value
                        .serialize_to()
                        .map_err(|_| TupleError::InvalidOperation)?[offset..],
                );
            }
        }

        Ok(Self::new(RID::default(), data))
    }

    pub fn is_null(&self, schema: &Schema, column_idx: usize) -> Result<bool, TupleError> {
        return self.get_value(schema, column_idx).map(|v| v.is_null());
    }

    pub fn serialize_to(&self) -> Vec<u8> {
        let mut storage = vec![];
        storage.extend_from_slice(&self.data.len().to_ne_bytes());
        storage.extend_from_slice(&self.data);
        storage
    }

    pub fn deserialize_from(storage: &[u8]) -> Result<Self, TupleError> {
        let size = u32::from_ne_bytes(storage[..4].try_into().unwrap()) as usize;
        let data = storage[4..4 + size].to_vec();
        Ok(Self {
            rid: RID::default(),
            data,
        }) // Assuming a default RID for now
    }

    pub fn is_content_equal(&self, other: &Tuple) -> bool {
        self.data == other.data
    }

    // TODO: impl methods and functionality
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        s.push_str(&format!("Tuple({:?}, ", self.rid));
        s.push_str(&format!("{:?})", self.data));
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::Column;
    use pretty_assertions_sorted::assert_eq;
    use ty::{DataType, DataTypeKind};

    #[test]
    fn test_new_and_empty_tuple() {
        let tuple = Tuple::new(RID::default(), vec![1, 2, 3]);
        assert_eq!(tuple.len(), 3);
        assert!(!tuple.is_empty());
        assert_eq!(tuple.rid(), &RID::default());
        assert_eq!(tuple.data(), &[1, 2, 3]);

        let empty_tuple = Tuple::empty();
        assert!(empty_tuple.is_empty());
    }

    #[test]
    #[ignore = "Not yet implemented"]
    fn test_serialization_and_deserialization() {
        let tuple = Tuple::new(RID::default(), vec![1, 2, 3, 4, 5]);
        let serialized = tuple.serialize_to();
        let deserialized = Tuple::deserialize_from(&serialized).unwrap();

        assert_eq!(tuple, deserialized);
    }

    #[test]
    #[ignore = "Not yet implemented"]
    fn test_get_value() {
        //... create a Schema instance
        let schema = Schema::new(vec![
            Column::new_fixed("id", DataTypeKind::Integer).unwrap(),
            Column::new_fixed("name", DataTypeKind::VarChar).unwrap(),
        ]);

        //... create a Tuple instance that matches the schema
        let tuple = Tuple::new(RID::new(1, 5), vec![1, 2, 3, 4, 5]);
        let valid_column_index = 0;
        let invalid_column_index = 2;

        // Test retrieving a valid value
        let expected_value = Value::new(DataType::Integer(1));
        let value = tuple.get_value(&schema, valid_column_index).unwrap();
        assert_eq!(value, expected_value);

        // Test retrieving a value with an invalid column index
        assert!(tuple.get_value(&schema, invalid_column_index).is_err());
    }

    #[test]
    fn test_error_conditions() {
        //... construct a Schema instance
        let schema = Schema::new(vec![
            Column::new_fixed("id", DataTypeKind::Integer).unwrap(),
            Column::new_varlen_with_offset("name", DataTypeKind::VarChar, 255, 4).unwrap(),
        ]);
        //... create a Tuple instance
        let tuple = Tuple::new(RID::new(1, 5), vec![1, 2, 3, 4, 5]);
        let invalid_column_index = 2;

        // Test error condition, such as invalid column index
        assert!(tuple.get_value(&schema, invalid_column_index).is_err());
    }
}

#[cfg(test)]
mod integration_tests {
    use catalog::Column;
    use ty::DataTypeKind;

    use super::*;

    #[test]
    #[ignore = "Not yet implemented"]
    fn table_heap_test() {
        let create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
        let col1 = Column::new_varlen("a", DataTypeKind::VarChar, 20).unwrap();
        let col2 = Column::new_fixed_with_offset("b", DataTypeKind::SmallInt, 20).unwrap();
        let col3 = Column::new_fixed_with_offset("c", DataTypeKind::BigInt, 22).unwrap();
        let col4 = Column::new_fixed_with_offset("d", DataTypeKind::Boolean, 30).unwrap();
        let col5 = Column::new_varlen_with_offset("e", DataTypeKind::VarChar, 16, 31).unwrap();

        let cols = vec![col1, col2, col3, col4, col5];
        let schema = Schema::new(cols);

        // Construct a tuple for the test
        let tuple = construct_tuple(&schema);

        // TODO: Implement the following
        // Create disk manager, buffer pool manager, and table
        // let disk_manager = DiskManager::new("test.db");
        // let buffer_pool_manager = BufferPoolManager::new(50, disk_manager);
        // let mut table = TableHeap::new(buffer_pool_manager);

        // Insert tuples
        // let mut rid_v = Vec::new();
        // for _ in 0..5000 {
        //     let rid = table.insert_tuple(&tuple); // assuming this function exists
        //     rid_v.push(rid);
        // }

        // // Iterate over the table
        // let mut itr = table.make_iterator();
        // while let Some(t) = itr.next() {
        //     // println!("{}", t.to_string(&schema));
        // }
    }

    fn construct_tuple(schema: &Schema) -> Tuple {
        // Construct a Tuple instance based on the provided schema.
        unimplemented!()
    }
}

// TODO: Add apis
// pub fn conforms_to_schema(&self, schema: &Schema) -> bool {
//         todo!()
//     }
