//! # Values (and their operations)
//!
//! This module provides a [`Value`] which represents a wrapped [`DataType`].
//!
//! It offers a variety of operations such as arithmetic, logical, string manipulations,
//! and date/time functions. These operations are designed to be safe, emitting warnings
//! when used incorrectly, and handle `DataType::Null` appropriately following SQL semantics.

#![allow(dead_code)]

use crate::{DataType, DataTypeKind, TypeError};
use std::{
    fmt,
    ops::{Add, Div, Mul, Sub},
};
use tracing::warn;
use typed_builder::TypedBuilder;

/// Represents a value in the database with various data types.
///
/// [`Value`] is a core struct in the database system that encapsulates different data types like
/// integers, floats, booleans, strings, and more complex types like DateTime and UUID. It provides
/// a variety of operations such as arithmetic, logical, string manipulations, and date/time functions.
/// These operations are designed to be safe and SQL-semantics compliant, especially in handling `Null` values.
///
/// # Examples
///
/// Creating a new `Value` with an integer and performing arithmetic operations:
///
/// ```rust
/// use ty::{DataType, Value};
///
/// let value1 = Value::new(DataType::Integer(10));
/// let value2 = Value::new(DataType::Integer(20));
/// let sum = value1 + value2;
///
/// assert_eq!(sum, Value::new(DataType::Integer(30)));
/// ```
///
/// ## String operations:
///
/// ```
/// use ty::{DataType, Value};
///
/// let hello = Value::new(DataType::Text("Hello".to_string()));
/// let world = Value::new(DataType::Text("World".to_string()));
/// let concatenated = hello.concat(&world);
///
/// assert_eq!(concatenated, Value::new(DataType::Text("HelloWorld".to_string())));
///
/// let substring = concatenated.substring(5, 5);
/// assert_eq!(substring, Value::new(DataType::Text("World".to_string())));
/// ```
///
/// ## Date operations:
///
/// ```
/// use ty::{DataType, Value};
///
/// let date = Value::new(DataType::DateTime(chrono::NaiveDateTime::from_timestamp(1609459200, 0)));
/// let added_duration = date.date_add(chrono::Duration::days(1));
///
/// assert_eq!(added_duration, Value::new(DataType::DateTime(chrono::NaiveDateTime::from_timestamp(1609545600, 0))));
/// ```
///
/// ## Logical operations:
///
/// ```
/// use ty::{DataType, Value};
///
/// let true_value = Value::new(DataType::Boolean(true));
/// let false_value = Value::new(DataType::Boolean(false));
///
/// assert_eq!(true_value.and(&true_value), Value::new(DataType::Boolean(true))); // true && true == true
/// assert_eq!(true_value.and(&false_value), Value::new(DataType::Boolean(false))); // true && false == false
/// assert_eq!(false_value.or(&true_value), Value::new(DataType::Boolean(true))); // false || true == true
/// // ...
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, TypedBuilder)]
pub struct Value {
    data: DataType,
}

impl Value {
    /// Creates a new [`Value`] from the given [`DataType`].
    pub fn new(data: DataType) -> Self {
        Self { data }
    }

    /// Determines if the [`Value`] is a [`Null`] type.
    fn is_null(&self) -> bool {
        matches!(self.data, DataType::Null)
    }

    /// Coerces the [`Value`] to a specified [`DataTypeKind`], returning a [`TypeError`] on failure.
    pub fn coerce_to(&self, target_type: DataTypeKind) -> Result<Self, TypeError> {
        self.data.coerce_to(&target_type).map(Value::new)
    }

    /// Returns the minimum of two [`Value`] instances, respecting SQL `Null` semantics.
    pub fn min(self, other: Self) -> Self {
        if self.data <= other.data {
            self
        } else {
            other
        }
    }

    /// Returns the maximum of two [`Value`] instances, respecting SQL `Null` semantics.
    pub fn max(self, other: Self) -> Self {
        if self.data >= other.data {
            self
        } else {
            other
        }
    }

    /// Counts the number of non-null `Value` instances in the given slice.
    pub fn count(values: &[Value]) -> usize {
        values.iter().filter(|v| !v.is_null()).count()
    }

    // - Logical operations -

    /// Performs a logical AND operation between two boolean [`Value`] instances.
    /// Returns `Null` if either value is not a boolean.
    pub fn and(&self, other: &Self) -> Self {
        match (self, other) {
            (
                Value {
                    data: DataType::Boolean(a),
                },
                Value {
                    data: DataType::Boolean(b),
                },
            ) => Value::new(DataType::Boolean(*a && *b)),
            _ => Value::new(DataType::Null),
        }
    }

    /// Performs logical OR on two boolean [`Value`] instances.
    /// Returns `Null` if either value is not a boolean.
    pub fn or(&self, other: &Self) -> Self {
        match (self, other) {
            (
                Value {
                    data: DataType::Boolean(a),
                },
                Value {
                    data: DataType::Boolean(b),
                },
            ) => Value::new(DataType::Boolean(*a || *b)),
            _ => Value::new(DataType::Null),
        }
    }

    /// Performs logical NOT on a boolean [`Value`] instance.
    /// Returns `Null` if the value is not a boolean.
    pub fn not(&self) -> Self {
        match self {
            Value {
                data: DataType::Boolean(a),
            } => Value::new(DataType::Boolean(!*a)),
            _ => Value::new(DataType::Null),
        }
    }

    // -- String operations --

    /// Concatenates two [`Value`] instances of `DataType::Text`.
    /// Emits a warning and returns `Null` if either value is not `Text`.
    pub fn concat(&self, other: &Self) -> Self {
        match (self, other) {
            (
                Value {
                    data: DataType::Text(a),
                },
                Value {
                    data: DataType::Text(b),
                },
            ) => Value::new(DataType::Text(format!("{}{}", a, b))),
            _ => {
                warn!("Concat operation called on non-text values");
                Value::new(DataType::Null)
            }
        }
    }

    /// Extracts a substring from a [`Value`] of `DataType::Text`.
    /// Emits a warning and returns `Null` if called on a non-text value.
    pub fn substring(&self, start: usize, length: usize) -> Self {
        match &self.data {
            DataType::Text(text) => Value::new(DataType::Text(
                text.chars().skip(start).take(length).collect(),
            )),
            _ => {
                warn!("Substring operation called on a non-text value");
                Value::new(DataType::Null)
            }
        }
    }

    /// Returns the length of a [`Value`] of `DataType::Text`.
    /// Emits a warning and returns `Null` if called on a non-text value.
    pub fn length(&self) -> Self {
        match &self.data {
            DataType::Text(text) => Value::new(DataType::Integer(text.len() as i32)),
            _ => {
                warn!("Length operation called on a non-text value");
                Value::new(DataType::Null)
            }
        }
    }

    // -- Date/time operations --

    /// Adds a `chrono::Duration` to a [`Value`] of [`DataType::DateTime`].
    /// Emits a warning and returns `Null` if called on a non-date value.
    pub fn date_add(&self, interval: chrono::Duration) -> Self {
        match &self.data {
            DataType::DateTime(datetime) => Value::new(DataType::DateTime(*datetime + interval)),
            _ => {
                warn!("Date add operation called on a non-date value");
                Value::new(DataType::Null)
            }
        }
    }

    /// Subtracts a `chrono::Duration` from a [`Value`] of [`DataType::DateTime`].
    /// Emits a warning and returns `Null` if called on a non-date value.
    pub fn date_sub(&self, interval: chrono::Duration) -> Self {
        match &self.data {
            DataType::DateTime(datetime) => Value::new(DataType::DateTime(*datetime - interval)),
            _ => Value::new(DataType::Null),
        }
    }

    /// Returns the difference between two [`Value`] instances of [`DataType::DateTime`].
    /// Emits a warning and returns `Null` if called on a non-date value.
    pub fn date_diff(&self, other: &Self) -> Self {
        match (&self.data, &other.data) {
            (DataType::DateTime(a), DataType::DateTime(b)) => {
                let duration = *a - *b;
                Value::new(DataType::Integer(duration.num_seconds() as i32))
            }
            _ => Value::new(DataType::Null),
        }
    }

    /// Formats a [`Value`] of [`DataType::DateTime`] as a string.
    /// Emits a warning and returns `Null` if called on a non-date value.
    pub fn format_date(&self, format: &str) -> Self {
        match &self.data {
            DataType::DateTime(datetime) => {
                Value::new(DataType::Text(datetime.format(format).to_string()))
            }
            _ => Value::new(DataType::Null),
        }
    }
}

macro_rules! impl_arith_op {
    ($trait:ident, $method:ident, $checked_method:ident) => {
        impl $trait for Value {
            type Output = Result<Value, TypeError>;

            fn $method(self, other: Self) -> Self::Output {
                match (self.data, other.data) {
                    (DataType::Integer(a), DataType::Integer(b)) => a
                        .$checked_method(b)
                        .map(DataType::Integer)
                        .ok_or(TypeError::OverflowError {
                            data_type: "Integer".to_string(),
                        })
                        .map(Value::new),
                    // TODO: Handle other combinations of data types...
                    _ => Err(TypeError::IncompatibleType {
                        expected: "Numeric".to_string(),
                        found: "Non-numeric".to_string(),
                    }),
                }
            }
        }
    };
}

impl_arith_op!(Add, add, checked_add);
impl_arith_op!(Sub, sub, checked_sub);
impl_arith_op!(Mul, mul, checked_mul);
impl_arith_op!(Div, div, checked_div);

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl Eq for Value {}
