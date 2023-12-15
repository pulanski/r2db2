//! # Type System
//!
//! Encapsulates different data types like integers, floats, strings,
//! complex types (arrays, maps, enums), and special types like DateTime and UUID.
//!
//! Example Usage:
//!
//! ```
//! use ty::{DataType, DataTypeKind};
//! use serde_json::json;
//! let integer_data = DataType::Integer(42);
//! let text_data = DataType::Text("Hello, World!".to_string());
//! let json_data = DataType::Json(json!({"key": "value"}));
//! // ...
//! ```
//!
//! ## Encoding and Coercion:
//!
//! Each data type can be encoded into bytes and coerced into other compatible types,
//! facilitating data storage and type conversions.
//!
//! ## Error Handling:
//!
//! The system robustly handles errors like incompatible types, invalid casts, and overflows.
//!
//! ## Serialization/Deserialization:
//!
//! DataTypes can be serialized for storage or network transmission and deserialized back.
//! This feature supports complex nested types and ensures data integrity across different platforms.

// TODO: refactor and reorganize the code
// mod array;
// mod boolean;
// mod datetime;
// mod decimal;
// mod enum;
// mod float;
// mod integer;
// mod json;
// mod map;
// mod null;
// mod range;
// mod text;
// mod uuid;
// mod varchar;

pub mod value;
pub use value::*;

use chrono::NaiveDateTime;
use common::traits::encode::{Encodable, EncodingError};
use core::fmt;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeError {
    IncompatibleType { expected: String, found: String },
    InvalidCast { from: String, to: String },
    OverflowError { data_type: String },
    PrecisionError { data_type: String },
    // ...
}

impl std::fmt::Display for TypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TypeError::IncompatibleType { expected, found } => {
                write!(f, "Expected type {}, but found {}", expected, found)
            }
            TypeError::InvalidCast { from, to } => write!(f, "Cannot cast from {} to {}", from, to),
            TypeError::OverflowError { data_type } => write!(f, "Overflow error for {}", data_type),
            TypeError::PrecisionError { data_type } => {
                write!(f, "Precision error for {}", data_type)
            }
        }
    }
}

impl std::error::Error for TypeError {}

pub trait TypeCheck {
    fn is_compatible_with(&self, other: &Self) -> bool;
    fn try_cast_to(&self, target: &Self) -> Result<Self, TypeError>
    where
        Self: Sized;
}

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DataTypeKind {
    #[default]
    Null,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Real,
    DoublePrecision,
    SmallSerial,
    Serial,
    BigSerial,
    Float,
    Text,
    VarChar,
    Blob,
    DateTime,
    Json,
    Uuid,
    Array,
    Map,
    Enum,
    Range,
    Boolean,
    Point,
    Line,
    LineSegment,
    Box,
    Path,
    Polygon,
    Circle,
    // Geospatial(GeospatialType),          // TODO: impl GeospatialType
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Null,
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(Decimal),
    Real(f32),
    DoublePrecision(f64),
    SmallSerial(i16),
    Serial(i32),
    BigSerial(i64),
    Boolean(bool),
    Float(f64),
    Text(String),
    VarChar(String),
    Blob(Vec<u8>),
    DateTime(chrono::NaiveDateTime),
    Json(serde_json::Value),
    Uuid(uuid::Uuid),
    Array(Vec<DataType>),
    Map(HashMap<String, DataType>),
    Enum(String, Vec<String>),           // Enum name and possible values
    Range(Box<DataType>, Box<DataType>), // Start and End
    Point(Point),
    Line(Line),
    LineSegment(LineSegment),
    Box(BoxType),
    Path(PathType),
    Polygon(Polygon),
    Circle(Circle),
    // Geospatial(GeospatialType),          // TODO: impl GeospatialType
}

impl DataType {
    fn coerce_to(&self, target_type: &DataTypeKind) -> Result<DataType, TypeError> {
        match target_type {
            DataTypeKind::SmallInt => match self {
                DataType::SmallInt(_) => Ok(self.clone()),
                DataType::Text(val) => match val.parse::<i16>() {
                    Ok(val) => Ok(DataType::SmallInt(val)),
                    Err(_) => Err(TypeError::InvalidCast {
                        from: "Text".to_string(),
                        to: "Integer".to_string(),
                    }),
                },
                _ => Err(TypeError::IncompatibleType {
                    expected: "Integer".to_string(),
                    found: self.kind(),
                }),
            },
            DataTypeKind::Float => match self {
                DataType::SmallInt(val) => Ok(DataType::Float(*val as f64)),
                DataType::Float(_) => Ok(self.clone()),
                DataType::Text(val) => match val.parse::<f64>() {
                    Ok(val) => Ok(DataType::Float(val)),
                    Err(_) => Err(TypeError::InvalidCast {
                        from: "Text".to_string(),
                        to: "Float".to_string(),
                    }),
                },
                _ => Err(TypeError::IncompatibleType {
                    expected: "Float".to_string(),
                    found: self.kind(),
                }),
            },
            DataTypeKind::Text => match self {
                DataType::SmallInt(val) => Ok(DataType::Text(val.to_string())),
                DataType::Float(val) => Ok(DataType::Text(val.to_string())),
                DataType::Text(_) => Ok(self.clone()),
                DataType::DateTime(val) => Ok(DataType::Text(val.to_string())),
                DataType::Json(val) => Ok(DataType::Text(val.to_string())),
                DataType::Boolean(val) => Ok(DataType::Text(val.to_string())),
                _ => Err(TypeError::IncompatibleType {
                    expected: "Text".to_string(),
                    found: self.kind(),
                }),
            },
            DataTypeKind::Blob => match self {
                DataType::Blob(_) => Ok(self.clone()),
                _ => Err(TypeError::IncompatibleType {
                    expected: "Blob".to_string(),
                    found: self.kind(),
                }),
            },
            _ => Err(TypeError::IncompatibleType {
                expected: format!("{:?}", target_type),
                found: self.kind(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        match self {
            DataType::SmallInt(val) => *val == 0,
            DataType::Float(val) => *val == 0.0,
            DataType::BigInt(val) => *val == 0,
            DataType::Decimal(val) => val.is_zero(),
            DataType::Real(val) => *val == 0.0,
            DataType::DoublePrecision(val) => *val == 0.0,
            DataType::SmallSerial(val) => *val == 0,
            DataType::Serial(val) => *val == 0,
            DataType::BigSerial(val) => *val == 0,
            DataType::Text(val) => val.is_empty(),
            DataType::Blob(val) => val.is_empty(),
            DataType::DateTime(val) => val.timestamp() == 0,
            DataType::Json(val) => val.is_null(),
            DataType::Uuid(val) => val.is_nil(),
            DataType::Array(val) => val.is_empty(),
            DataType::Map(val) => val.is_empty(),
            DataType::Enum(val, _) => val.is_empty(),
            DataType::Range(start, end) => start.is_null() && end.is_null(),
            DataType::Boolean(val) => !*val,
            DataType::Integer(val) => *val == 0,
            DataType::Point(val) => val.x == 0.0 && val.y == 0.0,
            DataType::Line(val) => val.a == 0.0 && val.b == 0.0 && val.c == 0.0,
            DataType::LineSegment(val) => {
                val.start.x == 0.0 && val.start.y == 0.0 && val.end.x == 0.0 && val.end.y == 0.0
            }
            DataType::Box(val) => {
                val.upper_right.x == 0.0
                    && val.upper_right.y == 0.0
                    && val.lower_left.x == 0.0
                    && val.lower_left.y == 0.0
            }
            DataType::Path(val) => match val {
                PathType::Open(points) => points.is_empty(),
                PathType::Closed(points) => points.is_empty(),
            },
            DataType::Polygon(val) => val.points.is_empty(),
            DataType::Circle(val) => {
                val.center.x == 0.0 && val.center.y == 0.0 && val.radius == 0.0
            }
            DataType::VarChar(val) => val.is_empty(),
            DataType::Null => true,
        }
    }

    fn kind(&self) -> String {
        match self {
            DataType::SmallInt(_) => "SMALLINT".to_string(),
            DataType::Float(_) => "FLOAT".to_string(),
            DataType::BigInt(_) => "BIGINT".to_string(),
            DataType::Decimal(_) => "DECIMAL".to_string(),
            DataType::Real(_) => "REAL".to_string(),
            DataType::DoublePrecision(_) => "DOUBLE PRECISION".to_string(),
            DataType::SmallSerial(_) => "SMALLSERIAL".to_string(),
            DataType::Serial(_) => "SERIAL".to_string(),
            DataType::BigSerial(_) => "BIGSERIAL".to_string(),
            DataType::Text(_) => "TEXT".to_string(),
            DataType::Blob(_) => "BLOB".to_string(),
            DataType::DateTime(_) => "DATETIME".to_string(),
            DataType::Json(_) => "JSON".to_string(),
            DataType::Uuid(_) => "UUID".to_string(),
            DataType::Array(_) => "ARRAY".to_string(),
            DataType::Map(_) => "MAP".to_string(),
            DataType::Enum(_, _) => "ENUM".to_string(),
            DataType::Range(_, _) => "RANGE".to_string(),
            DataType::Boolean(_) => "BOOLEAN".to_string(),
            DataType::Integer(_) => "INTEGER".to_string(),
            DataType::Point(_) => "POINT".to_string(),
            DataType::Line(_) => "LINE".to_string(),
            DataType::LineSegment(_) => "LINESEGMENT".to_string(),
            DataType::Box(_) => "BOX".to_string(),
            DataType::Path(_) => "PATH".to_string(),
            DataType::Polygon(_) => "POLYGON".to_string(),
            DataType::Circle(_) => "CIRCLE".to_string(),
            DataType::VarChar(_) => "VARCHAR".to_string(),
            DataType::Null => "NULL".to_string(),
        }
    }
}

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (DataType::SmallInt(a), DataType::SmallInt(b)) => a.partial_cmp(b),
            (DataType::Float(a), DataType::Float(b)) => a.partial_cmp(b),
            (DataType::BigInt(a), DataType::BigInt(b)) => a.partial_cmp(b),
            (DataType::Decimal(a), DataType::Decimal(b)) => a.partial_cmp(b),
            (DataType::Real(a), DataType::Real(b)) => a.partial_cmp(b),
            (DataType::DoublePrecision(a), DataType::DoublePrecision(b)) => a.partial_cmp(b),
            (DataType::SmallSerial(a), DataType::SmallSerial(b)) => a.partial_cmp(b),
            (DataType::Serial(a), DataType::Serial(b)) => a.partial_cmp(b),
            (DataType::BigSerial(a), DataType::BigSerial(b)) => a.partial_cmp(b),
            (DataType::Text(a), DataType::Text(b)) => a.partial_cmp(b),
            (DataType::Blob(a), DataType::Blob(b)) => a.partial_cmp(b),
            (DataType::DateTime(a), DataType::DateTime(b)) => a.partial_cmp(b),
            (DataType::Json(a), DataType::Json(b)) => {
                if a == b {
                    // TODO: Add a better comparison for Json
                    Some(std::cmp::Ordering::Equal)
                } else {
                    None
                }
            }
            (DataType::Uuid(a), DataType::Uuid(b)) => a.partial_cmp(b),
            (DataType::Array(a), DataType::Array(b)) => a.partial_cmp(b),
            (DataType::Map(a), DataType::Map(b)) => {
                if a == b {
                    // TODO: Add a better comparison for Map
                    Some(std::cmp::Ordering::Equal)
                } else {
                    None
                }
            }
            (DataType::Enum(a, _), DataType::Enum(b, _)) => a.partial_cmp(b),
            (DataType::Range(a, b), DataType::Range(c, d)) => {
                if a == c {
                    b.partial_cmp(d)
                } else {
                    a.partial_cmp(c)
                }
            }
            (DataType::Boolean(a), DataType::Boolean(b)) => a.partial_cmp(b),
            (DataType::Integer(a), DataType::Integer(b)) => a.partial_cmp(b),
            (DataType::Point(a), DataType::Point(b)) => a.partial_cmp(b),
            (DataType::Line(a), DataType::Line(b)) => a.partial_cmp(b),
            (DataType::LineSegment(a), DataType::LineSegment(b)) => a.partial_cmp(b),
            (DataType::Box(a), DataType::Box(b)) => a.partial_cmp(b),
            (DataType::Path(a), DataType::Path(b)) => a.partial_cmp(b),
            (DataType::Polygon(a), DataType::Polygon(b)) => a.partial_cmp(b),
            (DataType::Circle(a), DataType::Circle(b)) => a.partial_cmp(b),
            (DataType::VarChar(a), DataType::VarChar(b)) => a.partial_cmp(b),
            (DataType::Null, DataType::Null) => Some(std::cmp::Ordering::Equal),
            _ => None,
        }
    }
}

impl Eq for DataType {}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::SmallInt(val) => write!(f, "{}", val),
            DataType::Integer(val) => write!(f, "{}", val),
            DataType::BigInt(val) => write!(f, "{}", val),
            DataType::Decimal(val) => write!(f, "{}", val),
            DataType::Real(val) => write!(f, "{}", val),
            DataType::DoublePrecision(val) => write!(f, "{}", val),
            DataType::SmallSerial(val) => write!(f, "{}", val),
            DataType::Serial(val) => write!(f, "{}", val),
            DataType::BigSerial(val) => write!(f, "{}", val),
            DataType::Float(val) => write!(f, "{}", val),
            DataType::DateTime(val) => write!(f, "{}", val),
            DataType::Text(val) => write!(f, "{}", val),
            DataType::Blob(val) => write!(f, "{:?}", val),
            DataType::Json(val) => write!(f, "{}", val),
            DataType::Uuid(val) => write!(f, "{}", val),
            DataType::Array(val) => {
                let mut result = String::from("[");
                for item in val {
                    result.push_str(&format!("{}, ", item));
                }
                result.push_str("]");
                write!(f, "{}", result)
            }
            DataType::Map(val) => {
                let mut result = String::from("{");
                for (key, value) in val {
                    result.push_str(&format!("{}: {}, ", key, value));
                }
                result.push_str("}");
                write!(f, "{}", result)
            }
            DataType::Enum(val, _) => write!(f, "{}", val),
            DataType::Range(start, end) => write!(f, "{}..{}", start, end),
            DataType::Boolean(val) => write!(f, "{}", val),
            DataType::Point(val) => write!(f, "({}, {})", val.x, val.y),
            DataType::Line(val) => write!(f, "{}x + {}y + {} = 0", val.a, val.b, val.c),
            DataType::LineSegment(val) => {
                write!(f, "LineSegment(start: {}, end: {})", val.start, val.end)
            }
            DataType::Box(val) => write!(
                f,
                "Box(upper_right: {}, lower_left: {})",
                val.upper_right, val.lower_left
            ),
            DataType::Path(val) => match val {
                PathType::Open(points) => {
                    let mut result = String::from("Path(");
                    for point in points {
                        result.push_str(&format!("{}, ", point));
                    }
                    result.push_str(")");
                    write!(f, "{}", result)
                }
                PathType::Closed(points) => {
                    let mut result = String::from("Path(");
                    for point in points {
                        result.push_str(&format!("{}, ", point));
                    }
                    result.push_str(")");
                    write!(f, "{}", result)
                }
            },
            DataType::Polygon(val) => {
                let mut result = String::from("Polygon(");
                for point in &val.points {
                    result.push_str(&format!("{}, ", point));
                }
                result.push_str(")");
                write!(f, "{}", result)
            }
            DataType::Circle(val) => {
                write!(f, "Circle(center: {}, radius: {})", val.center, val.radius)
            }
            DataType::VarChar(val) => write!(f, "{}", val),
            DataType::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Point {
    x: f64,
    y: f64,
}

impl Encodable for Point {
    fn encode(&self) -> Result<Vec<u8>, EncodingError> {
        let mut result = Vec::new();
        result.append(&mut self.x.to_be_bytes().to_vec());
        result.append(&mut self.y.to_be_bytes().to_vec());
        Ok(result)
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.x, self.y)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Line {
    a: f64,
    b: f64,
    c: f64,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct LineSegment {
    start: Point,
    end: Point,
}

impl Encodable for LineSegment {
    fn encode(&self) -> Result<Vec<u8>, EncodingError> {
        let mut result = Vec::new();
        result.append(&mut self.start.encode()?);
        result.append(&mut self.end.encode()?);
        Ok(result)
    }
}

impl fmt::Display for LineSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LineSegment(start: {}, end: {})", self.start, self.end)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BoxType {
    upper_right: Point,
    lower_left: Point,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum PathType {
    Open(Vec<Point>),
    Closed(Vec<Point>),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Polygon {
    points: Vec<Point>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Circle {
    center: Point,
    radius: f64,
}

pub struct TypeMetadata {
    name: String,
    description: String,
    size: Option<usize>, // Size in bytes
}

impl Encodable for DataType {
    fn encode(&self) -> Result<Vec<u8>, EncodingError> {
        match self {
            DataType::SmallInt(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::Integer(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::BigInt(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::Decimal(val) => Ok(val.to_f64().unwrap().to_be_bytes().to_vec()),
            DataType::Real(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::DoublePrecision(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::SmallSerial(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::Serial(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::BigSerial(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::Text(val) => Ok(val.as_bytes().to_vec()),
            DataType::Json(val) => Ok(serde_json::to_vec(val)?),
            DataType::Float(val) => Ok(val.to_be_bytes().to_vec()),
            DataType::Blob(val) => Ok(val.clone()),
            DataType::DateTime(val) => Ok(val.timestamp().to_be_bytes().to_vec()),
            DataType::Uuid(val) => Ok(val.as_bytes().to_vec()),
            DataType::Array(val) => {
                let mut result = Vec::new();
                for item in val {
                    result.append(&mut item.encode()?);
                }
                Ok(result)
            }
            DataType::Map(val) => {
                let mut result = Vec::new();
                for (key, value) in val {
                    result.append(&mut key.clone().into_bytes());
                    result.append(&mut value.encode()?);
                }
                Ok(result)
            }
            DataType::Enum(val, _) => Ok(val.as_bytes().to_vec()),
            DataType::Range(start, end) => {
                let mut result = Vec::new();
                result.append(&mut start.encode()?);
                result.append(&mut end.encode()?);
                Ok(result)
            }
            DataType::Boolean(val) => Ok((*val as u8).to_be_bytes().to_vec()),
            DataType::Point(val) => val.encode(),
            DataType::Line(val) => {
                let mut result = Vec::new();
                result.append(&mut val.a.to_be_bytes().to_vec());
                result.append(&mut val.b.to_be_bytes().to_vec());
                result.append(&mut val.c.to_be_bytes().to_vec());
                Ok(result)
            }
            DataType::LineSegment(val) => val.encode(),
            DataType::Box(val) => {
                let mut result = Vec::new();
                result.append(&mut val.upper_right.encode()?);
                result.append(&mut val.lower_left.encode()?);
                Ok(result)
            }
            DataType::Path(val) => match val {
                PathType::Open(points) => {
                    let mut result = Vec::new();
                    for point in points {
                        result.append(&mut point.encode()?);
                    }
                    Ok(result)
                }
                PathType::Closed(points) => {
                    let mut result = Vec::new();
                    for point in points {
                        result.append(&mut point.encode()?);
                    }
                    Ok(result)
                }
            },
            DataType::Polygon(val) => {
                let mut result = Vec::new();
                for point in &val.points {
                    result.append(&mut point.encode()?);
                }
                Ok(result)
            }
            DataType::Circle(val) => {
                let mut result = Vec::new();
                result.append(&mut val.center.encode()?);
                result.append(&mut val.radius.to_be_bytes().to_vec());
                Ok(result)
            }
            DataType::VarChar(val) => Ok(val.as_bytes().to_vec()),
            DataType::Null => Ok(Vec::new()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Nullable<T> {
    Null,
    Value(T),
}

impl<T: Encodable> Encodable for Nullable<T> {
    fn encode(&self) -> Result<Vec<u8>, EncodingError> {
        match self {
            Nullable::Null => Ok(Vec::new()),
            Nullable::Value(val) => val.encode(),
        }
    }
}

impl TypeCheck for DataType {
    fn is_compatible_with(&self, other: &Self) -> bool {
        match (self, other) {
            (DataType::SmallInt(_), DataType::SmallInt(_)) => true,
            (DataType::Float(_), DataType::Float(_)) => true,
            (DataType::Text(_), DataType::Text(_)) => true,
            (DataType::Blob(_), DataType::Blob(_)) => true,
            (DataType::DateTime(_), DataType::DateTime(_)) => true,
            (DataType::Json(_), DataType::Json(_)) => true,
            _ => false,
        }
    }

    fn try_cast_to(&self, target: &Self) -> Result<Self, TypeError>
    where
        Self: Sized,
    {
        match (self, target) {
            (DataType::SmallInt(val), DataType::Float(_)) => Ok(DataType::Float(*val as f64)),
            (DataType::Float(val), DataType::SmallInt(_)) => {
                if *val > i16::MAX as f64 || *val < i16::MIN as f64 {
                    Err(TypeError::OverflowError {
                        data_type: "Integer".to_string(),
                    })
                } else {
                    Ok(DataType::SmallInt(*val as i16))
                }
            }
            (DataType::SmallInt(val), DataType::Text(_)) => Ok(DataType::Text(val.to_string())),
            (DataType::Float(val), DataType::Text(_)) => Ok(DataType::Text(val.to_string())),
            (DataType::Text(val), DataType::SmallInt(_)) => match val.parse::<i16>() {
                Ok(val) => Ok(DataType::SmallInt(val)),
                Err(_) => Err(TypeError::InvalidCast {
                    from: "Text".to_string(),
                    to: "Integer".to_string(),
                }),
            },
            (DataType::Text(val), DataType::Float(_)) => match val.parse::<f64>() {
                Ok(val) => Ok(DataType::Float(val)),
                Err(_) => Err(TypeError::InvalidCast {
                    from: "Text".to_string(),
                    to: "Float".to_string(),
                }),
            },
            (DataType::DateTime(val), DataType::Text(_)) => Ok(DataType::Text(val.to_string())),
            (DataType::Text(val), DataType::DateTime(_)) => {
                match NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S") {
                    Ok(val) => Ok(DataType::DateTime(val)),
                    Err(_) => Err(TypeError::InvalidCast {
                        from: "Text".to_string(),
                        to: "DateTime".to_string(),
                    }),
                }
            }
            (DataType::Json(val), DataType::Text(_)) => Ok(DataType::Text(val.to_string())),
            (DataType::Text(val), DataType::Json(_)) => match serde_json::from_str(val) {
                Ok(val) => Ok(DataType::Json(val)),
                Err(_) => Err(TypeError::InvalidCast {
                    from: "Text".to_string(),
                    to: "Json".to_string(),
                }),
            },
            _ => Err(TypeError::IncompatibleType {
                expected: format!("{:?}", target),
                found: format!("{:?}", self),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime};
    use serde_json::json;
    use uuid::Uuid;

    // Test Basic Functionality of Each DataType
    #[test]
    fn test_basic_datatype_encoding() {
        // Test integer types
        assert_eq!(
            DataType::SmallInt(42).encode().unwrap(),
            42i16.to_be_bytes().to_vec()
        );
        assert_eq!(
            DataType::Integer(42).encode().unwrap(),
            42i32.to_be_bytes().to_vec()
        );
        assert_eq!(
            DataType::BigInt(42).encode().unwrap(),
            42i64.to_be_bytes().to_vec()
        );

        // Test floating-point types
        assert_eq!(
            DataType::Float(3.14).encode().unwrap(),
            3.14f64.to_be_bytes().to_vec()
        );
        assert_eq!(
            DataType::DoublePrecision(3.14).encode().unwrap(),
            3.14f64.to_be_bytes().to_vec()
        );

        // Test string and binary types
        assert_eq!(
            DataType::Text("Hello".to_string()).encode().unwrap(),
            "Hello".as_bytes()
        );
        assert_eq!(
            DataType::Blob(vec![1, 2, 3]).encode().unwrap(),
            vec![1, 2, 3]
        );

        // Test DateTime type
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd(2020, 1, 1),
            chrono::NaiveTime::from_hms(0, 0, 0),
        );
        assert_eq!(
            DataType::DateTime(datetime).encode().unwrap(),
            datetime.timestamp().to_be_bytes().to_vec()
        );

        // Test Json type
        let json_data = json!({"key": "value"});
        assert_eq!(
            DataType::Json(json_data.clone()).encode().unwrap(),
            serde_json::to_vec(&json_data).unwrap()
        );

        // Test Uuid type
        let uuid_data = Uuid::new_v4();
        assert_eq!(
            DataType::Uuid(uuid_data.clone()).encode().unwrap(),
            uuid_data.as_bytes().to_vec()
        );
    }

    // Test Complex Data Types like Array, Map, etc.
    #[test]
    fn test_complex_datatype_encoding() {
        // Test Array type
        let array_data = DataType::Array(vec![DataType::SmallInt(1), DataType::SmallInt(2)]);
        let encoded = array_data.encode().unwrap();
        let expected = vec![1i16.to_be_bytes().to_vec(), 2i16.to_be_bytes().to_vec()]
            .into_iter()
            .flatten()
            .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test Map type
        let mut map_data = HashMap::new();
        map_data.insert("key".to_string(), DataType::Integer(42));
        let map_data = DataType::Map(map_data);
        let encoded = map_data.encode().unwrap();
        let expected = vec!["key".as_bytes().to_vec(), 42i32.to_be_bytes().to_vec()]
            .into_iter()
            .flatten()
            .collect::<Vec<u8>>();

        assert_eq!(encoded, expected);

        // Test Enum type
        let enum_data = DataType::Enum("value".to_string(), vec!["value".to_string()]);
        let encoded = enum_data.encode().unwrap();
        let expected = "value".as_bytes().to_vec();
        assert_eq!(encoded, expected);

        // Test Range type
        let range_data = DataType::Range(
            Box::new(DataType::SmallInt(1)),
            Box::new(DataType::SmallInt(2)),
        );
        let encoded = range_data.encode().unwrap();
        let expected = vec![1i16.to_be_bytes().to_vec(), 2i16.to_be_bytes().to_vec()]
            .into_iter()
            .flatten()
            .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test Point type
        let point_data = DataType::Point(Point { x: 1.0, y: 2.0 });
        let encoded = point_data.encode().unwrap();
        let expected = vec![1.0f64.to_be_bytes().to_vec(), 2.0f64.to_be_bytes().to_vec()]
            .into_iter()
            .flatten()
            .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test Line type
        let line_data = DataType::Line(Line {
            a: 1.0,
            b: 2.0,
            c: 3.0,
        });
        let encoded = line_data.encode().unwrap();
        let expected = vec![
            1.0f64.to_be_bytes().to_vec(),
            2.0f64.to_be_bytes().to_vec(),
            3.0f64.to_be_bytes().to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test LineSegment type
        let line_segment_data = DataType::LineSegment(LineSegment {
            start: Point { x: 1.0, y: 2.0 },
            end: Point { x: 3.0, y: 4.0 },
        });
        let encoded = line_segment_data.encode().unwrap();
        let expected = vec![
            1.0f64.to_be_bytes().to_vec(),
            2.0f64.to_be_bytes().to_vec(),
            3.0f64.to_be_bytes().to_vec(),
            4.0f64.to_be_bytes().to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test Box type
        let box_data = DataType::Box(BoxType {
            upper_right: Point { x: 1.0, y: 2.0 },
            lower_left: Point { x: 3.0, y: 4.0 },
        });
        let encoded = box_data.encode().unwrap();
        let expected = vec![
            1.0f64.to_be_bytes().to_vec(),
            2.0f64.to_be_bytes().to_vec(),
            3.0f64.to_be_bytes().to_vec(),
            4.0f64.to_be_bytes().to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test Path type
        let path_data = DataType::Path(PathType::Open(vec![
            Point { x: 1.0, y: 2.0 },
            Point { x: 3.0, y: 4.0 },
        ]));
        let encoded = path_data.encode().unwrap();
        let expected = vec![
            1.0f64.to_be_bytes().to_vec(),
            2.0f64.to_be_bytes().to_vec(),
            3.0f64.to_be_bytes().to_vec(),
            4.0f64.to_be_bytes().to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test Polygon type
        let polygon_data = DataType::Polygon(Polygon {
            points: vec![Point { x: 1.0, y: 2.0 }, Point { x: 3.0, y: 4.0 }],
        });
        let encoded = polygon_data.encode().unwrap();
        let expected = vec![
            1.0f64.to_be_bytes().to_vec(),
            2.0f64.to_be_bytes().to_vec(),
            3.0f64.to_be_bytes().to_vec(),
            4.0f64.to_be_bytes().to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);

        // Test Circle type
        let circle_data = DataType::Circle(Circle {
            center: Point { x: 1.0, y: 2.0 },
            radius: 3.0,
        });
        let encoded = circle_data.encode().unwrap();
        let expected = vec![
            1.0f64.to_be_bytes().to_vec(),
            2.0f64.to_be_bytes().to_vec(),
            3.0f64.to_be_bytes().to_vec(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<u8>>();
        assert_eq!(encoded, expected);
    }

    // Test Type Coercion and Compatibility
    #[test]
    #[ignore = "Not implemented yet"]
    fn test_type_coercion() {
        // Coercion between numeric types
        let small_int_data = DataType::SmallInt(42);
        assert_eq!(
            small_int_data.coerce_to(&DataTypeKind::Integer).unwrap(),
            DataType::Integer(42)
        );

        // Coercion from numeric to string
        assert_eq!(
            small_int_data.coerce_to(&DataTypeKind::Text).unwrap(),
            DataType::Text("42".to_string())
        );

        // Add tests for other coercions and edge cases
    }

    // Test Serialization and Deserialization
    #[test]
    fn test_serialization_deserialization() {
        let text_data = DataType::Text("Hello".to_string());
        let encoded = text_data.encode().unwrap();
        // Define the expected deserialized data and assert it

        // Add tests for other data types
    }

    // Test Error Handling Scenarios
    #[test]
    fn test_error_handling() {
        // Test invalid cast
        let text_data = DataType::Text("Not a number".to_string());
        assert!(text_data.coerce_to(&DataTypeKind::Integer).is_err());

        // Test overflow error
        let large_float = DataType::Float(f64::MAX);
        assert!(large_float.coerce_to(&DataTypeKind::SmallInt).is_err());

        // Add tests for precision errors and other error types
    }
}
