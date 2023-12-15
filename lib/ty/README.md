# Type System Documentation

## Overview

The Type System is a comprehensive framework designed to handle various data types in a database management system (DBMS). It covers standard types such as integers, floats, and strings, as well as complex types like arrays, maps, and enums. Specialized types like DateTime and UUID are also supported. This system is designed for flexibility, robust encoding/decoding, and ease of integration into larger DBMS architectures.

### DataTypes

- **Basic Types**: Include `SmallInt`, `Integer`, `BigInt`, `Decimal`, `Real`, `DoublePrecision`, `Float`, `Text`, `Blob`, `DateTime`, `Json`, `Uuid`, and `Boolean`.
- **Complex Types**: Support for `Array`, `Map`, `Enum`, `Range`, and various geometric types like `Point`, `Line`, `LineSegment`, `Box`, `Path`, `Polygon`, and `Circle`.
- **Special Types**: Planned support for Geospatial types (`GeospatialType`).

#### Key Features

- **Encoding and Coercion**: Allows each data type to be encoded into bytes and coerced into other compatible types, facilitating data storage and type conversions.
- **Serialization/Deserialization**: Support for converting data types to/from storage formats or network transmission formats, ensuring data integrity and compatibility.
- **Type Checking**: Implements compatibility checks and type casting capabilities to maintain type safety and integrity.
- **Error Handling**: Robustly handles various errors like incompatible types, invalid casts, overflows, and precision issues.

#### Usage Patterns

- **Creating DataTypes**: Data types can be instantiated directly, e.g., `DataType::Integer(42)` or `DataType::Text("example".to_string())`.
- **Type Coercion**: Convert a data type into another (if compatible) using `coerce_to` method, e.g., `data.coerce_to(&DataTypeKind::Float)`.
- **Encoding/Decoding**: Serialize a `DataType` instance into bytes using `encode` method and deserialize using appropriate methods from bytes back to `DataType`.

#### Examples

```rust
let integer_data = DataType::Integer(42);
let text_data = DataType::Text("Hello, World!".to_string());
let json_data = DataType::Json(json!({"key": "value"}));

let encoded = integer_data.encode().unwrap();
let coerced_data = text_data.coerce_to(&DataTypeKind::Text).unwrap();
```

#### Error Handling

- Handle errors gracefully using the `TypeError` enum which includes variants like `IncompatibleType`, `InvalidCast`, `OverflowError`, and `PrecisionError`.
- Example: Handling an overflow error while casting a large float to a small integer.

```rust
let large_float = DataType::Float(f64::MAX);
match large_float.coerce_to(&DataTypeKind::SmallInt) {
    Ok(_) => println!("Coercion successful"),
    Err(e) => println!("Error: {}", e),
}
```

#### Future Enhancements

- Integration of Geospatial data types for advanced location-based queries.
- Expansion of serialization/deserialization capabilities for complex nested types.
- Further enhancements in type coercion logic to cover a wider range of type conversions.

#### Conclusion

This Type System is a versatile and robust component, essential for the functioning of a comprehensive DBMS. It provides a solid foundation for data manipulation, storage, and retrieval, ensuring type safety and data integrity across the system.
