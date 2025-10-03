use std::io::{Cursor, Read};

const MAX_FIELDS_COUNT: u32 = 100000;
const MAX_STRING_LENGTH: u64 = 1024 * 1024 * 1024;
const MAX_LIST_LENGTH: u32 = 10_000_000;

pub trait C4Encode {
    fn serialize(&self) -> Result<Vec<u8>, EncodeError>;
    fn deserialize(bytes: &[u8]) -> Result<Self, DecodeError>
    where
        Self: Sized;
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecodeError {
    InvalidTypeId(u8),
    InsufficientData(String),
    InvalidUtf8String,
    InvalidFieldCount,
    InvalidFieldNumber,
    InvalidListLength,
    ListTypeMismatch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EncodeError {
    ListTypeMismatch,
    NestedListsNotSupported,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::InvalidTypeId(id) => write!(f, "Invalid type ID: {}", id),
            DecodeError::InsufficientData(msg) => write!(f, "Insufficient data: {}", msg),
            DecodeError::InvalidUtf8String => write!(f, "Invalid UTF-8 string"),
            DecodeError::InvalidFieldCount => write!(f, "Invalid field count"),
            DecodeError::InvalidFieldNumber => write!(f, "Invalid field number"),
            DecodeError::InvalidListLength => write!(f, "Invalid list length"),
            DecodeError::ListTypeMismatch => write!(f, "List contains inconsistent types"),
        }
    }
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodeError::ListTypeMismatch => write!(f, "List contains inconsistent types"),
            EncodeError::NestedListsNotSupported => {
                write!(f, "Nested lists are not supported in encoding")
            }
        }
    }
}

impl std::error::Error for DecodeError {}
impl std::error::Error for EncodeError {}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    String(String),
    Message(Vec<Field>),
    List(Vec<Value>),
}

impl Value {
    pub const INT32_ID: u8 = 1;
    pub const INT64_ID: u8 = 2;
    pub const FLOAT32_ID: u8 = 3;
    pub const FLOAT64_ID: u8 = 4;
    pub const BOOL_ID: u8 = 5;
    pub const STRING_ID: u8 = 6;
    pub const MESSAGE_ID: u8 = 7;
    pub const LIST_ID: u8 = 8;

    pub fn type_id(&self) -> u8 {
        match self {
            Value::Int32(_) => Self::INT32_ID,
            Value::Int64(_) => Self::INT64_ID,
            Value::Float32(_) => Self::FLOAT32_ID,
            Value::Float64(_) => Self::FLOAT64_ID,
            Value::Bool(_) => Self::BOOL_ID,
            Value::String(_) => Self::STRING_ID,
            Value::Message(_) => Self::MESSAGE_ID,
            Value::List(_) => Self::LIST_ID,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub number: u32,
    pub value: Value,
}

pub fn encode_value(value: &Value) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::new();
    out.extend_from_slice(&value.type_id().to_be_bytes());

    match value {
        Value::Int32(i) => out.extend_from_slice(&i.to_be_bytes()),
        Value::Int64(i) => out.extend_from_slice(&i.to_be_bytes()),
        Value::Float32(f) => out.extend_from_slice(&f.to_be_bytes()),
        Value::Float64(f) => out.extend_from_slice(&f.to_be_bytes()),
        Value::Bool(b) => out.extend_from_slice(&bool_to_be_bytes(*b)),
        Value::String(s) => {
            let bytes = s.as_bytes();
            let string_length = bytes.len() as u64;
            out.extend_from_slice(&string_length.to_be_bytes());
            out.extend_from_slice(bytes);
        }
        Value::Message(fields) => {
            let fields_count = fields.len() as u32;
            out.extend_from_slice(&fields_count.to_be_bytes());
            for field in fields {
                out.extend_from_slice(&field.number.to_be_bytes());
                out.extend_from_slice(&encode_value(&field.value)?);
            }
        }
        Value::List(values) => {
            let length = values.len() as u32;
            out.extend_from_slice(&length.to_be_bytes());

            if length > 0 {
                let element_type = values[0].type_id();
                out.extend_from_slice(&element_type.to_be_bytes());

                for v in values {
                    if v.type_id() != element_type {
                        return Err(EncodeError::ListTypeMismatch);
                    }
                    out.extend_from_slice(&encode_value_without_type(v)?);
                }
            }
        }
    }

    Ok(out)
}

fn encode_value_without_type(value: &Value) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::new();
    match value {
        Value::Int32(i) => out.extend_from_slice(&i.to_be_bytes()),
        Value::Int64(i) => out.extend_from_slice(&i.to_be_bytes()),
        Value::Float32(f) => out.extend_from_slice(&f.to_be_bytes()),
        Value::Float64(f) => out.extend_from_slice(&f.to_be_bytes()),
        Value::Bool(b) => out.extend_from_slice(&bool_to_be_bytes(*b)),
        Value::String(s) => {
            let bytes = s.as_bytes();
            let string_length = bytes.len() as u64;
            out.extend_from_slice(&string_length.to_be_bytes());
            out.extend_from_slice(bytes);
        }
        Value::Message(fields) => {
            let fields_count = fields.len() as u32;
            out.extend_from_slice(&fields_count.to_be_bytes());
            for field in fields {
                out.extend_from_slice(&field.number.to_be_bytes());
                out.extend_from_slice(&encode_value(&field.value)?);
            }
        }
        Value::List(_) => return Err(EncodeError::NestedListsNotSupported),
    }
    Ok(out)
}

pub fn decode_value(cursor: &mut Cursor<&[u8]>) -> Result<Value, DecodeError> {
    let mut type_id_bytes: [u8; 1] = [0; 1];
    read_exact_or_error(cursor, &mut type_id_bytes, "type ID")?;
    let type_id = u8::from_be_bytes(type_id_bytes);

    match type_id {
        Value::INT32_ID => {
            let mut buf = [0; 4];
            read_exact_or_error(cursor, &mut buf, "i32")?;
            Ok(Value::Int32(i32::from_be_bytes(buf)))
        }
        Value::INT64_ID => {
            let mut buf = [0; 8];
            read_exact_or_error(cursor, &mut buf, "i64")?;
            Ok(Value::Int64(i64::from_be_bytes(buf)))
        }
        Value::FLOAT32_ID => {
            let mut buf = [0; 4];
            read_exact_or_error(cursor, &mut buf, "f32")?;
            Ok(Value::Float32(f32::from_be_bytes(buf)))
        }
        Value::FLOAT64_ID => {
            let mut buf = [0; 8];
            read_exact_or_error(cursor, &mut buf, "f64")?;
            Ok(Value::Float64(f64::from_be_bytes(buf)))
        }
        Value::BOOL_ID => {
            let mut buf = [0; 1];
            read_exact_or_error(cursor, &mut buf, "bool")?;
            Ok(Value::Bool(bool_from_be_bytes(buf)))
        }
        Value::STRING_ID => {
            let mut len_buf = [0; 8];
            read_exact_or_error(cursor, &mut len_buf, "string length")?;
            let length = u64::from_be_bytes(len_buf);

            if length > MAX_STRING_LENGTH {
                return Err(DecodeError::InsufficientData(
                    "String length too large".to_string(),
                ));
            }

            let mut data = vec![0; length as usize];
            read_exact_or_error(cursor, &mut data, "string data")?;
            String::from_utf8(data)
                .map(Value::String)
                .map_err(|_| DecodeError::InvalidUtf8String)
        }
        Value::MESSAGE_ID => {
            let mut count_buf = [0; 4];
            read_exact_or_error(cursor, &mut count_buf, "field count")?;
            let count = u32::from_be_bytes(count_buf);

            if count > MAX_FIELDS_COUNT {
                return Err(DecodeError::InvalidFieldCount);
            }

            let mut fields = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let mut num_buf = [0; 4];
                read_exact_or_error(cursor, &mut num_buf, "field number")?;
                let number = u32::from_be_bytes(num_buf);
                if number == 0 {
                    return Err(DecodeError::InvalidFieldNumber);
                }
                let value = decode_value(cursor)?;
                fields.push(Field { number, value });
            }
            Ok(Value::Message(fields))
        }
        Value::LIST_ID => {
            let mut len_buf = [0; 4];
            read_exact_or_error(cursor, &mut len_buf, "list length")?;
            let length = u32::from_be_bytes(len_buf);

            if length > MAX_LIST_LENGTH {
                return Err(DecodeError::InvalidListLength);
            }

            if length == 0 {
                return Ok(Value::List(Vec::new()));
            }

            let mut elem_type_buf = [0; 1];
            read_exact_or_error(cursor, &mut elem_type_buf, "list element type")?;
            let elem_type = u8::from_be_bytes(elem_type_buf);

            let mut values = Vec::with_capacity(length as usize);
            for _ in 0..length {
                let v = decode_value_without_type(cursor, elem_type)?;
                values.push(v);
            }
            Ok(Value::List(values))
        }
        _ => Err(DecodeError::InvalidTypeId(type_id)),
    }
}

fn decode_value_without_type(cursor: &mut Cursor<&[u8]>, type_id: u8) -> Result<Value, DecodeError> {
    match type_id {
        Value::INT32_ID => {
            let mut buf = [0; 4];
            read_exact_or_error(cursor, &mut buf, "i32")?;
            Ok(Value::Int32(i32::from_be_bytes(buf)))
        }
        Value::INT64_ID => {
            let mut buf = [0; 8];
            read_exact_or_error(cursor, &mut buf, "i64")?;
            Ok(Value::Int64(i64::from_be_bytes(buf)))
        }
        Value::FLOAT32_ID => {
            let mut buf = [0; 4];
            read_exact_or_error(cursor, &mut buf, "f32")?;
            Ok(Value::Float32(f32::from_be_bytes(buf)))
        }
        Value::FLOAT64_ID => {
            let mut buf = [0; 8];
            read_exact_or_error(cursor, &mut buf, "f64")?;
            Ok(Value::Float64(f64::from_be_bytes(buf)))
        }
        Value::BOOL_ID => {
            let mut buf = [0; 1];
            read_exact_or_error(cursor, &mut buf, "bool")?;
            Ok(Value::Bool(bool_from_be_bytes(buf)))
        }
        Value::STRING_ID => {
            let mut len_buf = [0; 8];
            read_exact_or_error(cursor, &mut len_buf, "string length")?;
            let length = u64::from_be_bytes(len_buf);
            if length > MAX_STRING_LENGTH {
                return Err(DecodeError::InsufficientData(
                    "String length too large".to_string(),
                ));
            }
            let mut data = vec![0; length as usize];
            read_exact_or_error(cursor, &mut data, "string data")?;
            String::from_utf8(data)
                .map(Value::String)
                .map_err(|_| DecodeError::InvalidUtf8String)
        }
        _ => Err(DecodeError::InvalidTypeId(type_id)),
    }
}

fn read_exact_or_error(
    cursor: &mut Cursor<&[u8]>,
    buf: &mut [u8],
    context: &str,
) -> Result<(), DecodeError> {
    cursor
        .read_exact(buf)
        .map_err(|_| DecodeError::InsufficientData(format!("Failed to read {}", context)))
}

fn bool_to_be_bytes(b: bool) -> [u8; 1] {
    (b as u8).to_be_bytes()
}

fn bool_from_be_bytes(bytes: [u8; 1]) -> bool {
    bytes[0] != 0
}
