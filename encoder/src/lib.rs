use std::io::{Cursor, Read};

const MAX_FIELDS_COUNT: u32 = 100000;
const MAX_STRING_LENGTH: u64 = 1024 * 1024 * 1024;

pub trait C4Encode {
    fn serialize(&self) -> Vec<u8>;
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
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::InvalidTypeId(id) => write!(f, "Invalid type ID: {}", id),
            DecodeError::InsufficientData(msg) => write!(f, "Insufficient data: {}", msg),
            DecodeError::InvalidUtf8String => write!(f, "Invalid UTF-8 string"),
            DecodeError::InvalidFieldCount => write!(f, "Invalid field count"),
            DecodeError::InvalidFieldNumber => write!(f, "Invalid field number"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub enum Value {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    String(String),
    Message(Vec<Field>),
}

impl Value {
    pub const INT32_ID: u8 = 1;
    pub const INT64_ID: u8 = 2;
    pub const FLOAT32_ID: u8 = 3;
    pub const FLOAT64_ID: u8 = 4;
    pub const BOOL_ID: u8 = 5;
    pub const STRING_ID: u8 = 6;
    pub const MESSAGE_ID: u8 = 7;

    pub fn type_id(&self) -> u8 {
        match self {
            Value::Int32(_) => Self::INT32_ID,
            Value::Int64(_) => Self::INT64_ID,
            Value::Float32(_) => Self::FLOAT32_ID,
            Value::Float64(_) => Self::FLOAT64_ID,
            Value::Bool(_) => Self::BOOL_ID,
            Value::String(_) => Self::STRING_ID,
            Value::Message(_fields) => Self::MESSAGE_ID,
        }
    }
}

pub struct Field {
    pub number: u32,
    pub value: Value,
}

pub fn encode_value(value: &Value) -> Vec<u8> {
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
            out.extend_from_slice(&bytes);
        }
        Value::Message(fields) => {
            let fields_count = fields.len() as u32;
            out.extend_from_slice(&fields_count.to_be_bytes());
            for field in fields {
                out.extend_from_slice(&field.number.to_be_bytes());
                out.extend_from_slice(&encode_value(&field.value));
            }
        }
    }

    out
}

pub fn decode_value(cursor: &mut Cursor<&[u8]>) -> Result<Value, DecodeError> {
    let mut type_id_bytes: [u8; 1] = [0; 1];
    read_exact_or_error(cursor, &mut type_id_bytes, "type ID")?;
    let type_id = u8::from_be_bytes(type_id_bytes);

    match type_id {
        Value::INT32_ID => {
            let mut value_bytes: [u8; 4] = [0; 4];
            read_exact_or_error(cursor, &mut value_bytes, "i32 value")?;
            Ok(Value::Int32(i32::from_be_bytes(value_bytes)))
        }
        Value::INT64_ID => {
            let mut value_bytes: [u8; 8] = [0; 8];
            read_exact_or_error(cursor, &mut value_bytes, "i64 value")?;
            Ok(Value::Int64(i64::from_be_bytes(value_bytes)))
        }
        Value::FLOAT32_ID => {
            let mut value_bytes: [u8; 4] = [0; 4];
            read_exact_or_error(cursor, &mut value_bytes, "f32 value")?;
            Ok(Value::Float32(f32::from_be_bytes(value_bytes)))
        }
        Value::FLOAT64_ID => {
            let mut value_bytes: [u8; 8] = [0; 8];
            read_exact_or_error(cursor, &mut value_bytes, "f64 value")?;
            Ok(Value::Float64(f64::from_be_bytes(value_bytes)))
        }
        Value::BOOL_ID => {
            let mut value_bytes: [u8; 1] = [0; 1];
            read_exact_or_error(cursor, &mut value_bytes, "bool value")?;
            Ok(Value::Bool(bool_from_be_bytes(value_bytes)))
        }
        Value::STRING_ID => {
            let mut length_bytes: [u8; 8] = [0; 8];
            read_exact_or_error(cursor, &mut length_bytes, "string length")?;
            let length = u64::from_be_bytes(length_bytes);

            if length > MAX_STRING_LENGTH {
                return Err(DecodeError::InsufficientData(
                    "String length too large".to_string(),
                ));
            }

            let mut string_bytes = vec![0; length as usize];
            read_exact_or_error(cursor, &mut string_bytes, "string data")?;

            String::from_utf8(string_bytes)
                .map(Value::String)
                .map_err(|_| DecodeError::InvalidUtf8String)
        }
        Value::MESSAGE_ID => {
            let mut fields_count_bytes: [u8; 4] = [0; 4];
            read_exact_or_error(cursor, &mut fields_count_bytes, "fields count")?;
            let fields_count = u32::from_be_bytes(fields_count_bytes);

            if fields_count > MAX_FIELDS_COUNT {
                return Err(DecodeError::InvalidFieldCount);
            }

            let mut fields = Vec::with_capacity(fields_count as usize);
            for _ in 0..fields_count {
                let mut field_number_bytes: [u8; 4] = [0; 4];
                read_exact_or_error(cursor, &mut field_number_bytes, "field number")?;
                let field_number = u32::from_be_bytes(field_number_bytes);

                if field_number == 0 {
                    return Err(DecodeError::InvalidFieldNumber);
                }

                let value = decode_value(cursor)?;
                fields.push(Field {
                    number: field_number,
                    value,
                });
            }
            Ok(Value::Message(fields))
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
