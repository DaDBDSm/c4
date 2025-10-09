use std::io::Cursor;

use encoder::{Field, Value};

pub const OBJECT_FILE_MAGIC: i32 = 0xABCA;

pub struct ObjectFileHeader {
    pub magic: i32,
    pub created_at: i64,
}

impl ObjectFileHeader {
    pub const SIZE: usize = 27;

    pub fn to_bytes(&self) -> Vec<u8> {
        let header_value = Value::Message(vec![
            Field {
                number: 1,
                value: Value::Int32(self.magic)
            },
            Field {
                number: 2,
                value: Value::Int64(self.created_at)
            }
        ]);
        let bytes = encoder::encode_value(&header_value).expect("Encoding error");
        return bytes;
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut cursor = Cursor::new(bytes);
        let decoded = encoder::decode_value(&mut cursor).expect("Decoding error");
        let Value::Message(fields) = decoded else {
            panic!("Decoding error")
        };
        let magic_field = fields.get(0).expect("Invalid magic field");

        let Value::Int32(magic_value) = magic_field.value else {
            panic!("Decoding error")
        };
 
        let created_at_field = fields.get(1).expect("Invalid created_at field");

        let Value::Int64(created_at_value) = created_at_field.value else {
            panic!("Decoding error")
        };
 
        Self {
            magic: magic_value,
            created_at: created_at_value,
        }
    }
}
