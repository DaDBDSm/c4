use std::io::Cursor;

use encoder::{DecodeError, EncodeError, Field, Value, decode_value, encode_value};

pub struct BucketsMetadata {
    pub buckets: Vec<BucketMetadata>,
}

pub struct BucketMetadata {
    pub name: String,
    pub objects: Vec<String>,
}

impl BucketsMetadata {
    pub fn to_bytes(&self) -> Result<Vec<u8>, EncodeError> {
        let bucket_values: Vec<Value> = self
            .buckets
            .iter()
            .map(|bucket| Self::bucket_to_value(bucket))
            .collect();

        let top_level_fields = vec![Field {
            number: 1,            value: Value::List(bucket_values),
        }];

        encode_value(&Value::Message(top_level_fields))
    }

    fn bucket_to_value(bucket: &BucketMetadata) -> Value {
        let object_values: Vec<Value> = bucket
            .objects
            .iter()
            .map(|obj| Value::String(obj.clone()))
            .collect();

        let fields = vec![
            Field {
                number: 1,                value: Value::String(bucket.name.clone()),
            },
            Field {
                number: 2,                value: Value::List(object_values),
            },
        ];

        Value::Message(fields)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut cursor = Cursor::new(bytes);
        let value = decode_value(&mut cursor)?;

        Self::value_to_buckets_metadata(value)
    }

    fn value_to_buckets_metadata(value: Value) -> Result<Self, DecodeError> {
        let fields = match value {
            Value::Message(fields) => fields,
            _ => return Err(DecodeError::InvalidTypeId(value.type_id())),
        };

        let buckets_field = fields
            .iter()
            .find(|field| field.number == 1)
            .ok_or(DecodeError::InvalidFieldNumber)?;

        let bucket_values = match &buckets_field.value {
            Value::List(values) => values,
            _ => return Err(DecodeError::InvalidTypeId(buckets_field.value.type_id())),
        };

        let buckets = bucket_values
            .iter()
            .map(|value| Self::value_to_bucket_metadata(value))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(BucketsMetadata { buckets })
    }

    fn value_to_bucket_metadata(value: &Value) -> Result<BucketMetadata, DecodeError> {
        let bucket_fields = match value {
            Value::Message(fields) => fields,
            _ => return Err(DecodeError::ListTypeMismatch),
        };

        let mut name = None;
        let mut objects = Vec::new();

        for field in bucket_fields {
            match field.number {
                1 => name = Some(Self::extract_string_field(&field.value)?),
                2 => objects = Self::extract_string_list_field(&field.value)?,
                _ => return Err(DecodeError::InvalidFieldNumber),
            }
        }

        let name = name.ok_or(DecodeError::InvalidFieldNumber)?;
        Ok(BucketMetadata { name, objects })
    }

    fn extract_string_field(value: &Value) -> Result<String, DecodeError> {
        match value {
            Value::String(s) => Ok(s.clone()),
            _ => Err(DecodeError::InvalidTypeId(value.type_id())),
        }
    }

    fn extract_string_list_field(value: &Value) -> Result<Vec<String>, DecodeError> {
        match value {
            Value::List(values) => values
                .iter()
                .map(|value| match value {
                    Value::String(s) => Ok(s.clone()),
                    _ => Err(DecodeError::ListTypeMismatch),
                })
                .collect(),
            _ => Err(DecodeError::InvalidTypeId(value.type_id())),
        }
    }
}

impl Default for BucketsMetadata {
    fn default() -> Self {
        Self {
            buckets: Vec::new(),
        }
    }
}

impl BucketsMetadata {
    pub fn new(buckets: Vec<BucketMetadata>) -> Self {
        Self { buckets }
    }

    pub fn buckets(&self) -> &[BucketMetadata] {
        &self.buckets
    }

    pub fn buckets_mut(&mut self) -> &mut [BucketMetadata] {
        &mut self.buckets
    }
}

impl BucketMetadata {
    pub fn new(name: String, objects: Vec<String>) -> Self {
        Self { name, objects }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn objects(&self) -> &[String] {
        &self.objects
    }

    pub fn objects_mut(&mut self) -> &mut [String] {
        &mut self.objects
    }
}
