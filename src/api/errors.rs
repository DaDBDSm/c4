use crate::api::object_storage::{BucketName, ObjectKey};

pub enum StorageError {
    IoError(io::Error),
    BucketAlreadyExists(BucketName),
    BucketNotFound(BucketName),
    ObjectNotFound { bucket: BucketName, key: ObjectKey },
    InvalidInput(String),
    Internal(String),
}
