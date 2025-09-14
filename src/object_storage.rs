use std::{
    io::{self, Read},
    time::Instant,
};

pub type BucketName = String;
pub type ObjectKey = String;

pub enum StorageError {
    IoError(io::Error),
    BucketAlreadyExists(BucketName),
    BucketNotFound(BucketName),
    ObjectNotFound { bucket: BucketName, key: ObjectKey },
    InvalidInput(String),
    Internal(String),
}

pub struct ObjectMetadata {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: u64,
    pub created: Instant,
    pub etag: String,
}

pub trait ObjectStorage {
    fn create_bucket(&mut self, bucket_name: &BucketName) -> Result<(), StorageError>;

    fn delete_bucket(&mut self, bucket_name: &BucketName) -> Result<(), StorageError>;

    fn list_buckets(&self) -> Result<Vec<BucketName>, StorageError>;

    fn put_object<R: Read>(
        &mut self,
        bucket_name: &BucketName,
        key: &ObjectKey,
        reader: &mut R,
    ) -> Result<ObjectMetadata, StorageError>;

    fn get_object(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn Read>, StorageError>;

    fn head_object(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, StorageError>;

    fn delete_object(
        &mut self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), StorageError>;

    fn list_objects(&self, offset: u64, limit: u64) -> Result<Vec<ObjectMetadata>, StorageError>;
}
