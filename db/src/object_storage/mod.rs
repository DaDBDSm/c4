pub mod errors;
pub mod simple;

use std::io::Read;

use crate::object_storage::errors::StorageError;

pub type BucketName = String;
pub type ObjectKey = String;

pub enum SortingOrder {
    ASC,
    DESC,
}

pub struct ObjectMetadata {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: u64,
    pub created_at: i64,
}

pub struct CreateBucketDTO {
    pub bucket_name: BucketName,
}

pub struct DeleteBucketDTO {
    pub bucket_name: BucketName,
}

pub struct ListBucketsDTO {
    pub offset: u64,
    pub limit: u64,
}

pub struct PutObjectDTO {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub reader: Box<dyn Read>,
}

pub struct GetObjectDTO {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
}

pub struct HeadObjectDTO {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
}

pub struct DeleteObjectDTO {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
}

pub struct ListObjectsDTO {
    pub bucket_name: BucketName,
    pub limit: u64,
    pub offset: u64,
    pub sorting_order: SortingOrder,
    pub prefix: Option<String>,
}

pub trait ObjectStorage {
    fn create_bucket(&self, dto: &CreateBucketDTO) -> Result<(), StorageError>;

    fn delete_bucket(&self, dto: &DeleteBucketDTO) -> Result<(), StorageError>;

    fn list_buckets(&self, dto: &ListBucketsDTO) -> Result<Vec<BucketName>, StorageError>;

    fn put_object(&self, dto: &mut PutObjectDTO) -> Result<ObjectMetadata, StorageError>;

    fn get_object(&self, dto: &GetObjectDTO) -> Result<Box<dyn Read>, StorageError>;

    fn head_object(&self, dto: &HeadObjectDTO) -> Result<ObjectMetadata, StorageError>;

    fn delete_object(&self, dto: &DeleteObjectDTO) -> Result<(), StorageError>;

    fn list_objects(&self, dto: &ListObjectsDTO) -> Result<Vec<ObjectMetadata>, StorageError>;
}
