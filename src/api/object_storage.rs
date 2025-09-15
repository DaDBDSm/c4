use std::{
    io::Read,
    time::Instant,
};

use crate::api::errors::StorageError;

pub type BucketName = String;
pub type ObjectKey = String;

pub struct ObjectMetadata {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: u64,
    pub created_at: Instant,
    pub etag: String,
}

pub struct CreateBucketDto {
    pub bucket_name: BucketName,
}

pub struct DeleteBucketDto {
    pub bucket_name: BucketName,
}

pub struct ListBucketsDto {
    pub offset: u64,
    pub limit: u64,
}

pub struct PutObjectDto {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub reader: Box<dyn Read>,
}

pub struct GetObjectDto {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
}

pub struct HeadObjectDto {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
}

pub struct DeleteObjectDto {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
}

pub struct ListObjectsDto {
    pub bucket_name: BucketName,
    pub offset: u64,
    pub limit: u64,
    pub sorting_order: SortingOrder,
    pub prefix: Option<String>,
}

pub trait ObjectStorage {
    fn create_bucket(&mut self, dto: &CreateBucketDto) -> Result<(), StorageError>;

    fn delete_bucket(&mut self, dto: &DeleteBucketDto) -> Result<(), StorageError>;

    fn list_buckets(&self, dto: &ListBucketsDto) -> Result<Vec<BucketName>, StorageError>;

    fn put_object<R: Read>(&mut self, dto: &PutObjectDto) -> Result<ObjectMetadata, StorageError>;

    fn get_object(
        &self,
        dto: &GetObjectDto,
    ) -> Result<(Box<dyn Read>, ObjectMetadata), StorageError>;

    fn head_object(&self, dto: &HeadObjectDto) -> Result<ObjectMetadata, StorageError>;

    fn delete_object(&mut self, dto: &DeleteObjectDto) -> Result<(), StorageError>;

    fn list_objects(&self, bucket_name: &BucketName, offset: u64, limit: u64) -> Result<Vec<ObjectMetadata>, StorageError>;
}
