pub mod errors;
pub mod simple;
use std::future::Future;
use tokio_stream::Stream;

use crate::storage::errors::StorageError;

pub type BucketName = String;
pub type ObjectKey = String;

pub enum SortingOrder {
    ASC,
    DESC,
}

impl SortingOrder {
    pub fn new_option(sorting: Option<&String>) -> Option<SortingOrder> {
        match sorting.unwrap_or(&"".to_string()).to_uppercase().as_str() {
            "ASC" => Some(SortingOrder::ASC),
            "DESC" => Some(SortingOrder::DESC),
            _ => None,
        }
    }
}

pub struct ObjectMetadata {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: u64,
    pub created_at: i64,
    pub version: u64,
}

pub struct CreateBucketDTO {
    pub bucket_name: BucketName,
}

pub struct DeleteBucketDTO {
    pub bucket_name: BucketName,
}

pub struct ListBucketsDTO {
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

pub struct PutObjectDTO {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub stream: Box<dyn Stream<Item = Vec<u8>> + Unpin + Send>,
    pub version: u64,
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
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub sorting_order: Option<SortingOrder>,
    pub prefix: Option<String>,
}

pub trait ObjectStorage {
    fn create_bucket(
        &self,
        dto: CreateBucketDTO,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn list_buckets(
        &self,
        dto: ListBucketsDTO,
    ) -> impl Future<Output = Result<Vec<BucketName>, StorageError>> + Send;

    fn delete_bucket(
        &self,
        dto: DeleteBucketDTO,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn put_object(
        &self,
        dto: PutObjectDTO,
    ) -> impl Future<Output = Result<ObjectMetadata, StorageError>> + Send;

    fn get_object(
        &self,
        dto: GetObjectDTO,
    ) -> impl Future<Output = Result<impl Stream<Item = Vec<u8>>, StorageError>> + Send;

    fn list_objects(
        &self,
        dto: ListObjectsDTO,
    ) -> impl Future<Output = Result<Vec<ObjectMetadata>, StorageError>> + Send;

    fn head_object(
        &self,
        dto: HeadObjectDTO,
    ) -> impl Future<Output = Result<ObjectMetadata, StorageError>> + Send;

    fn delete_object(
        &self,
        dto: DeleteObjectDTO,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;
}
