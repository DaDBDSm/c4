pub mod errors;
pub mod simple;
use tokio::io;
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
    async fn create_bucket(&self, dto: &CreateBucketDTO) -> Result<(), StorageError>;

    async fn list_buckets(&self, dto: &ListBucketsDTO) -> Result<Vec<BucketName>, StorageError>;

    async fn delete_bucket(&self, dto: &DeleteBucketDTO) -> Result<(), StorageError>;

    async fn put_object(&self, dto: &mut PutObjectDTO) -> Result<ObjectMetadata, StorageError>;

    async fn get_object(
        &self,
        dto: &GetObjectDTO,
    ) -> Result<impl Stream<Item = io::Result<Vec<u8>>>, StorageError>;

    async fn list_objects(&self, dto: &ListObjectsDTO)
    -> Result<Vec<ObjectMetadata>, StorageError>;

    async fn head_object(&self, dto: &HeadObjectDTO) -> Result<ObjectMetadata, StorageError>;

    async fn delete_object(&self, dto: &DeleteObjectDTO) -> Result<(), StorageError>;
}
