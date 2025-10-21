mod buckets_metadata_model;
mod buckets_metadata_storage;
mod chunk_file_storage;

use regex::Regex;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt};
use tokio_stream::{Stream, StreamExt};

use crate::storage::errors::StorageError;
use crate::storage::simple::buckets_metadata_storage::BucketsMetadataStorage;
use crate::storage::simple::chunk_file_storage::PartitionedBytesStorage;
use crate::storage::{
    BucketName, CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, GetObjectDTO, HeadObjectDTO,
    ListBucketsDTO, ListObjectsDTO, ObjectKey, ObjectMetadata, ObjectStorage, SortingOrder,
};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::ErrorKind;

const DEFAULT_LIMIT: u64 = 20;
const OBJECT_CHUNK_SIZE: usize = 1024; // 1 kb
const FILE_NAME_REGEX: &str = r"^[a-zA-Z0-9_]$";

#[derive(Clone)]
pub struct ObjectStorageSimple {
    pub base_dir: PathBuf,
    pub bytes_storage: PartitionedBytesStorage,
    pub buckets_metadata_storage: BucketsMetadataStorage,
}

impl ObjectStorage for ObjectStorageSimple {
    async fn create_bucket(&self, dto: CreateBucketDTO) -> Result<(), StorageError> {
        self.buckets_metadata_storage
            .add_bucket(&dto.bucket_name)
            .await
            .unwrap();
        Ok(())
    }

    async fn list_buckets(&self, dto: ListBucketsDTO) -> Result<Vec<BucketName>, StorageError> {
        Ok(self.buckets_metadata_storage.list_buckets().await.unwrap())
        // todo handle offset, limit, handle exepction correctly
    }

    async fn delete_bucket(&self, dto: DeleteBucketDTO) -> Result<(), StorageError> {
        self.buckets_metadata_storage
            .remove_bucket(&dto.bucket_name)
            .await
            .unwrap();
        // todo handle exception correctly
        Ok(())
    }

    async fn put_object(
        &self,
        dto: crate::storage::PutObjectDTO,
    ) -> Result<ObjectMetadata, StorageError> {
        if !file_name_is_correct(&dto.bucket_name) {
            return Err(StorageError::InvalidInput(
                "Invalid bucket name".to_string(),
            ));
        }
        if !file_name_is_correct(&dto.key) {
            return Err(StorageError::InvalidInput(
                "Invalid bucket name".to_string(),
            ));
        }

        if !self
            .buckets_metadata_storage
            .bucket_exists(&dto.bucket_name)
            .await
        {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }

        let object_chunk_id = object_chunk_id(&dto.key, &dto.bucket_name);
        let size = self
            .bytes_storage
            .save_chunk(dto.stream, object_chunk_id)
            .await
            .map_err(|_| StorageError::Internal("Error".to_string()))?;

        Ok(ObjectMetadata {
            bucket_name: dto.bucket_name.clone(),
            key: dto.key.clone(),
            size: size,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        })
    }

    async fn get_object(
        &self,
        dto: GetObjectDTO,
    ) -> Result<impl Stream<Item = Vec<u8>>, StorageError> {
        let object_chunk_id = object_chunk_id(&dto.key, &dto.bucket_name);
        let stream = self
            .bytes_storage
            .get_chunk(object_chunk_id)
            .await
            .unwrap()
            .stream;
        Ok(stream)
    }

    async fn list_objects(&self, dto: ListObjectsDTO) -> Result<Vec<ObjectMetadata>, StorageError> {
        if !self
            .buckets_metadata_storage
            .bucket_exists(&dto.bucket_name)
            .await
        {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }

        let object_keys = self
            .buckets_metadata_storage
            .list_objects(&dto.bucket_name)
            .await
            .map_err(|_| StorageError::Internal("Error".to_string()))?; // todo handle correctly

        if dto.offset.unwrap_or(0) as usize >= object_keys.len() {
            return Ok(Vec::new());
        }

        let mut metas: Vec<ObjectMetadata> = Vec::new();
        for object_key in object_keys {
            if let Some(prefix) = &dto.prefix
                && !object_key.starts_with(Some(prefix).unwrap())
            {
                continue;
            }

            // todo handle error
            let chunk_metadata = self
                .bytes_storage
                .get_chunk_metadata(object_chunk_id(&object_key, &dto.bucket_name))
                .await
                .unwrap();

            let object_metadata = ObjectMetadata {
                bucket_name: dto.bucket_name.to_string(),
                key: object_key.clone(),
                size: chunk_metadata.size,
                created_at: chunk_metadata.created_at,
            };

            metas.push(object_metadata);
        }

        metas.sort_by(
            |a, b| match dto.sorting_order.as_ref().unwrap_or(&SortingOrder::DESC) {
                SortingOrder::ASC => a.key.cmp(&b.key),
                SortingOrder::DESC => b.key.cmp(&a.key),
            },
        );

        Ok(metas
            .into_iter()
            .skip(dto.offset.unwrap_or(0) as usize)
            .take(dto.limit.unwrap_or(DEFAULT_LIMIT) as usize)
            .collect())
    }

    async fn head_object(&self, dto: HeadObjectDTO) -> Result<ObjectMetadata, StorageError> {
        let chunk_metadata = self
            .bytes_storage
            .get_chunk_metadata(object_chunk_id(&dto.key, &dto.bucket_name))
            .await
            .unwrap(); // todo handle errors correctly

        let object_metadata = ObjectMetadata {
            bucket_name: dto.bucket_name.to_string(),
            key: dto.key.clone(),
            size: chunk_metadata.size,
            created_at: chunk_metadata.created_at,
        };

        Ok(object_metadata)
    }

    async fn delete_object(&self, dto: DeleteObjectDTO) -> Result<(), StorageError> {
        if !file_name_is_correct(&dto.bucket_name) {
            return Err(StorageError::InvalidInput(
                "Invalid bucket name".to_string(),
            ));
        }
        if !file_name_is_correct(&dto.key) {
            return Err(StorageError::InvalidInput(
                "Invalid bucket name".to_string(),
            ));
        }

        if !self
            .buckets_metadata_storage
            .bucket_exists(&dto.bucket_name)
            .await
        {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }

        self.buckets_metadata_storage
            .remove_bucket(&dto.bucket_name)
            .await
            .unwrap();

        self.bytes_storage
            .delete_chunk(object_chunk_id(&dto.key, &dto.bucket_name))
            .await
            .unwrap();

        Ok(())
    }
}

fn file_name_is_correct(file_name: &str) -> bool {
    Regex::new(FILE_NAME_REGEX).unwrap().is_match(file_name)
}

fn object_chunk_id(object_key: &ObjectKey, bucket_name: &BucketName) -> u64 {
    let string = format!("{bucket_name}_{object_key}");
    hash_string_64(&string)
}

fn hash_string_64(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}
