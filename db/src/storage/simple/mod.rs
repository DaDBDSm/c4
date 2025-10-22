pub mod buckets_metadata_model;
pub mod buckets_metadata_storage;
pub mod chunk_file_storage;

use crate::storage::errors::StorageError;
use crate::storage::simple::buckets_metadata_storage::BucketsMetadataStorage;
use crate::storage::simple::chunk_file_storage::PartitionedBytesStorage;
use crate::storage::{
    BucketName, CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, GetObjectDTO, HeadObjectDTO,
    ListBucketsDTO, ListObjectsDTO, ObjectKey, ObjectMetadata, ObjectStorage, SortingOrder,
};
use regex::Regex;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_stream::Stream;

const DEFAULT_LIMIT: u64 = 20;
const FILE_NAME_REGEX: &str = r"^[a-zA-Z0-9_.-]+$";

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
            .map_err(|e| {
                if e.to_string().contains("already exists") {
                    StorageError::BucketAlreadyExists(dto.bucket_name.clone())
                } else {
                    StorageError::Internal(format!("Failed to create bucket: {}", e))
                }
            })?;
        Ok(())
    }

    async fn list_buckets(&self, dto: ListBucketsDTO) -> Result<Vec<BucketName>, StorageError> {
        let all_buckets = self
            .buckets_metadata_storage
            .list_buckets()
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to list buckets: {}", e)))?;

        let offset = dto.offset.unwrap_or(0) as usize;
        let limit = dto.limit.unwrap_or(DEFAULT_LIMIT) as usize;

        if offset >= all_buckets.len() {
            return Ok(Vec::new());
        }

        Ok(all_buckets.into_iter().skip(offset).take(limit).collect())
    }

    async fn delete_bucket(&self, dto: DeleteBucketDTO) -> Result<(), StorageError> {
        self.buckets_metadata_storage
            .remove_bucket(&dto.bucket_name)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to delete bucket: {}", e)))?;
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
            return Err(StorageError::InvalidInput("Invalid object key".to_string()));
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

        // Add object to metadata storage - if it already exists, remove it first then add it
        // This handles the case where we're overwriting an existing object
        let _ = self
            .buckets_metadata_storage
            .remove_object(&dto.bucket_name, &dto.key)
            .await;

        self.buckets_metadata_storage
            .add_object(&dto.bucket_name, dto.key.clone())
            .await
            .map_err(|e| {
                StorageError::Internal(format!("Failed to add object to metadata: {}", e))
            })?;

        Ok(ObjectMetadata {
            bucket_name: dto.bucket_name.clone(),
            key: dto.key.clone(),
            size: size,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| StorageError::Internal("System time error".to_string()))?
                .as_millis() as i64,
        })
    }

    async fn get_object(
        &self,
        dto: GetObjectDTO,
    ) -> Result<impl Stream<Item = Vec<u8>>, StorageError> {
        if !self
            .buckets_metadata_storage
            .bucket_exists(&dto.bucket_name)
            .await
        {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }

        let object_chunk_id = object_chunk_id(&dto.key, &dto.bucket_name);
        let chunk_with_metadata = self
            .bytes_storage
            .get_chunk(object_chunk_id)
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") {
                    StorageError::ObjectNotFound {
                        bucket: dto.bucket_name.clone(),
                        key: dto.key.clone(),
                    }
                } else {
                    StorageError::Internal(format!("Failed to get object: {}", e))
                }
            })?;
        Ok(chunk_with_metadata.stream)
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
            .map_err(|e| StorageError::Internal(format!("Failed to list objects: {}", e)))?;

        if dto.offset.unwrap_or(0) as usize >= object_keys.len() {
            return Ok(Vec::new());
        }

        let mut metas: Vec<ObjectMetadata> = Vec::new();
        for object_key in object_keys {
            if let Some(prefix) = &dto.prefix
                && !object_key.starts_with(prefix)
            {
                continue;
            }

            let chunk_metadata = self
                .bytes_storage
                .get_chunk_metadata(object_chunk_id(&object_key, &dto.bucket_name))
                .await
                .map_err(|e| {
                    StorageError::Internal(format!(
                        "Failed to get chunk metadata for object {}: {}",
                        object_key, e
                    ))
                })?;

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
        if !self
            .buckets_metadata_storage
            .bucket_exists(&dto.bucket_name)
            .await
        {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }

        let chunk_metadata = self
            .bytes_storage
            .get_chunk_metadata(object_chunk_id(&dto.key, &dto.bucket_name))
            .await
            .map_err(|e| {
                if e.to_string().contains("not found") || e.to_string().contains("deleted") {
                    StorageError::ObjectNotFound {
                        bucket: dto.bucket_name.clone(),
                        key: dto.key.clone(),
                    }
                } else {
                    StorageError::Internal(format!(
                        "Failed to get chunk metadata for head object: {}",
                        e
                    ))
                }
            })?;

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
            return Err(StorageError::InvalidInput("Invalid object key".to_string()));
        }

        if !self
            .buckets_metadata_storage
            .bucket_exists(&dto.bucket_name)
            .await
        {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }

        // Try to remove object from metadata, but don't error if it doesn't exist
        let _ = self
            .buckets_metadata_storage
            .remove_object(&dto.bucket_name, &dto.key)
            .await;

        self.bytes_storage
            .delete_chunk(object_chunk_id(&dto.key, &dto.bucket_name))
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to delete chunk: {}", e)))?;

        Ok(())
    }
}

fn file_name_is_correct(file_name: &str) -> bool {
    // This regex should always compile since it's a constant
    Regex::new(FILE_NAME_REGEX)
        .expect("Invalid FILE_NAME_REGEX constant")
        .is_match(file_name)
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
