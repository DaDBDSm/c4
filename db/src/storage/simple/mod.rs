mod chunk_file_storage;
use regex::Regex;
use tokio::fs::File;
use tokio::io::{self, AsyncReadExt};
use tokio_stream::{Stream, StreamExt};

use crate::storage::errors::StorageError;
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
}

impl ObjectStorageSimple {
    fn bucket_dir(&self, bucket_name: &BucketName) -> Result<String, StorageError> {
        self.base_dir
            .join(bucket_name)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket name".to_string()))
            .and_then(|dir_path| Ok(String::from(dir_path)))
    }

    fn object_path(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<String, StorageError> {
        self.base_dir
            .join(bucket_name)
            .join(key)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket or object name".to_string()))
            .and_then(|object_path| Ok(String::from(object_path)))
    }

    pub async fn bucket_exists(&self, bucket_name: &BucketName) -> Result<bool, StorageError> {
        let bucket_path = self.bucket_dir(bucket_name)?;
        match tokio::fs::try_exists(&bucket_path).await {
            Ok(exists) => Ok(exists),
            Err(e) => Err(StorageError::IoError(e)),
        }
    }

    pub async fn get_object_stream(
        &self,
        bucket_name: BucketName,
        key: ObjectKey,
    ) -> Result<
        (
            impl Stream<Item = io::Result<Vec<u8>>> + Unpin + Send + Sync,
            ObjectMetadata,
        ),
        StorageError,
    > {
        if !self.bucket_exists(&bucket_name).await? {
            return Err(StorageError::BucketNotFound(bucket_name.clone()));
        }
        let path = self.object_path(&bucket_name, &key)?;

        let raw_stream = match self.file_manager.stream_file(&path).await {
            Ok(s) => s,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(StorageError::ObjectNotFound {
                    bucket: bucket_name.clone(),
                    key: key.clone(),
                });
            }
            Err(e) => return Err(StorageError::IoError(e)),
        };

        let header_size = ObjectHeader::SIZE;
        let mut header_bytes_read = 0;
        let mut _header_skipped = false;

        let mut chunks = Vec::new();
        let mut stream = raw_stream;

        while header_bytes_read < header_size {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    if header_bytes_read + chunk.len() <= header_size {
                        header_bytes_read += chunk.len();
                        if header_bytes_read == header_size {
                            _header_skipped = true;
                        }
                    } else {
                        let data_start = header_size - header_bytes_read;
                        let data_chunk = chunk[data_start..].to_vec();
                        chunks.push(Ok(data_chunk));
                        _header_skipped = true;
                        break;
                    }
                }
                Some(Err(e)) => return Err(StorageError::IoError(e)),
                None => return Err(StorageError::Internal("File too short".to_string())),
            }
        }

        let data_stream = tokio_stream::iter(chunks);
        let remaining_stream = stream.map(|chunk| chunk);
        let combined_stream = tokio_stream::StreamExt::chain(data_stream, remaining_stream);

        let stream: Box<dyn Stream<Item = io::Result<Vec<u8>>> + Unpin + Send + Sync> =
            Box::new(combined_stream);
        let object_metadata = self.get_object_metadata(&path, &bucket_name, &key).await?;

        Ok((stream, object_metadata))
    }

    async fn get_object_metadata(
        &self,
        path: &str,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, StorageError> {
        let file_metadata = tokio::fs::metadata(path)
            .await
            .map_err(StorageError::IoError)?;

        let mut object_reader = File::open(path).await.unwrap();
        let mut object_header_bytes = [0u8; ObjectHeader::SIZE];
        object_reader
            .read(&mut object_header_bytes)
            .await
            .map_err(StorageError::IoError)?;

        let object_header = ObjectHeader::from_bytes(&object_header_bytes);
        if object_header.magic != OBJECT_MAGIC {
            return Err(StorageError::Internal("Invalid object magic".to_string()));
        };

        let actual_size = file_metadata.len() - ObjectHeader::SIZE as u64;

        Ok(ObjectMetadata {
            bucket_name: bucket_name.clone(),
            key: key.clone(),
            size: actual_size,
            created_at: object_header.created_at,
        })
    }
}

impl ObjectStorage for ObjectStorageSimple {
    async fn create_bucket(&self, dto: &CreateBucketDTO) -> Result<(), StorageError> {
        todo!()
    }

    async fn list_buckets(&self, dto: &ListBucketsDTO) -> Result<Vec<BucketName>, StorageError> {
        todo!()
    }

    async fn delete_bucket(&self, dto: &DeleteBucketDTO) -> Result<(), StorageError> {
        todo!()
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

        if !self.bucket_exists(&dto.bucket_name).await? {
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
        dto: &GetObjectDTO,
    ) -> Result<impl Stream<Item = io::Result<Vec<u8>>>, StorageError> {
        let object_chunk_id = object_chunk_id(&dto.key, &dto.bucket_name);
        let stream = self.bytes_storage.get_chunk(object_chunk_id).await?;
        Ok(stream)
    }

    async fn list_objects(
        &self,
        dto: &ListObjectsDTO,
    ) -> Result<Vec<ObjectMetadata>, StorageError> {
        if !self.bucket_exists(&dto.bucket_name).await? {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }
        let object_keys = self
            .file_manager
            .list_dir(&self.bucket_dir(&dto.bucket_name)?)
            .await
            .map_err(StorageError::IoError)?;

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

            metas.push(
                self.get_object_stream(dto.bucket_name.clone(), object_key.clone())
                    .await?
                    .1,
            );
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

    async fn head_object(&self, dto: &HeadObjectDTO) -> Result<ObjectMetadata, StorageError> {
        Ok(self
            .get_object_stream(dto.bucket_name.clone(), dto.key.clone())
            .await?
            .1)
    }

    async fn delete_object(&self, dto: &DeleteObjectDTO) -> Result<(), StorageError> {
        if dto.bucket_name.len() < 1 {
            return Err(StorageError::InvalidInput("Empty bucket name".to_string()));
        }
        if dto.bucket_name.contains("..") || dto.bucket_name.starts_with("/") {
            return Err(StorageError::InvalidInput(
                "incorrect bucket name".to_string(),
            ));
        }
        if dto.key.contains("..") || dto.key.contains("/") {
            return Err(StorageError::InvalidInput(
                "incorrect object key".to_string(),
            ));
        }

        if !self.bucket_exists(&dto.bucket_name).await? {
            return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
        }
        self.file_manager
            .delete_file(&self.object_path(&dto.bucket_name, &dto.key)?)
            .await
            .map_err(StorageError::IoError)
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
