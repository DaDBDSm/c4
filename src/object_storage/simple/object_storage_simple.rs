use std::{
    io::{ErrorKind, Read},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::object_storage::errors::StorageError;
use crate::object_storage::simple::file::FileManager;
use crate::object_storage::simple::model::{OBJECT_FILE_MAGIC, ObjectFileHeader};
use crate::object_storage::{
    BucketName, CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, GetObjectDTO, HeadObjectDTO,
    ListBucketsDTO, ListObjectsDTO, ObjectKey, ObjectMetadata, ObjectStorage, SortingOrder,
};

pub struct ObjectStorageSimple {
    pub base_dir: PathBuf,
    pub file_manager: FileManager,
}

impl ObjectStorageSimple {
    fn bucket_dir(&self, bucket_name: &BucketName) -> PathBuf {
        self.base_dir.join(bucket_name)
    }

    fn object_path(&self, bucket_name: &BucketName, key: &ObjectKey) -> PathBuf {
        self.base_dir.join(bucket_name).join(key)
    }
}

impl ObjectStorage for ObjectStorageSimple {
    fn create_bucket(&self, dto: &CreateBucketDTO) -> Result<(), StorageError> {
        self.bucket_dir(&dto.bucket_name)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket name".to_string()))
            .and_then(
                |bucket_dir| match self.file_manager.create_dir(bucket_dir) {
                    Ok(_) => Ok(()),
                    Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                        Err(StorageError::BucketAlreadyExists(dto.bucket_name.clone()))
                    }
                    Err(e) => Err(StorageError::IoError(e)),
                },
            )
    }

    fn delete_bucket(&self, dto: &DeleteBucketDTO) -> Result<(), StorageError> {
        self.bucket_dir(&dto.bucket_name)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket name".to_string()))
            .and_then(|bucket_dir| {
                self.file_manager
                    .delete_dir(bucket_dir)
                    .map_err(StorageError::IoError)
            })
    }

    fn list_buckets(&self, dto: &ListBucketsDTO) -> Result<Vec<BucketName>, StorageError> {
        let mut buckets = self
            .base_dir
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid base directory".to_string()))
            .and_then(|base_dir| {
                self.file_manager
                    .list_dir(base_dir)
                    .map_err(StorageError::IoError)
            })?;
        buckets.sort();
        let start = dto.offset as usize;
        let end = std::cmp::min(start.saturating_add(dto.limit as usize), buckets.len());
        if start >= buckets.len() {
            return Ok(Vec::new());
        }
        Ok(buckets[start..end].to_vec())
    }

    fn put_object(
        &self,
        dto: &mut crate::object_storage::PutObjectDTO,
    ) -> Result<ObjectMetadata, StorageError> {
        let file_header = ObjectFileHeader {
            magic: OBJECT_FILE_MAGIC,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        };
        let header_bytes = file_header.to_bytes();
        let mut reader_with_header = self
            .file_manager
            .add_prefix_to_reader(&header_bytes, dto.reader.as_mut());
        let file_size = self
            .object_path(&dto.bucket_name, &dto.key)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket or object name".to_string()))
            .and_then(|object_dir| {
                match self
                    .file_manager
                    .create_file(&mut reader_with_header, object_dir)
                {
                    Ok(sz) => Ok(sz),
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        Err(StorageError::BucketNotFound(dto.bucket_name.clone()))
                    }
                    Err(e) => Err(StorageError::IoError(e)),
                }
            })?;
        if file_size < ObjectFileHeader::SIZE as u64 {
            return Err(StorageError::Internal(
                "Written file smaller than header".to_string(),
            ));
        }

        let etag = self
            .object_path(&dto.bucket_name, &dto.key)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket or object name".to_string()))
            .and_then(|path| {
                self.file_manager
                    .compute_sha256_file_payload(path, ObjectFileHeader::SIZE)
                    .map_err(StorageError::IoError)
            })?;

        let metadata = ObjectMetadata {
            bucket_name: dto.bucket_name.clone(),
            key: dto.key.clone(),
            size: file_size - ObjectFileHeader::SIZE as u64,
            created_at: file_header.created_at,
            etag,
        };
        Ok(metadata)
    }

    fn get_object(
        &self,
        dto: &GetObjectDTO,
    ) -> Result<(Box<dyn Read>, ObjectMetadata), StorageError> {
        let path_buf = self.object_path(&dto.bucket_name, &dto.key);
        let path_str = path_buf.to_str().ok_or_else(|| {
            StorageError::InvalidInput("Invalid bucket or object name".to_string())
        })?;
        let mut file_reader = match self.file_manager.open_file_checked(path_str) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let bucket_dir = self
                    .bucket_dir(&dto.bucket_name)
                    .to_str()
                    .ok_or_else(|| StorageError::InvalidInput("Invalid bucket name".to_string()))?
                    .to_string();
                return match self.file_manager.list_dir(&bucket_dir) {
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        Err(StorageError::BucketNotFound(dto.bucket_name.clone()))
                    }
                    _ => Err(StorageError::ObjectNotFound {
                        bucket: dto.bucket_name.clone(),
                        key: dto.key.clone(),
                    }),
                };
            }
            Err(e) => return Err(StorageError::IoError(e)),
        };

        let mut file_header_bytes = [0u8; ObjectFileHeader::SIZE];
        file_reader
            .read_exact(&mut file_header_bytes)
            .map_err(StorageError::IoError)?;
        let file_header = ObjectFileHeader::from_bytes(&file_header_bytes);
        if file_header.magic != OBJECT_FILE_MAGIC {
            return Err(StorageError::Internal(
                "Invalid object file magic".to_string(),
            ));
        }
        let file_len = std::fs::metadata(path_str)
            .map_err(StorageError::IoError)?
            .len();
        let payload_size = file_len.saturating_sub(ObjectFileHeader::SIZE as u64);

        let etag = self
            .file_manager
            .compute_sha256_file_payload(path_str, ObjectFileHeader::SIZE)
            .map_err(StorageError::IoError)?;
        let metadata = ObjectMetadata {
            bucket_name: dto.bucket_name.clone(),
            key: dto.key.clone(),
            size: payload_size,
            created_at: file_header.created_at,
            etag,
        };
        Ok((Box::new(file_reader), metadata))
    }

    fn head_object(&self, dto: &HeadObjectDTO) -> Result<ObjectMetadata, StorageError> {
        let get_dto = GetObjectDTO {
            bucket_name: dto.bucket_name.clone(),
            key: dto.key.clone(),
        };
        self.get_object(&get_dto).map(|(_, metadata)| metadata)
    }

    fn delete_object(&self, dto: &DeleteObjectDTO) -> Result<(), StorageError> {
        self.object_path(&dto.bucket_name, &dto.key)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket or object name".to_string()))
            .and_then(|object_dir| {
                self.file_manager
                    .delete_file(object_dir)
                    .map_err(StorageError::IoError)
            })
    }

    fn list_objects(&self, dto: &ListObjectsDTO) -> Result<Vec<ObjectMetadata>, StorageError> {
        let bucket_dir = self
            .bucket_dir(&dto.bucket_name)
            .to_str()
            .ok_or_else(|| StorageError::InvalidInput("Invalid bucket name".to_string()))
            .map(|s| s.to_string())?;

        let names = match self.file_manager.list_dir(&bucket_dir) {
            Ok(v) => v,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(StorageError::BucketNotFound(dto.bucket_name.clone()));
            }
            Err(e) => return Err(StorageError::IoError(e)),
        };

        let mut metas: Vec<ObjectMetadata> = Vec::new();
        for name in names {
            if let Some(prefix) = &dto.prefix {
                if !name.starts_with(prefix) {
                    continue;
                }
            }
            let object_path = self
                .object_path(&dto.bucket_name, &name)
                .to_str()
                .ok_or_else(|| StorageError::InvalidInput("Invalid object name".to_string()))
                .map(|s| s.to_string())?;

            let file_len = match std::fs::metadata(&object_path) {
                Ok(m) => m.len(),
                Err(e) => return Err(StorageError::IoError(e)),
            };
            if file_len < ObjectFileHeader::SIZE as u64 {
                continue;
            }

            let mut reader = self
                .file_manager
                .open_file_checked(&object_path)
                .map_err(StorageError::IoError)?;
            let mut header_bytes = [0u8; ObjectFileHeader::SIZE];
            if let Err(e) = Read::read_exact(&mut reader, &mut header_bytes) {
                return Err(StorageError::IoError(e));
            }
            let header = ObjectFileHeader::from_bytes(&header_bytes);
            if header.magic != OBJECT_FILE_MAGIC {
                continue;
            }
            let etag = self
                .file_manager
                .compute_sha256_file_payload(&object_path, ObjectFileHeader::SIZE)
                .map_err(StorageError::IoError)?;

            metas.push(ObjectMetadata {
                bucket_name: dto.bucket_name.clone(),
                key: name,
                size: file_len - ObjectFileHeader::SIZE as u64,
                created_at: header.created_at,
                etag,
            });
        }

        metas.sort_by(|a, b| match dto.sorting_order {
            SortingOrder::ASC => a.key.cmp(&b.key),
            SortingOrder::DESC => b.key.cmp(&a.key),
        });

        let start = dto.offset as usize;
        if start >= metas.len() {
            return Ok(Vec::new());
        }
        let limit = dto.limit as usize;
        Ok(metas.into_iter().skip(start).take(limit).collect())
    }
}
