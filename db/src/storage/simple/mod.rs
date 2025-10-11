pub mod file;
mod meta;

use crate::storage::errors::StorageError;
use crate::storage::simple::file::FileManager;
use crate::storage::simple::meta::{OBJECT_MAGIC, ObjectHeader};
use crate::storage::{
    BucketName, CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, GetObjectDTO, HeadObjectDTO,
    ListBucketsDTO, ListObjectsDTO, ObjectKey, ObjectMetadata, ObjectStorage, SortingOrder,
};
use std::fs::File;
use std::{
    io::{ErrorKind, Read},
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const DEFAULT_LIMIT: u64 = 20;

pub struct ObjectStorageSimple {
    pub base_dir: PathBuf,
    pub file_manager: FileManager,
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

    fn get_object_reader(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<(Box<dyn Read>, ObjectMetadata), StorageError> {
        let object_reader = match self
            .file_manager
            .open_file_checked(&self.object_path(bucket_name, key)?)
        {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(StorageError::ObjectNotFound {
                    bucket: bucket_name.clone(),
                    key: key.clone(),
                });
            }
            Err(e) => return Err(StorageError::IoError(e)),
        };

        let object_metadata = self.get_object_metadata(&object_reader, bucket_name, key)?;
        Ok((Box::new(object_reader), object_metadata))
    }

    fn get_object_metadata(
        &self,
        mut object_reader: &File,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, StorageError> {
        let mut object_header_bytes = [0u8; ObjectHeader::SIZE];
        object_reader
            .read_exact(&mut object_header_bytes)
            .map_err(StorageError::IoError)?;

        let object_header = ObjectHeader::from_bytes(&object_header_bytes);
        if object_header.magic != OBJECT_MAGIC {
            return Err(StorageError::Internal("Invalid object magic".to_string()));
        };

        let file_metadata = object_reader.metadata().map_err(StorageError::IoError)?;

        Ok(ObjectMetadata {
            bucket_name: bucket_name.clone(),
            key: key.clone(),
            size: file_metadata.len() - ObjectHeader::SIZE as u64,
            created_at: object_header.created_at,
        })
    }
}

impl ObjectStorage for ObjectStorageSimple {
    fn create_bucket(&self, dto: &CreateBucketDTO) -> Result<(), StorageError> {
        match self
            .file_manager
            .create_dir(&self.bucket_dir(&dto.bucket_name)?)
        {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                Err(StorageError::BucketAlreadyExists(dto.bucket_name.clone()))
            }
            Err(e) => Err(StorageError::IoError(e)),
        }
    }

    fn list_buckets(&self, dto: &ListBucketsDTO) -> Result<Vec<BucketName>, StorageError> {
        let mut buckets = self
            .file_manager
            .list_dir(&self.bucket_dir(&BucketName::new())?)
            .map_err(StorageError::IoError)?;

        buckets.sort();
        let start = dto.offset.unwrap_or(DEFAULT_LIMIT) as usize;
        let end = std::cmp::min(
            start.saturating_add(dto.limit.unwrap_or(0) as usize),
            buckets.len(),
        );
        if start >= buckets.len() {
            return Ok(Vec::new());
        }
        Ok(buckets[start..end].to_vec())
    }

    fn delete_bucket(&self, dto: &DeleteBucketDTO) -> Result<(), StorageError> {
        self.file_manager
            .delete_dir(&self.bucket_dir(&dto.bucket_name)?)
            .map_err(StorageError::IoError)
    }

    fn put_object(
        &self,
        dto: &mut crate::storage::PutObjectDTO,
    ) -> Result<ObjectMetadata, StorageError> {
        let object_path = self.object_path(&dto.bucket_name, &dto.key)?;

        let object_header = ObjectHeader {
            magic: OBJECT_MAGIC,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        };
        let object_header_bytes = object_header.to_bytes();

        let mut reader_with_header = self
            .file_manager
            .add_prefix_to_reader(&object_header_bytes, dto.reader.as_mut());

        let object_size = match self
            .file_manager
            .create_file(&mut reader_with_header, &object_path)
        {
            Ok(sz) => sz,
            Err(e) => return Err(StorageError::IoError(e)),
        };
        if object_size < ObjectHeader::SIZE as u64 {
            return Err(StorageError::Internal(
                "Written object smaller than header".to_string(),
            ));
        }

        Ok(ObjectMetadata {
            bucket_name: dto.bucket_name.clone(),
            key: dto.key.clone(),
            size: object_size - ObjectHeader::SIZE as u64,
            created_at: object_header.created_at,
        })
    }

    fn get_object(&self, dto: &GetObjectDTO) -> Result<Box<dyn Read>, StorageError> {
        Ok(self.get_object_reader(&dto.bucket_name, &dto.key)?.0)
    }

    fn list_objects(&self, dto: &ListObjectsDTO) -> Result<Vec<ObjectMetadata>, StorageError> {
        let object_keys = self
            .file_manager
            .list_dir(&self.bucket_dir(&dto.bucket_name)?)
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

            metas.push(self.get_object_reader(&dto.bucket_name, &object_key)?.1);
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

    fn head_object(&self, dto: &HeadObjectDTO) -> Result<ObjectMetadata, StorageError> {
        Ok(self.get_object_reader(&dto.bucket_name, &dto.key)?.1)
    }

    fn delete_object(&self, dto: &DeleteObjectDTO) -> Result<(), StorageError> {
        self.file_manager
            .delete_file(&self.object_path(&dto.bucket_name, &dto.key)?)
            .map_err(StorageError::IoError)
    }
}
