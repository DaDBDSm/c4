use std::{
    fs::{create_dir, remove_dir},
    io::{Cursor, Read},
    path::PathBuf, time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    api::{
        errors::StorageError,
        object_storage::{BucketName, ObjectKey, ObjectMetadata, ObjectStorage},
    },
    storage::{
        file::{create_file, delete_dir, delete_file, get_file, get_file_reader, list_dir},
        model::{ObjectFileHeader, OBJECT_FILE_MAGIC},
    },
};

pub struct ObjectStorageSimple {
    pub base_dir: PathBuf,
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
    fn create_bucket(&mut self, bucket_name: &BucketName) -> Result<(), StorageError> {
        create_dir(self.bucket_dir(bucket_name));
        return Result::Ok(());
    }

    fn delete_bucket(&mut self, bucket_name: &BucketName) -> Result<(), StorageError> {
        remove_dir(self.bucket_dir(bucket_name));
        Result::Ok(())
    }

    fn list_buckets(&self) -> Result<Vec<BucketName>, StorageError> {
        Result::Ok(list_dir(self.base_dir.to_str().unwrap()).unwrap())
    }

    fn put_object<R: std::io::Read>(
        &mut self,
        bucket_name: &BucketName,
        key: &ObjectKey,
        reader: &mut R,
    ) -> Result<ObjectMetadata, StorageError> {
        let create_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

        let file_header = ObjectFileHeader {
            magic: OBJECT_FILE_MAGIC, 
            create_timestamp: create_ts,
        };

        let mut reader_with_header = add_prefix_to_reader(&file_header.to_bytes(), reader);

        create_file(&mut reader_with_header, self.object_path(bucket_name, key).to_str().unwrap());
        
        let result = ObjectMetadata {
            bucket_name: bucket_name.to_string(),
            key: key.to_string(),
            size: todo!(),
            created: todo!(),
            etag: todo!(),
        }

        return Result::Ok(result);
    }

    fn get_object(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn std::io::Read>, StorageError> {
        let path = self.object_path(bucket_name, key);
        let path_str = path.to_str().unwrap();
        let reader = get_file_reader(path_str).unwrap();
    
        Ok(Box::new(reader))
    }

    fn head_object(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, StorageError> {
        todo!()
    }

    fn delete_object(
        &mut self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), StorageError> {
        delete_file(self.object_path(bucket_name, key));
        return Result::Ok(());
    }

    fn list_objects(&self, offset: u64, limit: u64) -> Result<Vec<ObjectMetadata>, StorageError> {
        todo!()
    }
}

fn add_prefix_to_reader<R: Read>(prefix: &[u8], reader: R) -> impl Read {
    Cursor::new(prefix).chain(reader)
}
