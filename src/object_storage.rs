use std::{fmt::Error, io::Read, time::Instant};

pub type BucketName = String;
pub type ObjectKey = String;

pub struct ObjectMetadata {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: u64,
    pub created: Instant,
    pub etag: String,
}

pub trait ObjectStorage {
    fn create_bucket(&mut self, bucket_name: &BucketName) -> Result<(), Error>;

    fn delete_bucket(&mut self, bucket_name: &BucketName) -> Result<(), Error>;

    fn list_buckets(&self) -> Result<Vec<BucketName>, Error>;

    fn put_object<R: Read>(
        &mut self,
        bucket_name: &BucketName,
        key: &ObjectKey,
        reader: &mut R,
    ) -> Result<ObjectMetadata, Error>;

    fn get_object<'a>(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<Box<dyn Read + 'a>, Error>;

    fn head_object(
        &self,
        bucket_name: &BucketName,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, Error>;

    fn delete_object(&mut self, bucket_name: &BucketName, key: &ObjectKey) -> Result<(), Error>;

    fn list_objects(&self, offset: u64, limit: u64) -> Result<Vec<ObjectMetadata>, Error>;
}
