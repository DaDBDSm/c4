use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::storage::simple::buckets_metadata_model::{BucketMetadata, BucketsMetadata};

#[derive(Clone)]
pub struct BucketsMetadataStorage {
    data: Arc<RwLock<BucketsMetadata>>,
    file_path: String,
}

impl BucketsMetadataStorage {
    pub async fn new(file_path: String) -> Result<Self, Box<dyn std::error::Error>> {
        let storage = Self {
            data: Arc::new(RwLock::new(BucketsMetadata::default())),
            file_path,
        };

        storage.load_from_file().await?;
        Ok(storage)
    }

    async fn load_from_file(&self) -> Result<(), Box<dyn std::error::Error>> {
        match File::open(&self.file_path).await {
            Ok(mut file) => {
                let mut contents = Vec::new();
                file.read_to_end(&mut contents).await?;

                let metadata = BucketsMetadata::from_bytes(&contents)
                    .map_err(|e| format!("Failed to decode metadata: {}", e))?;

                let mut data = self.data.write().await;
                *data = metadata;
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    async fn save_to_file(&self) -> Result<(), Box<dyn std::error::Error>> {
        let data = self.data.read().await;
        let bytes = data
            .to_bytes()
            .map_err(|e| format!("Failed to encode metadata: {}", e))?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.file_path)
            .await?;

        file.write_all(&bytes).await?;
        file.sync_all().await?;
        Ok(())
    }

    pub async fn add_bucket(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;

        if data.buckets.iter().any(|bucket| bucket.name == name) {
            return Err("Bucket already exists".into());
        }

        data.buckets
            .push(BucketMetadata::new(name.to_string(), Vec::new()));

        drop(data);
        self.save_to_file().await
    }

    pub async fn list_objects(
        &self,
        bucket_name: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let data = self.data.read().await;

        let bucket = data
            .buckets
            .iter()
            .find(|bucket| bucket.name == bucket_name)
            .ok_or("Bucket not found")?;

        Ok(bucket.objects.clone())
    }

    pub async fn add_object(
        &self,
        bucket_name: &str,
        object_name: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;

        let bucket = data
            .buckets
            .iter_mut()
            .find(|bucket| bucket.name == bucket_name)
            .ok_or("Bucket not found")?;

        if bucket.objects.iter().any(|obj| obj == &object_name) {
            return Err("Object already exists in bucket".into());
        }

        bucket.objects.push(object_name);

        drop(data);
        self.save_to_file().await
    }

    pub async fn remove_object(
        &self,
        bucket_name: &str,
        object_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;

        let bucket = data
            .buckets
            .iter_mut()
            .find(|bucket| bucket.name == bucket_name)
            .ok_or("Bucket not found")?;

        let initial_len = bucket.objects.len();
        bucket.objects.retain(|obj| obj != object_name);

        if bucket.objects.len() == initial_len {
            return Err("Object not found in bucket".into());
        }

        drop(data);
        self.save_to_file().await
    }

    pub async fn remove_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = self.data.write().await;

        let initial_len = data.buckets.len();
        data.buckets.retain(|bucket| bucket.name != bucket_name);

        if data.buckets.len() == initial_len {
            return Err("Bucket not found".into());
        }

        drop(data);
        self.save_to_file().await
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let data = self.data.read().await;
        Ok(data
            .buckets
            .iter()
            .map(|bucket| bucket.name.clone())
            .collect())
    }

    pub async fn bucket_exists(&self, bucket_name: &str) -> bool {
        let data = self.data.read().await;
        data.buckets.iter().any(|bucket| bucket.name == bucket_name)
    }

    pub async fn object_exists(&self, bucket_name: &str, object_name: &str) -> bool {
        let data = self.data.read().await;
        data.buckets
            .iter()
            .find(|bucket| bucket.name == bucket_name)
            .map(|bucket| bucket.objects.iter().any(|obj| obj == object_name))
            .unwrap_or(false)
    }

    pub fn file_path(&self) -> &str {
        &self.file_path
    }
}
