use tempfile::TempDir;
use tokio_stream::StreamExt;

use db::storage::errors::StorageError;
use db::storage::simple::ObjectStorageSimple;
use db::storage::simple::file::FileManager;
use db::storage::{
    CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, GetObjectDTO, HeadObjectDTO, ListBucketsDTO,
    ListObjectsDTO, ObjectStorage, PutObjectDTO, SortingOrder,
};

async fn create_test_storage() -> (ObjectStorageSimple, TempDir) {
    let temp_dir: TempDir = TempDir::new().expect("Failed to create temp dir");
    let storage = ObjectStorageSimple {
        base_dir: temp_dir.path().to_path_buf(),
        file_manager: FileManager::new(10 * 1024 * 1024, 1024),
    };
    (storage, temp_dir)
}

fn cursor_to_stream(data: &[u8]) -> Box<dyn tokio_stream::Stream<Item = Vec<u8>> + Unpin + Send> {
    let stream = tokio_stream::iter(vec![data.to_vec()]);
    Box::new(stream)
}

async fn collect_stream_to_vec(mut stream: impl tokio_stream::Stream<Item = std::io::Result<Vec<u8>>> + Unpin) -> Vec<u8> {
    let mut result = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(data) => result.extend(data),
            Err(e) => panic!("Stream error: {}", e),
        }
    }
    result
}

#[tokio::test]
async fn test_create_and_list_buckets() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket1 = "test-bucket-1".to_string();
    let bucket2 = "test-bucket-2".to_string();

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket1.clone(),
        })
        .await
        .expect("Failed to create bucket 1");
    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket2.clone(),
        })
        .await
        .expect("Failed to create bucket 2");
    match storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket2.clone(),
        })
        .await
    {
        Err(StorageError::BucketAlreadyExists { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::BucketAlreadyExists),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    let buckets = storage
        .list_buckets(&ListBucketsDTO {
            offset: Some(0),
            limit: Some(10),
        })
        .await
        .expect("Failed to list buckets");
    assert_eq!(buckets.len(), 2);
    assert!(buckets.contains(&bucket1));
    assert!(buckets.contains(&bucket2));

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket1.clone(),
        })
        .await
        .expect("Failed to delete bucket");
    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket2.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
async fn test_delete_bucket() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        stream: cursor_to_stream(b"test_data"),
    };
    storage
        .put_object(&mut put_dto)
        .await
        .expect("Failed to put object");

    let buckets = storage
        .list_buckets(&ListBucketsDTO {
            offset: Some(0),
            limit: Some(10),
        })
        .await
        .expect("Failed to list buckets");
    assert_eq!(buckets.len(), 1);
    assert!(buckets.contains(&bucket_name));

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");

    let buckets = storage
        .list_buckets(&ListBucketsDTO {
            offset: Some(0),
            limit: Some(10),
        })
        .await
        .expect("Failed to list buckets");
    assert_eq!(buckets.len(), 0);

    match storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: Some(0),
            limit: Some(10),
            sorting_order: Some(SortingOrder::ASC),
            prefix: None,
        })
        .await
    {
        Err(StorageError::IoError { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::IoError),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };
}

#[tokio::test]
async fn test_put_and_get_object() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    let test_data = b"Hello, World!";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        stream: cursor_to_stream(test_data),
    };
    let metadata = storage
        .put_object(&mut put_dto)
        .await
        .expect("Failed to put object");
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    put_dto.stream = cursor_to_stream(test_data);
    let metadata = storage
        .put_object(&mut put_dto)
        .await
        .expect("Failed to put object");
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    let get_dto = GetObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    };
    let stream = storage
        .get_object(&get_dto)
        .await
        .expect("Failed to get object");

    let read_data = collect_stream_to_vec(stream).await;
    assert_eq!(read_data, test_data);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
async fn test_head_object() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    let test_data = b"Hello, World!";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        stream: cursor_to_stream(test_data),
    };
    storage
        .put_object(&mut put_dto)
        .await
        .expect("Failed to put object");

    let metadata = storage
        .head_object(&HeadObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
        .expect("Failed to head object");
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
async fn test_delete_object() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    let test_data = b"Hello, World!";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        stream: cursor_to_stream(test_data),
    };
    storage
        .put_object(&mut put_dto)
        .await
        .expect("Failed to put object");

    storage
        .delete_object(&DeleteObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
        .expect("Failed to delete object");

    match storage
        .head_object(&HeadObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
    {
        Err(StorageError::ObjectNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::ObjectNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
async fn test_list_objects() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let test_objects = vec!["object1.txt", "object2.txt", "prefix_object.txt"];

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    for object_name in test_objects.iter() {
        let test_data = format!("Data for {}", object_name).into_bytes();
        let mut put_dto = PutObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_name.to_string(),
            stream: cursor_to_stream(&test_data),
        };
        storage
            .put_object(&mut put_dto)
            .await
            .expect(&format!("Failed to put object {}", object_name));
    }

    let objects = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: Some(0),
            limit: Some(10),
            sorting_order: Some(SortingOrder::ASC),
            prefix: None,
        })
        .await
        .expect("Failed to list objects");
    assert_eq!(objects.len(), 3);
    assert!(objects[0].key < objects[1].key);
    assert!(objects[1].key < objects[2].key);

    let prefixed_objects = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: Some(0),
            limit: Some(10),
            sorting_order: Some(SortingOrder::ASC),
            prefix: Some("prefix".to_string()),
        })
        .await
        .expect("Failed to list prefixed objects");
    assert_eq!(prefixed_objects.len(), 1);
    assert_eq!(prefixed_objects[0].key, "prefix_object.txt");

    let first_page = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: Some(0),
            limit: Some(2),
            sorting_order: Some(SortingOrder::ASC),
            prefix: None,
        })
        .await
        .expect("Failed to list first page");
    assert_eq!(first_page.len(), 2);

    let second_page = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: Some(2),
            limit: Some(2),
            sorting_order: Some(SortingOrder::ASC),
            prefix: None,
        })
        .await
        .expect("Failed to list second page");
    assert_eq!(second_page.len(), 1);

    let desc_objects = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: Some(0),
            limit: Some(10),
            sorting_order: Some(SortingOrder::DESC),
            prefix: None,
        })
        .await
        .expect("Failed to list objects in DESC order");
    assert_eq!(desc_objects.len(), 3);
    assert!(desc_objects[0].key > desc_objects[1].key);
    assert!(desc_objects[1].key > desc_objects[2].key);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
async fn test_bucket_pagination() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_names = vec!["bucket-a", "bucket-b", "bucket-c"];
    for bucket_name in &bucket_names {
        storage
            .create_bucket(&CreateBucketDTO {
                bucket_name: bucket_name.to_string(),
            })
            .await
            .expect("Failed to create bucket");
    }

    let first_page = storage
        .list_buckets(&ListBucketsDTO {
            offset: Some(0),
            limit: Some(2),
        })
        .await
        .expect("Failed to list first page");
    assert_eq!(first_page.len(), 2);

    let second_page = storage
        .list_buckets(&ListBucketsDTO {
            offset: Some(2),
            limit: Some(2),
        })
        .await
        .expect("Failed to list second page");
    assert_eq!(second_page.len(), 1);

    let empty_page = storage
        .list_buckets(&ListBucketsDTO {
            offset: Some(10),
            limit: Some(5),
        })
        .await
        .expect("Failed to list empty page");
    assert_eq!(empty_page.len(), 0);

    for bucket_name in &bucket_names {
        storage
            .delete_bucket(&DeleteBucketDTO {
                bucket_name: bucket_name.to_string(),
            })
            .await
            .expect("Failed to delete bucket");
    }
}

#[tokio::test]
async fn test_large_object() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let object_key = "large-object.bin".to_string();

    let large_data = vec![77u8; 1024 * 1024];

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        stream: cursor_to_stream(&large_data),
    };
    let metadata = storage
        .put_object(&mut put_dto)
        .await
        .expect("Failed to put large object");
    assert_eq!(metadata.size, large_data.len() as u64);
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.size, large_data.len().try_into().unwrap());

    let get_dto = GetObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    };
    let stream = storage
        .get_object(&get_dto)
        .await
        .expect("Failed to get large object");

    let read_data = collect_stream_to_vec(stream).await;
    assert_eq!(read_data.len(), large_data.len());

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
async fn test_empty_object() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let object_key = "empty.txt".to_string();
    let empty_data = b"";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        stream: cursor_to_stream(empty_data),
    };
    let metadata = storage
        .put_object(&mut put_dto)
        .await
        .expect("Failed to put empty object");
    assert_eq!(metadata.size, 0);

    let get_dto = GetObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    };
    let stream = storage
        .get_object(&get_dto)
        .await
        .expect("Failed to get empty object");

    let read_data = collect_stream_to_vec(stream).await;
    assert_eq!(read_data.len(), 0);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}

#[tokio::test]
async fn test_nonexistent_bucket_operations() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "nonexistent-bucket".to_string();
    let object_key = "test-object.txt".to_string();

    match storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: Some(0),
            limit: Some(10),
            sorting_order: Some(SortingOrder::ASC),
            prefix: None,
        })
        .await
    {
        Err(StorageError::IoError { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::IoError),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    match storage
        .get_object(&GetObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
    {
        Err(StorageError::ObjectNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::ObjectNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    match storage
        .head_object(&HeadObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
    {
        Err(StorageError::ObjectNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::ObjectNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };
}

#[tokio::test]
async fn test_nonexistent_object_operations() {
    let (storage, _temp_dir) = create_test_storage().await;

    let bucket_name = "test-bucket".to_string();
    let object_key = "nonexistent-object.txt".to_string();

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to create bucket");

    match storage
        .get_object(&GetObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
    {
        Err(StorageError::ObjectNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::ObjectNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    match storage
        .head_object(&HeadObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
    {
        Err(StorageError::ObjectNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::ObjectNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    storage
        .delete_object(&DeleteObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .await
        .expect("Delete nonexistent object should not error");

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .await
        .expect("Failed to delete bucket");
}
