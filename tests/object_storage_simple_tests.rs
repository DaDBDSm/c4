use std::io::Cursor;
use tempfile::TempDir;

use c4::object_storage::errors::StorageError;
use c4::object_storage::simple::file::FileManager;
use c4::object_storage::simple::object_storage_simple::ObjectStorageSimple;
use c4::object_storage::{
    CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, GetObjectDTO, HeadObjectDTO, ListBucketsDTO,
    ListObjectsDTO, ObjectStorage, PutObjectDTO, SortingOrder,
};

fn create_test_storage() -> (ObjectStorageSimple, TempDir) {
    let temp_dir: TempDir = TempDir::new().expect("Failed to create temp dir");
    let storage = ObjectStorageSimple {
        base_dir: temp_dir.path().to_path_buf(),
        file_manager: FileManager::new(10 * 1024 * 1024, 1024),
    };
    (storage, temp_dir)
}

#[test]
fn test_create_and_list_buckets() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket1 = "test-bucket-1".to_string();
    let bucket2 = "test-bucket-2".to_string();

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket1.clone(),
        })
        .expect("Failed to create bucket 1");
    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket2.clone(),
        })
        .expect("Failed to create bucket 2");
    match storage.create_bucket(&CreateBucketDTO {
        bucket_name: bucket2.clone(),
    }) {
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
            offset: 0,
            limit: 10,
        })
        .expect("Failed to list buckets");
    assert_eq!(buckets.len(), 2);
    assert!(buckets.contains(&bucket1));
    assert!(buckets.contains(&bucket2));

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket1.clone(),
        })
        .expect("Failed to delete bucket");
    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket2.clone(),
        })
        .expect("Failed to delete bucket");
}

#[test]
fn test_delete_bucket() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    let reader: Cursor<&[u8]> = Cursor::new(b"test_data");

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        reader: Box::new(reader),
    };
    storage
        .put_object(&mut put_dto)
        .expect("Failed to put object");

    let buckets = storage
        .list_buckets(&ListBucketsDTO {
            offset: 0,
            limit: 10,
        })
        .expect("Failed to list buckets");
    assert_eq!(buckets.len(), 1);
    assert!(buckets.contains(&bucket_name));

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to delete bucket");

    let buckets = storage
        .list_buckets(&ListBucketsDTO {
            offset: 0,
            limit: 10,
        })
        .expect("Failed to list buckets");
    assert_eq!(buckets.len(), 0);

    match storage.list_objects(&ListObjectsDTO {
        bucket_name: bucket_name.clone(),
        offset: 0,
        limit: 10,
        sorting_order: SortingOrder::ASC,
        prefix: None,
    }) {
        Err(StorageError::BucketNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::BucketNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };
}

#[test]
fn test_put_and_get_object() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    let test_data = b"Hello, World!";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    let reader: Cursor<&[u8]> = Cursor::new(test_data);
    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        reader: Box::new(reader),
    };
    let metadata = storage
        .put_object(&mut put_dto)
        .expect("Failed to put object");
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    let reader: Cursor<&[u8]> = Cursor::new(test_data);
    put_dto.reader = Box::new(reader);
    let metadata = storage
        .put_object(&mut put_dto)
        .expect("Failed to put object");
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    let (mut reader, get_metadata) = storage
        .get_object(&GetObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .expect("Failed to get object");
    assert_eq!(get_metadata.bucket_name, bucket_name);
    assert_eq!(get_metadata.key, object_key);
    assert_eq!(get_metadata.size, test_data.len() as u64);

    let mut read_data = Vec::new();
    reader
        .read_to_end(&mut read_data)
        .expect("Failed to read object data");
    assert_eq!(read_data, test_data);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to delete bucket");
}

#[test]
fn test_head_object() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    let test_data = b"Hello, World!";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    let reader: Cursor<&[u8]> = Cursor::new(test_data);
    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        reader: Box::new(reader),
    };
    storage
        .put_object(&mut put_dto)
        .expect("Failed to put object");

    let metadata = storage
        .head_object(&HeadObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .expect("Failed to head object");
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to delete bucket");
}

#[test]
fn test_delete_object() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let object_key = "test-object.txt".to_string();
    let test_data = b"Hello, World!";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    let reader: Cursor<&[u8]> = Cursor::new(test_data);
    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        reader: Box::new(reader),
    };
    storage
        .put_object(&mut put_dto)
        .expect("Failed to put object");

    storage
        .delete_object(&DeleteObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .expect("Failed to delete object");

    match storage.head_object(&HeadObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    }) {
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
        .expect("Failed to delete bucket");
}

#[test]
fn test_list_objects() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let test_objects = vec!["object1.txt", "object2.txt", "prefix_object.txt"];

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    for object_name in test_objects.iter() {
        let test_data = format!("Data for {}", object_name).into_bytes();
        let reader: Cursor<Vec<u8>> = Cursor::new(test_data);
        let mut put_dto = PutObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_name.to_string(),
            reader: Box::new(reader),
        };
        storage
            .put_object(&mut put_dto)
            .expect(&format!("Failed to put object {}", object_name));
    }

    let objects = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: 0,
            limit: 10,
            sorting_order: SortingOrder::ASC,
            prefix: None,
        })
        .expect("Failed to list objects");
    assert_eq!(objects.len(), 3);
    assert!(objects[0].key < objects[1].key);
    assert!(objects[1].key < objects[2].key);

    let prefixed_objects = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: 0,
            limit: 10,
            sorting_order: SortingOrder::ASC,
            prefix: Some("prefix".to_string()),
        })
        .expect("Failed to list prefixed objects");
    assert_eq!(prefixed_objects.len(), 1);
    assert_eq!(prefixed_objects[0].key, "prefix_object.txt");

    let first_page = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: 0,
            limit: 2,
            sorting_order: SortingOrder::ASC,
            prefix: None,
        })
        .expect("Failed to list first page");
    assert_eq!(first_page.len(), 2);

    let second_page = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: 2,
            limit: 2,
            sorting_order: SortingOrder::ASC,
            prefix: None,
        })
        .expect("Failed to list second page");
    assert_eq!(second_page.len(), 1);

    let desc_objects = storage
        .list_objects(&ListObjectsDTO {
            bucket_name: bucket_name.clone(),
            offset: 0,
            limit: 10,
            sorting_order: SortingOrder::DESC,
            prefix: None,
        })
        .expect("Failed to list objects in DESC order");
    assert_eq!(desc_objects.len(), 3);
    assert!(desc_objects[0].key > desc_objects[1].key);
    assert!(desc_objects[1].key > desc_objects[2].key);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to delete bucket");
}

#[test]
fn test_bucket_pagination() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_names = vec!["bucket-a", "bucket-b", "bucket-c"];
    for bucket_name in &bucket_names {
        storage
            .create_bucket(&CreateBucketDTO {
                bucket_name: bucket_name.to_string(),
            })
            .expect("Failed to create bucket");
    }

    let first_page = storage
        .list_buckets(&ListBucketsDTO {
            offset: 0,
            limit: 2,
        })
        .expect("Failed to list first page");
    assert_eq!(first_page.len(), 2);

    let second_page = storage
        .list_buckets(&ListBucketsDTO {
            offset: 2,
            limit: 2,
        })
        .expect("Failed to list second page");
    assert_eq!(second_page.len(), 1);

    let empty_page = storage
        .list_buckets(&ListBucketsDTO {
            offset: 10,
            limit: 5,
        })
        .expect("Failed to list empty page");
    assert_eq!(empty_page.len(), 0);

    for bucket_name in &bucket_names {
        storage
            .delete_bucket(&DeleteBucketDTO {
                bucket_name: bucket_name.to_string(),
            })
            .expect("Failed to delete bucket");
    }
}

#[test]
fn test_large_object() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let object_key = "large-object.bin".to_string();

    let large_data = vec![77u8; 1024 * 1024];

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    let reader: Cursor<Vec<u8>> = Cursor::new(large_data.clone());
    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        reader: Box::new(reader),
    };
    let metadata = storage
        .put_object(&mut put_dto)
        .expect("Failed to put large object");
    assert_eq!(metadata.size, large_data.len() as u64);

    let (mut reader, metadata) = storage
        .get_object(&GetObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .expect("Failed to get large object");
    assert_eq!(metadata.key, object_key);
    assert_eq!(metadata.bucket_name, bucket_name);
    assert_eq!(metadata.size, large_data.len().try_into().unwrap());

    let mut read_data = Vec::new();
    reader
        .read_to_end(&mut read_data)
        .expect("Failed to read large object");
    assert_eq!(read_data.len(), large_data.len());

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to delete bucket");
}

#[test]
fn test_empty_object() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let object_key = "empty.txt".to_string();
    let empty_data = b"";

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    let reader: Cursor<&[u8]> = Cursor::new(empty_data);
    let mut put_dto = PutObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
        reader: Box::new(reader),
    };
    let metadata = storage
        .put_object(&mut put_dto)
        .expect("Failed to put empty object");
    assert_eq!(metadata.size, 0);

    let (mut reader, _) = storage
        .get_object(&GetObjectDTO {
            bucket_name: bucket_name.clone(),
            key: object_key.clone(),
        })
        .expect("Failed to get empty object");

    let mut read_data = Vec::new();
    reader
        .read_to_end(&mut read_data)
        .expect("Failed to read empty object");
    assert_eq!(read_data.len(), 0);

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to delete bucket");
}

#[test]
fn test_nonexistent_bucket_operations() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "nonexistent-bucket".to_string();
    let object_key = "test-object.txt".to_string();

    match storage.list_objects(&ListObjectsDTO {
        bucket_name: bucket_name.clone(),
        offset: 0,
        limit: 10,
        sorting_order: SortingOrder::ASC,
        prefix: None,
    }) {
        Err(StorageError::BucketNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::BucketNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    match storage.get_object(&GetObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    }) {
        Err(StorageError::BucketNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::BucketNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    match storage.head_object(&HeadObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    }) {
        Err(StorageError::BucketNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::BucketNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };
}

#[test]
fn test_nonexistent_object_operations() {
    let (storage, _temp_dir) = create_test_storage();

    let bucket_name = "test-bucket".to_string();
    let object_key = "nonexistent-object.txt".to_string();

    storage
        .create_bucket(&CreateBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to create bucket");

    match storage.get_object(&GetObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    }) {
        Err(StorageError::ObjectNotFound { .. }) => {}
        Err(other) => panic!(
            "expected {}, got: {:?}",
            stringify!(StorageError::ObjectNotFound),
            other
        ),
        Ok(_) => panic!("expected error, got Ok"),
    };

    match storage.head_object(&HeadObjectDTO {
        bucket_name: bucket_name.clone(),
        key: object_key.clone(),
    }) {
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
        .expect("Delete nonexistent object should not error");

    storage
        .delete_bucket(&DeleteBucketDTO {
            bucket_name: bucket_name.clone(),
        })
        .expect("Failed to delete bucket");
}
