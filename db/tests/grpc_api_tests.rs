use tempfile::TempDir;
use tokio::time::{Duration, sleep};
use tonic::Request;
use tonic::transport::{Channel, Server};

use db::api::grpc::C4Handler;
use db::storage::simple::ObjectStorageSimple;
use db::storage::simple::buckets_metadata_storage::BucketsMetadataStorage;
use db::storage::simple::chunk_file_storage::PartitionedBytesStorage;
use grpc_server::object_storage::c4_client::C4Client;
use grpc_server::object_storage::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    HeadObjectRequest, ListBucketsRequest, ListObjectsRequest, ObjectId, PutObjectRequest,
};

async fn create_test_server() -> (C4Client<Channel>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let base_dir = temp_dir.path().to_path_buf();

    let bytes_storage = PartitionedBytesStorage::new(base_dir.join("data"), 4);
    let buckets_metadata_storage =
        BucketsMetadataStorage::new(base_dir.join("metadata.json").to_string_lossy().to_string())
            .await
            .expect("Failed to create buckets metadata storage");

    let storage = ObjectStorageSimple {
        base_dir,
        bytes_storage,
        buckets_metadata_storage,
    };

    let handler = C4Handler {
        c4_storage: storage,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind to port");
    let addr = listener.local_addr().expect("Failed to get local address");

    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(grpc_server::object_storage::c4_server::C4Server::new(
                handler,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    sleep(Duration::from_millis(500)).await;

    let client = C4Client::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect to server");

    (client, temp_dir)
}

#[tokio::test]
async fn test_create_and_list_buckets_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    let bucket1 = "test-bucket-1";
    let bucket2 = "test-bucket-2";

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket1.to_string(),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to create bucket 1: {:?}",
        response.err()
    );

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket2.to_string(),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to create bucket 2: {:?}",
        response.err()
    );

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket1.to_string(),
        }))
        .await;
    assert!(response.is_err(), "Creating duplicate bucket should fail");

    let response = client
        .list_buckets(Request::new(ListBucketsRequest {
            limit: Some(10),
            offset: Some(0),
        }))
        .await
        .expect("Failed to list buckets");

    let buckets = &response.get_ref().bucket_names;
    assert_eq!(buckets.len(), 2);
    assert!(buckets.contains(&bucket1.to_string()));
    assert!(buckets.contains(&bucket2.to_string()));

    let response = client
        .list_buckets(Request::new(ListBucketsRequest {
            limit: Some(1),
            offset: Some(0),
        }))
        .await
        .expect("Failed to list buckets with pagination");

    assert_eq!(response.get_ref().bucket_names.len(), 1);

    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket1.to_string(),
        }))
        .await;
    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket2.to_string(),
        }))
        .await;
}

#[tokio::test]
async fn test_put_and_get_object_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    let bucket_name = "test-bucket";
    let object_key = "test-object.txt";
    let test_data = b"Hello, World! This is a test object.";

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to create bucket: {:?}",
        response.err()
    );

    let stream = tokio_stream::iter(vec![
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                ObjectId {
                version: 0,
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                },
            )),
        },
        PutObjectRequest {
            req: Some(
                grpc_server::object_storage::put_object_request::Req::ObjectPart(
                    test_data.to_vec(),
                ),
            ),
        },
    ]);

    let response = client
        .put_object(Request::new(stream))
        .await
        .expect("Failed to put object");

    let metadata = response.get_ref().metadata.as_ref().unwrap();
    assert_eq!(metadata.id.as_ref().unwrap().bucket_name, bucket_name);
    assert_eq!(metadata.id.as_ref().unwrap().object_key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    let response = client
        .get_object(Request::new(GetObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await
        .expect("Failed to get object");

    let mut stream = response.into_inner();
    let mut received_data = Vec::new();
    while let Some(chunk) = stream.message().await.expect("Failed to read stream") {
        received_data.extend_from_slice(&chunk.object_part);
    }

    assert_eq!(received_data, test_data);

    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
}

#[tokio::test]
async fn test_list_objects_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    let bucket_name = "test-bucket";
    let objects = vec!["object1.txt", "object2.txt", "prefix_object.txt"];

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to create bucket: {:?}",
        response.err()
    );

    for object_name in objects {
        let test_data = format!("Data for {}", object_name).into_bytes();
        let stream = tokio_stream::iter(vec![
            PutObjectRequest {
                req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                    ObjectId {
                version: 0,
                        bucket_name: bucket_name.to_string(),
                        object_key: object_name.to_string(),
                    },
                )),
            },
            PutObjectRequest {
                req: Some(
                    grpc_server::object_storage::put_object_request::Req::ObjectPart(test_data),
                ),
            },
        ]);

        let response = client
            .put_object(Request::new(stream))
            .await
            .expect(&format!("Failed to put object {}", object_name));

        assert!(response.get_ref().metadata.is_some());
    }

    let response = client
        .list_objects(Request::new(ListObjectsRequest {
            bucket_name: bucket_name.to_string(),
            limit: Some(10),
            offset: Some(0),
            sorting_order: Some("ASC".to_string()),
            prefix: None,
        }))
        .await
        .expect("Failed to list objects");

    let metadata_list = &response.get_ref().metadata;
    assert_eq!(metadata_list.len(), 3);

    let response = client
        .list_objects(Request::new(ListObjectsRequest {
            bucket_name: bucket_name.to_string(),
            limit: Some(10),
            offset: Some(0),
            sorting_order: Some("ASC".to_string()),
            prefix: Some("prefix".to_string()),
        }))
        .await
        .expect("Failed to list objects with prefix");

    let metadata_list = &response.get_ref().metadata;
    assert_eq!(metadata_list.len(), 1);
    assert_eq!(
        metadata_list[0].id.as_ref().unwrap().object_key,
        "prefix_object.txt"
    );

    let response = client
        .list_objects(Request::new(ListObjectsRequest {
            bucket_name: bucket_name.to_string(),
            limit: Some(2),
            offset: Some(0),
            sorting_order: Some("ASC".to_string()),
            prefix: None,
        }))
        .await
        .expect("Failed to list objects with pagination");

    let metadata_list = &response.get_ref().metadata;
    assert_eq!(metadata_list.len(), 2);

    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
}

#[tokio::test]
async fn test_head_object_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    let bucket_name = "test-bucket";
    let object_key = "test-object.txt";
    let test_data = b"Hello, World!";

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to create bucket: {:?}",
        response.err()
    );

    let stream = tokio_stream::iter(vec![
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                ObjectId {
                version: 0,
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                },
            )),
        },
        PutObjectRequest {
            req: Some(
                grpc_server::object_storage::put_object_request::Req::ObjectPart(
                    test_data.to_vec(),
                ),
            ),
        },
    ]);

    let _response = client
        .put_object(Request::new(stream))
        .await
        .expect("Failed to put object");

    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await
        .expect("Failed to head object");

    let metadata = response.get_ref().metadata.as_ref().unwrap();
    assert_eq!(metadata.id.as_ref().unwrap().bucket_name, bucket_name);
    assert_eq!(metadata.id.as_ref().unwrap().object_key, object_key);
    assert_eq!(metadata.size, test_data.len() as u64);

    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
}

#[tokio::test]
async fn test_delete_object_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    let bucket_name = "test-bucket";
    let object_key = "test-object.txt";
    let test_data = b"Hello, World!";

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to create bucket: {:?}",
        response.err()
    );

    let stream = tokio_stream::iter(vec![
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                ObjectId {
                version: 0,
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                },
            )),
        },
        PutObjectRequest {
            req: Some(
                grpc_server::object_storage::put_object_request::Req::ObjectPart(
                    test_data.to_vec(),
                ),
            ),
        },
    ]);

    let _response = client
        .put_object(Request::new(stream))
        .await
        .expect("Failed to put object");

    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await;
    assert!(response.is_ok(), "Object should exist before deletion");

    let response = client
        .delete_object(Request::new(DeleteObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to delete object: {:?}",
        response.err()
    );

    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await;
    assert!(response.is_err(), "Object should not exist after deletion");

    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
}

#[tokio::test]
async fn test_error_handling_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    let response = client
        .get_object(Request::new(GetObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: "non-existent-bucket".to_string(),
                object_key: "some-key".to_string(),
            }),
        }))
        .await;
    assert!(
        response.is_err(),
        "Getting object from non-existent bucket should fail"
    );

    let response = client
        .list_objects(Request::new(ListObjectsRequest {
            bucket_name: "non-existent-bucket".to_string(),
            limit: Some(10),
            offset: Some(0),
            sorting_order: Some("ASC".to_string()),
            prefix: None,
        }))
        .await;
    assert!(
        response.is_err(),
        "Listing objects from non-existent bucket should fail"
    );

    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: "non-existent-bucket".to_string(),
                object_key: "some-key".to_string(),
            }),
        }))
        .await;
    assert!(
        response.is_err(),
        "Head object from non-existent bucket should fail"
    );

    let response = client
        .delete_object(Request::new(DeleteObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: "non-existent-bucket".to_string(),
                object_key: "some-key".to_string(),
            }),
        }))
        .await;
    assert!(
        response.is_err(),
        "Delete object from non-existent bucket should fail"
    );
}

#[tokio::test]
async fn test_large_object_streaming_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    let bucket_name = "test-bucket";
    let object_key = "large-object.bin";

    let large_data = vec![77u8; 1024 * 1024];

    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(
        response.is_ok(),
        "Failed to create bucket: {:?}",
        response.err()
    );

    let stream = tokio_stream::iter(vec![
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                ObjectId {
                version: 0,
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                },
            )),
        },
        PutObjectRequest {
            req: Some(
                grpc_server::object_storage::put_object_request::Req::ObjectPart(
                    large_data.clone(),
                ),
            ),
        },
    ]);

    let response = client
        .put_object(Request::new(stream))
        .await
        .expect("Failed to put large object");

    let metadata = response.get_ref().metadata.as_ref().unwrap();
    assert_eq!(metadata.size, large_data.len() as u64);

    let response = client
        .get_object(Request::new(GetObjectRequest {
            id: Some(ObjectId {
                version: 0,
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await
        .expect("Failed to get large object");

    let mut stream = response.into_inner();
    let mut received_data = Vec::new();
    while let Some(chunk) = stream.message().await.expect("Failed to read stream") {
        received_data.extend_from_slice(&chunk.object_part);
    }

    assert_eq!(received_data.len(), large_data.len());
    assert_eq!(received_data, large_data);

    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
}
