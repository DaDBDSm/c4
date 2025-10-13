use tempfile::TempDir;
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Server};
use tonic::Request;

use db::api::grpc::C4Handler;
use db::storage::simple::file::FileManager;
use db::storage::simple::ObjectStorageSimple;
use grpc_server::object_storage::c4_client::C4Client;
use grpc_server::object_storage::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest, HeadObjectRequest,
    ListBucketsRequest, ListObjectsRequest, ObjectId, PutObjectRequest,
};

async fn create_test_server() -> (C4Client<Channel>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage = ObjectStorageSimple {
        base_dir: temp_dir.path().to_path_buf(),
        file_manager: FileManager::new(10 * 1024 * 1024, 1024),
    };

    let handler = C4Handler { c4_storage: storage };

    // Use a random available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // Free the port
    
    // Spawn server task
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(grpc_server::object_storage::c4_server::C4Server::new(handler))
            .serve(addr)
            .await;
    });

    // Give server time to start
    sleep(Duration::from_millis(500)).await;

    // Create client
    let client = C4Client::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect to server");

    (client, temp_dir)
}

#[tokio::test]
async fn test_create_and_list_buckets_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    // Test creating buckets
    let bucket1 = "test-bucket-1";
    let bucket2 = "test-bucket-2";

    // Create first bucket
    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket1.to_string(),
        }))
        .await;
    assert!(response.is_ok(), "Failed to create bucket 1: {:?}", response.err());

    // Create second bucket
    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket2.to_string(),
        }))
        .await;
    assert!(response.is_ok(), "Failed to create bucket 2: {:?}", response.err());

    // Try to create duplicate bucket (should fail)
    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket1.to_string(),
        }))
        .await;
    assert!(response.is_err(), "Creating duplicate bucket should fail");

    // List buckets
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

    // Test pagination
    let response = client
        .list_buckets(Request::new(ListBucketsRequest {
            limit: Some(1),
            offset: Some(0),
        }))
        .await
        .expect("Failed to list buckets with pagination");

    assert_eq!(response.get_ref().bucket_names.len(), 1);

    // Clean up
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

    // Create bucket
    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(response.is_ok(), "Failed to create bucket: {:?}", response.err());

    // Put object - create a simple stream
    let stream = tokio_stream::iter(vec![
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                ObjectId {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                },
            )),
        },
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(
                test_data.to_vec(),
            )),
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

    // Get object
    let response = client
        .get_object(Request::new(GetObjectRequest {
            id: Some(ObjectId {
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

    // Clean up
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

    // Create bucket
    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(response.is_ok(), "Failed to create bucket: {:?}", response.err());

    // Put multiple objects
    for object_name in objects.iter() {
        let test_data = format!("Data for {}", object_name).into_bytes();
        let stream = tokio_stream::iter(vec![
            PutObjectRequest {
                req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                    ObjectId {
                        bucket_name: bucket_name.to_string(),
                        object_key: object_name.to_string(),
                    },
                )),
            },
            PutObjectRequest {
                req: Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(
                    test_data,
                )),
            },
        ]);

        let response = client
            .put_object(Request::new(stream))
            .await
            .expect(&format!("Failed to put object {}", object_name));
        
        assert!(response.get_ref().metadata.is_some());
    }

    // List all objects
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

    // Test prefix filtering
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
    assert_eq!(metadata_list[0].id.as_ref().unwrap().object_key, "prefix_object.txt");

    // Test pagination
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

    // Clean up
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

    // Create bucket
    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(response.is_ok(), "Failed to create bucket: {:?}", response.err());

    // Put object
    let stream = tokio_stream::iter(vec![
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                ObjectId {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                },
            )),
        },
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(
                test_data.to_vec(),
            )),
        },
    ]);

    let _response = client
        .put_object(Request::new(stream))
        .await
        .expect("Failed to put object");

    // Head object
    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
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

    // Clean up
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

    // Create bucket
    let response = client
        .create_bucket(Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
    assert!(response.is_ok(), "Failed to create bucket: {:?}", response.err());

    // Put object
    let stream = tokio_stream::iter(vec![
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                ObjectId {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                },
            )),
        },
        PutObjectRequest {
            req: Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(
                test_data.to_vec(),
            )),
        },
    ]);

    let _response = client
        .put_object(Request::new(stream))
        .await
        .expect("Failed to put object");

    // Verify object exists
    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await;
    assert!(response.is_ok(), "Object should exist before deletion");

    // Delete object
    let response = client
        .delete_object(Request::new(DeleteObjectRequest {
            id: Some(ObjectId {
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await;
    assert!(response.is_ok(), "Failed to delete object: {:?}", response.err());

    // Verify object no longer exists
    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
            }),
        }))
        .await;
    assert!(response.is_err(), "Object should not exist after deletion");

    // Clean up
    let _ = client
        .delete_bucket(Request::new(DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
        }))
        .await;
}

#[tokio::test]
async fn test_error_handling_grpc() {
    let (mut client, _temp_dir) = create_test_server().await;

    // Test getting object from non-existent bucket
    let response = client
        .get_object(Request::new(GetObjectRequest {
            id: Some(ObjectId {
                bucket_name: "non-existent-bucket".to_string(),
                object_key: "some-key".to_string(),
            }),
        }))
        .await;
    
    // get_object returns a stream, so we need to consume it to see the error
    if let Ok(response) = response {
        let mut stream = response.into_inner();
        let mut has_error = false;
        while let Some(result) = stream.next().await {
            match result {
                Ok(_) => {
                    // If we get data, that's unexpected for a non-existent bucket
                    panic!("Expected error for non-existent bucket, but got data");
                }
                Err(_) => {
                    has_error = true;
                    break;
                }
            }
        }
        assert!(has_error, "Getting object from non-existent bucket should fail");
    } else {
        // If the stream creation itself failed, that's also an error case
        assert!(true, "Stream creation failed as expected");
    }

    // Test listing objects from non-existent bucket
    let response = client
        .list_objects(Request::new(ListObjectsRequest {
            bucket_name: "non-existent-bucket".to_string(),
            limit: Some(10),
            offset: Some(0),
            sorting_order: Some("ASC".to_string()),
            prefix: None,
        }))
        .await;
    assert!(response.is_err(), "Listing objects from non-existent bucket should fail");

    // Test head object from non-existent bucket
    let response = client
        .head_object(Request::new(HeadObjectRequest {
            id: Some(ObjectId {
                bucket_name: "non-existent-bucket".to_string(),
                object_key: "some-key".to_string(),
            }),
        }))
        .await;
    assert!(response.is_err(), "Head object from non-existent bucket should fail");

    // Test delete object from non-existent bucket
    let response = client
        .delete_object(Request::new(DeleteObjectRequest {
            id: Some(ObjectId {
                bucket_name: "non-existent-bucket".to_string(),
                object_key: "some-key".to_string(),
            }),
        }))
        .await;
    assert!(response.is_err(), "Delete object from non-existent bucket should fail");
}
