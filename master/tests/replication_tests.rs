// use std::collections::HashSet;
// use std::time::Duration;
// use tempfile::TempDir;
// use tokio::time::sleep;
// use tonic::transport::{Channel, Server};
// use tonic::Request;

// use db::api::grpc::C4Handler;
// use db::storage::simple::buckets_metadata_storage::BucketsMetadataStorage;
// use db::storage::simple::chunk_file_storage::PartitionedBytesStorage;
// use db::storage::simple::ObjectStorageSimple;
// use grpc_server::object_storage::c4_client::C4Client;
// use grpc_server::object_storage::{
//     CreateBucketRequest, DeleteObjectRequest, GetObjectRequest, HeadObjectRequest,
//     ObjectId, PutObjectRequest,
// };
// use master::hashing::consistent::{ConsistentHashRing, Node};

// /// Helper to create a storage node
// async fn create_storage_node(port: u16, temp_dir: &TempDir) -> C4Client<Channel> {
//     let base_dir = temp_dir.path().to_path_buf();

//     let bytes_storage = PartitionedBytesStorage::new(base_dir.join("data"), 4);
//     let buckets_metadata_storage =
//         BucketsMetadataStorage::new(base_dir.join("metadata.json").to_string_lossy().to_string())
//             .await
//             .expect("Failed to create buckets metadata storage");

//     let storage = ObjectStorageSimple {
//         base_dir,
//         bytes_storage,
//         buckets_metadata_storage,
//     };

//     let handler = C4Handler {
//         c4_storage: storage,
//     };

//     let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
//         .await
//         .expect("Failed to bind to port");
//     let addr = listener.local_addr().expect("Failed to get local address");

//     tokio::spawn(async move {
//         let _ = Server::builder()
//             .add_service(grpc_server::object_storage::c4_server::C4Server::new(
//                 handler,
//             ))
//             .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
//             .await;
//     });

//     sleep(Duration::from_millis(500)).await;

//     C4Client::connect(format!("http://{}", addr))
//         .await
//         .expect("Failed to connect to storage node")
// }

// #[tokio::test]
// async fn test_replication_basic_write_and_read() {
//     // Create 3 storage nodes
//     let temp1 = TempDir::new().unwrap();
//     let temp2 = TempDir::new().unwrap();
//     let temp3 = TempDir::new().unwrap();

//     let mut client1 = create_storage_node(14001, &temp1).await;
//     let mut client2 = create_storage_node(14002, &temp2).await;
//     let mut client3 = create_storage_node(14003, &temp3).await;

//     // Create bucket on all nodes
//     let bucket = "test-bucket";
//     for client in [&mut client1, &mut client2, &mut client3] {
//         let _ = client
//             .create_bucket(Request::new(CreateBucketRequest {
//                 bucket_name: bucket.to_string(),
//             }))
//             .await;
//     }

//     // Put object with version on node1 and node2 (simulating replication)
//     let object_key = "test-object";
//     let data = b"Hello, replicated world!";

//     for client in [&mut client1, &mut client2] {
//         let stream = tokio_stream::iter(vec![
//             PutObjectRequest {
//                 req: Some(grpc_server::object_storage::put_object_request::Req::Id(
//                     ObjectId {
//                         bucket_name: bucket.to_string(),
//                         object_key: object_key.to_string(),
//                         version: 1,
//                     },
//                 )),
//             },
//             PutObjectRequest {
//                 req: Some(
//                     grpc_server::object_storage::put_object_request::Req::ObjectPart(
//                         data.to_vec(),
//                     ),
//                 ),
//             },
//         ]);

//         let response = client
//             .put_object(Request::new(stream))
//             .await
//             .expect("Failed to put object");

//         let metadata = response.get_ref().metadata.as_ref().unwrap();
//         assert_eq!(metadata.size, data.len() as u64);
//         assert_eq!(metadata.version, 1);
//     }

//     // Read from node1
//     let response = client1
//         .get_object(Request::new(GetObjectRequest {
//             id: Some(ObjectId {
//                 bucket_name: bucket.to_string(),
//                 object_key: object_key.to_string(),
//                 version: 0,
//             }),
//         }))
//         .await
//         .expect("Failed to get object from node1");

//     let mut received_data = Vec::new();
//     let mut stream = response.into_inner();
//     while let Some(chunk) = stream.message().await.expect("Failed to read stream") {
//         received_data.extend_from_slice(&chunk.object_part);
//     }
//     assert_eq!(received_data, data);

//     // Read from node2 (replica)
//     let response = client2
//         .get_object(Request::new(GetObjectRequest {
//             id: Some(ObjectId {
//                 bucket_name: bucket.to_string(),
//                 object_key: object_key.to_string(),
//                 version: 0,
//             }),
//         }))
//         .await
//         .expect("Failed to get object from node2");

//     let mut received_data = Vec::new();
//     let mut stream = response.into_inner();
//     while let Some(chunk) = stream.message().await.expect("Failed to read stream") {
//         received_data.extend_from_slice(&chunk.object_part);
//     }
//     assert_eq!(received_data, data);
// }

// #[tokio::test]
// async fn test_replication_failover_read() {
//     // Create 3 storage nodes
//     let temp1 = TempDir::new().unwrap();
//     let temp2 = TempDir::new().unwrap();
//     let temp3 = TempDir::new().unwrap();

//     let mut client1 = create_storage_node(14101, &temp1).await;
//     let mut client2 = create_storage_node(14102, &temp2).await;
//     let mut client3 = create_storage_node(14103, &temp3).await;

//     // Create bucket on all nodes
//     let bucket = "failover-bucket";
//     for client in [&mut client1, &mut client2, &mut client3] {
//         let _ = client
//             .create_bucket(Request::new(CreateBucketRequest {
//                 bucket_name: bucket.to_string(),
//             }))
//             .await;
//     }

//     // Put object only on node2 (simulating primary is down, but replica has data)
//     let object_key = "failover-object";
//     let data = b"Failover test data";

//     let stream = tokio_stream::iter(vec![
//         PutObjectRequest {
//             req: Some(grpc_server::object_storage::put_object_request::Req::Id(
//                 ObjectId {
//                     bucket_name: bucket.to_string(),
//                     object_key: object_key.to_string(),
//                     version: 1,
//                 },
//             )),
//         },
//         PutObjectRequest {
//             req: Some(
//                 grpc_server::object_storage::put_object_request::Req::ObjectPart(data.to_vec()),
//             ),
//         },
//     ]);

//     let _ = client2
//         .put_object(Request::new(stream))
//         .await
//         .expect("Failed to put object");

//     // Try to read from node1 (should fail or return empty)
//     let result = client1
//         .get_object(Request::new(GetObjectRequest {
//             id: Some(ObjectId {
//                 bucket_name: bucket.to_string(),
//                 object_key: object_key.to_string(),
//                 version: 0,
//             }),
//         }))
//         .await;

//     // Either it fails, or it succeeds but returns no data (object doesn't exist)
//     match result {
//         Ok(response) => {
//             let mut received_data = Vec::new();
//             let mut stream = response.into_inner();
//             while let Some(chunk_result) = stream.message().await.transpose() {
//                 match chunk_result {
//                     Ok(chunk) => received_data.extend_from_slice(&chunk.object_part),
//                     Err(_) => break, // NotFound or other error
//                 }
//             }
//             assert_eq!(received_data.len(), 0, "Node1 should not have the object data");
//         }
//         Err(e) => {
//             // It's OK if it returns an error (e.g., NotFound)
//             assert!(
//                 e.code() == tonic::Code::NotFound,
//                 "Expected NotFound error, got: {:?}",
//                 e
//             );
//         }
//     }

//     // Read from node2 (should succeed)
//     let response = client2
//         .get_object(Request::new(GetObjectRequest {
//             id: Some(ObjectId {
//                 bucket_name: bucket.to_string(),
//                 object_key: object_key.to_string(),
//                 version: 0,
//             }),
//         }))
//         .await
//         .expect("Failed to get object from node2");

//     let mut received_data = Vec::new();
//     let mut stream = response.into_inner();
//     while let Some(chunk) = stream.message().await.expect("Failed to read stream") {
//         received_data.extend_from_slice(&chunk.object_part);
//     }
//     assert_eq!(received_data, data);
// }

// #[tokio::test]
// async fn test_replication_version_increment() {
//     let temp = TempDir::new().unwrap();
//     let mut client = create_storage_node(14201, &temp).await;

//     let bucket = "version-bucket";
//     let _ = client
//         .create_bucket(Request::new(CreateBucketRequest {
//             bucket_name: bucket.to_string(),
//         }))
//         .await;

//     let object_key = "versioned-object";

//     // Write version 1
//     let stream = tokio_stream::iter(vec![
//         PutObjectRequest {
//             req: Some(grpc_server::object_storage::put_object_request::Req::Id(
//                 ObjectId {
//                     bucket_name: bucket.to_string(),
//                     object_key: object_key.to_string(),
//                     version: 1,
//                 },
//             )),
//         },
//         PutObjectRequest {
//             req: Some(
//                 grpc_server::object_storage::put_object_request::Req::ObjectPart(b"v1".to_vec()),
//             ),
//         },
//     ]);

//     let response = client
//         .put_object(Request::new(stream))
//         .await
//         .expect("Failed to put object v1");
//     assert_eq!(response.get_ref().metadata.as_ref().unwrap().version, 1);

//     // Write version 2
//     let stream = tokio_stream::iter(vec![
//         PutObjectRequest {
//             req: Some(grpc_server::object_storage::put_object_request::Req::Id(
//                 ObjectId {
//                     bucket_name: bucket.to_string(),
//                     object_key: object_key.to_string(),
//                     version: 2,
//                 },
//             )),
//         },
//         PutObjectRequest {
//             req: Some(
//                 grpc_server::object_storage::put_object_request::Req::ObjectPart(b"v2".to_vec()),
//             ),
//         },
//     ]);

//     let response = client
//         .put_object(Request::new(stream))
//         .await
//         .expect("Failed to put object v2");
//     assert_eq!(response.get_ref().metadata.as_ref().unwrap().version, 2);

//     // Check that metadata has version 2
//     let response = client
//         .head_object(Request::new(HeadObjectRequest {
//             id: Some(ObjectId {
//                 bucket_name: bucket.to_string(),
//                 object_key: object_key.to_string(),
//                 version: 0,
//             }),
//         }))
//         .await
//         .expect("Failed to head object");

//     let metadata = response.get_ref().metadata.as_ref().unwrap();
//     assert_eq!(metadata.version, 2);
// }

// #[tokio::test]
// async fn test_replication_delete_from_all_replicas() {
//     let temp1 = TempDir::new().unwrap();
//     let temp2 = TempDir::new().unwrap();

//     let mut client1 = create_storage_node(14301, &temp1).await;
//     let mut client2 = create_storage_node(14302, &temp2).await;

//     let bucket = "delete-bucket";
//     for client in [&mut client1, &mut client2] {
//         let _ = client
//             .create_bucket(Request::new(CreateBucketRequest {
//                 bucket_name: bucket.to_string(),
//             }))
//             .await;
//     }

//     let object_key = "to-delete";
//     let data = b"Will be deleted";

//     // Put on both nodes
//     for client in [&mut client1, &mut client2] {
//         let stream = tokio_stream::iter(vec![
//             PutObjectRequest {
//                 req: Some(grpc_server::object_storage::put_object_request::Req::Id(
//                     ObjectId {
//                         bucket_name: bucket.to_string(),
//                         object_key: object_key.to_string(),
//                         version: 1,
//                     },
//                 )),
//             },
//             PutObjectRequest {
//                 req: Some(
//                     grpc_server::object_storage::put_object_request::Req::ObjectPart(
//                         data.to_vec(),
//                     ),
//                 ),
//             },
//         ]);

//         let _ = client.put_object(Request::new(stream)).await;
//     }

//     // Verify object exists on both
//     for client in [&mut client1, &mut client2] {
//         let result = client
//             .head_object(Request::new(HeadObjectRequest {
//                 id: Some(ObjectId {
//                     bucket_name: bucket.to_string(),
//                     object_key: object_key.to_string(),
//                     version: 0,
//                 }),
//             }))
//             .await;
//         assert!(result.is_ok(), "Object should exist before delete");
//     }

//     // Delete from both nodes
//     for client in [&mut client1, &mut client2] {
//         let _ = client
//             .delete_object(Request::new(DeleteObjectRequest {
//                 id: Some(ObjectId {
//                     bucket_name: bucket.to_string(),
//                     object_key: object_key.to_string(),
//                     version: 0,
//                 }),
//             }))
//             .await;
//     }

//     // Verify object is deleted from both
//     for client in [&mut client1, &mut client2] {
//         let result = client
//             .head_object(Request::new(HeadObjectRequest {
//                 id: Some(ObjectId {
//                     bucket_name: bucket.to_string(),
//                     object_key: object_key.to_string(),
//                     version: 0,
//                 }),
//             }))
//             .await;
//         assert!(result.is_err(), "Object should not exist after delete");
//     }
// }

// #[test]
// fn test_consistent_hash_ring_get_n_nodes() {
//     let nodes = vec![
//         Node {
//             id: "node1".to_string(),
//             address: "localhost:4001".to_string(),
//         },
//         Node {
//             id: "node2".to_string(),
//             address: "localhost:4002".to_string(),
//         },
//         Node {
//             id: "node3".to_string(),
//             address: "localhost:4003".to_string(),
//         },
//     ];

//     let ring = ConsistentHashRing::new(nodes, 10);

//     // Test getting 2 replicas
//     let replicas = ring.get_n_nodes("test_key", 2);
//     assert_eq!(replicas.len(), 2);

//     // Should return unique physical nodes
//     assert_ne!(replicas[0].id, replicas[1].id);

//     // Same key should always return same nodes
//     let replicas2 = ring.get_n_nodes("test_key", 2);
//     assert_eq!(replicas[0].id, replicas2[0].id);
//     assert_eq!(replicas[1].id, replicas2[1].id);

//     // Test getting 3 replicas
//     let replicas3 = ring.get_n_nodes("test_key", 3);
//     assert_eq!(replicas3.len(), 3);

//     let ids: HashSet<_> = replicas3.iter().map(|n| &n.id).collect();
//     assert_eq!(ids.len(), 3, "All replicas should be unique");

//     // Test requesting more replicas than available nodes
//     let replicas_all = ring.get_n_nodes("test_key", 10);
//     assert_eq!(replicas_all.len(), 3, "Should only return 3 nodes max");
// }

// #[test]
// fn test_consistent_hash_ring_replica_distribution() {
//     let nodes = vec![
//         Node {
//             id: "node1".to_string(),
//             address: "localhost:4001".to_string(),
//         },
//         Node {
//             id: "node2".to_string(),
//             address: "localhost:4002".to_string(),
//         },
//         Node {
//             id: "node3".to_string(),
//             address: "localhost:4003".to_string(),
//         },
//     ];

//     let ring = ConsistentHashRing::new(nodes, 100);

//     // Test that different keys get distributed across nodes
//     let mut first_replica_counts: std::collections::HashMap<String, usize> =
//         std::collections::HashMap::new();

//     for i in 0..100 {
//         let key = format!("key_{}", i);
//         let replicas = ring.get_n_nodes(&key, 2);
//         if replicas.len() >= 1 {
//             *first_replica_counts.entry(replicas[0].id.clone()).or_insert(0) += 1;
//         }
//     }

//     // All nodes should be used as primary for some keys
//     assert_eq!(
//         first_replica_counts.len(),
//         3,
//         "All nodes should be used as primary"
//     );

//     // Check distribution is somewhat balanced (within 2x of average)
//     let total: usize = first_replica_counts.values().sum();
//     let avg = total / 3;

//     for (node, count) in &first_replica_counts {
//         assert!(
//             *count > avg / 2 && *count < avg * 2,
//             "Node {} has unbalanced distribution: {} (avg: {})",
//             node,
//             count,
//             avg
//         );
//     }
// }

// #[tokio::test]
// async fn test_replication_large_object() {
//     let temp1 = TempDir::new().unwrap();
//     let temp2 = TempDir::new().unwrap();

//     let mut client1 = create_storage_node(14401, &temp1).await;
//     let mut client2 = create_storage_node(14402, &temp2).await;

//     let bucket = "large-bucket";
//     for client in [&mut client1, &mut client2] {
//         let _ = client
//             .create_bucket(Request::new(CreateBucketRequest {
//                 bucket_name: bucket.to_string(),
//             }))
//             .await;
//     }

//     let object_key = "large-object";
//     let large_data = vec![42u8; 1024 * 1024]; // 1MB

//     // Put on both nodes
//     for client in [&mut client1, &mut client2] {
//         let stream = tokio_stream::iter(vec![
//             PutObjectRequest {
//                 req: Some(grpc_server::object_storage::put_object_request::Req::Id(
//                     ObjectId {
//                         bucket_name: bucket.to_string(),
//                         object_key: object_key.to_string(),
//                         version: 1,
//                     },
//                 )),
//             },
//             PutObjectRequest {
//                 req: Some(
//                     grpc_server::object_storage::put_object_request::Req::ObjectPart(
//                         large_data.clone(),
//                     ),
//                 ),
//             },
//         ]);

//         let response = client
//             .put_object(Request::new(stream))
//             .await
//             .expect("Failed to put large object");

//         assert_eq!(
//             response.get_ref().metadata.as_ref().unwrap().size,
//             large_data.len() as u64
//         );
//     }

//     // Read from both and verify
//     for client in [&mut client1, &mut client2] {
//         let response = client
//             .get_object(Request::new(GetObjectRequest {
//                 id: Some(ObjectId {
//                     bucket_name: bucket.to_string(),
//                     object_key: object_key.to_string(),
//                     version: 0,
//                 }),
//             }))
//             .await
//             .expect("Failed to get large object");

//         let mut received_data = Vec::new();
//         let mut stream = response.into_inner();
//         while let Some(chunk) = stream.message().await.expect("Failed to read stream") {
//             received_data.extend_from_slice(&chunk.object_part);
//         }

//         assert_eq!(received_data.len(), large_data.len());
//         assert_eq!(received_data, large_data);
//     }
// }
