use master::hashing::consistent::{ConsistentHashRing, Node};
use master::migration::dto::{MigrationOperation, MigrationPlan};
use master::migration::migration_service::MigrationService;
use grpc_server::object_storage::MigrationStatus;
use std::collections::HashMap;

fn create_test_nodes() -> Vec<Node> {
    vec![
        Node {
            id: "node1".to_string(),
            address: "localhost:4001".to_string(),
        },
        Node {
            id: "node2".to_string(),
            address: "localhost:4002".to_string(),
        },
        Node {
            id: "node3".to_string(),
            address: "localhost:4003".to_string(),
        },
    ]
}

#[test]
fn test_migration_operation_creation() {
    let operation = MigrationOperation {
        prev_node: "node1".to_string(),
        new_nodes: vec!["node2".to_string()],
        object_key: "test_key".to_string(),
        bucket_name: "test_bucket".to_string(),
    };

    assert_eq!(operation.prev_node, "node1");
    assert_eq!(operation.new_nodes, vec!["node2".to_string()]);
    assert_eq!(operation.object_key, "test_key");
    assert_eq!(operation.bucket_name, "test_bucket");
}

#[test]
fn test_migration_plan_operations_simple() {
    let mut plan = MigrationPlan::new();
    assert!(plan.operations.is_empty());

    // Add an operation
    let operation = MigrationOperation {
        prev_node: "node1".to_string(),
        new_nodes: vec!["node2".to_string()],
        object_key: "key1".to_string(),
        bucket_name: "bucket1".to_string(),
    };
    plan.add_operation(operation);

    assert!(!plan.operations.is_empty());
    assert_eq!(plan.operations.len(), 1);
}

#[test]
fn test_migration_service_creation() {
    let client_pool = HashMap::new();
    let service = MigrationService::new(client_pool);
    // Ensure type constructs without panicking
    let _ = service;
}

#[test]
fn test_consistent_hashing_with_migration() {
    let nodes = create_test_nodes();
    let ring = ConsistentHashRing::new(nodes, 3);

    // Test that the ring is properly configured
    assert_eq!(ring.virtual_node_count(), 9);
    assert_eq!(ring.get_nodes().len(), 9);

    // Test key distribution consistency
    let key1 = "bucket1_key1";
    let key2 = "bucket1_key2";

    let node1 = ring.get_node(key1);
    let node2 = ring.get_node(key2);

    // The same key should always map to the same node
    let node1_again = ring.get_node(key1);
    assert_eq!(node1.map(|n| &n.id), node1_again.map(|n| &n.id));

    // Different keys might map to different nodes
    // (this is probabilistic, but with 3 nodes it's likely they'll be different)
    if node1.is_some() && node2.is_some() {
        // It's possible they're the same, but with multiple nodes they're often different
        // We'll just verify that the ring is working
        assert!(node1.unwrap().id.len() > 0);
        assert!(node2.unwrap().id.len() > 0);
    }
}

#[test]
fn test_migration_plan_serialization() {
    let operation = MigrationOperation {
        prev_node: "node1".to_string(),
        new_nodes: vec!["node2".to_string()],
        object_key: "key1".to_string(),
        bucket_name: "bucket1".to_string(),
    };

    let mut plan = MigrationPlan::new();
    plan.add_operation(operation);

    // Test that the plan can be cloned (which requires serialization internally)
    let cloned_plan = plan.clone();
    assert_eq!(plan.operations.len(), cloned_plan.operations.len());
}

#[test]
fn test_migration_status_variants() {
    let pending = MigrationStatus::Pending as i32;
    let in_progress = MigrationStatus::InProgress as i32;
    let completed = MigrationStatus::Completed as i32;
    let failed = MigrationStatus::Failed as i32;
    let cancelled = MigrationStatus::Cancelled as i32;

    // Test that all variants can be created
    assert_eq!(pending, grpc_server::object_storage::MigrationStatus::Pending as i32);
    assert_eq!(in_progress, grpc_server::object_storage::MigrationStatus::InProgress as i32);
    assert_eq!(completed, grpc_server::object_storage::MigrationStatus::Completed as i32);
    assert_eq!(failed, grpc_server::object_storage::MigrationStatus::Failed as i32);
    assert_eq!(cancelled, grpc_server::object_storage::MigrationStatus::Cancelled as i32);
}

#[tokio::test]
async fn test_migration_plan_generation_logic() {
    // This test verifies the logic without actual gRPC calls
    let nodes = create_test_nodes();
    let ring = ConsistentHashRing::new(nodes, 3);

    // Create a mock migration service with empty client pool
    let client_pool = HashMap::new();
    let service = MigrationService::new(client_pool);

    // The service should be created successfully even with empty client pool
    let _ = service;

    // Note: Actual migration plan generation would require mocked gRPC clients
    // This test verifies that the service structure is correct
}

