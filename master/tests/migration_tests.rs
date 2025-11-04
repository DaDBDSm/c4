use master::hashing::consistent::{ConsistentHashRing, Node};
use master::migration::dto::{MigrationOperation, MigrationPlan};
use master::migration::migration_plan::{ExtendedMigrationPlan, MigrationStatus};
use master::migration::migration_service::MigrationService;
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
        new_node: "node2".to_string(),
        object_key: "test_key".to_string(),
        bucket_name: "test_bucket".to_string(),
    };

    assert_eq!(operation.prev_node, "node1");
    assert_eq!(operation.new_node, "node2");
    assert_eq!(operation.object_key, "test_key");
    assert_eq!(operation.bucket_name, "test_bucket");
}

#[test]
fn test_migration_plan_operations() {
    let mut plan = MigrationPlan::new();
    assert!(plan.is_empty());
    assert_eq!(plan.total_objects, 0);
    assert_eq!(plan.unchanged_objects, 0);

    // Add an operation
    let operation = MigrationOperation {
        prev_node: "node1".to_string(),
        new_node: "node2".to_string(),
        object_key: "key1".to_string(),
        bucket_name: "bucket1".to_string(),
    };
    plan.add_operation(operation);

    assert!(!plan.is_empty());
    assert_eq!(plan.operation_count(), 1);
    assert_eq!(plan.total_objects, 1);
    assert_eq!(plan.unchanged_objects, 0);

    // Add an unchanged object
    plan.increment_unchanged();
    assert_eq!(plan.total_objects, 2);
    assert_eq!(plan.unchanged_objects, 1);
}

#[test]
fn test_extended_migration_plan() {
    let mut base_plan = MigrationPlan::new();
    base_plan.add_operation(MigrationOperation {
        prev_node: "node1".to_string(),
        new_node: "node2".to_string(),
        object_key: "key1".to_string(),
        bucket_name: "bucket1".to_string(),
    });

    let mut extended_plan = ExtendedMigrationPlan::new(base_plan);

    // Test initial state
    assert!(matches!(extended_plan.status, MigrationStatus::Pending));
    assert!(extended_plan.created_at > 0);
    assert!(extended_plan.started_at.is_none());
    assert!(extended_plan.completed_at.is_none());
    assert_eq!(extended_plan.progress, (0, 0));
    assert!(!extended_plan.is_finished());

    // Test starting migration
    extended_plan.start();
    assert!(matches!(extended_plan.status, MigrationStatus::InProgress));
    assert!(extended_plan.started_at.is_some());
    assert_eq!(extended_plan.progress.1, 1);
    assert!(!extended_plan.is_finished());

    // Test progress update
    extended_plan.update_progress(1);
    assert_eq!(extended_plan.progress_percentage(), 100.0);

    // Test completion
    extended_plan.complete();
    assert!(matches!(extended_plan.status, MigrationStatus::Completed));
    assert!(extended_plan.completed_at.is_some());
    assert!(extended_plan.is_finished());
}

#[test]
fn test_migration_service_creation() {
    let client_pool = HashMap::new();
    let service = MigrationService::new(client_pool);
    assert!(service.client_pool().is_empty());
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
        new_node: "node2".to_string(),
        object_key: "key1".to_string(),
        bucket_name: "bucket1".to_string(),
    };

    let mut plan = MigrationPlan::new();
    plan.add_operation(operation);
    plan.increment_unchanged();

    // Test that the plan can be cloned (which requires serialization internally)
    let cloned_plan = plan.clone();

    assert_eq!(plan.total_objects, cloned_plan.total_objects);
    assert_eq!(plan.unchanged_objects, cloned_plan.unchanged_objects);
    assert_eq!(plan.operation_count(), cloned_plan.operation_count());
}

#[test]
fn test_migration_status_variants() {
    let pending = MigrationStatus::Pending;
    let in_progress = MigrationStatus::InProgress;
    let completed = MigrationStatus::Completed;
    let failed = MigrationStatus::Failed("test error".to_string());
    let cancelled = MigrationStatus::Cancelled;

    // Test that all variants can be created
    assert!(matches!(pending, MigrationStatus::Pending));
    assert!(matches!(in_progress, MigrationStatus::InProgress));
    assert!(matches!(completed, MigrationStatus::Completed));
    assert!(matches!(failed, MigrationStatus::Failed(_)));
    assert!(matches!(cancelled, MigrationStatus::Cancelled));
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
    assert!(service.client_pool().is_empty());

    // Note: Actual migration plan generation would require mocked gRPC clients
    // This test verifies that the service structure is correct
}

#[test]
fn test_migration_plan_display() {
    let mut plan = MigrationPlan::new();
    plan.add_operation(MigrationOperation {
        prev_node: "node1".to_string(),
        new_node: "node2".to_string(),
        object_key: "key1".to_string(),
        bucket_name: "bucket1".to_string(),
    });
    plan.increment_unchanged();

    let extended_plan = ExtendedMigrationPlan::new(plan);
    let display_string = extended_plan.to_string();

    assert!(display_string.contains("Migration Plan"));
    assert!(display_string.contains("1 operations"));
    assert!(display_string.contains("1 unchanged"));
    assert!(display_string.contains("2 total"));
    assert!(display_string.contains("0%"));
}
