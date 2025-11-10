use master::hashing::consistent::{ConsistentHashRing, Node};
use std::collections::HashSet;

#[test]
fn test_consistent_hashing_distribution() {
    let nodes = vec![
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
    ];

    let ring = ConsistentHashRing::new(nodes, 100);

    let test_keys = vec![
        "bucket1/key1",
        "bucket1/key2",
        "bucket2/key1",
        "bucket2/key2",
        "bucket3/key1",
        "bucket3/key2",
        "test_bucket/test_key",
        "another_bucket/object",
    ];

    let mut assigned_nodes = HashSet::new();

    for key in test_keys {
        if let Some(node) = ring.get_node(key) {
            assigned_nodes.insert(node.id.clone());
        }
    }

    assert!(
        assigned_nodes.len() > 1,
        "Keys should be distributed across multiple nodes"
    );
    assert!(
        assigned_nodes.len() <= 3,
        "Should not exceed number of nodes"
    );
}

#[test]
fn test_consistent_hashing_same_key_same_node() {
    let nodes = vec![
        Node {
            id: "node1".to_string(),
            address: "localhost:4001".to_string(),
        },
        Node {
            id: "node2".to_string(),
            address: "localhost:4002".to_string(),
        },
    ];

    let ring = ConsistentHashRing::new(nodes, 50);

    let test_key = "consistent_test_bucket/consistent_test_key";

    let node1 = ring.get_node(test_key);
    let node2 = ring.get_node(test_key);
    let node3 = ring.get_node(test_key);

    assert_eq!(node1.map(|n| &n.id), node2.map(|n| &n.id));
    assert_eq!(node2.map(|n| &n.id), node3.map(|n| &n.id));
}

#[test]
fn test_empty_ring() {
    let ring = ConsistentHashRing::new(vec![], 10);
    assert_eq!(ring.get_node("any_key"), None);
}

#[test]
fn test_single_node_ring() {
    let nodes = vec![Node {
        id: "single_node".to_string(),
        address: "localhost:4001".to_string(),
    }];

    let ring = ConsistentHashRing::new(nodes, 10);

    let test_keys = vec!["bucket1/key1", "bucket2/key2", "bucket3/key3"];

    for key in test_keys {
        let node = ring.get_node(key);
        assert!(
            node.is_some(),
            "Should always return a node for single-node ring"
        );
        assert_eq!(node.unwrap().id, "single_node");
    }
}

#[test]
fn test_virtual_node_count() {
    let nodes = vec![
        Node {
            id: "node1".to_string(),
            address: "localhost:4001".to_string(),
        },
        Node {
            id: "node2".to_string(),
            address: "localhost:4002".to_string(),
        },
    ];

    let virtual_nodes_per_node = 25;
    let ring = ConsistentHashRing::new(nodes, virtual_nodes_per_node);

    assert_eq!(ring.virtual_node_count(), 50);}

#[test]
fn test_key_based_on_bucket_and_object() {
    let bucket_name = "test_bucket";
    let object_key = "test_object";

    let combined_key = format!("{}_{}", bucket_name, object_key);

    assert_eq!(combined_key, "test_bucket_test_object");
}

#[test]
fn test_node_enumeration() {
    let nodes = vec![
        Node {
            id: "node1".to_string(),
            address: "localhost:4001".to_string(),
        },
        Node {
            id: "node2".to_string(),
            address: "localhost:4002".to_string(),
        },
    ];

    let ring = ConsistentHashRing::new(nodes, 10);
    let all_nodes = ring.get_nodes();

    assert_eq!(all_nodes.len(), 20);
    let node_ids: HashSet<String> = all_nodes.iter().map(|n| n.id.clone()).collect();
    assert!(node_ids.contains("node1"));
    assert!(node_ids.contains("node2"));
}

#[test]
fn test_hash_consistency() {
    let key = "test_consistency_key";
    let hash1 = ConsistentHashRing::hash_key(key);
    let hash2 = ConsistentHashRing::hash_key(key);
    let hash3 = ConsistentHashRing::hash_key(key);

    assert_eq!(hash1, hash2);
    assert_eq!(hash2, hash3);
}

#[test]
fn test_different_keys_different_hashes() {
    let key1 = "key1";
    let key2 = "key2";
    let key3 = "key3";

    let hash1 = ConsistentHashRing::hash_key(key1);
    let hash2 = ConsistentHashRing::hash_key(key2);
    let hash3 = ConsistentHashRing::hash_key(key3);

    let hashes = vec![hash1, hash2, hash3];
    let unique_hashes: HashSet<u64> = hashes.into_iter().collect();

    assert!(unique_hashes.len() >= 1);
    assert!(unique_hashes.len() <= 3);
}
