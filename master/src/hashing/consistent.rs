use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Represents a node in the consistent hash ring
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub id: String,
    pub address: String,
}

/// Consistent hashing implementation with virtual nodes
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    ring: BTreeMap<u64, Node>,
    virtual_nodes_per_node: usize,
}

impl ConsistentHashRing {
    /// Create a new consistent hash ring with the given nodes
    pub fn new(nodes: Vec<Node>, virtual_nodes_per_node: usize) -> Self {
        let mut ring = BTreeMap::new();

        for node in nodes {
            Self::add_node_to_ring(&mut ring, &node, virtual_nodes_per_node);
        }

        Self {
            ring,
            virtual_nodes_per_node,
        }
    }

    /// Add a node to the ring with virtual nodes
    fn add_node_to_ring(
        ring: &mut BTreeMap<u64, Node>,
        node: &Node,
        virtual_nodes_per_node: usize,
    ) {
        for i in 0..virtual_nodes_per_node {
            let virtual_node_key = format!("{}#{}", node.id, i);
            let hash = Self::hash_key(&virtual_node_key);
            ring.insert(hash, node.clone());
        }
    }

    /// Get the node responsible for the given key
    pub fn get_node(&self, key: &str) -> Option<&Node> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = Self::hash_key(key);

        // Find the first node with hash >= our key's hash
        let entry = self.ring.range(hash..).next();

        // If we didn't find any node with hash >= our key's hash,
        // wrap around to the first node in the ring
        match entry {
            Some((_, node)) => Some(node),
            None => self.ring.values().next(),
        }
    }

    /// Hash a key using the default hasher
    pub fn hash_key(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get all nodes in the ring
    pub fn get_nodes(&self) -> Vec<&Node> {
        self.ring.values().collect()
    }

    /// Get the number of virtual nodes in the ring
    pub fn virtual_node_count(&self) -> usize {
        self.ring.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_ring_creation() {
        let nodes = create_test_nodes();
        let ring = ConsistentHashRing::new(nodes, 3);

        assert_eq!(ring.virtual_node_count(), 9); // 3 nodes * 3 virtual nodes
        assert_eq!(ring.get_nodes().len(), 9);
    }

    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new(vec![], 3);
        assert_eq!(ring.get_node("test_key"), None);
    }

    #[test]
    fn test_key_distribution() {
        let nodes = create_test_nodes();
        let ring = ConsistentHashRing::new(nodes, 100);

        // Test multiple keys to ensure they get distributed
        let keys = vec![
            "bucket1/key1",
            "bucket1/key2",
            "bucket2/key1",
            "bucket2/key2",
        ];
        let mut assigned_nodes = std::collections::HashSet::new();

        for key in keys {
            if let Some(node) = ring.get_node(key) {
                assigned_nodes.insert(node.id.clone());
            }
        }

        // With 3 nodes and 100 virtual nodes, keys should be distributed
        // across multiple nodes
        assert!(assigned_nodes.len() > 1);
    }

    #[test]
    fn test_consistent_hashing() {
        let nodes = create_test_nodes();
        let ring = ConsistentHashRing::new(nodes, 10);

        let key = "test_bucket/test_key";
        let node1 = ring.get_node(key);
        let node2 = ring.get_node(key);

        // Same key should always map to same node
        assert_eq!(node1.map(|n| &n.id), node2.map(|n| &n.id));
    }

    #[test]
    fn test_hash_key_consistency() {
        let key = "test_key";
        let hash1 = ConsistentHashRing::hash_key(key);
        let hash2 = ConsistentHashRing::hash_key(key);

        assert_eq!(hash1, hash2);
    }
}
