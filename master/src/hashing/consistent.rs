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
}

impl ConsistentHashRing {
    pub fn new(nodes: Vec<Node>, virtual_nodes_per_node: usize) -> Self {
        let mut ring = BTreeMap::new();

        for node in nodes {
            Self::add_node_to_ring(&mut ring, &node, virtual_nodes_per_node);
        }

        Self { ring }
    }

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

    pub fn hash_key(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn get_nodes(&self) -> Vec<&Node> {
        self.ring.values().collect()
    }

    pub fn virtual_node_count(&self) -> usize {
        self.ring.len()
    }

    /// Add a node to the ring with virtual nodes
    pub fn add_node(&mut self, node: Node, virtual_nodes_per_node: usize) {
        Self::add_node_to_ring(&mut self.ring, &node, virtual_nodes_per_node);
    }

    pub fn get_n_nodes(&self, key: &str, n: usize) -> Vec<&Node> {
        if self.ring.is_empty() || n == 0 {
            return vec![];
        }
        let hash = Self::hash_key(key);
        let mut result = Vec::new();

        // Iterate starting from hash.. and then wrap-around
        for (_, node) in self.ring.range(hash..) {
            if !result.iter().any(|existing: &&Node| existing.id == node.id) {
                result.push(node);
                if result.len() == n {
                    return result;
                }
            }
        }
        for (_, node) in &self.ring {
            if result.len() == n {
                break;
            }
            if !result.iter().any(|existing: &&Node| existing.id == node.id) {
                result.push(node);
            }
        }
        result
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

        assert_eq!(ring.virtual_node_count(), 9);
        assert_eq!(ring.get_nodes().len(), 9);
    }

    // #[test]
    // fn test_empty_ring() {
    //     let ring = ConsistentHashRing::new(vec![], 3);
    //     assert_eq!(ring.get_node("test_key"), None);
    // }

    // #[test]
    // fn test_key_distribution() {
    //     let nodes = create_test_nodes();
    //     let ring = ConsistentHashRing::new(nodes, 100);

    //     let keys = vec![
    //         "bucket1/key1",
    //         "bucket1/key2",
    //         "bucket2/key1",
    //         "bucket2/key2",
    //     ];
    //     let mut assigned_nodes = std::collections::HashSet::new();

    //     for key in keys {
    //         if let Some(node) = ring.get_node(key) {
    //             assigned_nodes.insert(node.id.clone());
    //         }
    //     }

    //     assert!(assigned_nodes.len() > 1);
    // }

    // #[test]
    // fn test_consistent_hashing() {
    //     let nodes = create_test_nodes();
    //     let ring = ConsistentHashRing::new(nodes, 10);

    //     let key = "test_bucket/test_key";
    //     let node1 = ring.get_node(key);
    //     let node2 = ring.get_node(key);

    //     assert_eq!(node1.map(|n| &n.id), node2.map(|n| &n.id));
    // }

    #[test]
    fn test_get_n_nodes() {
        let nodes = create_test_nodes();
        let ring = ConsistentHashRing::new(nodes, 10);

        let replicas = ring.get_n_nodes("test_key", 2);
        assert_eq!(replicas.len(), 2);
        assert_ne!(replicas[0].id, replicas[1].id);

        // Same key should return same nodes
        let replicas2 = ring.get_n_nodes("test_key", 2);
        assert_eq!(replicas[0].id, replicas2[0].id);
        assert_eq!(replicas[1].id, replicas2[1].id);

        // Request more than available nodes
        let all_replicas = ring.get_n_nodes("test_key", 5);
        assert_eq!(all_replicas.len(), 3); // Only 3 nodes available
    }
}
