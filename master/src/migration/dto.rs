/// Represents a migration operation for moving an object from one node to another
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationOperation {
    /// The node where the object is currently stored
    pub prev_node: String,
    /// The node where the object should be moved to
    pub new_node: String,
    /// The key of the object
    pub object_key: String,
    /// The name of the bucket containing the object
    pub bucket_name: String,
}

/// Represents a complete migration plan containing all operations
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationPlan {
    /// List of migration operations to be performed
    pub operations: Vec<MigrationOperation>,
    /// Total number of objects that need to be migrated
    pub total_objects: usize,
    /// Number of objects that will remain on their current nodes
    pub unchanged_objects: usize,
}

impl MigrationPlan {
    /// Creates a new empty migration plan
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
            total_objects: 0,
            unchanged_objects: 0,
        }
    }

    /// Adds a migration operation to the plan
    pub fn add_operation(&mut self, operation: MigrationOperation) {
        self.operations.push(operation);
        self.total_objects += 1;
    }

    /// Increments the count of unchanged objects
    pub fn increment_unchanged(&mut self) {
        self.unchanged_objects += 1;
        self.total_objects += 1;
    }

    /// Returns the number of operations in the plan
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }

    /// Returns true if the plan is empty (no operations needed)
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

impl Default for MigrationPlan {
    fn default() -> Self {
        Self::new()
    }
}
