#[derive(Debug, Clone, PartialEq)]
pub struct MigrationOperation {
    pub prev_node: String,
    pub new_nodes: Vec<String>,
    pub object_key: String,
    pub bucket_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MigrationPlan {
    pub operations: Vec<MigrationOperation>,
}

impl MigrationPlan {
    pub fn new() -> Self {
        return Self {
            operations: Vec::new(),
        };
    }

    pub fn add_operation(&mut self, operation: MigrationOperation) {
        self.operations.push(operation);
    }
}
