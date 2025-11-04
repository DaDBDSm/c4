use crate::hashing::consistent::{ConsistentHashRing, Node};
use crate::migration::dto::{MigrationOperation, MigrationPlan};
use crate::migration::migration_plan::ExtendedMigrationPlan;
use grpc_server::object_storage::c4_client::C4Client;
use grpc_server::object_storage::{
    GetObjectRequest, ListBucketsRequest, ListObjectsRequest, ObjectMetadata, PutObjectRequest,
};
use std::collections::HashMap;
use std::error::Error;
use tokio::sync::mpsc;
use tonic::transport::Channel;

/// Service responsible for generating migration plans based on current cluster state
pub struct MigrationService {
    /// Client pool for connecting to storage nodes
    client_pool: HashMap<String, C4Client<Channel>>,
}

impl MigrationService {
    /// Creates a new MigrationService with the given client pool
    pub fn new(client_pool: HashMap<String, C4Client<Channel>>) -> Self {
        Self { client_pool }
    }

    /// Generates a migration plan by querying all nodes and recomputing object locations
    pub async fn generate_migration_plan(
        &self,
        ring: &ConsistentHashRing,
    ) -> Result<MigrationPlan, Box<dyn Error>> {
        let mut plan = MigrationPlan::new();

        // Get all nodes from the ring
        let nodes: Vec<&Node> = ring.get_nodes();
        let node_ids: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();

        // Query all bucket-objects from each node
        for node_id in &node_ids {
            if let Some(client) = self.client_pool.get(node_id) {
                match self.query_node_objects(client).await {
                    Ok(objects) => {
                        for object in objects {
                            self.process_object(&mut plan, ring, node_id, object);
                        }
                    }
                    Err(e) => {
                        log::warn!("Failed to query objects from node {}: {}", node_id, e);
                    }
                }
            } else {
                log::warn!("No client found for node: {}", node_id);
            }
        }

        log::info!(
            "Generated migration plan: {} operations, {} unchanged objects, {} total objects",
            plan.operation_count(),
            plan.unchanged_objects,
            plan.total_objects
        );

        Ok(plan)
    }

    /// Queries all objects from a specific node
    async fn query_node_objects(
        &self,
        client: &C4Client<Channel>,
    ) -> Result<Vec<ObjectMetadata>, Box<dyn Error>> {
        let mut all_objects = Vec::new();

        // First, get all buckets from the node
        let buckets_request = tonic::Request::new(ListBucketsRequest {
            limit: None,
            offset: None,
        });

        let buckets_response = client.clone().list_buckets(buckets_request).await?;
        let buckets = buckets_response.into_inner().bucket_names;

        // Then, get all objects from each bucket
        for bucket_name in buckets {
            let objects_request = tonic::Request::new(ListObjectsRequest {
                bucket_name: bucket_name.clone(),
                limit: None,
                offset: None,
                prefix: None,
                sorting_order: None,
            });

            match client.clone().list_objects(objects_request).await {
                Ok(response) => {
                    let mut objects = response.into_inner().metadata;
                    all_objects.append(&mut objects);
                }
                Err(e) => {
                    log::warn!("Failed to list objects from bucket {}: {}", bucket_name, e);
                }
            }
        }

        Ok(all_objects)
    }

    /// Processes an individual object to determine if migration is needed
    fn process_object(
        &self,
        plan: &mut MigrationPlan,
        ring: &ConsistentHashRing,
        current_node_id: &str,
        object: ObjectMetadata,
    ) {
        let object_id = object.id.expect("Object ID should be present");
        let bucket_name = object_id.bucket_name;
        let object_key = object_id.object_key;

        // Generate the key used for consistent hashing
        let key = format!("{}_{}", bucket_name, object_key);

        // Determine where the object should be based on current ring
        if let Some(target_node) = ring.get_node(&key) {
            if target_node.id != current_node_id {
                // Object needs to be migrated
                let operation = MigrationOperation {
                    prev_node: current_node_id.to_string(),
                    new_node: target_node.id.clone(),
                    object_key: object_key.clone(),
                    bucket_name: bucket_name.clone(),
                };
                plan.add_operation(operation);
            } else {
                // Object is already on the correct node
                plan.increment_unchanged();
            }
        } else {
            log::warn!(
                "Could not determine target node for object {}/{}",
                bucket_name,
                object_key
            );
        }
    }

    /// Executes a migration plan by moving objects between nodes using streaming
    pub async fn execute_migration(
        &self,
        plan: MigrationPlan,
    ) -> Result<ExtendedMigrationPlan, Box<dyn Error>> {
        let mut extended_plan = ExtendedMigrationPlan::new(plan);
        extended_plan.start();

        log::info!(
            "Starting migration execution: {} operations",
            extended_plan.plan.operation_count()
        );

        let total_operations = extended_plan.plan.operation_count();
        let mut completed_operations = 0;

        // Execute each migration operation
        for operation in extended_plan.plan.operations.clone() {
            match self.execute_migration_operation(&operation).await {
                Ok(_) => {
                    completed_operations += 1;
                    extended_plan.update_progress(completed_operations);
                    log::info!(
                        "Successfully migrated object {}/{} from {} to {} ({}/{})",
                        operation.bucket_name,
                        operation.object_key,
                        operation.prev_node,
                        operation.new_node,
                        completed_operations,
                        total_operations
                    );
                }
                Err(e) => {
                    log::error!(
                        "Failed to migrate object {}/{} from {} to {}: {}",
                        operation.bucket_name,
                        operation.object_key,
                        operation.prev_node,
                        operation.new_node,
                        e
                    );
                    // Continue with other operations even if one fails
                }
            }
        }

        if completed_operations == total_operations {
            extended_plan.complete();
            log::info!(
                "Migration completed successfully: {} operations",
                completed_operations
            );
        } else {
            extended_plan.fail(format!(
                "Migration partially completed: {}/{} operations succeeded",
                completed_operations, total_operations
            ));
            log::warn!(
                "Migration partially completed: {}/{} operations succeeded",
                completed_operations,
                total_operations
            );
        }

        Ok(extended_plan)
    }

    /// Executes a single migration operation by streaming object data between nodes
    async fn execute_migration_operation(
        &self,
        operation: &MigrationOperation,
    ) -> Result<(), Box<dyn Error>> {
        // Get clients for source and destination nodes
        let source_client = self
            .client_pool
            .get(&operation.prev_node)
            .ok_or_else(|| format!("Source node not found: {}", operation.prev_node))?;

        let destination_client = self
            .client_pool
            .get(&operation.new_node)
            .ok_or_else(|| format!("Destination node not found: {}", operation.new_node))?;

        // Create object ID for the object to migrate
        let object_id = grpc_server::object_storage::ObjectId {
            bucket_name: operation.bucket_name.clone(),
            object_key: operation.object_key.clone(),
        };

        // Step 1: Get object from source node using streaming
        let get_request = tonic::Request::new(GetObjectRequest {
            id: Some(object_id.clone()),
        });

        let mut get_response = source_client
            .clone()
            .get_object(get_request)
            .await?
            .into_inner();

        // Step 2: Create a channel to stream data from source to destination
        let (tx, rx) = mpsc::channel(10);

        // Spawn a task to read from the source stream and write to the channel
        let object_id_clone = object_id.clone();
        tokio::spawn(async move {
            let mut first_message_sent = false;

            while let Some(chunk) = get_response.message().await.transpose() {
                match chunk {
                    Ok(response) => {
                        if !first_message_sent {
                            // First message: send object ID
                            let put_request = PutObjectRequest {
                                req: Some(
                                    grpc_server::object_storage::put_object_request::Req::Id(
                                        object_id_clone.clone(),
                                    ),
                                ),
                            };
                            if tx.send(put_request).await.is_err() {
                                break;
                            }
                            first_message_sent = true;
                        }

                        // Send object data chunks
                        let data = response.object_part;
                        let put_request = PutObjectRequest {
                            req: Some(
                                grpc_server::object_storage::put_object_request::Req::ObjectPart(
                                    data,
                                ),
                            ),
                        };
                        if tx.send(put_request).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!("Error reading from source stream: {}", e);
                        break;
                    }
                }
            }
        });

        // Step 3: Put object to destination node using streaming
        let put_request_stream = async_stream::stream! {
            let mut rx = rx;
            while let Some(msg) = rx.recv().await {
                yield msg;
            }
        };

        let put_request = tonic::Request::new(Box::pin(put_request_stream));

        // Execute the put operation
        destination_client.clone().put_object(put_request).await?;

        Ok(())
    }

    /// Returns a reference to the client pool
    pub fn client_pool(&self) -> &HashMap<String, C4Client<Channel>> {
        &self.client_pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_migration_plan_creation() {
        let plan = MigrationPlan::new();
        assert!(plan.is_empty());
        assert_eq!(plan.total_objects, 0);
        assert_eq!(plan.unchanged_objects, 0);
    }

    #[test]
    fn test_migration_operation_creation() {
        let operation = MigrationOperation {
            prev_node: "node1".to_string(),
            new_node: "node2".to_string(),
            object_key: "key1".to_string(),
            bucket_name: "bucket1".to_string(),
        };

        assert_eq!(operation.prev_node, "node1");
        assert_eq!(operation.new_node, "node2");
        assert_eq!(operation.object_key, "key1");
        assert_eq!(operation.bucket_name, "bucket1");
    }

    #[tokio::test]
    async fn test_migration_service_creation() {
        let client_pool = HashMap::new();
        let service = MigrationService::new(client_pool);
        assert!(service.client_pool().is_empty());
    }
}
