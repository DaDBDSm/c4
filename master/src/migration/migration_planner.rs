use crate::hashing::consistent::{ConsistentHashRing, Node};
use crate::migration::dto::{MigrationOperation, MigrationPlan};
use crate::model::ObjectIdentifier;
use grpc_server::object_storage::c4_client::C4Client;
use grpc_server::object_storage::{ListBucketsRequest, ListObjectsRequest};
use std::collections::HashMap;
use std::error::Error;
use tonic::transport::Channel;

pub struct MigrationPlanner {
    client_pool: HashMap<String, C4Client<Channel>>,
    virtual_nodes_per_node: usize,
}

impl MigrationPlanner {
    pub fn new(
        client_pool: HashMap<String, C4Client<Channel>>,
        virtual_nodes_per_node: usize,
    ) -> Self {
        Self {
            client_pool,
            virtual_nodes_per_node,
        }
    }

    pub async fn generate_migration_plan(
        &self,
        previous_nodes: Vec<Node>,
        new_nodes: Vec<Node>,
    ) -> Result<MigrationPlan, Box<dyn Error>> {
        let mut plan = MigrationPlan::new();

        let previous_ring =
            ConsistentHashRing::new(previous_nodes.clone(), self.virtual_nodes_per_node);
        let new_ring = ConsistentHashRing::new(new_nodes.clone(), self.virtual_nodes_per_node);

        for node in &previous_nodes {
            let Ok(objects) = self.query_node_objects(&node.id).await else {
                log::warn!("Failed to query objects from node {}", node.id);
                continue;
            };

            for object in objects {
                let (previous_node, new_node) =
                    self.check_object_location(&object, &previous_ring, &new_ring)?;

                log::debug!(
                    "Checking object {}/{} - previous node: {}, new node: {}",
                    object.bucket_name,
                    object.object_key,
                    previous_node.id,
                    new_node.id
                );

                if previous_node.id != new_node.id {
                    log::info!(
                        "Object {}/{} needs migration: {} -> {}",
                        object.bucket_name,
                        object.object_key,
                        previous_node.id,
                        new_node.id
                    );
                    plan.add_operation(MigrationOperation {
                        prev_node: previous_node.id.clone(),
                        new_node: new_node.id.clone(),
                        object_key: object.object_key.clone(),
                        bucket_name: object.bucket_name.clone(),
                    });
                } else {
                    log::debug!(
                        "Object {}/{} remains on same node: {}",
                        object.bucket_name,
                        object.object_key,
                        previous_node.id
                    );
                }
            }
        }

        Ok(plan)
    }

    /// Queries all objects from a specific node
    async fn query_node_objects(
        &self,
        node_id: &str,
    ) -> Result<Vec<ObjectIdentifier>, Box<dyn Error>> {
        let mut all_objects = Vec::new();

        let buckets_request = tonic::Request::new(ListBucketsRequest {
            limit: None,
            offset: None,
        });

        let client = self.client_pool.get(node_id).unwrap();

        let buckets_response = client.clone().list_buckets(buckets_request).await?;
        let buckets = buckets_response.into_inner().bucket_names;

        for bucket_name in buckets {
            let objects_request = tonic::Request::new(ListObjectsRequest {
                bucket_name: bucket_name.clone(),
                limit: None,
                offset: None,
                prefix: None,
                sorting_order: None,
            });

            let Ok(response) = client.clone().list_objects(objects_request).await else {
                log::warn!("Failed to list objects from bucket {bucket_name}");
                continue;
            };

            let objects = response.into_inner().metadata;
            all_objects.extend(objects.into_iter().map(|object| ObjectIdentifier {
                bucket_name: bucket_name.clone(),
                object_key: object.id.unwrap().object_key,
            }));
        }

        Ok(all_objects)
    }

    fn check_object_location(
        &self,
        object: &ObjectIdentifier,
        previous_ring: &ConsistentHashRing,
        new_ring: &ConsistentHashRing,
    ) -> Result<(Node, Node), Box<dyn Error>> {
        let previous_node = previous_ring.get_node(&object.object_key).unwrap();
        let new_node = new_ring.get_node(&object.object_key).unwrap();
        Ok((previous_node.clone(), new_node.clone()))
    }
}
