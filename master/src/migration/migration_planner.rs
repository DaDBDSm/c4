use crate::hashing::consistent::{ConsistentHashRing, Node};
use crate::hashing::get_key_for_object;
use crate::migration::dto::{MigrationOperation, MigrationPlan};
use crate::model::ObjectIdentifier;
use grpc_server::object_storage::c4_client::C4Client;
use grpc_server::object_storage::{ListBucketsRequest, ListObjectsRequest};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use tonic::transport::Channel;

pub struct MigrationPlanner {
    client_pool: HashMap<String, C4Client<Channel>>,
    virtual_nodes_per_node: usize,
    replication_factor: usize,
}

impl MigrationPlanner {
    pub fn new(
        client_pool: HashMap<String, C4Client<Channel>>,
        virtual_nodes_per_node: usize,
        replication_factor: usize,
    ) -> Self {
        Self {
            client_pool,
            virtual_nodes_per_node,
            replication_factor,
        }
    }

    pub async fn generate_migration_plan(
        &self,
        previous_nodes: Vec<Node>,
        new_nodes: Vec<Node>,
    ) -> Result<MigrationPlan, Box<dyn Error>> {
        let mut plan = MigrationPlan::new();
        let mut processed_objects = HashSet::new(); // Track objects we've already planned

        let previous_ring =
            ConsistentHashRing::new(previous_nodes.clone(), self.virtual_nodes_per_node);
        let new_ring = ConsistentHashRing::new(new_nodes.clone(), self.virtual_nodes_per_node);

        log::debug!("Previous ring nodes: {:?}", previous_nodes);
        log::debug!("New ring nodes: {:?}", new_nodes);
        log::debug!(
            "Previous ring virtual nodes per node: {}",
            self.virtual_nodes_per_node
        );
        log::debug!(
            "New ring virtual nodes per node: {}",
            self.virtual_nodes_per_node
        );

        for node in &previous_nodes {
            let Ok(objects) = self.query_node_objects(&node.id).await else {
                log::warn!("Failed to query objects from node {}", node.id);
                continue;
            };

            for object in objects {
                // Skip if we've already processed this object
                let object_key = format!("{}/{}", object.bucket_name, object.object_key);
                if processed_objects.contains(&object_key) {
                    continue;
                }
                processed_objects.insert(object_key);

                let (previous_nodes, new_nodes) =
                    self.check_object_location(&object, &previous_ring, &new_ring)?;

                log::debug!(
                    "Checking object {}/{} - previous nodes: {:?}, new nodes: {:?}",
                    object.bucket_name,
                    object.object_key,
                    previous_nodes.iter().map(|n| &n.id).collect::<Vec<_>>(),
                    new_nodes.iter().map(|n| &n.id).collect::<Vec<_>>()
                );

                // if previous nodes == new nodes, skip
                if previous_nodes == new_nodes {
                    log::debug!(
                        "No change in nodes for object {}/{}",
                        object.bucket_name,
                        object.object_key
                    );
                    continue;
                }

                let prev_node_ids = previous_nodes
                    .iter()
                    .map(|node| node.id.clone())
                    .collect::<Vec<_>>();

                let new_node_ids = new_nodes
                    .iter()
                    .map(|node| node.id.clone())
                    .collect::<Vec<_>>();

                plan.add_operation(MigrationOperation {
                    previous_nodes: prev_node_ids,
                    new_nodes: new_node_ids,
                    object_key: object.object_key.clone(),
                    bucket_name: object.bucket_name.clone(),
                });
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
    ) -> Result<(Vec<Node>, Vec<Node>), Box<dyn Error>> {
        let key = &get_key_for_object(&object.bucket_name, &object.object_key);

        let previous_node = previous_ring.get_n_nodes(key, self.replication_factor);
        let new_nodes = new_ring.get_n_nodes(key, self.replication_factor);

        log::debug!(
            "Object {}/{} - previous ring selected nodes: {:?}, new ring selected nodes: {:?}",
            object.bucket_name,
            object.object_key,
            previous_node.iter().map(|n| &n.id).collect::<Vec<_>>(),
            new_nodes.iter().map(|n| &n.id).collect::<Vec<_>>(),
        );

        Ok((
            previous_node.into_iter().cloned().collect(),
            new_nodes.into_iter().cloned().collect(),
        ))
    }
}
