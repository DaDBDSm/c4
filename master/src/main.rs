use crate::hashing::consistent::{ConsistentHashRing, Node};
use crate::hashing::get_key_for_object;
use crate::migration::migration_planner::MigrationPlanner;
use crate::migration::migration_service::MigrationService;
use clap::Parser;
use grpc_server::object_storage::c4_client::C4Client;
use grpc_server::object_storage::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    HeadObjectRequest, ListBucketsRequest, ListObjectsRequest,
    MigrationOperation as ProtoMigrationOperation, MigrationPlanResponse, PutObjectRequest,
};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status, Streaming};

pub mod hashing;
pub mod migration;
pub mod model;

/// Master node for distributed object storage
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 5000)]
    port: u16,

    #[arg(
        short,
        long,
        default_value = "node1-localhost:4001,node2-localhost:4002,node3-localhost:4003"
    )]
    nodes: String,

    #[arg(long, default_value_t = 100)]
    virtual_nodes: usize,

    #[arg(long, default_value_t = 2)]
    replication_factor: usize,
}

#[derive(Clone)]
struct MasterHandler {
    ring: Arc<RwLock<ConsistentHashRing>>,
    client_pool: Arc<RwLock<HashMap<String, C4Client<Channel>>>>,
    active_nodes: Arc<RwLock<Vec<Node>>>,
    virtual_nodes_per_node: usize,
    replication_factor: usize,
    object_versions: Arc<RwLock<HashMap<(String, String), u64>>>,
}

#[derive(Clone, Debug)]
enum HealthEvent {
    NodeDown(String),
    NodeUp(String),
}

#[derive(Clone, Debug)]
struct NodeHealth {
    healthy: bool,
    consecutive_failures: u32,
}

impl MasterHandler {
    async fn new(
        nodes: Vec<Node>,
        virtual_nodes_per_node: usize,
        replication_factor: usize,
    ) -> Result<Self, Box<dyn Error>> {
        log::info!("Configuring master with {} nodes", nodes.len());
        log::info!("Virtual nodes per node: {}", virtual_nodes_per_node);
        log::info!("Replication factor: {}", replication_factor);

        if nodes.is_empty() {
            return Err("No storage nodes configured".into());
        }

        let ring = Arc::new(RwLock::new(ConsistentHashRing::new(
            nodes.clone(),
            virtual_nodes_per_node,
        )));

        let mut client_pool = HashMap::new();
        for node in &nodes {
            let address = format!("http://{}", node.address);
            let client = C4Client::connect(address.clone()).await?;
            client_pool.insert(node.id.clone(), client);
            log::info!("Connected to storage node: {}", node.address);
        }

        let handler = Self {
            ring,
            client_pool: Arc::new(RwLock::new(client_pool)),
            active_nodes: Arc::new(RwLock::new(nodes)),
            virtual_nodes_per_node,
            replication_factor,
            object_versions: Arc::new(RwLock::new(HashMap::new())),
        };

        Ok(handler)
    }

    async fn get_replica_clients_for_key(&self, key: &str) -> Vec<C4Client<Channel>> {
        let ring_guard = self.ring.read().unwrap();
        let replica_nodes = ring_guard.get_n_nodes(key, self.replication_factor);

        return replica_nodes
            .iter()
            .map(|n| self.client_pool.read().unwrap().get(&n.id).unwrap().clone())
            .collect::<Vec<_>>();
    }
}

#[tonic::async_trait]
impl grpc_server::object_storage::c4_server::C4 for MasterHandler {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<()>, Status> {
        let bucket_name = request.get_ref().bucket_name.clone();
        log::info!("Creating bucket: {}", bucket_name);

        let mut errors = Vec::new();

        // Clone the client pool to avoid holding the lock across await points
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };

        for client in client_pool_clone.values() {
            let req = Request::new(CreateBucketRequest {
                bucket_name: bucket_name.clone(),
            });
            match client.clone().create_bucket(req).await {
                Ok(_) => {}
                Err(e) => {
                    // Ignore "already exists" errors as they're not actual failures
                    if e.code() != tonic::Code::AlreadyExists {
                        errors.push(e.to_string());
                    }
                }
            }
        }

        if !errors.is_empty() {
            log::error!("Failed to create bucket on some nodes: {:?}", errors);
            return Err(Status::internal(format!(
                "Failed to create bucket on some nodes: {:?}",
                errors
            )));
        }

        log::info!("Successfully created bucket: {}", bucket_name);
        Ok(Response::new(()))
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<grpc_server::object_storage::ListBucketsResponse>, Status> {
        let limit = request.get_ref().limit;
        let offset = request.get_ref().offset;

        log::info!("Listing buckets (limit: {:?}, offset: {:?})", limit, offset);

        let mut all_buckets = std::collections::HashSet::new();
        let mut errors = Vec::new();

        // Clone the client pool to avoid holding the lock across await points
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };

        for client in client_pool_clone.values() {
            let req = Request::new(ListBucketsRequest { limit, offset });
            match client.clone().list_buckets(req).await {
                Ok(response) => {
                    let buckets = response.into_inner().bucket_names;
                    for bucket in buckets {
                        all_buckets.insert(bucket);
                    }
                }
                Err(e) => {
                    errors.push(e.to_string());
                }
            }
        }

        if !errors.is_empty() && all_buckets.is_empty() {
            log::error!("Failed to list buckets from all nodes: {:?}", errors);
            return Err(Status::internal(format!(
                "Failed to list buckets: {:?}",
                errors
            )));
        }

        let mut sorted_buckets: Vec<String> = all_buckets.into_iter().collect();
        sorted_buckets.sort();

        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.unwrap_or(20) as usize;

        let paginated_buckets: Vec<String> = sorted_buckets
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect();

        log::info!("Successfully listed {} buckets", paginated_buckets.len());
        Ok(Response::new(
            grpc_server::object_storage::ListBucketsResponse {
                bucket_names: paginated_buckets,
            },
        ))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<()>, Status> {
        let bucket_name = request.get_ref().bucket_name.clone();
        log::info!("Deleting bucket: {}", bucket_name);

        let mut errors = Vec::new();

        // Clone the client pool to avoid holding the lock across await points
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };

        for client in client_pool_clone.values() {
            let req = Request::new(DeleteBucketRequest {
                bucket_name: bucket_name.clone(),
            });
            match client.clone().delete_bucket(req).await {
                Ok(_) => {}
                Err(e) => {
                    errors.push(e.to_string());
                }
            }
        }

        if !errors.is_empty() {
            log::error!("Failed to delete bucket from some nodes: {:?}", errors);
            return Err(Status::internal(format!(
                "Failed to delete bucket from some nodes: {:?}",
                errors
            )));
        }

        log::info!("Successfully deleted bucket: {}", bucket_name);
        Ok(Response::new(()))
    }

    async fn put_object(
        &self,
        request: Request<Streaming<PutObjectRequest>>,
    ) -> Result<Response<grpc_server::object_storage::PutObjectResponse>, Status> {
        let mut stream = request.into_inner();

        let first_msg = stream
            .message()
            .await
            .map_err(|e| {
                log::error!("Failed to read first message from stream: {}", e);
                Status::internal(format!("stream read error: {}", e))
            })?
            .ok_or_else(|| {
                log::error!("Empty request stream received");
                Status::invalid_argument("empty request stream")
            })?;

        let object_id = match &first_msg.req {
            Some(grpc_server::object_storage::put_object_request::Req::Id(id)) => id.clone(),
            _ => {
                log::error!("First message does not contain ObjectId");
                return Err(Status::invalid_argument(
                    "first message must contain ObjectId",
                ));
            }
        };

        let bucket_name = object_id.bucket_name.clone();
        let object_key = object_id.object_key.clone();
        log::info!("Putting object: {}/{}", bucket_name, object_key);

        let key = get_key_for_object(&bucket_name, &object_key);
        let replica_clients = self.get_replica_clients_for_key(&key).await;

        // Buffer all data messages
        let mut data_messages = Vec::new();
        while let Some(msg) = stream.message().await.transpose() {
            match msg {
                Ok(msg) => {
                    if let Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(
                        _,
                    )) = &msg.req
                    {
                        data_messages.push(msg);
                    }
                }
                Err(e) => {
                    log::error!("Error reading stream: {}", e);
                    return Err(Status::internal("stream read error"));
                }
            }
        }

        // Create streams for each replica
        let mut handles = Vec::new();
        for client in replica_clients {
            let client_clone = client.clone();
            let bucket_name_clone = bucket_name.clone();
            let object_key_clone = object_key.clone();
            let data_messages_clone = data_messages.clone();

            let request_stream = async_stream::stream! {
                // First message with ObjectID including version
                yield PutObjectRequest {
                    req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                        grpc_server::object_storage::ObjectId {
                            bucket_name: bucket_name_clone.clone(),
                            object_key: object_key_clone.clone(),
                        }
                    )),
                };

                // Then all data messages
                for msg in data_messages_clone {
                    yield msg;
                }
            };

            let handle = tokio::spawn(async move {
                let request = Request::new(Box::pin(request_stream));
                client_clone.clone().put_object(request).await
            });
            handles.push(handle);
        }

        // Wait for all replicas to complete
        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    log::error!("Task panicked: {}", e);
                    return Err(Status::internal("replication task panicked"));
                }
            }
        }

        // Check if all succeeded
        let mut success_response = None;
        for result in results {
            match result {
                Ok(response) => {
                    if success_response.is_none() {
                        success_response = Some(response);
                    }
                }
                Err(e) => {
                    log::error!("Replication failed on one node: {}", e);
                    return Err(Status::internal("replication failed on some nodes"));
                }
            }
        }

        let response = success_response.ok_or_else(|| {
            log::error!("No successful responses from replicas");
            Status::internal("no successful responses")
        })?;

        log::info!("Successfully put object: {}/{}", bucket_name, object_key);
        Ok(response)
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let get_object_request = request.into_inner();
        let object_id = get_object_request.id.ok_or_else(|| {
            log::error!("Object ID is required but not provided");
            Status::invalid_argument("Object ID is required")
        })?;

        let bucket_name = object_id.bucket_name.clone();
        let object_key = object_id.object_key.clone();
        log::info!("Getting object: {}/{}", bucket_name, object_key);

        let key = get_key_for_object(&bucket_name, &object_key);
        let replica_clients = self.get_replica_clients_for_key(&key).await;

        // Try each replica in order until we get a valid response
        let mut last_error = None;

        for (i, client) in replica_clients.iter().enumerate() {
            let request = Request::new(GetObjectRequest {
                id: Some(object_id.clone()),
            });

            match client.clone().get_object(request).await {
                Ok(response) => {
                    log::info!(
                        "Attempting to retrieve object: {}/{} from replica {}",
                        bucket_name,
                        object_key,
                        i
                    );

                    // Try to peek at the stream to see if object exists, and re-insert first chunk
                    let mut inner = response.into_inner();
                    match inner.message().await {
                        Ok(Some(first_chunk)) => {
                            log::info!(
                                "Successfully retrieved object: {}/{} from replica {}",
                                bucket_name,
                                object_key,
                                i
                            );
                            let s = async_stream::stream! {
                                // yield the first chunk we peeked
                                yield Ok(first_chunk);
                                // then stream the rest
                                while let Some(next) = inner.message().await.transpose() {
                                    match next {
                                        Ok(msg) => yield Ok(msg),
                                        Err(e) => {
                                            yield Err(e);
                                            break;
                                        }
                                    }
                                }
                            };
                            return Ok(Response::new(Box::pin(s)));
                        }
                        Ok(None) => {
                            log::warn!(
                                "Object {}/{} not found on replica {} (empty stream)",
                                bucket_name,
                                object_key,
                                i
                            );
                            last_error = Some(Status::not_found(format!(
                                "object {}/{} not found",
                                bucket_name, object_key
                            )));
                        }
                        Err(e) => {
                            log::warn!(
                                "Error reading object {}/{} from replica {}: {}",
                                bucket_name,
                                object_key,
                                i,
                                e
                            );
                            last_error = Some(e);
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Failed to get object {}/{} from replica {}: {}",
                        bucket_name,
                        object_key,
                        i,
                        e
                    );
                    last_error = Some(e);
                }
            }
        }

        log::error!(
            "Failed to get object {}/{} from all replicas",
            bucket_name,
            object_key
        );
        Err(last_error.unwrap_or_else(|| {
            Status::not_found(format!(
                "object {}/{} not found on any replica",
                bucket_name, object_key
            ))
        }))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<grpc_server::object_storage::ListObjectsResponse>, Status> {
        let bucket_name = request.get_ref().bucket_name.clone();
        let limit = request.get_ref().limit;
        let offset = request.get_ref().offset;
        let prefix = request.get_ref().prefix.clone();
        let sorting_order = request.get_ref().sorting_order.clone();

        log::info!("Listing objects in bucket: {}", bucket_name);

        let mut all_objects = Vec::new();
        let mut errors = Vec::new();

        // Clone the client pool to avoid holding the lock across await points
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };

        for client in client_pool_clone.values() {
            let req = Request::new(ListObjectsRequest {
                bucket_name: bucket_name.clone(),
                limit,
                offset,
                prefix: prefix.clone(),
                sorting_order: sorting_order.clone(),
            });
            match client.clone().list_objects(req).await {
                Ok(response) => {
                    let mut objects = response.into_inner().metadata;
                    all_objects.append(&mut objects);
                }
                Err(e) => {
                    errors.push(e.to_string());
                }
            }
        }

        if !errors.is_empty() && all_objects.is_empty() {
            log::error!("Failed to list objects from all nodes: {:?}", errors);
            return Err(Status::internal(format!(
                "Failed to list objects: {:?}",
                errors
            )));
        }

        // Dedupe by object key (within the same bucket) across replicas; keep newest version
        use std::collections::HashMap;
        let mut by_key: HashMap<String, grpc_server::object_storage::ObjectMetadata> =
            HashMap::new();
        for meta in all_objects.into_iter() {
            if let Some(id) = &meta.id {
                if id.bucket_name != bucket_name {
                    continue;
                }
            } else {
                continue;
            }
            let key = meta.id.as_ref().unwrap().object_key.clone();
            by_key.entry(key).or_insert(meta);
        }

        // Optional: deterministic sort by object key
        let mut deduped: Vec<_> = by_key.into_values().collect();
        deduped.sort_by(|a, b| {
            let ka =
                a.id.as_ref()
                    .map(|i| i.object_key.clone())
                    .unwrap_or_default();
            let kb =
                b.id.as_ref()
                    .map(|i| i.object_key.clone())
                    .unwrap_or_default();
            ka.cmp(&kb)
        });

        // Apply pagination on deduped set
        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.unwrap_or(20) as usize;
        let deduped = deduped.into_iter().skip(offset).take(limit).collect();

        log::info!("Successfully listed objects (deduped)");
        Ok(Response::new(
            grpc_server::object_storage::ListObjectsResponse { metadata: deduped },
        ))
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<grpc_server::object_storage::HeadObjectResponse>, Status> {
        let head_object_request = request.into_inner();
        let object_id = head_object_request.id.ok_or_else(|| {
            log::error!("Object ID is required but not provided");
            Status::invalid_argument("Object ID is required")
        })?;

        let bucket_name = object_id.bucket_name.clone();
        let object_key = object_id.object_key.clone();
        log::info!("Head object: {}/{}", bucket_name, object_key);

        let key = get_key_for_object(&bucket_name, &object_key);
        let replica_clients = self.get_replica_clients_for_key(&key).await;

        // Try each replica in order until success
        for (i, client) in replica_clients.iter().enumerate() {
            let request = Request::new(HeadObjectRequest {
                id: Some(object_id.clone()),
            });

            match client.clone().head_object(request).await {
                Ok(response) => {
                    log::info!(
                        "Successfully retrieved head object: {}/{} from replica {}",
                        bucket_name,
                        object_key,
                        i
                    );
                    return Ok(response);
                }
                Err(e) => {
                    log::warn!(
                        "Failed to head object {}/{} from replica {}: {}",
                        bucket_name,
                        object_key,
                        i,
                        e
                    );
                    // Continue to next replica
                }
            }
        }

        log::error!(
            "Failed to head object {}/{} from all replicas",
            bucket_name,
            object_key
        );
        Err(Status::not_found(format!(
            "object {}/{} not found on any replica",
            bucket_name, object_key
        )))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<()>, Status> {
        let delete_object_request = request.into_inner();
        let object_id = delete_object_request.id.ok_or_else(|| {
            log::error!("Object ID is required but not provided");
            Status::invalid_argument("Object ID is required")
        })?;

        let bucket_name = object_id.bucket_name.clone();
        let object_key = object_id.object_key.clone();
        log::info!("Deleting object: {}/{}", bucket_name, object_key);

        let key = get_key_for_object(&bucket_name, &object_key);
        let replica_clients = self.get_replica_clients_for_key(&key).await;

        // Delete from all replicas
        let mut handles = Vec::new();
        for client in replica_clients {
            let client_clone = client.clone();
            let object_id_clone = object_id.clone();

            let handle = tokio::spawn(async move {
                let request = Request::new(DeleteObjectRequest {
                    id: Some(object_id_clone),
                });
                client_clone.clone().delete_object(request).await
            });
            handles.push(handle);
        }

        // Wait for all replicas to complete
        let mut errors = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => {
                    if let Err(e) = result {
                        errors.push(e.to_string());
                    }
                }
                Err(e) => {
                    log::error!("Delete task panicked: {}", e);
                    errors.push("task panicked".to_string());
                }
            }
        }

        if !errors.is_empty() {
            log::error!("Failed to delete object from some replicas: {:?}", errors);
            return Err(Status::internal("failed to delete from some replicas"));
        }

        log::info!(
            "Successfully deleted object: {}/{}",
            bucket_name,
            object_key
        );
        Ok(Response::new(()))
    }

    type GetObjectStream = Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<grpc_server::object_storage::GetObjectResponse, Status>,
                > + Send
                + 'static,
        >,
    >;

    async fn get_migration_plan(
        &self,
        request: Request<grpc_server::object_storage::AddNodeRequest>,
    ) -> Result<Response<MigrationPlanResponse>, Status> {
        let req = request.into_inner();
        let node_to_add = Node {
            id: req.node_id.clone(),
            address: req.node_address.clone(),
        };

        log::info!(
            "Generating migration plan for adding node: {} at {}",
            node_to_add.id,
            node_to_add.address
        );

        // Get current nodes from active_nodes (physical nodes only, no virtual nodes)
        let current_nodes = {
            let active_nodes_guard = self
                .active_nodes
                .read()
                .map_err(|_| Status::internal("Failed to read active nodes"))?;
            active_nodes_guard.clone()
        };

        // Create new node list with the added node
        let mut new_nodes = current_nodes.clone();
        if !new_nodes.iter().any(|n| n.id == node_to_add.id) {
            new_nodes.push(node_to_add.clone());
        }

        // Generate migration plan
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };
        let planner = MigrationPlanner::new(
            client_pool_clone,
            self.virtual_nodes_per_node,
            self.replication_factor,
        );
        let migration_plan = planner
            .generate_migration_plan(current_nodes, new_nodes)
            .await
            .map_err(|e| {
                log::error!("Failed to generate migration plan: {}", e);
                Status::internal(format!("Failed to generate migration plan: {}", e))
            })?;

        // Convert to protobuf response
        let proto_operations: Vec<ProtoMigrationOperation> = migration_plan
            .operations
            .into_iter()
            .map(|op| ProtoMigrationOperation {
                prev_nodes: op.previous_nodes,
                new_nodes: op.new_nodes,
                object_key: op.object_key,
                bucket_name: op.bucket_name,
            })
            .collect();

        let operation_count = proto_operations.len() as u64;

        log::info!(
            "Generated migration plan with {} operations",
            operation_count
        );

        Ok(Response::new(MigrationPlanResponse {
            operations: proto_operations,
            total_objects: operation_count, // For simplicity, assuming each operation is one object
            operation_count,
        }))
    }

    async fn add_node(
        &self,
        request: Request<grpc_server::object_storage::AddNodeRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let node_to_add = Node {
            id: req.node_id.clone(),
            address: req.node_address.clone(),
        };

        // Add new node to client pool
        let address = format!("http://{}", node_to_add.address);
        match C4Client::connect(address.clone()).await {
            Ok(client) => {
                let mut client_pool_guard = self
                    .client_pool
                    .write()
                    .map_err(|_| Status::internal("Failed to write client pool"))?;
                client_pool_guard.insert(node_to_add.id.clone(), client);
                log::info!("Connected to new storage node: {}", node_to_add.address);
            }
            Err(e) => {
                log::error!(
                    "Failed to connect to new node {}: {}",
                    node_to_add.address,
                    e
                );
                return Err(Status::internal(format!(
                    "Failed to connect to new node: {}",
                    e
                )));
            }
        }

        log::info!("Adding node: {} at {}", node_to_add.id, node_to_add.address);

        // Get current nodes from active_nodes (physical nodes only, no virtual nodes)
        let current_nodes = {
            let active_nodes_guard = self
                .active_nodes
                .read()
                .map_err(|_| Status::internal("Failed to read active nodes"))?;
            active_nodes_guard.clone()
        };

        // Create new node list with the added node
        let mut new_nodes = current_nodes.clone();
        if !new_nodes.iter().any(|n| n.id == node_to_add.id) {
            new_nodes.push(node_to_add.clone());
        }

        // Generate migration plan
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };
        let planner = MigrationPlanner::new(
            client_pool_clone.clone(),
            self.virtual_nodes_per_node,
            self.replication_factor,
        );
        let migration_plan = planner
            .generate_migration_plan(current_nodes.clone(), new_nodes.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to generate migration plan for node addition: {}", e);
                Status::internal(format!("Failed to generate migration plan: {}", e))
            })?;

        // Execute migration
        let migration_service = MigrationService::new(client_pool_clone);
        migration_service
            .migrate_data(migration_plan.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to execute migration: {}", e);
                Status::internal(format!("Failed to execute migration: {}", e))
            })?;

        // Update master ring with new nodes
        {
            let mut ring_guard = self
                .ring
                .write()
                .map_err(|_| Status::internal("Failed to write ring"))?;
            *ring_guard = ConsistentHashRing::new(new_nodes.clone(), self.virtual_nodes_per_node);
        }

        // Update active nodes
        {
            let mut active_nodes_guard = self
                .active_nodes
                .write()
                .map_err(|_| Status::internal("Failed to write active nodes"))?;
            *active_nodes_guard = new_nodes;
        }

        log::info!("Successfully added node: {}", node_to_add.id);

        Ok(Response::new(()))
    }

    async fn get_migration_plan_by_removing_node(
        &self,
        request: Request<grpc_server::object_storage::RemoveNodeRequest>,
    ) -> Result<Response<MigrationPlanResponse>, Status> {
        let req = request.into_inner();
        let node_to_remove = Node {
            id: req.node_id.clone(),
            address: "".to_string(), // Address not needed for removal planning
        };

        log::info!(
            "Generating migration plan for removing node: {}",
            node_to_remove.id
        );

        // Get current nodes from active_nodes (physical nodes only, no virtual nodes)
        let current_nodes = {
            let active_nodes_guard = self
                .active_nodes
                .read()
                .map_err(|_| Status::internal("Failed to read active nodes"))?;
            active_nodes_guard.clone()
        };

        // Create new node list without the removed node
        let new_nodes: Vec<Node> = current_nodes
            .iter()
            .filter(|n| n.id != node_to_remove.id)
            .cloned()
            .collect();

        log::info!(
            "Current nodes: {:?}, new_nodes: {:?}",
            current_nodes,
            new_nodes
        );

        // Generate migration plan
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };
        let planner = MigrationPlanner::new(
            client_pool_clone,
            self.virtual_nodes_per_node,
            self.replication_factor,
        );
        let migration_plan = planner
            .generate_migration_plan(current_nodes, new_nodes)
            .await
            .map_err(|e| {
                log::error!("Failed to generate migration plan for node removal: {}", e);
                Status::internal(format!("Failed to generate migration plan: {}", e))
            })?;

        // Convert to protobuf response
        let proto_operations: Vec<ProtoMigrationOperation> = migration_plan
            .operations
            .into_iter()
            .map(|op| ProtoMigrationOperation {
                prev_nodes: op.previous_nodes,
                new_nodes: op.new_nodes,
                object_key: op.object_key,
                bucket_name: op.bucket_name,
            })
            .collect();

        let operation_count = proto_operations.len() as u64;

        log::info!(
            "Generated migration plan with {} operations for removing node {}",
            operation_count,
            node_to_remove.id
        );

        Ok(Response::new(MigrationPlanResponse {
            operations: proto_operations,
            total_objects: operation_count, // For simplicity, assuming each operation is one object
            operation_count,
        }))
    }

    async fn remove_node(
        &self,
        request: Request<grpc_server::object_storage::RemoveNodeRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let node_to_remove = Node {
            id: req.node_id.clone(),
            address: "".to_string(), // Address not needed for removal
        };

        log::info!("Removing node: {}", node_to_remove.id);

        // Get current nodes from active_nodes (physical nodes only, no virtual nodes)
        let current_nodes = {
            let active_nodes_guard = self
                .active_nodes
                .read()
                .map_err(|_| Status::internal("Failed to read active nodes"))?;
            active_nodes_guard.clone()
        };

        // Create new node list without the removed node
        let new_nodes: Vec<Node> = current_nodes
            .iter()
            .filter(|n| n.id != node_to_remove.id)
            .cloned()
            .collect();

        // Generate migration plan
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };
        let planner = MigrationPlanner::new(
            client_pool_clone.clone(),
            self.virtual_nodes_per_node,
            self.replication_factor,
        );
        let migration_plan = planner
            .generate_migration_plan(current_nodes.clone(), new_nodes.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to generate migration plan for node removal: {}", e);
                Status::internal(format!("Failed to generate migration plan: {}", e))
            })?;

        // Execute migration
        let migration_service = MigrationService::new(client_pool_clone);
        migration_service
            .migrate_data(migration_plan.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to execute migration: {}", e);
                Status::internal(format!("Failed to execute migration: {}", e))
            })?;

        // Update master ring with new nodes
        {
            let mut ring_guard = self
                .ring
                .write()
                .map_err(|_| Status::internal("Failed to write ring"))?;
            *ring_guard = ConsistentHashRing::new(new_nodes.clone(), self.virtual_nodes_per_node);
        }

        // Update active nodes
        {
            let mut active_nodes_guard = self
                .active_nodes
                .write()
                .map_err(|_| Status::internal("Failed to write active nodes"))?;
            *active_nodes_guard = new_nodes;
        }

        // Remove node from client pool
        {
            let mut client_pool_guard = self
                .client_pool
                .write()
                .map_err(|_| Status::internal("Failed to write client pool"))?;
            client_pool_guard.remove(&node_to_remove.id);
        }

        log::info!("Successfully removed node: {}", node_to_remove.id);

        Ok(Response::new(()))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let args = Args::parse();

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;

    log::info!("Starting C4 master node on {}", addr);

    let nodes: Vec<Node> = args
        .nodes
        .split(',')
        .map(|node_str| {
            let parts: Vec<&str> = node_str.split('-').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "Invalid node format: '{}'. Expected format: <node_id>-<node_address>",
                    node_str
                )
                .into());
            }
            Ok(Node {
                id: parts[0].trim().to_string(),
                address: parts[1].trim().to_string(),
            })
        })
        .collect::<Result<Vec<Node>, Box<dyn Error>>>()?;

    let handler = MasterHandler::new(nodes, args.virtual_nodes, args.replication_factor).await?;

    Server::builder()
        .add_service(grpc_server::object_storage::c4_server::C4Server::new(
            handler,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
