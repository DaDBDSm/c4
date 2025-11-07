use crate::hashing::consistent::{ConsistentHashRing, Node};
use crate::migration::migration_planner::MigrationPlanner;
use crate::migration::migration_service::MigrationService;
use clap::Parser;
use grpc_server::object_storage::c4_client::C4Client;
use grpc_server::object_storage::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    HeadObjectRequest, ListBucketsRequest, ListObjectsRequest,
    MigrationOperation as ProtoMigrationOperation, MigrationPlanResponse, PutObjectRequest,
};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
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
}

#[derive(Clone)]
struct MasterHandler {
    ring: ConsistentHashRing,
    client_pool: HashMap<String, C4Client<Channel>>,
    active_nodes: Vec<Node>,
}

impl MasterHandler {
    async fn new(nodes: Vec<Node>, virtual_nodes_per_node: usize) -> Result<Self, Box<dyn Error>> {
        log::info!("Configuring master with {} nodes", nodes.len());
        log::info!("Virtual nodes per node: {}", virtual_nodes_per_node);

        if nodes.is_empty() {
            return Err("No storage nodes configured".into());
        }

        // Clone nodes for use in multiple places

        let ring = ConsistentHashRing::new(nodes.clone(), virtual_nodes_per_node);

        let mut client_pool = HashMap::new();
        for node in &nodes {
            let address = format!("http://{}", node.address);
            let client = C4Client::connect(address.clone()).await?;
            client_pool.insert(node.id.clone(), client);
            log::info!("Connected to storage node: {}", node.address);
        }

        Ok(Self {
            ring,
            client_pool,
            active_nodes: nodes,
        })
    }

    fn get_key_for_object(bucket_name: &str, object_key: &str) -> String {
        format!("{}_{}", bucket_name, object_key)
    }

    async fn get_client_for_key(&self, key: &str) -> Result<&C4Client<Channel>, Status> {
        let node = self
            .ring
            .get_node(key)
            .ok_or_else(|| Status::internal("No storage nodes available"))?;

        self.client_pool
            .get(&node.id)
            .ok_or_else(|| Status::internal(format!("Client not found for node: {}", node.id)))
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

        for client in self.client_pool.values() {
            let req = Request::new(CreateBucketRequest {
                bucket_name: bucket_name.clone(),
            });
            match client.clone().create_bucket(req).await {
                Ok(_) => {}
                Err(e) => {
                    errors.push(e.to_string());
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

        for client in self.client_pool.values() {
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

        for client in self.client_pool.values() {
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

        let key = Self::get_key_for_object(&bucket_name, &object_key);
        let client = self.get_client_for_key(&key).await?;

        let request_stream = async_stream::stream! {
            yield first_msg;
            while let Some(msg) = stream.message().await.transpose() {
                if let Ok(msg) = msg {
                    yield msg;
                }
            }
        };

        let request = Request::new(Box::pin(request_stream));

        match client.clone().put_object(request).await {
            Ok(response) => {
                log::info!("Successfully put object: {}/{}", bucket_name, object_key);
                Ok(response)
            }
            Err(e) => {
                log::error!("Failed to put object {}/{}: {}", bucket_name, object_key, e);
                Err(e)
            }
        }
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

        let key = Self::get_key_for_object(&bucket_name, &object_key);
        let client = self.get_client_for_key(&key).await?;

        let request = Request::new(GetObjectRequest {
            id: Some(object_id),
        });

        match client.clone().get_object(request).await {
            Ok(response) => {
                log::info!(
                    "Successfully retrieved object: {}/{}",
                    bucket_name,
                    object_key
                );
                Ok(response)
            }
            Err(e) => {
                log::error!("Failed to get object {}/{}: {}", bucket_name, object_key, e);
                Err(e)
            }
        }
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

        for client in self.client_pool.values() {
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

        log::info!("Successfully listed {} objects", all_objects.len());
        Ok(Response::new(
            grpc_server::object_storage::ListObjectsResponse {
                metadata: all_objects,
            },
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

        let key = Self::get_key_for_object(&bucket_name, &object_key);
        let client = self.get_client_for_key(&key).await?;

        let request = Request::new(HeadObjectRequest {
            id: Some(object_id),
        });

        match client.clone().head_object(request).await {
            Ok(response) => {
                log::info!(
                    "Successfully retrieved head object: {}/{}",
                    bucket_name,
                    object_key
                );
                Ok(response)
            }
            Err(e) => {
                log::error!(
                    "Failed to head object {}/{}: {}",
                    bucket_name,
                    object_key,
                    e
                );
                Err(e)
            }
        }
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

        let key = Self::get_key_for_object(&bucket_name, &object_key);
        let client = self.get_client_for_key(&key).await?;

        let request = Request::new(DeleteObjectRequest {
            id: Some(object_id),
        });

        match client.clone().delete_object(request).await {
            Ok(response) => {
                log::info!(
                    "Successfully deleted object: {}/{}",
                    bucket_name,
                    object_key
                );
                Ok(response)
            }
            Err(e) => {
                log::error!(
                    "Failed to delete object {}/{}: {}",
                    bucket_name,
                    object_key,
                    e
                );
                Err(e)
            }
        }
    }

    type GetObjectStream = tonic::codec::Streaming<grpc_server::object_storage::GetObjectResponse>;

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
        let current_nodes: Vec<Node> = self.active_nodes.clone();

        // Create new node list with the added node
        let mut new_nodes = current_nodes.clone();
        if !new_nodes.iter().any(|n| n.id == node_to_add.id) {
            new_nodes.push(node_to_add.clone());
        }

        // Generate migration plan
        let planner = MigrationPlanner::new(self.client_pool.clone(), 100); // Using default virtual nodes
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
                prev_node: op.prev_node,
                new_node: op.new_node,
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
            unchanged_objects: 0,           // This would require more complex tracking
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

        log::info!("Adding node: {} at {}", node_to_add.id, node_to_add.address);

        // Get current nodes from active_nodes (physical nodes only, no virtual nodes)
        let current_nodes: Vec<Node> = self.active_nodes.clone();

        // Create new node list with the added node
        let mut new_nodes = current_nodes.clone();
        if !new_nodes.iter().any(|n| n.id == node_to_add.id) {
            new_nodes.push(node_to_add.clone());
        }

        // Generate migration plan
        let planner = MigrationPlanner::new(self.client_pool.clone(), 100);
        let migration_plan = planner
            .generate_migration_plan(current_nodes, new_nodes)
            .await
            .map_err(|e| {
                log::error!("Failed to generate migration plan for node addition: {}", e);
                Status::internal(format!("Failed to generate migration plan: {}", e))
            })?;

        // Execute migration
        let migration_service = MigrationService::new(self.client_pool.clone());
        migration_service
            .migrate_data(migration_plan)
            .await
            .map_err(|e| {
                log::error!("Failed to execute migration: {}", e);
                Status::internal(format!("Failed to execute migration: {}", e))
            })?;

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
        let current_nodes: Vec<Node> = self.active_nodes.clone();

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
        let planner = MigrationPlanner::new(self.client_pool.clone(), 100); // Using default virtual nodes
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
                prev_node: op.prev_node,
                new_node: op.new_node,
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
            unchanged_objects: 0,           // This would require more complex tracking
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
        let current_nodes: Vec<Node> = self.active_nodes.clone();

        // Create new node list without the removed node
        let new_nodes: Vec<Node> = current_nodes
            .iter()
            .filter(|n| n.id != node_to_remove.id)
            .cloned()
            .collect();

        // Generate migration plan
        let planner = MigrationPlanner::new(self.client_pool.clone(), 100);
        let migration_plan = planner
            .generate_migration_plan(current_nodes, new_nodes)
            .await
            .map_err(|e| {
                log::error!("Failed to generate migration plan for node removal: {}", e);
                Status::internal(format!("Failed to generate migration plan: {}", e))
            })?;

        // Execute migration
        let migration_service = MigrationService::new(self.client_pool.clone());
        migration_service
            .migrate_data(migration_plan)
            .await
            .map_err(|e| {
                log::error!("Failed to execute migration: {}", e);
                Status::internal(format!("Failed to execute migration: {}", e))
            })?;

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

    let handler = MasterHandler::new(nodes, args.virtual_nodes).await?;

    Server::builder()
        .add_service(grpc_server::object_storage::c4_server::C4Server::new(
            handler,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
