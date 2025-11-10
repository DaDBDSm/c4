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
use std::sync::{Arc, RwLock};
use tonic::transport::{Channel, Server};
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio::sync::mpsc;

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
    // Healthcheck state
    health_status: Arc<RwLock<HashMap<String, NodeHealth>>>,
    health_tasks: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
    health_interval_secs: u64,
    health_fail_threshold: u32,
    health_rpc_timeout_ms: u64,
    // Channel to notify about node health events
    health_event_tx: mpsc::UnboundedSender<HealthEvent>,
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
    async fn new(nodes: Vec<Node>, virtual_nodes_per_node: usize, replication_factor: usize) -> Result<Self, Box<dyn Error>> {
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

        // Create health event channel
        let (health_event_tx, mut health_event_rx) = mpsc::unbounded_channel();

        let handler = Self {
            ring,
            client_pool: Arc::new(RwLock::new(client_pool)),
            active_nodes: Arc::new(RwLock::new(nodes)),
            virtual_nodes_per_node,
            replication_factor,
            object_versions: Arc::new(RwLock::new(HashMap::new())),
            health_status: Arc::new(RwLock::new(HashMap::new())),
            health_tasks: Arc::new(RwLock::new(HashMap::new())),
            health_interval_secs: 2,
            health_fail_threshold: 3,
            health_rpc_timeout_ms: 700,
            health_event_tx,
        };

        // Clone handler for health event processor
        let handler_clone = handler.clone();
        
        // Spawn background task to handle health events
        tokio::spawn(async move {
            while let Some(event) = health_event_rx.recv().await {
                match event {
                    HealthEvent::NodeDown(node_id) => {
                        log::error!("🔴 Processing NodeDown event for: {}", node_id);
                        
                        // IMPORTANT: When node goes DOWN, we do NOT reshard immediately!
                        // Resharding changes the consistent hash ring, which would break
                        // reads for objects that are still on healthy nodes but were mapped
                        // to the old topology.
                        //
                        // Instead, we:
                        // 1. Keep the dead node in the ring (marked as unhealthy)
                        // 2. Scan and repair to ensure all objects have replication_factor replicas
                        // 3. The dead node will be deprioritized in replica selection (already implemented)
                        //
                        // Auto-resharding should ONLY happen when:
                        // - Operator explicitly removes a node (RemoveNode RPC)
                        // - Or implement delayed resharding after node is down for X minutes
                        
                        log::info!("📋 Starting auto-repair for under-replicated objects...");
                        if let Err(e) = handler_clone.scan_and_repair_all_objects().await {
                            log::error!("Failed to scan and repair objects after node down: {}", e);
                        } else {
                            log::info!("✅ Auto-repair completed successfully");
                        }
                        
                        log::info!("ℹ️  Note: Ring topology unchanged - use RemoveNode RPC for explicit resharding");
                    }
                    HealthEvent::NodeUp(node_id) => {
                        log::info!("🟢 Node {} came back UP", node_id);
                        
                        // When a node comes back online, we need to sync it with current ring state
                        log::info!("� Starting data synchronization for recovered node: {}", node_id);
                        if let Err(e) = handler_clone.sync_recovered_node(&node_id).await {
                            log::error!("Failed to sync recovered node {}: {}", node_id, e);
                        } else {
                            log::info!("✅ Node {} synchronized successfully", node_id);
                        }
                    }
                }
            }
        });

        // Spawn healthchecks for initial nodes
        {
            let active_nodes = handler
                .active_nodes
                .read()
                .map_err(|_| "Failed to read active nodes")?
                .clone();
            for node in active_nodes {
                handler.spawn_healthcheck_for_node(node.id.clone()).await;
            }
        }

        Ok(handler)
    }

    async fn spawn_healthcheck_for_node(&self, node_id: String) {
        // Avoid duplicating tasks
        if let Ok(guard) = self.health_tasks.read() {
            if guard.contains_key(&node_id) {
                return;
            }
        }

        // Initialize status as healthy unknown (optimistic)
        if let Ok(mut status_guard) = self.health_status.write() {
            status_guard.entry(node_id.clone()).or_insert(NodeHealth { healthy: true, consecutive_failures: 0 });
        }

        let client_pool = self.client_pool.clone();
        let health_status = self.health_status.clone();
        let interval = self.health_interval_secs;
        let threshold = self.health_fail_threshold;
        let timeout_ms = self.health_rpc_timeout_ms;
        let node_id_clone = node_id.clone();
        let event_tx = self.health_event_tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Obtain client for node
                let client_opt = {
                    // Acquire without awaiting in this scope to keep guard local
                    match client_pool.read() {
                        Ok(g) => g.get(&node_id_clone).cloned(),
                        Err(_) => None,
                    }
                };

                if client_opt.is_none() {
                    // No client yet, back off a bit
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }

                let ok = if let Some(client) = client_opt {
                    // Use list_buckets as a lightweight health probe
                    let req = Request::new(ListBucketsRequest { limit: None, offset: None });
                    let mut c = client.clone();
                    let fut = c.list_buckets(req);
                    match tokio::time::timeout(Duration::from_millis(timeout_ms), fut).await {
                        Ok(Ok(_)) => true,
                        _ => false,
                    }
                } else {
                    false
                };

                // Update status with transitions
                let became_unhealthy = if let Ok(mut status_guard) = health_status.write() {
                    let entry = status_guard.entry(node_id_clone.clone()).or_insert(NodeHealth { healthy: true, consecutive_failures: 0 });
                    if ok {
                        let was_unhealthy = !entry.healthy;
                        entry.healthy = true;
                        entry.consecutive_failures = 0;
                        if was_unhealthy {
                            log::warn!("Health UP: node {} became healthy", node_id_clone);
                            let _ = event_tx.send(HealthEvent::NodeUp(node_id_clone.clone()));
                        }
                        false
                    } else {
                        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                        if entry.healthy && entry.consecutive_failures >= threshold {
                            entry.healthy = false;
                            log::error!("Health DOWN: node {} marked unhealthy ({} consecutive failures)", node_id_clone, entry.consecutive_failures);
                            true // Node just became unhealthy
                        } else {
                            false
                        }
                    }
                } else {
                    false
                };

                // CRITICAL: When node goes DOWN, trigger replication repair and resharding
                if became_unhealthy {
                    log::error!("⚠️  Node {} went DOWN - sending event to trigger auto-repair and resharding", node_id_clone);
                    let _ = event_tx.send(HealthEvent::NodeDown(node_id_clone.clone()));
                }

                sleep(Duration::from_secs(interval)).await;
            }
        });

        if let Ok(mut guard) = self.health_tasks.write() {
            guard.insert(node_id, handle);
        }
    }

    async fn stop_healthcheck_for_node(&self, node_id: &str) {
        if let Ok(mut guard) = self.health_tasks.write() {
            if let Some(handle) = guard.remove(node_id) {
                handle.abort();
                log::info!("Stopped healthcheck for node {}", node_id);
            }
        }
        if let Ok(mut status_guard) = self.health_status.write() {
            status_guard.remove(node_id);
        }
    }

    /// Scan all objects across all healthy nodes and repair under-replicated ones
    async fn scan_and_repair_all_objects(&self) -> Result<(), Status> {
        log::info!("🔍 Starting scan for under-replicated objects...");
        
        // Clone data to avoid holding locks across await
        let healthy_clients: Vec<(String, C4Client<Channel>)> = {
            let client_pool_guard = self.client_pool.read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            let health_guard = self.health_status.read()
                .map_err(|_| Status::internal("Failed to read health status"))?;
            
            // Get list of healthy nodes
            client_pool_guard
                .iter()
                .filter(|(node_id, _)| {
                    health_guard.get(*node_id).map(|h| h.healthy).unwrap_or(false)
                })
                .map(|(id, client)| (id.clone(), client.clone()))
                .collect()
        };
        
        if healthy_clients.is_empty() {
            log::error!("No healthy nodes available for scanning");
            return Err(Status::unavailable("No healthy nodes"));
        }
        
        log::info!("Scanning {} healthy nodes for objects", healthy_clients.len());
        
        // Collect all unique bucket/object pairs across all healthy nodes
        let mut all_objects: HashSet<(String, String)> = HashSet::new();
        
        for (node_id, mut client) in healthy_clients {
            // List buckets on this node
            let buckets_result = client.list_buckets(Request::new(ListBucketsRequest {
                limit: None,
                offset: None,
            })).await;
            
            let buckets = match buckets_result {
                Ok(resp) => resp.into_inner().bucket_names,
                Err(e) => {
                    log::warn!("Failed to list buckets on node {}: {}", node_id, e);
                    continue;
                }
            };
            
            // For each bucket, list objects
            for bucket in buckets {
                let objects_result = client.list_objects(Request::new(ListObjectsRequest {
                    bucket_name: bucket.clone(),
                    limit: None,
                    offset: None,
                    sorting_order: None,
                    prefix: None,
                })).await;
                
                let objects = match objects_result {
                    Ok(resp) => resp.into_inner().metadata,
                    Err(e) => {
                        log::warn!("Failed to list objects in bucket {} on node {}: {}", bucket, node_id, e);
                        continue;
                    }
                };
                
                for obj_meta in objects {
                    if let Some(obj_id) = obj_meta.id {
                        all_objects.insert((bucket.clone(), obj_id.object_key));
                    }
                }
            }
        }
        
        log::info!("Found {} unique objects across cluster", all_objects.len());
        
        // Now check replication factor for each object
        let mut under_replicated = 0;
        let mut repaired = 0;
        
        for (bucket_name, object_key) in all_objects {
            // Count how many replicas have this object
            let key = get_key_for_object(&bucket_name, &object_key);
            let expected_replicas = self.get_replica_clients_for_key(&key).await?;
            
            let mut actual_replica_count = 0;
            for client in &expected_replicas {
                let head_result = client.clone().head_object(Request::new(HeadObjectRequest {
                    id: Some(grpc_server::object_storage::ObjectId {
                        bucket_name: bucket_name.clone(),
                        object_key: object_key.clone(),
                        version: 0,
                    }),
                })).await;
                
                if head_result.is_ok() {
                    actual_replica_count += 1;
                }
            }
            
            if actual_replica_count < self.replication_factor {
                log::warn!(
                    "Object {}/{} is under-replicated: {} replicas (expected {})",
                    bucket_name, object_key, actual_replica_count, self.replication_factor
                );
                under_replicated += 1;
                
                // Trigger repair
                match self.check_and_repair_replication(bucket_name.clone(), object_key.clone()).await {
                    Ok(true) => {
                        log::info!("✅ Repaired replication for {}/{}", bucket_name, object_key);
                        repaired += 1;
                    }
                    Ok(false) => {
                        log::debug!("Object {}/{} replication already healthy", bucket_name, object_key);
                    }
                    Err(e) => {
                        log::error!("Failed to repair {}/{}: {}", bucket_name, object_key, e);
                    }
                }
            }
        }
        
        log::info!(
            "✅ Scan complete: {} under-replicated objects found, {} repaired",
            under_replicated, repaired
        );
        
        Ok(())
    }

    /// Synchronize a recovered node with current cluster state
    /// When a node comes back online after being down, it needs to:
    /// 1. Receive all objects that should be on it according to consistent hash ring
    /// 2. This ensures the node has all replicas it's responsible for
    async fn sync_recovered_node(&self, node_id: &str) -> Result<(), Status> {
        log::info!("🔄 Starting synchronization for recovered node: {}", node_id);
        
        // Get the client for the recovered node
        let recovered_client = {
            let client_pool_guard = self.client_pool.read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.get(node_id).cloned()
                .ok_or_else(|| Status::internal(format!("Client not found for node: {}", node_id)))?
        };
        
        // Get list of all healthy nodes (excluding the recovered one for now, to scan existing data)
        let source_clients: Vec<(String, C4Client<Channel>)> = {
            let client_pool_guard = self.client_pool.read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            let health_guard = self.health_status.read()
                .map_err(|_| Status::internal("Failed to read health status"))?;
            
            client_pool_guard
                .iter()
                .filter(|(nid, _)| {
                    *nid != node_id && health_guard.get(*nid).map(|h| h.healthy).unwrap_or(false)
                })
                .map(|(id, client)| (id.clone(), client.clone()))
                .collect()
        };
        
        if source_clients.is_empty() {
            log::warn!("No other healthy nodes to sync from");
            return Ok(());
        }
        
        log::info!("Scanning {} healthy nodes to find objects for {}", source_clients.len(), node_id);
        
        // Collect all objects from healthy nodes
        let mut all_objects: HashMap<(String, String), Vec<String>> = HashMap::new(); // (bucket, key) -> [node_ids that have it]
        
        for (source_node_id, mut client) in source_clients {
            // List buckets
            let buckets_result = client.list_buckets(Request::new(ListBucketsRequest {
                limit: None,
                offset: None,
            })).await;
            
            let buckets = match buckets_result {
                Ok(resp) => resp.into_inner().bucket_names,
                Err(e) => {
                    log::warn!("Failed to list buckets on node {}: {}", source_node_id, e);
                    continue;
                }
            };
            
            // For each bucket, list objects
            for bucket in buckets {
                let objects_result = client.list_objects(Request::new(ListObjectsRequest {
                    bucket_name: bucket.clone(),
                    limit: None,
                    offset: None,
                    sorting_order: None,
                    prefix: None,
                })).await;
                
                let objects = match objects_result {
                    Ok(resp) => resp.into_inner().metadata,
                    Err(e) => {
                        log::warn!("Failed to list objects in bucket {} on node {}: {}", bucket, source_node_id, e);
                        continue;
                    }
                };
                
                for obj_meta in objects {
                    if let Some(obj_id) = obj_meta.id {
                        all_objects.entry((bucket.clone(), obj_id.object_key))
                            .or_insert_with(Vec::new)
                            .push(source_node_id.clone());
                    }
                }
            }
        }
        
        log::info!("Found {} unique objects in cluster", all_objects.len());
        
        // For each object, check if recovered node should have it according to ring
        let mut objects_to_sync = 0;
        let mut objects_synced = 0;
        
        for ((bucket_name, object_key), source_nodes) in all_objects {
            let key = get_key_for_object(&bucket_name, &object_key);
            
            // Get expected replica nodes for this key
            let expected_replicas = {
                let ring_guard = self.ring.read()
                    .map_err(|_| Status::internal("Failed to read ring"))?;
                ring_guard.get_n_nodes(&key, self.replication_factor)
                    .iter()
                    .map(|n| n.id.clone())
                    .collect::<Vec<_>>()
            };
            
            // Check if recovered node should have this object
            if !expected_replicas.contains(&node_id.to_string()) {
                continue; // This object shouldn't be on the recovered node
            }
            
            objects_to_sync += 1;
            
            // Check if recovered node already has it
            let has_object = {
                let head_result = recovered_client.clone().head_object(Request::new(HeadObjectRequest {
                    id: Some(grpc_server::object_storage::ObjectId {
                        bucket_name: bucket_name.clone(),
                        object_key: object_key.clone(),
                        version: 0,
                    }),
                })).await;
                head_result.is_ok()
            };
            
            if has_object {
                log::debug!("Object {}/{} already on node {}", bucket_name, object_key, node_id);
                continue;
            }
            
            // Object missing on recovered node - copy it
            log::info!("Copying {}/{} to recovered node {}", bucket_name, object_key, node_id);
            
            // Get object from one of the source nodes
            if source_nodes.is_empty() {
                log::warn!("No source nodes have {}/{}", bucket_name, object_key);
                continue;
            }
            
            let source_node_id = &source_nodes[0];
            let mut source_client = {
                let client_pool_guard = self.client_pool.read()
                    .map_err(|_| Status::internal("Failed to read client pool"))?;
                client_pool_guard.get(source_node_id).cloned()
                    .ok_or_else(|| Status::internal(format!("Source client not found: {}", source_node_id)))?
            };
            
            // GET from source
            let get_request = Request::new(GetObjectRequest {
                id: Some(grpc_server::object_storage::ObjectId {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                    version: 0,
                }),
            });
            
            let response = match source_client.get_object(get_request).await {
                Ok(r) => r,
                Err(e) => {
                    log::error!("Failed to GET {}/{} from {}: {}", bucket_name, object_key, source_node_id, e);
                    continue;
                }
            };
            
            // Buffer the data
            let mut stream = response.into_inner();
            let mut data_chunks = Vec::new();
            while let Some(chunk) = stream.message().await
                .map_err(|e| Status::internal(format!("Failed to read stream: {}", e)))? {
                data_chunks.push(chunk.object_part);
            }
            
            // Get version
            let version = {
                let versions = self.object_versions.read()
                    .map_err(|_| Status::internal("Failed to read versions"))?;
                versions.get(&(bucket_name.clone(), object_key.clone()))
                    .copied()
                    .unwrap_or(1)
            };
            
            // Ensure bucket exists on recovered node
            let _ = recovered_client.clone().create_bucket(Request::new(CreateBucketRequest {
                bucket_name: bucket_name.clone(),
            })).await; // Ignore AlreadyExists errors
            
            // PUT to recovered node
            let bucket_name_clone = bucket_name.clone();
            let object_key_clone = object_key.clone();
            let data_chunks_clone = data_chunks.clone();
            
            let stream = async_stream::stream! {
                yield PutObjectRequest {
                    req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                        grpc_server::object_storage::ObjectId {
                            bucket_name: bucket_name_clone,
                            object_key: object_key_clone,
                            version,
                        }
                    )),
                };
                
                for chunk in data_chunks_clone {
                    yield PutObjectRequest {
                        req: Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(chunk)),
                    };
                }
            };
            
            match recovered_client.clone().put_object(Request::new(Box::pin(stream))).await {
                Ok(_) => {
                    log::info!("✅ Synced {}/{} to node {}", bucket_name, object_key, node_id);
                    objects_synced += 1;
                }
                Err(e) => {
                    log::error!("Failed to PUT {}/{} to {}: {}", bucket_name, object_key, node_id, e);
                }
            }
        }
        
        log::info!(
            "✅ Sync complete for node {}: {} objects should be on this node, {} were copied",
            node_id, objects_to_sync, objects_synced
        );
        
        Ok(())
    }

    /// Trigger automatic resharding when a node goes down
    async fn trigger_auto_resharding(&self, dead_node_id: &str) -> Result<(), Status> {
        log::info!("🔄 Triggering auto-resharding due to node failure: {}", dead_node_id);
        
        // Get current active nodes
        let current_nodes = {
            let active_nodes_guard = self.active_nodes.read()
                .map_err(|_| Status::internal("Failed to read active nodes"))?;
            active_nodes_guard.clone()
        };
        
        // Create new topology without the dead node
        let new_nodes: Vec<Node> = current_nodes
            .iter()
            .filter(|n| n.id != dead_node_id)
            .cloned()
            .collect();
        
        if new_nodes.is_empty() {
            log::error!("Cannot reshard: no healthy nodes remaining!");
            return Err(Status::unavailable("No healthy nodes for resharding"));
        }
        
        log::info!("Resharding from {} to {} nodes", current_nodes.len(), new_nodes.len());
        
        // Generate migration plan
        let client_pool_clone = {
            let client_pool_guard = self.client_pool.read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };
        
        let planner = MigrationPlanner::new(
            client_pool_clone.clone(),
            self.virtual_nodes_per_node,
            self.replication_factor
        );
        
        let migration_plan = planner
            .generate_migration_plan(current_nodes.clone(), new_nodes.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to generate migration plan for resharding: {}", e);
                Status::internal(format!("Failed to generate migration plan: {}", e))
            })?;
        
        log::info!("Generated migration plan with {} operations", migration_plan.operations.len());
        
        // Execute migration
        let migration_service = MigrationService::new(client_pool_clone);
        migration_service
            .migrate_data(migration_plan.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to execute resharding migration: {}", e);
                Status::internal(format!("Failed to execute migration: {}", e))
            })?;
        
        // Update master ring with new topology
        {
            let mut ring_guard = self.ring.write()
                .map_err(|_| Status::internal("Failed to write ring"))?;
            *ring_guard = ConsistentHashRing::new(new_nodes.clone(), self.virtual_nodes_per_node);
        }
        
        // Update active nodes
        {
            let mut active_nodes_guard = self.active_nodes.write()
                .map_err(|_| Status::internal("Failed to write active nodes"))?;
            *active_nodes_guard = new_nodes;
        }
        
        log::info!("✅ Auto-resharding completed successfully");
        
        Ok(())
    }

    #[allow(dead_code)]
    async fn get_client_for_key(&self, key: &str) -> Result<C4Client<Channel>, Status> {
        let ring_guard = self
            .ring
            .read()
            .map_err(|_| Status::internal("Failed to read ring"))?;
        let node = ring_guard
            .get_node(key)
            .ok_or_else(|| Status::internal("No storage nodes available"))?;

        log::debug!(
            "Key '{}' mapped to node: {} (address: {})",
            key,
            node.id,
            node.address
        );

        let client_pool_guard = self
            .client_pool
            .read()
            .map_err(|_| Status::internal("Failed to read client pool"))?;
        client_pool_guard
            .get(&node.id)
            .cloned()
            .ok_or_else(|| Status::internal(format!("Client not found for node: {}", node.id)))
    }

    async fn get_replica_clients_for_key(&self, key: &str) -> Result<Vec<C4Client<Channel>>, Status> {
        let ring_guard = self
            .ring
            .read()
            .map_err(|_| Status::internal("Failed to read ring"))?;
        let replica_nodes = ring_guard.get_n_nodes(key, self.replication_factor);

        if replica_nodes.is_empty() {
            return Err(Status::internal("No storage nodes available"));
        }

        log::debug!(
            "Key '{}' mapped to {} replica nodes: {:?}",
            key,
            replica_nodes.len(),
            replica_nodes.iter().map(|n| &n.id).collect::<Vec<_>>()
        );

        // Log active nodes for debugging
        if replica_nodes.len() < self.replication_factor {
            let active_nodes_guard = self.active_nodes.read()
                .map_err(|_| Status::internal("Failed to read active nodes"))?;
            log::warn!(
                "⚠️  Ring returned {} replicas but replication_factor is {}. Active nodes in cluster: {:?}",
                replica_nodes.len(),
                self.replication_factor,
                active_nodes_guard.iter().map(|n| &n.id).collect::<Vec<_>>()
            );
        }

        // Build clients preferring healthy nodes first
        let client_pool_guard = self
            .client_pool
            .read()
            .map_err(|_| Status::internal("Failed to read client pool"))?;
        let health_guard = self
            .health_status
            .read()
            .map_err(|_| Status::internal("Failed to read health status"))?;

        let mut healthy_clients = Vec::new();
        let mut unhealthy_node_ids = Vec::new();
        let mut used_node_ids = std::collections::HashSet::new();

        // First pass: collect healthy nodes from ring's suggestions
        for node in &replica_nodes {
            used_node_ids.insert(node.id.clone());
            let client = client_pool_guard
                .get(&node.id)
                .cloned()
                .ok_or_else(|| Status::internal(format!("Client not found for node: {}", node.id)))?;
            let is_healthy = health_guard.get(&node.id).map(|h| h.healthy).unwrap_or(true);
            if is_healthy {
                healthy_clients.push(client);
            } else {
                unhealthy_node_ids.push(node.id.clone());
            }
        }

        // If we have unhealthy nodes and need more replicas, find alternative healthy nodes
        if !unhealthy_node_ids.is_empty() && healthy_clients.len() < self.replication_factor {
            log::warn!(
                "🔄 Ring returned {} unhealthy nodes, finding alternatives to meet replication_factor={}",
                unhealthy_node_ids.len(),
                self.replication_factor
            );

            let active_nodes_guard = self.active_nodes.read()
                .map_err(|_| Status::internal("Failed to read active nodes"))?;
            
            // Find healthy nodes that are not already used
            for node in active_nodes_guard.iter() {
                if healthy_clients.len() >= self.replication_factor {
                    break;
                }
                if used_node_ids.contains(&node.id) {
                    continue;
                }
                let is_healthy = health_guard.get(&node.id).map(|h| h.healthy).unwrap_or(true);
                if is_healthy {
                    if let Some(client) = client_pool_guard.get(&node.id).cloned() {
                        log::info!(
                            "✅ Using alternative healthy node '{}' instead of unhealthy nodes {:?}",
                            node.id,
                            unhealthy_node_ids
                        );
                        healthy_clients.push(client);
                        used_node_ids.insert(node.id.clone());
                    }
                }
            }
        }

        if healthy_clients.is_empty() {
            return Err(Status::unavailable("No healthy storage nodes available"));
        }

        if healthy_clients.len() < self.replication_factor {
            log::warn!(
                "⚠️  Only {} healthy nodes available (replication_factor={})",
                healthy_clients.len(),
                self.replication_factor
            );
        }

        log::debug!(
            "Selected {} healthy clients for key '{}'",
            healthy_clients.len(),
            key
        );

        Ok(healthy_clients)
    }

        /// Check and repair replication for an object
        /// Returns true if replication was repaired, false if already healthy
        async fn check_and_repair_replication(
            &self,
               bucket_name: String,
               object_key: String,
        ) -> Result<bool, Status> {
               let key = get_key_for_object(&bucket_name, &object_key);
            let replica_clients = self.get_replica_clients_for_key(&key).await?;
        
            log::debug!(
                "Checking replication for {}/{} across {} replicas",
                bucket_name, object_key, replica_clients.len()
            );

            // Find which replicas have the object
            let mut replicas_with_object = Vec::new();
            let mut replicas_without_object = Vec::new();

            for (i, client) in replica_clients.iter().enumerate() {
                let request = Request::new(HeadObjectRequest {
                    id: Some(grpc_server::object_storage::ObjectId {
                        bucket_name: bucket_name.to_string(),
                        object_key: object_key.to_string(),
                        version: 0,
                    }),
                });

                match client.clone().head_object(request).await {
                    Ok(_) => {
                        log::debug!("Replica {} has object {}/{}", i, bucket_name, object_key);
                        replicas_with_object.push(i);
                    }
                    Err(e) if e.code() == tonic::Code::NotFound => {
                        log::debug!("Replica {} missing object {}/{}", i, bucket_name, object_key);
                        replicas_without_object.push(i);
                    }
                    Err(e) => {
                        log::warn!("Error checking replica {}: {}", i, e);
                        replicas_without_object.push(i);
                    }
                }
            }

            // If we have the right number of replicas, nothing to do
            if replicas_without_object.is_empty() {
                log::debug!("Replication healthy for {}/{}", bucket_name, object_key);
                return Ok(false);
            }

            // If no replica has the object, we can't repair
            if replicas_with_object.is_empty() {
                log::error!("No replica has object {}/{}, cannot repair", bucket_name, object_key);
                return Err(Status::data_loss("object lost on all replicas"));
            }

            log::info!(
                "Repairing replication for {}/{}: {} replicas have it, {} missing",
                bucket_name, object_key, replicas_with_object.len(), replicas_without_object.len()
            );

            // Get the object from first available replica
            let source_client = &replica_clients[replicas_with_object[0]];
            let get_request = Request::new(GetObjectRequest {
                id: Some(grpc_server::object_storage::ObjectId {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    version: 0,
                }),
            });

            let response = source_client.clone().get_object(get_request).await
                .map_err(|e| Status::internal(format!("Failed to get source object: {}", e)))?;
        
            // Buffer the stream
            let mut stream = response.into_inner();
            let mut data_chunks = Vec::new();
            while let Some(chunk) = stream.message().await
                .map_err(|e| Status::internal(format!("Failed to read source stream: {}", e)))? {
                data_chunks.push(chunk.object_part);
            }

            // Get version from source
            let version = {
                let versions = self.object_versions.read()
                    .map_err(|_| Status::internal("Failed to read versions"))?;
                versions.get(&(bucket_name.to_string(), object_key.to_string()))
                    .copied()
                    .unwrap_or(1)
            };

            // Copy to missing replicas
            for replica_idx in replicas_without_object {
                let dest_client = &replica_clients[replica_idx];
            
                // First ensure bucket exists
                let _ = dest_client.clone().create_bucket(Request::new(CreateBucketRequest {
                    bucket_name: bucket_name.to_string(),
                })).await; // Ignore errors (bucket may already exist)

                // Create put stream
                let data_chunks_clone = data_chunks.clone();
                    let bucket_name_clone = bucket_name.clone();
                    let object_key_clone = object_key.clone();
                let stream = async_stream::stream! {
                    yield PutObjectRequest {
                        req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                            grpc_server::object_storage::ObjectId {
                                    bucket_name: bucket_name_clone,
                                    object_key: object_key_clone,
                                version,
                            }
                        )),
                    };

                    for chunk in data_chunks_clone {
                        yield PutObjectRequest {
                            req: Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(chunk)),
                        };
                    }
                };

                match dest_client.clone().put_object(Request::new(Box::pin(stream))).await {
                    Ok(_) => {
                        log::info!(
                            "Successfully replicated {}/{} to replica {}",
                            bucket_name, object_key, replica_idx
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to replicate {}/{} to replica {}: {}",
                            bucket_name, object_key, replica_idx, e
                        );
                    }
                }
            }

            Ok(true)
        }

    /// Clean up unnecessary objects from previous nodes after migration
    async fn cleanup_unnecessary_objects(
        &self,
        migration_plan: &crate::migration::dto::MigrationPlan,
    ) -> Result<(), Status> {
        log::info!("Starting cleanup of unnecessary objects after migration");

        let mut cleanup_errors = Vec::new();

        // Group operations by previous node to batch deletions
        let mut operations_by_prev_node: HashMap<
            String,
            Vec<&crate::migration::dto::MigrationOperation>,
        > = HashMap::new();

        for operation in &migration_plan.operations {
            operations_by_prev_node
                .entry(operation.prev_node.clone())
                .or_insert_with(Vec::new)
                .push(operation);
        }

        // Clone the client pool to avoid holding the lock across await points
        let client_pool_clone = {
            let client_pool_guard = self
                .client_pool
                .read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            client_pool_guard.clone()
        };

        // Delete objects from previous nodes
        for (prev_node_id, operations) in operations_by_prev_node {
            if let Some(client) = client_pool_clone.get(&prev_node_id) {
                log::info!(
                    "Cleaning up {} objects from node {}",
                    operations.len(),
                    prev_node_id
                );

                for operation in operations {
                    let object_id = grpc_server::object_storage::ObjectId {
                        bucket_name: operation.bucket_name.clone(),
                        object_key: operation.object_key.clone(),
                        version: 0,
                    };

                    let delete_request = tonic::Request::new(DeleteObjectRequest {
                        id: Some(object_id),
                    });

                    match client.clone().delete_object(delete_request).await {
                        Ok(_) => {
                            log::debug!(
                                "Successfully deleted object {}/{} from node {}",
                                operation.bucket_name,
                                operation.object_key,
                                prev_node_id
                            );
                        }
                        Err(e) => {
                            log::warn!(
                                "Failed to delete object {}/{} from node {}: {}",
                                operation.bucket_name,
                                operation.object_key,
                                prev_node_id,
                                e
                            );
                            cleanup_errors.push(format!(
                                "Failed to delete {}/{} from {}: {}",
                                operation.bucket_name, operation.object_key, prev_node_id, e
                            ));
                        }
                    }
                }
            } else {
                log::warn!("Client not found for previous node: {}", prev_node_id);
                cleanup_errors.push(format!("Client not found for node: {}", prev_node_id));
            }
        }

        if !cleanup_errors.is_empty() {
            log::warn!(
                "Some cleanup operations failed: {} errors",
                cleanup_errors.len()
            );
            // We don't fail the entire operation for cleanup errors, just log them
            // This allows the system to continue functioning even if some cleanup fails
        }

        log::info!(
            "Cleanup completed with {} errors out of {} operations",
            cleanup_errors.len(),
            migration_plan.operations.len()
        );

        Ok(())
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
        let replica_clients = self.get_replica_clients_for_key(&key).await?;

        // Generate version
        let version = {
            let mut versions = self.object_versions.write().map_err(|_| Status::internal("Failed to write versions"))?;
            let entry = versions.entry((bucket_name.clone(), object_key.clone())).or_insert(0);
            *entry += 1;
            *entry
        };

        log::info!("Using version {} for object {}/{}", version, bucket_name, object_key);

        // Buffer all data messages
        let mut data_messages = Vec::new();
        while let Some(msg) = stream.message().await.transpose() {
            match msg {
                Ok(msg) => {
                    if let Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(_)) = &msg.req {
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
            let version_clone = version;
            let data_messages_clone = data_messages.clone();

            let request_stream = async_stream::stream! {
                // First message with ObjectID including version
                yield PutObjectRequest {
                    req: Some(grpc_server::object_storage::put_object_request::Req::Id(
                        grpc_server::object_storage::ObjectId {
                            bucket_name: bucket_name_clone.clone(),
                            object_key: object_key_clone.clone(),
                            version: version_clone,
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
        let replica_clients = self.get_replica_clients_for_key(&key).await?;

        // If ring returned fewer replicas than expected (e.g., due to node failures),
        // fallback to trying ALL healthy nodes as a safety mechanism
        let replica_clients = if replica_clients.is_empty() {
            log::warn!("Ring returned no replicas, falling back to all healthy nodes");
            let client_pool_guard = self.client_pool.read()
                .map_err(|_| Status::internal("Failed to read client pool"))?;
            let health_guard = self.health_status.read()
                .map_err(|_| Status::internal("Failed to read health status"))?;
            
            let all_healthy: Vec<C4Client<Channel>> = client_pool_guard
                .iter()
                .filter(|(node_id, _)| {
                    health_guard.get(*node_id).map(|h| h.healthy).unwrap_or(true)
                })
                .map(|(_, client)| client.clone())
                .collect();
            
            if all_healthy.is_empty() {
                return Err(Status::unavailable("No healthy nodes available"));
            }
            
            log::info!("Trying {} healthy nodes as fallback", all_healthy.len());
            all_healthy
        } else {
            replica_clients
        };

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
                                bucket_name, object_key, i
                            );
                            last_error = Some(Status::not_found(format!(
                                "object {}/{} not found",
                                bucket_name, object_key
                            )));
                        }
                        Err(e) => {
                            log::warn!(
                                "Error reading object {}/{} from replica {}: {}",
                                bucket_name, object_key, i, e
                            );
                            last_error = Some(e);
                        }
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Failed to get object {}/{} from replica {}: {}",
                        bucket_name, object_key, i, e
                    );
                    last_error = Some(e);
                }
            }
        }

        log::error!("Failed to get object {}/{} from all replicas", bucket_name, object_key);
        Err(last_error.unwrap_or_else(|| {
            Status::not_found(format!("object {}/{} not found on any replica", bucket_name, object_key))
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
        let mut by_key: HashMap<String, grpc_server::object_storage::ObjectMetadata> = HashMap::new();
        for meta in all_objects.into_iter() {
            if let Some(id) = &meta.id {
                if id.bucket_name != bucket_name { continue; }
            } else {
                continue;
            }
            let key = meta.id.as_ref().unwrap().object_key.clone();
            by_key
                .entry(key)
                .and_modify(|existing| {
                    if meta.version > existing.version { *existing = meta.clone(); }
                })
                .or_insert(meta);
        }

        // Optional: deterministic sort by object key
        let mut deduped: Vec<_> = by_key.into_values().collect();
        deduped.sort_by(|a, b| {
            let ka = a.id.as_ref().map(|i| i.object_key.clone()).unwrap_or_default();
            let kb = b.id.as_ref().map(|i| i.object_key.clone()).unwrap_or_default();
            ka.cmp(&kb)
        });

        // Apply pagination on deduped set
        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.unwrap_or(20) as usize;
        let deduped = deduped.into_iter().skip(offset).take(limit).collect();

        log::info!("Successfully listed objects (deduped)");
        Ok(Response::new(grpc_server::object_storage::ListObjectsResponse { metadata: deduped }))
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
        let replica_clients = self.get_replica_clients_for_key(&key).await?;

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
                        bucket_name, object_key, i, e
                    );
                    // Continue to next replica
                }
            }
        }

        log::error!("Failed to head object {}/{} from all replicas", bucket_name, object_key);
        Err(Status::not_found(format!("object {}/{} not found on any replica", bucket_name, object_key)))
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
        let replica_clients = self.get_replica_clients_for_key(&key).await?;

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

    type GetObjectStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<grpc_server::object_storage::GetObjectResponse, Status>> + Send + 'static>>;

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
        let planner = MigrationPlanner::new(client_pool_clone, self.virtual_nodes_per_node, self.replication_factor);
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
        let planner = MigrationPlanner::new(client_pool_clone.clone(), self.virtual_nodes_per_node, self.replication_factor);
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

        // Clean up unnecessary objects from previous nodes
        self.cleanup_unnecessary_objects(&migration_plan).await?;

        log::info!("Successfully added node: {}", node_to_add.id);

        // Start healthcheck for this node
        self.spawn_healthcheck_for_node(node_to_add.id.clone()).await;
        log::info!("Healthcheck started for new node: {}", node_to_add.id);

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
        let planner = MigrationPlanner::new(client_pool_clone, self.virtual_nodes_per_node, self.replication_factor);
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
        let planner = MigrationPlanner::new(client_pool_clone.clone(), self.virtual_nodes_per_node, self.replication_factor);
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

        // Clean up unnecessary objects from previous nodes
        self.cleanup_unnecessary_objects(&migration_plan).await?;

        log::info!("Successfully removed node: {}", node_to_remove.id);

        // Stop healthcheck for this node
        self.stop_healthcheck_for_node(&node_to_remove.id).await;

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
