use crate::storage::errors::StorageError;
use crate::storage::{self, PutObjectDTO};
use crate::storage::{
    CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, GetObjectDTO, HeadObjectDTO, ListBucketsDTO,
    ListObjectsDTO, ObjectStorage, SortingOrder,
};
use async_stream::stream;
use grpc_server::object_storage::c4_server::C4;
use grpc_server::object_storage::put_object_request;
use grpc_server::object_storage::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    GetObjectResponse, HeadObjectRequest, HeadObjectResponse, ListBucketsRequest,
    ListBucketsResponse, ListObjectsRequest, ListObjectsResponse, MigrationPlanResponse, ObjectId,
    ObjectMetadata, PutObjectRequest, PutObjectResponse,
};
use std::pin::Pin;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct C4Handler {
    pub c4_storage: storage::simple::ObjectStorageSimple,
}

#[tonic::async_trait]
impl C4 for C4Handler {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<()>, Status> {
        let bucket_name = request.get_ref().bucket_name.clone();
        log::info!("Creating bucket: {}", bucket_name);

        match self
            .c4_storage
            .create_bucket(CreateBucketDTO {
                bucket_name: bucket_name.clone(),
            })
            .await
        {
            Err(e) => {
                log::error!("Failed to create bucket '{}': {:?}", bucket_name, e);
                match e {
                    StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                    StorageError::IoError(_) => Err(Status::internal("internal io error")),
                    StorageError::BucketAlreadyExists(bucket) => {
                        Err(Status::already_exists(bucket))
                    }
                    _ => Err(Status::internal("internal error")),
                }
            }
            Ok(_) => {
                log::info!("Successfully created bucket: {}", bucket_name);
                Ok(Response::new(()))
            }
        }
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        let limit = request.get_ref().limit;
        let offset = request.get_ref().offset;
        log::info!("Listing buckets (limit: {:?}, offset: {:?})", limit, offset);

        match self
            .c4_storage
            .list_buckets(ListBucketsDTO { limit, offset })
            .await
        {
            Err(e) => {
                log::error!("Failed to list buckets: {:?}", e);
                match e {
                    StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                    StorageError::IoError(_) => Ok(Response::new(ListBucketsResponse {
                        bucket_names: Vec::new(),
                    })),
                    _ => Err(Status::internal("internal error")),
                }
            }
            Ok(buckets) => {
                log::info!("Successfully listed {} buckets", buckets.len());
                Ok(Response::new(ListBucketsResponse {
                    bucket_names: buckets,
                }))
            }
        }
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<()>, Status> {
        let bucket_name = request.get_ref().bucket_name.clone();
        log::info!("Deleting bucket: {}", bucket_name);

        match self
            .c4_storage
            .delete_bucket(DeleteBucketDTO {
                bucket_name: bucket_name.clone(),
            })
            .await
        {
            Err(e) => {
                log::error!("Failed to delete bucket '{}': {:?}", bucket_name, e);
                match e {
                    StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                    StorageError::IoError(_) => Err(Status::internal("internal io error")),
                    _ => Err(Status::internal("internal error")),
                }
            }
            Ok(_) => {
                log::info!("Successfully deleted bucket: {}", bucket_name);
                Ok(Response::new(()))
            }
        }
    }

    async fn put_object(
        &self,
        request: Request<Streaming<PutObjectRequest>>,
    ) -> Result<Response<PutObjectResponse>, Status> {
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

        let object_id = match first_msg.req {
            Some(put_object_request::Req::Id(id)) => id,
            _ => {
                log::error!("First message does not contain ObjectId");
                return Err(Status::invalid_argument(
                    "first message must contain ObjectId",
                ));
            }
        };

        log::info!(
            "Putting object: {}/{}",
            object_id.bucket_name,
            object_id.object_key
        );

        let byte_stream = stream.map(|res| match res {
            Err(_) => Vec::new(),
            Ok(msg) => match msg.req {
                Some(put_object_request::Req::ObjectPart(bytes)) => bytes,
                _ => Vec::new(),
            },
        });

        let dto = PutObjectDTO {
            bucket_name: object_id.bucket_name.clone(),
            key: object_id.object_key.clone(),
            stream: Box::new(byte_stream),
            version: object_id.version,
        };

        let metadata = match self.c4_storage.put_object(dto).await {
            Ok(metadata) => {
                log::info!(
                    "Successfully put object: {}/{} (size: {})",
                    metadata.bucket_name,
                    metadata.key,
                    metadata.size
                );
                metadata
            }
            Err(e) => {
                log::error!(
                    "Failed to put object {}/{}: {:?}",
                    object_id.bucket_name,
                    object_id.object_key,
                    e
                );
                match e {
                    StorageError::ObjectNotFound { bucket, key } => {
                        return Err(Status::not_found(format!("not found {bucket}/{key}")));
                    }
                    StorageError::BucketNotFound(bucket) => {
                        return Err(Status::not_found(format!("bucket not found: {bucket}")));
                    }
                    StorageError::InvalidInput(msg) => {
                        return Err(Status::invalid_argument(msg));
                    }
                    StorageError::IoError(_) => {
                        return Err(Status::internal("internal io error"));
                    }
                    _ => {
                        return Err(Status::internal("internal error"));
                    }
                }
            }
        };

        let response = PutObjectResponse {
            metadata: Some(ObjectMetadata {
                id: Some(ObjectId {
                    bucket_name: metadata.bucket_name,
                    object_key: metadata.key,
                    version: metadata.version,
                }),
                size: metadata.size,
                created_at: metadata.created_at,
                version: metadata.version,
            }),
        };

        Ok(Response::new(response))
    }

    type GetObjectStream =
        Pin<Box<dyn Stream<Item = Result<GetObjectResponse, Status>> + Send + 'static>>;

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
        let key = object_id.object_key.clone();

        log::info!("Getting object: {}/{}", bucket_name, key);

        if !self
            .c4_storage
            .buckets_metadata_storage
            .bucket_exists(&bucket_name)
            .await
        {
            log::error!("Bucket not found: {}", bucket_name);
            return Err(Status::not_found(format!(
                "bucket not found: {bucket_name}"
            )));
        }

        let storage = self.c4_storage.clone();
        let stream = stream! {
            match storage.get_object(GetObjectDTO {
                bucket_name: bucket_name.clone(),
                key: key.clone(),
            }).await {
                Ok(byte_stream) => {
                    log::debug!("Successfully retrieved object stream: {}/{}", bucket_name, key);
                    let mut stream = byte_stream;
                    while let Some(data) = stream.next().await {
                        yield Ok(GetObjectResponse { object_part: data });
                    }
                }
                Err(e) => {
                    log::error!("Failed to get object {}/{}: {:?}", bucket_name, key, e);
                    match e {
                        StorageError::ObjectNotFound { bucket, key } => {
                            yield Err(Status::not_found(format!("not found {bucket}/{key}")));
                        }
                        StorageError::BucketNotFound(bucket) => {
                            yield Err(Status::not_found(format!("bucket not found: {bucket}")));
                        }
                        _ => {
                            yield Err(Status::internal("storage error"));
                        }
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let bucket_name = request.get_ref().bucket_name.clone();
        let limit = request.get_ref().limit;
        let offset = request.get_ref().offset;
        let prefix = request.get_ref().prefix.clone();

        log::info!(
            "Listing objects in bucket: {} (limit: {:?}, offset: {:?}, prefix: {:?})",
            bucket_name,
            limit,
            offset,
            prefix
        );

        match self
            .c4_storage
            .list_objects(ListObjectsDTO {
                bucket_name: bucket_name.clone(),
                limit,
                offset,
                sorting_order: SortingOrder::new_option(request.get_ref().sorting_order.as_ref()),
                prefix,
            })
            .await
        {
            Err(e) => {
                log::info!(
                    "Failed to list objects in bucket '{}': {:?}",
                    bucket_name,
                    e
                );
                match e {
                    StorageError::ObjectNotFound { bucket, key } => {
                        Err(Status::not_found(format!("not found {bucket}/{key}")))
                    }
                    StorageError::BucketNotFound(bucket) => {
                        Err(Status::not_found(format!("bucket not found: {bucket}")))
                    }
                    StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                    StorageError::IoError(_) => Err(Status::internal("internal io error")),
                    _ => Err(Status::internal("internal error")),
                }
            }
            Ok(objects) => {
                log::info!(
                    "Successfully listed {} objects in bucket: {}",
                    objects.len(),
                    bucket_name
                );
                Ok(Response::new(ListObjectsResponse {
                    metadata: objects
                        .iter()
                        .map(|object_metadata| ObjectMetadata {
                            id: Some(ObjectId {
                                bucket_name: object_metadata.bucket_name.clone(),
                                object_key: object_metadata.key.clone(),
                                version: object_metadata.version,
                            }),
                            size: object_metadata.size,
                            created_at: object_metadata.created_at,
                            version: object_metadata.version,
                        })
                        .collect(),
                }))
            }
        }
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        let object_id = match request.get_ref().id.as_ref() {
            None => {
                log::error!("Empty request for head_object");
                return Err(Status::invalid_argument("empty request"));
            }
            Some(id) => id,
        };

        let bucket_name = object_id.bucket_name.clone();
        let key = object_id.object_key.clone();
        log::info!("Head object: {}/{}", bucket_name, key);

        match self
            .c4_storage
            .head_object(HeadObjectDTO {
                bucket_name: bucket_name.clone(),
                key: key.clone(),
            })
            .await
        {
            Err(e) => {
                log::error!("Failed to head object {}/{}: {:?}", bucket_name, key, e);
                match e {
                    StorageError::ObjectNotFound { bucket, key } => {
                        Err(Status::not_found(format!("not found {bucket}/{key}")))
                    }
                    StorageError::BucketNotFound(bucket) => {
                        Err(Status::not_found(format!("bucket not found: {bucket}")))
                    }
                    StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                    StorageError::IoError(_) => Err(Status::internal("internal io error")),
                    _ => Err(Status::internal("internal error")),
                }
            }
            Ok(metadata) => {
                log::info!(
                    "Successfully retrieved head object metadata: {}/{} (size: {})",
                    bucket_name,
                    key,
                    metadata.size
                );
                Ok(Response::new(HeadObjectResponse {
                    metadata: Some(ObjectMetadata {
                        id: Some(ObjectId {
                            bucket_name: metadata.bucket_name.clone(),
                            object_key: metadata.key.clone(),
                            version: metadata.version,
                        }),
                        size: metadata.size,
                        created_at: metadata.created_at,
                        version: metadata.version,
                    }),
                }))
            }
        }
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<()>, Status> {
        let object_id = match request.get_ref().id.as_ref() {
            None => {
                log::error!("Empty request for delete_object");
                return Err(Status::invalid_argument("empty request"));
            }
            Some(id) => id,
        };

        let bucket_name = object_id.bucket_name.clone();
        let key = object_id.object_key.clone();
        log::info!("Deleting object: {}/{}", bucket_name, key);

        match self
            .c4_storage
            .delete_object(DeleteObjectDTO {
                bucket_name: bucket_name.clone(),
                key: key.clone(),
            })
            .await
        {
            Err(e) => {
                log::error!("Failed to delete object {}/{}: {:?}", bucket_name, key, e);
                match e {
                    StorageError::ObjectNotFound { bucket, key } => {
                        Err(Status::not_found(format!("not found {bucket}/{key}")))
                    }
                    StorageError::BucketNotFound(bucket) => {
                        Err(Status::not_found(format!("bucket not found: {bucket}")))
                    }
                    StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                    StorageError::IoError(_) => Err(Status::internal("internal io error")),
                    _ => Err(Status::internal("internal error")),
                }
            }
            Ok(_) => {
                log::info!("Successfully deleted object: {}/{}", bucket_name, key);
                Ok(Response::new(()))
            }
        }
    }

    async fn remove_node(
        &self,
        _request: Request<grpc_server::object_storage::RemoveNodeRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "RemoveNode is not supported on storage nodes (handled by master)",
        ))
    }

    async fn add_node(
        &self,
        _request: Request<grpc_server::object_storage::AddNodeRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "AddNode is not supported on storage nodes (handled by master)",
        ))
    }

    async fn get_migration_plan(
        &self,
        _request: Request<grpc_server::object_storage::AddNodeRequest>,
    ) -> Result<Response<MigrationPlanResponse>, Status> {
        Err(Status::unimplemented(
            "GetMigrationPlan is not supported on storage nodes (handled by master)",
        ))
    }

    async fn get_migration_plan_by_removing_node(
        &self,
        _request: Request<grpc_server::object_storage::RemoveNodeRequest>,
    ) -> Result<Response<MigrationPlanResponse>, Status> {
        Err(Status::unimplemented(
            "GetMigrationPlanByRemovingNode is not supported on storage nodes (handled by master)",
        ))
    }
}
