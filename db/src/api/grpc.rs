use crate::storage::errors::StorageError;
use crate::storage::{self, PutObjectDTO};
use crate::storage::{
    CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, HeadObjectDTO, ListBucketsDTO,
    ListObjectsDTO, ObjectStorage, SortingOrder,
};
use grpc_server::object_storage::c4_server::C4;
use grpc_server::object_storage::put_object_request;
use grpc_server::object_storage::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    GetObjectResponse, HeadObjectRequest, HeadObjectResponse, ListBucketsRequest,
    ListBucketsResponse, ListObjectsRequest, ListObjectsResponse, ObjectId, ObjectMetadata,
    PutObjectRequest, PutObjectResponse,
};
use std::pin::Pin;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use async_stream::stream;

pub struct C4Handler {
    pub c4_storage: storage::simple::ObjectStorageSimple,
}

#[tonic::async_trait]
impl C4 for C4Handler {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<()>, Status> {
        match self
            .c4_storage
            .create_bucket(&CreateBucketDTO {
                bucket_name: request.get_ref().bucket_name.clone(),
            })
            .await
        {
            Err(e) => match e {
                StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                StorageError::IoError(_) => Err(Status::internal("internal io error")),
                StorageError::BucketAlreadyExists(bucket) => Err(Status::already_exists(bucket)),
                _ => Err(Status::internal("internal error")),
            },
            Ok(_) => Ok(Response::new(())),
        }
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        match self
            .c4_storage
            .list_buckets(&ListBucketsDTO {
                limit: request.get_ref().limit,
                offset: request.get_ref().offset,
            })
            .await
        {
            Err(e) => match e {
                StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                StorageError::IoError(_) => Err(Status::internal("internal io error")),
                _ => Err(Status::internal("internal error")),
            },
            Ok(buckets) => Ok(Response::new(ListBucketsResponse {
                bucket_names: buckets,
            })),
        }
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<()>, Status> {
        match self
            .c4_storage
            .delete_bucket(&DeleteBucketDTO {
                bucket_name: request.get_ref().bucket_name.clone(),
            })
            .await
        {
            Err(e) => match e {
                StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                StorageError::IoError(_) => Err(Status::internal("internal io error")),
                _ => Err(Status::internal("internal error")),
            },
            Ok(_) => Ok(Response::new(())),
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
            .map_err(|e| Status::internal(format!("stream read error: {}", e)))?
            .ok_or_else(|| Status::invalid_argument("empty request stream"))?;

        let object_id = match first_msg.req {
            Some(put_object_request::Req::Id(id)) => id,
            _ => {
                return Err(Status::invalid_argument(
                    "first message must contain ObjectId",
                ));
            }
        };

        let byte_stream = stream.map(|res| match res {
            Err(_) => Vec::new(),
            Ok(msg) => match msg.req {
                Some(put_object_request::Req::ObjectPart(bytes)) => bytes,
                _ => Vec::new(),
            },
        });

        let mut dto = PutObjectDTO {
            bucket_name: object_id.bucket_name,
            key: object_id.object_key,
            stream: Box::new(byte_stream),
        };

        let metadata = self
            .c4_storage
            .put_object(&mut dto)
            .await
            .map_err(|_e| Status::internal("storage error"))?;

        let response = PutObjectResponse {
            metadata: Some(ObjectMetadata {
                id: Some(ObjectId {
                    bucket_name: metadata.bucket_name,
                    object_key: metadata.key,
                }),
                size: metadata.size,
                created_at: metadata.created_at,
            }),
        };

        Ok(Response::new(response))
    }

    type GetObjectStream =
        Pin<Box<dyn Stream<Item = Result<GetObjectResponse, Status>> + Send + Sync + 'static>>;

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let get_object_request = request.into_inner();
        let object_id = get_object_request
            .id
            .ok_or_else(|| Status::invalid_argument("Object ID is required"))?;

        let bucket_name = object_id.bucket_name.clone();
        let key = object_id.object_key.clone();

        // Create a static stream by cloning the storage and moving it into the stream
        let storage = self.c4_storage.clone();
        let stream = stream! {
            match storage.get_object_stream(bucket_name, key).await {
                Ok((byte_stream, _metadata)) => {
                    let mut stream = byte_stream;
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(data) => yield Ok(GetObjectResponse { object_part: data }),
                            Err(e) => yield Err(Status::internal(format!("Stream error: {}", e))),
                        }
                    }
                }
                Err(_e) => {
                    yield Err(Status::internal("storage error"));
                }
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        match self
            .c4_storage
            .list_objects(&ListObjectsDTO {
                bucket_name: request.get_ref().bucket_name.clone(),
                limit: request.get_ref().limit,
                offset: request.get_ref().offset,
                sorting_order: SortingOrder::new_option(request.get_ref().sorting_order.as_ref()),
                prefix: request.get_ref().prefix.clone(),
            })
            .await
        {
            Err(e) => match e {
                StorageError::ObjectNotFound { bucket, key } => {
                    Err(Status::not_found(format!("not found {bucket}/{key}")))
                }
                StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                StorageError::IoError(_) => Err(Status::internal("internal io error")),
                _ => Err(Status::internal("internal error")),
            },
            Ok(objects) => Ok(Response::new(ListObjectsResponse {
                metadata: objects
                    .iter()
                    .map(|object_metadata| ObjectMetadata {
                        id: Some(ObjectId {
                            bucket_name: object_metadata.bucket_name.clone(),
                            object_key: object_metadata.key.clone(),
                        }),
                        size: object_metadata.size,
                        created_at: object_metadata.created_at,
                    })
                    .collect(),
            })),
        }
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        let object_id = match request.get_ref().id.as_ref() {
            None => {
                return Err(Status::invalid_argument("empty request"));
            }
            Some(id) => id,
        };

        match self
            .c4_storage
            .head_object(&HeadObjectDTO {
                bucket_name: object_id.bucket_name.clone(),
                key: object_id.object_key.clone(),
            })
            .await
        {
            Err(e) => match e {
                StorageError::ObjectNotFound { bucket, key } => {
                    Err(Status::not_found(format!("not found {bucket}/{key}")))
                }
                StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                StorageError::IoError(_) => Err(Status::internal("internal io error")),
                _ => Err(Status::internal("internal error")),
            },
            Ok(metadata) => Ok(Response::new(HeadObjectResponse {
                metadata: Some(ObjectMetadata {
                    id: Some(ObjectId {
                        bucket_name: metadata.bucket_name.clone(),
                        object_key: metadata.key.clone(),
                    }),
                    size: metadata.size,
                    created_at: metadata.created_at,
                }),
            })),
        }
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<()>, Status> {
        let object_id = match request.get_ref().id.as_ref() {
            None => {
                return Err(Status::invalid_argument("empty request"));
            }
            Some(id) => id,
        };

        match self
            .c4_storage
            .delete_object(&DeleteObjectDTO {
                bucket_name: object_id.bucket_name.clone(),
                key: object_id.object_key.clone(),
            })
            .await
        {
            Err(e) => match e {
                StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                StorageError::IoError(_) => Err(Status::internal("internal io error")),
                _ => Err(Status::internal("internal error")),
            },
            Ok(_) => Ok(Response::new(())),
        }
    }
}
