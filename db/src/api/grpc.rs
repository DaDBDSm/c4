use crate::storage;
use crate::storage::errors::StorageError;
use crate::storage::{
    CreateBucketDTO, DeleteBucketDTO, DeleteObjectDTO, HeadObjectDTO, ListBucketsDTO,
    ListObjectsDTO, ObjectStorage, SortingOrder,
};
use grpc_server::object_storage::c4_server::C4;
use grpc_server::object_storage::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    GetObjectResponse, HeadObjectRequest, HeadObjectResponse, ListBucketsRequest,
    ListBucketsResponse, ListObjectsRequest, ListObjectsResponse, ObjectId, ObjectMetadata,
    PutObjectRequest, PutObjectResponse,
};
use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

pub struct C4Handler {
    pub c4_storage: storage::simple::ObjectStorageSimple,
}

#[tonic::async_trait]
impl C4 for C4Handler {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<()>, Status> {
        match self.c4_storage.create_bucket(&CreateBucketDTO {
            bucket_name: request.get_ref().bucket_name.clone(),
        }) {
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
        match self.c4_storage.list_buckets(&ListBucketsDTO {
            limit: request.get_ref().limit,
            offset: request.get_ref().offset,
        }) {
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
        match self.c4_storage.delete_bucket(&DeleteBucketDTO {
            bucket_name: request.get_ref().bucket_name.clone(),
        }) {
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
        todo!()
        // let mut object_streamer = request.into_inner();
        //
        // // Get the first message which should contain the object ID
        // let object_id = match object_streamer.message().await? {
        //     None => {
        //         return Err(Status::invalid_argument("empty request"));
        //     }
        //     Some(s) => {
        //         if s.req.is_some() {
        //             match s.req.unwrap() {
        //                 Req::ObjectPart(_) => {
        //                     return Err(Status::invalid_argument(
        //                         "object id must be first request",
        //                     ));
        //                 }
        //                 Req::Id(id) => id,
        //             }
        //         } else {
        //             return Err(Status::invalid_argument("empty object id"));
        //         }
        //     }
        // };
        //
        // // Create a channel to bridge between async stream and sync Read trait
        // let (sender, receiver) = mpsc::channel::<Vec<u8>>();
        //
        // // Spawn a task to read from the stream and send data through the channel
        // let _stream_task = tokio::spawn(async move {
        //     while let Some(request_result) = object_streamer.next().await {
        //         match request_result {
        //             Ok(request) => {
        //                 if let Some(req) = request.req {
        //                     match req {
        //                         Req::ObjectPart(data) => {
        //                             if sender.send(data).is_err() {
        //                                 // Receiver was dropped, stop processing
        //                                 break;
        //                             }
        //                         }
        //                         Req::Id(_) => {
        //                             // Object ID should only be in the first message
        //                             // Skip this or handle as error
        //                             continue;
        //                         }
        //                     }
        //                 }
        //             }
        //             Err(e) => {
        //                 eprintln!("Error reading from stream: {:?}", e);
        //                 break;
        //             }
        //         }
        //     }
        //     // Send empty data to signal end of stream
        //     let _ = sender.send(Vec::new());
        // });
        //
        // // Create the reader that implements Read trait
        // let reader = Box::new(receiver);
        //
        // // Create the PutObjectDTO
        // let mut put_dto = PutObjectDTO {
        //     bucket_name: object_id.bucket_name,
        //     key: object_id.object_key,
        //     reader,
        // };
        //
        // // Call the storage implementation
        // match self.c4_storage.put_object(&mut put_dto) {
        //     Ok(metadata) => {
        //         // Convert to gRPC response format
        //         let response_metadata = grpc_server::object_storage::ObjectMetadata {
        //             id: Some(grpc_server::object_storage::ObjectId {
        //                 bucket_name: metadata.bucket_name,
        //                 object_key: metadata.key,
        //             }),
        //             size: metadata.size,
        //             created_at: metadata.created_at,
        //         };
        //
        //         Ok(Response::new(PutObjectResponse {
        //             metadata: Some(response_metadata),
        //         }))
        //     }
        //     Err(e) => match e {
        //         StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
        //         StorageError::IoError(_) => Err(Status::internal("internal io error")),
        //         _ => Err(Status::internal("internal error")),
        //     },
        // }
    }

    type GetObjectStream =
        Pin<Box<dyn Stream<Item = Result<GetObjectResponse, Status>> + Send + Sync + 'static>>;

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        todo!()
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        match self.c4_storage.list_objects(&ListObjectsDTO {
            bucket_name: request.get_ref().bucket_name.clone(),
            limit: request.get_ref().limit,
            offset: request.get_ref().offset,
            sorting_order: SortingOrder::new_option(request.get_ref().sorting_order.as_ref()),
            prefix: request.get_ref().prefix.clone(),
        }) {
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

        match self.c4_storage.head_object(&HeadObjectDTO {
            bucket_name: object_id.bucket_name.clone(),
            key: object_id.object_key.clone(),
        }) {
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

        match self.c4_storage.delete_object(&DeleteObjectDTO {
            bucket_name: object_id.bucket_name.clone(),
            key: object_id.object_key.clone(),
        }) {
            Err(e) => match e {
                StorageError::InvalidInput(msg) => Err(Status::invalid_argument(msg)),
                StorageError::IoError(_) => Err(Status::internal("internal io error")),
                _ => Err(Status::internal("internal error")),
            },
            Ok(_) => Ok(Response::new(())),
        }
    }
}
