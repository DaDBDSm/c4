use std::{collections::HashMap, error::Error};

use grpc_server::object_storage::{
    GetObjectRequest, ObjectId, PutObjectRequest, c4_client::C4Client,
};
use tonic::transport::Channel;

use crate::migration::dto::MigrationPlan;

pub struct MigrationService {
    client_pool: HashMap<String, C4Client<Channel>>,
}

impl MigrationService {
    pub fn new(client_pool: HashMap<String, C4Client<Channel>>) -> Self {
        Self { client_pool }
    }

    pub async fn migrate_data(&self, migration_plan: MigrationPlan) -> Result<(), Box<dyn Error>> {
        for operation in migration_plan.operations {
            let source_client = self.client_pool.get(&operation.prev_node).ok_or_else(|| {
                format!("Source client not found for node: {}", operation.prev_node)
            })?;
            let destination_client =
                self.client_pool.get(&operation.new_node).ok_or_else(|| {
                    format!(
                        "Destination client not found for node: {}",
                        operation.new_node
                    )
                })?;

            let object_id = ObjectId {
                bucket_name: operation.bucket_name.clone(),
                object_key: operation.object_key.clone(),
            };

            // stream data from source to destination
            let get_request = tonic::Request::new(GetObjectRequest {
                id: Some(object_id.clone()),
            });
            let mut get_response = source_client.clone().get_object(get_request).await?;

            let put_request_stream = async_stream::stream! {
                // first message: send object ID
                yield PutObjectRequest {
                    req: Some(grpc_server::object_storage::put_object_request::Req::Id(object_id.clone())),
                };

                while let Some(chunk) = get_response.get_mut().message().await.transpose() {
                    if let Ok(chunk) = chunk {
                        yield PutObjectRequest {
                            req: Some(grpc_server::object_storage::put_object_request::Req::ObjectPart(chunk.object_part)),
                        };
                    }
                }
            };

            // Convert the stream to a tonic::Request
            let put_request = tonic::Request::new(put_request_stream);

            // Send the stream to the destination client
            let _put_response = destination_client.clone().put_object(put_request).await?;
        }

        Ok(())
    }
}
