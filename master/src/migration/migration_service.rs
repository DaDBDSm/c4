use std::{collections::HashMap, error::Error};

use grpc_server::object_storage::{
    CreateBucketRequest, GetObjectRequest, ObjectId, PutObjectRequest, c4_client::C4Client,
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

            for new_node in &operation.new_nodes {
                let destination_client =
                    self.client_pool.get(new_node).ok_or_else(|| {
                        format!(
                            "Destination client not found for node: {}",
                            new_node
                        )
                    })?;

                let object_id = ObjectId {
                    bucket_name: operation.bucket_name.clone(),
                    object_key: operation.object_key.clone(),
                    version: 0, // Version will be set by the destination
                };

                // Ensure bucket exists on destination before migrating objects
                self.ensure_bucket_exists(&destination_client, &operation.bucket_name)
                    .await?;

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
        }

        Ok(())
    }

    /// Ensure bucket exists on destination node, create it if it doesn't
    async fn ensure_bucket_exists(
        &self,
        destination_client: &C4Client<Channel>,
        bucket_name: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Try to create the bucket - if it already exists, this will fail gracefully
        // but we don't want to fail the entire migration for existing buckets
        let create_bucket_request = tonic::Request::new(CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
        });

        match destination_client
            .clone()
            .create_bucket(create_bucket_request)
            .await
        {
            Ok(_) => {
                log::info!("Created bucket '{}' on destination node", bucket_name);
            }
            Err(e) => {
                // If bucket already exists, that's fine - just log it
                // If it's another error, we should log it but continue
                if e.message().contains("already exists") || e.message().contains("exists") {
                    log::debug!(
                        "Bucket '{}' already exists on destination node",
                        bucket_name
                    );
                } else {
                    log::warn!(
                        "Failed to create bucket '{}' on destination node: {}",
                        bucket_name,
                        e
                    );
                    // We don't fail here because the bucket might already exist
                    // and we want to continue with the migration
                }
            }
        }

        Ok(())
    }
}
