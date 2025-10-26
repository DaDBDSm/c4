use db::api::grpc::C4Handler;
use db::storage::simple::buckets_metadata_storage::BucketsMetadataStorage;
use db::storage::simple::chunk_file_storage::PartitionedBytesStorage;
use grpc_server::object_storage;

use std::{error::Error, net::SocketAddr};
use tonic::transport::Server;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .init();

    let addr: SocketAddr = "0.0.0.0:4000".parse().unwrap();

    log::info!("Starting C4 server on {}", addr);

    let base_dir = std::path::PathBuf::from("C:\\Users\\199-4\\labs\\c4\\tmp"); // fixme use another path

    let bytes_storage = PartitionedBytesStorage::new(base_dir.join("data"), 4);
    let buckets_metadata_storage =
        BucketsMetadataStorage::new(base_dir.join("metadata.json").to_string_lossy().to_string())
            .await
            .expect("Failed to create buckets metadata storage");

    let handler = C4Handler {
        c4_storage: db::storage::simple::ObjectStorageSimple {
            base_dir,
            bytes_storage,
            buckets_metadata_storage,
        },
    };

    Server::builder()
        .add_service(object_storage::c4_server::C4Server::new(handler))
        .serve(addr)
        .await?;

    Ok(())
}
