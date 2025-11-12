use clap::Parser;
use db::api::grpc::C4Handler;
use db::storage::simple::buckets_metadata_storage::BucketsMetadataStorage;
use db::storage::simple::chunk_file_storage::PartitionedBytesStorage;
use grpc_server::object_storage;
use std::{error::Error, net::SocketAddr};
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 4000)]
    port: u16,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long, default_value = "tmp")]
    base_dir: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let args = Args::parse();

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;

    if let Some(node_id) = &args.node_id {
        log::info!("Starting C4 storage node {} on {}", node_id, addr);
    } else {
        log::info!("Starting C4 storage node on {}", addr);
    }

    let base_dir = std::path::PathBuf::from(&args.base_dir);

    if !base_dir.exists() {
        log::info!("Creating base directory: {:?}", base_dir);
        std::fs::create_dir_all(&base_dir)?;
    }

    let bytes_storage = PartitionedBytesStorage::new_with_persistence(base_dir.join("data"), 4)
        .await
        .expect("Failed to create partitioned bytes storage");
    let buckets_metadata_storage =
        BucketsMetadataStorage::new(base_dir.join("metadata.c4").to_string_lossy().to_string())
            .await
            .expect("Failed to create buckets metadata storage");

    let handler = C4Handler {
        c4_storage: db::storage::simple::ObjectStorageSimple {
            base_dir: base_dir.clone(),
            bytes_storage: bytes_storage.clone(),
            buckets_metadata_storage,
        },
    };

    let server = Server::builder().add_service(object_storage::c4_server::C4Server::new(handler));

    log::info!("Server started successfully on {}", addr);

    server.serve(addr).await?;

    log::info!("C4 storage node shutdown complete");
    Ok(())
}
