use clap::Parser;
use db::api::grpc::C4Handler;
use db::storage::simple::buckets_metadata_storage::BucketsMetadataStorage;
use db::storage::simple::chunk_file_storage::PartitionedBytesStorage;
use grpc_server::object_storage;
use std::{error::Error, net::SocketAddr};
use tokio::signal;
use tonic::transport::Server;

/// Storage node for distributed object storage
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
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Args::parse();

    let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;

    if let Some(node_id) = &args.node_id {
        log::info!("Starting C4 storage node {} on {}", node_id, addr);
    } else {
        log::info!("Starting C4 storage node on {}", addr);
    }

    let base_dir = std::path::PathBuf::from(&args.base_dir);

    // Create base directory if it doesn't exist
    if !base_dir.exists() {
        log::info!("Creating base directory: {:?}", base_dir);
        std::fs::create_dir_all(&base_dir)?;
    }

    let bytes_storage = PartitionedBytesStorage::new_with_persistence(base_dir.join("data"), 4)
        .await
        .expect("Failed to create partitioned bytes storage");
    let buckets_metadata_storage =
        BucketsMetadataStorage::new(base_dir.join("metadata.json").to_string_lossy().to_string())
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

    // Setup graceful shutdown
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let mut server_handle = tokio::spawn(async move {
        if let Err(e) = server
            .serve_with_shutdown(addr, async {
                rx.await.ok();
            })
            .await
        {
            log::error!("Server error: {}", e);
        }
    });

    // Wait for shutdown signal
    let shutdown_reason = tokio::select! {
        _ = signal::ctrl_c() => {
            log::info!("Received shutdown signal, saving indexes...");
            "ctrl_c"
        }
        _result = &mut server_handle => {
            log::info!("Server stopped unexpectedly");
            "server_stopped"
        }
    };

    // Save indexes before shutdown (only if we received ctrl_c)
    if shutdown_reason == "ctrl_c" {
        log::info!("Saving partition indexes...");
        if let Err(e) = bytes_storage.save_indexes().await {
            log::error!("Failed to save partition indexes: {}", e);
        } else {
            log::info!("Successfully saved partition indexes");
        }
    }

    // Signal server to shutdown (if it's still running)
    let _ = tx.send(());

    // Wait for server to finish
    let _ = server_handle.await;

    log::info!("C4 storage node shutdown complete");
    Ok(())
}
