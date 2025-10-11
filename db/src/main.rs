use grpc_server::object_storage;
use db::api::grpc::C4Handler;

use std::{error::Error, net::SocketAddr};
use tonic::transport::Server;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr: SocketAddr = "0.0.0.0:50051".parse().unwrap();

    println!("Running server on {addr}");

    let handler = C4Handler {
        c4_storage: db::storage::simple::ObjectStorageSimple {
            base_dir: std::path::PathBuf::from("/tmp/c4_storage"),
            file_manager: db::storage::simple::file::FileManager::new(10 * 1024 * 1024, 1024),
        },
    };

    Server::builder()
        .add_service(object_storage::c4_server::C4Server::new(handler))
        .serve(addr)
        .await?;

    Ok(())
}
