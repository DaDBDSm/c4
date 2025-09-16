use std::env;

use log::{debug, error, info};

pub mod api;
pub mod storage;

fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .init();

    info!("Starting application");

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        error!("Usage: {} dir", &args[0]);
        return;
    }
    let data_store_dir = &args[1];

    debug!("Data store dir: {}", data_store_dir)
}
