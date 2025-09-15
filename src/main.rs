use std::env;

use log::{info, error, debug};

<<<<<<< HEAD
mod object_storage;
=======
mod api;
mod storage;
>>>>>>> 3c37709 (wip)

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
