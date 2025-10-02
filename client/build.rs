extern crate prost_build;

fn main() {
    let mut config = prost_build::Config::new();
    config.out_dir("src/proto");

    println!("cargo:rerun-if-changed=src/proto/object_storage.proto");
    println!("cargo:rerun-if-changed=build.rs");

    config
        .compile_protos(&["src/proto/object_storage.proto"], &["src/"])
        .unwrap();
}
