fn main() {
    let proto_file = "src/proto/encoder.proto";
    let out_dir = "src";

    // Generate Rust code from protobuf
    prost_build::Config::new()
        .out_dir(out_dir)
        .compile_protos(&[proto_file], &["src/proto"])
        .expect("Failed to compile protobuf");
}
