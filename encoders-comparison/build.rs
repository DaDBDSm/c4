fn main() {
    let proto_file = "src/proto/encoder.proto";
    let out_dir = "src";

    prost_build::Config::new()
        .out_dir(out_dir)
        .compile_protos(&[proto_file], &["src/proto"])
        .expect("Failed to compile protobuf");
}
