fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("src")
        .compile_protos(&["src/proto/c4.proto"], &[])?;
    Ok(())
}
