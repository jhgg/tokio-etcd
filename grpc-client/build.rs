fn main() -> Result<(), Box<dyn std::error::Error>> {
    // println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile(
            &["proto/auth.proto", "proto/kv.proto", "proto/rpc.proto"],
            &["proto/"],
        )?;
    Ok(())
}
