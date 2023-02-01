use std::env;
use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set.bin"),
        )
        .compile_protos(
            &["spacecorp.proto", "version_2.proto", "version_3.proto"],
            &["../protos/test"],
        )?;
    Ok(())
}
