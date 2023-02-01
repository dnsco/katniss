use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["version_3.proto"], &["../protos/test"])?;
    Ok(())
}
