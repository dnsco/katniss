use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(
        &["space_corp.proto", "version_2.proto", "version_3.proto"],
        &["../protos/test"],
    )?;
    Ok(())
}
