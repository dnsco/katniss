use std::path::PathBuf;

use anyhow::{Context, Result};

use arrow_schema::SchemaRef;
use prost_arrow::SchemaConverter;
use prost_reflect::MessageDescriptor;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.rs"));
}

#[allow(unused)]
fn schemas_for(proto_file: &str, short_name: &str) -> Result<(SchemaRef, MessageDescriptor)> {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("../protos/test");

    let proto = d.join(proto_file);
    let schemas = SchemaConverter::compile(&[proto], &[d])
        .context(format!("Failed to compile {proto_file}"))?;

    let arrow_schema = schemas
        .get_arrow_schema_by_short_name(short_name, &[])?
        .context("No schema found")?;

    let proto_schema = schemas
        .get_message_short_name(short_name)
        .context("no message")?;

    Ok((SchemaRef::new(arrow_schema), proto_schema))
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use prost::Message;
    use prost_arrow::RecordBatchConverter;
    use prost_reflect::{DynamicMessage, Value};

    use crate::schemas_for;

    use super::protos::{MessageWithNestedEnum, SomeRandomEnum};

    #[test]
    fn test_enums() -> Result<()> {
        let (arrow_schema, proto_schema) = schemas_for("version_3.proto", "MessageWithNestedEnum")?;

        let mut converter = RecordBatchConverter::new(arrow_schema, 1);

        // let message = MessageWithNestedEnum {
        //     status: SomeRandomEnum::Failing.into(),
        // };
        // let proto_bytes: &[u8] = &message.encode_to_vec();
        // dbg!(proto_bytes);
        // let dynamic_message = DynamicMessage::decode(proto_schema.clone(), proto_bytes)?;
        let mut dynamic_message = DynamicMessage::new(proto_schema);
        dynamic_message.set_field_by_name("status", Value::EnumNumber(2));
        converter.append_message(&dynamic_message)?;

        let batch = converter.records()?;
        dbg!(batch);

        Ok(())
    }
}
