use anyhow::Result;
use prost_arrow::SchemaConverter;
use prost_reflect::DescriptorPool;

pub mod protos {
    pub const FILE_DESCRIPTOR_BYTES: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

    pub mod spacecorp {
        include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.spacecorp.rs"));
    }

    pub mod v2 {
        include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.v2.rs"));
    }

    pub mod v3 {
        include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.v3.rs"));
    }
}

pub fn schema_converter() -> Result<SchemaConverter> {
    let pool = descriptor_pool()?;
    Ok(SchemaConverter::new(pool))
}

fn descriptor_pool() -> Result<DescriptorPool> {
    Ok(DescriptorPool::decode(protos::FILE_DESCRIPTOR_BYTES)?)
}

#[cfg(test)]
mod test {
    use anyhow::{Context, Result};
    use arrow_schema::SchemaRef;
    use prost::Message;
    use prost_arrow::RecordBatchConverter;
    use prost_reflect::{DynamicMessage, MessageDescriptor};

    use super::*;
    use crate::protos::v3::{MessageWithNestedEnum, SomeRandomEnum};

    #[test]
    fn test_enums() -> Result<()> {
        let (arrow_schema, proto_schema) = schemas_for("MessageWithNestedEnum")?;

        let mut converter = RecordBatchConverter::new(arrow_schema, 1);

        let message = MessageWithNestedEnum {
            status: SomeRandomEnum::Failing.into(),
        };

        converter.append_message(&DynamicMessage::decode(
            proto_schema.clone(),
            &message.encode_to_vec() as &[u8],
        )?)?;

        let batch = converter.records()?;
        dbg!(batch);

        Ok(())
    }

    fn schemas_for(short_name: &str) -> Result<(SchemaRef, MessageDescriptor)> {
        let schemas = schema_converter()?;

        let arrow_schema = schemas
            .get_arrow_schema_by_short_name(short_name, &[])?
            .context("No schema found")?;

        let proto_schema = schemas
            .get_message_short_name(short_name)
            .context("no message")?;

        Ok((SchemaRef::new(arrow_schema), proto_schema))
    }
}
