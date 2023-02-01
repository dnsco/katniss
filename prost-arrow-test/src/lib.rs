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
        let (arrow_schema, proto_schema) = first_schema_with("MessageWithNestedEnum")?;

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

    #[test]
    fn test_short_common_shortname_returns_multiples() -> Result<()> {
        let converter = schema_converter()?;

        let protos = converter.get_messages_from_short_name("Bar");
        assert_eq!(2, protos.len());
        assert!(protos.iter().all(|a| a.is_some()));

        let arrow = converter.get_arrow_schemas_by_short_name("Bar", &[])?;
        assert_eq!(2, arrow.len());
        assert!(arrow.iter().all(|a| a.is_some()));

        // TODO: filtering by projectin is weird
        // let projected_arrow = converter.get_arrow_schemas_by_short_name("Bar", &["v3_only"])?;
        // assert_eq!(2, projected_arrow.len());
        // assert_eq!(
        //     1,
        //     projected_arrow
        //         .into_iter()
        //         .filter_map(std::convert::identity) //filter out nones
        //         .count()
        // );

        Ok(())
    }

    fn first_schema_with(short_name: &str) -> Result<(SchemaRef, MessageDescriptor)> {
        let schemas = schema_converter()?;

        let arrow_schema = schemas
            .get_arrow_schemas_by_short_name(short_name, &[])?
            .remove(0)
            .context("No schema found")?;

        let proto_schema = schemas
            .get_messages_from_short_name(short_name)
            .remove(0)
            .context("no message")?;

        Ok((SchemaRef::new(arrow_schema), proto_schema))
    }
}
