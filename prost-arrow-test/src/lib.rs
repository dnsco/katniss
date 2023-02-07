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
    use std::{
        fs::File,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use arrow_schema::SchemaRef;
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use prost::Message;
    use prost_arrow::{RecordBatch, RecordBatchConverter};
    use prost_reflect::{DynamicMessage, MessageDescriptor};

    use super::*;
    use crate::protos::{
        spacecorp::{packet, ClimateStatus, JumpDriveStatus, Packet},
        v3::{
            simple_one_of_message::Inner, Foo, MessageWithNestedEnum, SimpleOneOfMessage,
            SomeRandomEnum,
        },
    };

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
    fn test_simple_oneof() -> Result<()> {
        let simple = SimpleOneOfMessage {
            words: "hullo".into(),
            inner: Some(Inner::Foo({
                Foo {
                    key: 22,
                    str_val: "I'm inside yr enum".into(),
                }
            })),
        };

        let batch = batch_for("eto.pb2arrow.tests.v3.SimpleOneOfMessage", &[simple])?;
        write_batch(batch, "simple_one_of")?;
        Ok(())
    }

    #[test]
    fn test_nested_null_struct() -> Result<()> {
        let packet = Packet {
            msg: Some(packet::Msg::ClimateStatus(ClimateStatus::default())),
            ..Default::default()
        };

        dbg!(&packet);

        let batch = batch_for("eto.pb2arrow.tests.spacecorp.Packet", &[packet])?;
        write_batch(batch, "nested_null_struct")?;
        Ok(())
    }

    #[test]
    fn test_heterogenous_batch() -> Result<()> {
        let packets = [
            Packet {
                msg: Some(packet::Msg::ClimateStatus(ClimateStatus::default())),
                ..Default::default()
            },
            Packet {
                msg: Some(packet::Msg::JumpDriveStatus(JumpDriveStatus::default())),
                ..Default::default()
            },
        ];

        let batch = batch_for("eto.pb2arrow.tests.spacecorp.Packet", &packets)?;
        write_batch(batch, "heterogenous_batch")?;
        Ok(())
    }

    fn batch_for<P: Message>(msg_name: &str, messages: &[P]) -> Result<RecordBatch, anyhow::Error> {
        let mut converter = schema_converter()?.converter_for(msg_name, messages.len())?;
        for m in messages {
            converter.append_message(&to_dynamic(m, msg_name)?)?;
        }

        Ok(converter.records()?)
    }

    fn write_batch(batch: RecordBatch, test_name: &str) -> Result<(), anyhow::Error> {
        let file = timestamped_data_file(test_name)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn timestamped_data_file(test_name: &str) -> Result<File, anyhow::Error> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("data");
        path.push(format!("{test_name}_{now}"));
        path.set_extension("parquet");
        let file = File::create(path)?;
        Ok(file)
    }

    fn to_dynamic<P: Message>(proto: &P, message_name: &str) -> Result<DynamicMessage> {
        let bytes: &[u8] = &proto.encode_to_vec();
        let desc = schema_converter()?.get_message_by_name(message_name)?;
        let message = DynamicMessage::decode(desc, bytes)?;
        Ok(message)
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
