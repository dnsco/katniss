use anyhow::Result;
use katniss_pb2arrow::SchemaConverter;
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
mod test_util {
    use std::{any::type_name, path::PathBuf};

    use anyhow::Result;
    use katniss_parquet::MultiBatchWriter;
    use prost::Message;
    use prost_reflect::DynamicMessage;

    use katniss_pb2arrow::RecordBatch;

    use super::*;

    pub enum ProtoBatch<'a, T: Message> {
        #[allow(unused)]
        V2(&'a [T]),
        V3(&'a [T]),
        SpaceCorp(&'a [T]),
    }

    impl<'a, T: Message> ProtoBatch<'a, T> {
        pub fn arrow_batch(self) -> anyhow::Result<RecordBatch> {
            let messages = self.messages();
            let msg_name = &self.msg_name();

            let mut converter = schema_converter()?.converter_for(msg_name, messages.len())?;
            for m in messages {
                converter.append_message(&to_dynamic(m, msg_name)?)?;
            }

            Ok(converter.records()?)
        }

        fn msg_name(&self) -> String {
            let package_name = match self {
                Self::V2(_) => "eto.pb2arrow.tests.v2",
                Self::V3(_) => "eto.pb2arrow.tests.v3",
                Self::SpaceCorp(_) => "eto.pb2arrow.tests.spacecorp",
            };

            let message_name = type_name_of_val(&self.messages()[0])
                .split("::")
                .last()
                .unwrap();

            format!("{package_name}.{message_name}")
        }

        fn messages(&self) -> &'a [T] {
            let (ProtoBatch::V2(messages)
            | ProtoBatch::V3(messages)
            | ProtoBatch::SpaceCorp(messages)) = self;

            messages
        }
    }

    pub fn to_dynamic<P: Message>(proto: &P, message_name: &str) -> Result<DynamicMessage> {
        let bytes: &[u8] = &proto.encode_to_vec();
        let desc = schema_converter()?.get_message_by_name(message_name)?;
        let message = DynamicMessage::decode(desc, bytes)?;
        Ok(message)
    }

    fn type_name_of_val<T: ?Sized>(_val: &T) -> &'static str {
        type_name::<T>()
    }

    pub fn write_batch(batch: RecordBatch, test_name: &str) -> anyhow::Result<()> {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("data");
        path.push("tests");
        path.push(test_name);
        std::fs::create_dir_all(&path)?;

        let mut writer = MultiBatchWriter::new(path, batch.schema(), 1)?;
        writer.write_batch(batch)?;
        writer.finalize_and_advance()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use super::*;
    use crate::protos::{
        spacecorp::{packet, ClimateStatus, JumpDriveStatus, Packet},
        v3::{
            simple_one_of_message::Inner, Foo, InnerUnitMessage, MessageWithNestedEnum,
            SimpleOneOfMessage, SomeRandomEnum, UnitContainer,
        },
    };
    use test_util::*;

    #[test]
    fn test_nested_unit_message() -> Result<()> {
        let batch = ProtoBatch::V3(&[
            UnitContainer {
                inner: Some(InnerUnitMessage {}),
            },
            UnitContainer { inner: None },
        ])
        .arrow_batch()?;
        write_batch(batch, "inner_unit")?;
        Ok(())
    }

    #[test]
    fn test_base_unit_messages() -> Result<()> {
        let batch = ProtoBatch::V3(&[InnerUnitMessage {}]).arrow_batch()?;
        write_batch(batch, "inner_unit")?;
        Ok(())
    }

    #[test]
    fn test_enums() -> Result<()> {
        let enum_message = MessageWithNestedEnum {
            status: SomeRandomEnum::Failing.into(),
        };

        let batch = ProtoBatch::V3(&[enum_message]).arrow_batch()?;
        write_batch(batch, "enums")?;
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

        let batch = ProtoBatch::V3(&[simple]).arrow_batch()?;
        write_batch(batch, "simple_one_of")?;
        Ok(())
    }

    #[test]
    fn test_nested_null_struct() -> Result<()> {
        let packet = Packet {
            msg: Some(packet::Msg::ClimateStatus(ClimateStatus::default())),
            ..Default::default()
        };

        let batch = ProtoBatch::SpaceCorp(&[packet]).arrow_batch()?;
        write_batch(batch, "nested_null_struct")?;
        Ok(())
    }

    #[test]
    fn test_heterogenous_batch() -> Result<()> {
        let batch = ProtoBatch::SpaceCorp(&[
            Packet {
                msg: Some(packet::Msg::ClimateStatus(ClimateStatus::default())),
                ..Default::default()
            },
            Packet {
                msg: Some(packet::Msg::JumpDriveStatus(JumpDriveStatus::default())),
                ..Default::default()
            },
        ])
        .arrow_batch()?;
        write_batch(batch, "heterogenous_batch")?;
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

        // TODO: filtering by projection is weird
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
}
