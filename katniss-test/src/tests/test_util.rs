use std::{any::type_name, path::PathBuf};

use anyhow::Result;
use katniss_ingestor::parquet::MultiBatchWriter;
use prost::Message;
use prost_reflect::DynamicMessage;

use katniss_pb2arrow::exports::RecordBatch;

use crate::schema_converter;

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
        let (ProtoBatch::V2(messages) | ProtoBatch::V3(messages) | ProtoBatch::SpaceCorp(messages)) =
            self;

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
    Ok(())
}
