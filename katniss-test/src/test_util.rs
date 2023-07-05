use std::{any::type_name, path::PathBuf};

use anyhow::Result;
use chrono::Utc;
use prost::Message;
use prost_reflect::DynamicMessage;

use katniss_ingestor::{LanceFsIngestor, TemporalBuffer};
use katniss_pb2arrow::{exports::RecordBatch, ArrowBatchProps, RecordConverter};

use crate::{descriptor_pool, schema_converter};

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

        let props = ArrowBatchProps::try_new(descriptor_pool()?, msg_name.to_owned())?
            .with_records_per_arrow_batch(messages.len());

        let mut converter = RecordConverter::try_new(&props)?;

        for m in messages {
            converter.append_message(&to_dynamic(m, msg_name)?)?;
        }

        Ok(converter.records())
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

pub async fn write_batch(batch: RecordBatch, test_name: &str) -> anyhow::Result<()> {
    let now = Utc::now();
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("tests");
    path.push(test_name);
    path.push(now.to_rfc2822());
    std::fs::create_dir_all(&path)?;

    let ingestor = LanceFsIngestor::new(path, batch.schema())?;

    let mut buffer = TemporalBuffer::new(now);
    buffer.batches = vec![batch];

    ingestor.write(buffer).await?;

    Ok(())
}
