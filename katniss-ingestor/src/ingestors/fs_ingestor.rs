use std::fs::File;

use arrow_schema::SchemaRef;

use katniss_pb2arrow::{
    exports::prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor},
    RecordBatchConverter, SchemaConverter,
};
use parquet::{arrow::ArrowWriter, format::FileMetaData};

use crate::{arrow::ProtobufBatchIngestor, Result};

pub struct FsIngestor {
    descriptor: MessageDescriptor,
    writer: ArrowWriter<File>,
    ingestor: ProtobufBatchIngestor,
}

pub struct FsIngestorProps<'a> {
    pub pool: DescriptorPool,
    pub filename: &'a str,
    pub msg_name: &'a str,
    pub arrow_record_batch_size: usize,
}

impl FsIngestor {
    pub fn new(props: FsIngestorProps) -> Result<Self> {
        let converter = SchemaConverter::new(props.pool);
        let schema = SchemaRef::new(
            converter
                .get_arrow_schema(props.msg_name, &[])?
                .ok_or_else(|| {
                    crate::errors::KatinssIngestorError::SchemaNotFound(props.msg_name.to_owned())
                })?,
        );

        let descriptor = converter.get_message_by_name(props.msg_name)?;

        let ingestor = ProtobufBatchIngestor::new(
            RecordBatchConverter::new(schema.clone(), props.arrow_record_batch_size),
            props.arrow_record_batch_size,
        );

        let file = File::create(props.filename)?;
        let writer = ArrowWriter::try_new(file, schema.clone(), None)?;

        Ok(Self {
            descriptor,
            writer,
            ingestor,
        })
    }

    pub fn ingest(&mut self, packets: Vec<DynamicMessage>) -> Result<()> {
        for packet in packets {
            if let Some(batch) = self.ingestor.ingest(packet)? {
                self.writer.write(&batch)?;
            }
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<FileMetaData> {
        let batch = self.ingestor.finish()?;
        self.writer.write(&batch)?;
        let metadata = self.writer.close()?;
        Ok(metadata)
    }

    /// Return a cloned reference of the Protobuf Messagedescriptor for downstream convenience
    pub fn descriptor(&self) -> MessageDescriptor {
        self.descriptor.clone()
    }
}
