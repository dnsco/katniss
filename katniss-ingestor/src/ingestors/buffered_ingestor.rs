use arrow_schema::SchemaRef;

use katniss_pb2arrow::{
    exports::prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor},
    RecordBatchConverter, SchemaConverter,
};

use crate::{arrow::ProtobufBatchIngestor, parquet::MultiBatchWriter, Result};

pub struct BufferedArrowParquetFileIngestor {
    descriptor: MessageDescriptor,
    writer: MultiBatchWriter,
    ingestor: ProtobufBatchIngestor,
}

pub struct BufferedIngestorProps<'a> {
    pub pool: DescriptorPool,
    pub msg_name: &'a str,
    pub arrow_record_batch_size: usize,
    pub arrow_batches_per_parquet: usize,
}

impl BufferedArrowParquetFileIngestor {
    pub fn new(props: BufferedIngestorProps) -> Result<Self> {
        let converter = SchemaConverter::new(props.pool);
        let schema = SchemaRef::new(
            converter
                .get_arrow_schema(props.msg_name, &[])?
                .ok_or_else(|| {
                    crate::errors::KatinssIngestorError::SchemaNotFound(props.msg_name.to_owned())
                })?,
        );

        let descriptor = converter.get_message_by_name(props.msg_name)?;
        let writer = MultiBatchWriter::new(
            "data/parquet".into(),
            schema.clone(),
            props.arrow_batches_per_parquet,
        )?;
        let ingestor = ProtobufBatchIngestor::new(
            RecordBatchConverter::try_new(schema.clone(), props.arrow_record_batch_size)?,
            props.arrow_record_batch_size,
        );

        Ok(Self {
            descriptor,
            writer,
            ingestor,
        })
    }

    pub fn ingest(&mut self, packet: DynamicMessage) -> Result<()> {
        if let Some(batch) = self.ingestor.ingest(packet)? {
            self.writer.write_batch(batch)?;
        }

        Ok(())
    }

    /// Return a cloned reference of the Protobuf Messagedescriptor for downstream convenience
    pub fn descriptor(&self) -> MessageDescriptor {
        self.descriptor.clone()
    }
}
