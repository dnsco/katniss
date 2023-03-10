/// Ingestion that strictly controls disk access to utlimately allow sending
/// over the wire without ever having touched disk (currently commits to disk)
use katniss_pb2arrow::{
    exports::prost_reflect::{DescriptorPool, DynamicMessage},
    ArrowBatchProps,
};

use crate::{arrow::ProtobufBatchIngestor, parquet::MultiBatchWriter, Result};

/// Buffered ingestor consumes individual packets from an external source
/// e.g. (a socket) and ultimately commits them to a parquet file. Currently
/// this works by waiting for a fixed number of arrow batches to have been
/// ingested, ideally, this would be temporally based. File system access
/// is strictly controlled to allow use at edge on devices with flash drives.
pub struct BufferedParquetIngestor {
    writer: MultiBatchWriter,
    ingestor: ProtobufBatchIngestor,
}

pub struct BufferedIngestorProps<'a> {
    pub pool: DescriptorPool,
    pub msg_name: &'a str,
    pub arrow_record_batch_size: usize,
    pub arrow_batches_per_parquet: usize,
}

impl BufferedParquetIngestor {
    pub fn new(arrow_props: &ArrowBatchProps, props: BufferedIngestorProps) -> Result<Self> {
        let writer = MultiBatchWriter::new(
            "data/parquet".into(),
            arrow_props.schema.clone(),
            props.arrow_batches_per_parquet,
        )?;

        let ingestor = ProtobufBatchIngestor::try_new(arrow_props)?;

        Ok(Self { writer, ingestor })
    }

    pub fn ingest(&mut self, packet: DynamicMessage) -> Result<()> {
        if let Some(batch) = self.ingestor.ingest_message(packet)? {
            self.writer.write_batch(batch)?;
        }

        Ok(())
    }
}
