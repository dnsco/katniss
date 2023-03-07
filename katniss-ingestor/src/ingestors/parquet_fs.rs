/// Ingestor that allows all disk access
use std::{fs::File, path::Path};

use katniss_pb2arrow::{exports::prost_reflect::DynamicMessage, ArrowBatchProps};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use crate::{arrow::ProtobufBatchIngestor, Result};

use super::BatchIngestor;

/// File Ingestor ingests arrow batches to parquet file, allowing all disk access
pub struct ParquetFileIngestor {
    writer: ArrowWriter<File>,
    ingestor: ProtobufBatchIngestor,
}

impl ParquetFileIngestor {
    pub fn new<P: AsRef<Path>>(arrow_props: &ArrowBatchProps, filename: P) -> Result<Self> {
        let file = File::create(filename)?;

        let writer_props = WriterProperties::builder()
            .set_max_row_group_size(1024 * 10)
            .build();

        let writer = ArrowWriter::try_new(file, arrow_props.schema.clone(), Some(writer_props))?;

        let ingestor = ProtobufBatchIngestor::new(arrow_props)?;

        Ok(Self { writer, ingestor })
    }
}

impl BatchIngestor for ParquetFileIngestor {
    fn ingest(&mut self, packets: Vec<DynamicMessage>) -> Result<()> {
        for packet in packets {
            if let Some(batch) = self.ingestor.ingest_message(packet)? {
                self.writer.write(&batch)?;
            }
        }

        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        let batch = self.ingestor.finish()?;
        self.writer.write(&batch)?;
        let _metadata = self.writer.close()?;
        Ok(())
    }
}
