use katniss_pb2arrow::{
    exports::{DynamicMessage, RecordBatch},
    ArrowBatchProps, RecordBatchConverter,
};

pub use crate::Result;
pub struct ProtobufIngestor {
    batch_size: usize,
    converter: RecordBatchConverter,
}

impl ProtobufIngestor {
    pub fn new(props: ArrowBatchProps) -> Result<Self> {
        let batch_size = props.arrow_record_batch_size;

        let converter = RecordBatchConverter::try_new(&props)?;

        Ok(Self {
            batch_size,
            converter,
        })
    }

    /// Ingests a single message, returns a Record Batch if batch size has been reached
    pub fn ingest_message(&mut self, msg: DynamicMessage) -> Result<Option<RecordBatch>> {
        self.converter.append_message(&msg)?;

        if self.converter.len() >= self.batch_size {
            Ok(Some(self.converter.records()?))
        } else {
            Ok(None)
        }
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let records = self.converter.records()?;
        Ok(records)
    }
}
