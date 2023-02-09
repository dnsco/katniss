use katniss_pb2arrow::{
    exports::{DynamicMessage, RecordBatch},
    RecordBatchConverter,
};

pub use crate::Result;
pub struct ProtobufBatchIngestor {
    batch_size: usize,
    converter: RecordBatchConverter,
}

impl ProtobufBatchIngestor {
    pub fn new(converter: RecordBatchConverter, batch_size: usize) -> Self {
        Self {
            batch_size,
            converter,
        }
    }
    pub fn ingest(&mut self, msg: DynamicMessage) -> Result<Option<RecordBatch>> {
        self.converter.append_message(&msg)?;

        if self.converter.len() >= self.batch_size {
            Ok(Some(self.converter.records()?))
        } else {
            Ok(None)
        }
    }
}
