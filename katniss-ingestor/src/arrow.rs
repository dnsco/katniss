use katniss_pb2arrow::{
    exports::{DynamicMessage, RecordBatch},
    RecordBatchConverter,
};

pub use crate::Result;
struct ArrowBatchIngestor {
    batch_size: usize,
    converter: RecordBatchConverter,
}

impl ArrowBatchIngestor {
    fn ingest(&mut self, msg: DynamicMessage) -> Result<Option<RecordBatch>> {
        self.converter.append_message(&msg)?;
        if self.converter.len() >= self.batch_size {
            Ok(Some(self.converter.records()?))
        } else {
            Ok(None)
        }
    }
}
