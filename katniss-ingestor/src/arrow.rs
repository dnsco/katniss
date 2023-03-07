use katniss_pb2arrow::{
    exports::{DynamicMessage, RecordBatch},
    ArrowBatchProps, RecordConverter,
};

pub use crate::Result;

/// Ingests individual Protobuf Messages, and returns a batch if batch_size threshhold is crossed.
pub struct ProtobufBatchIngestor {
    batch_size: usize,
    converter: RecordConverter,
}

impl ProtobufBatchIngestor {
    pub fn new(props: &ArrowBatchProps) -> Result<Self> {
        let batch_size = props.records_per_arrow_batch;

        let converter = RecordConverter::try_new(&props)?;

        Ok(Self {
            batch_size: props.records_per_arrow_batch,
            converter: RecordConverter::try_from(props)?,
        })
    }

    /// Ingests a single message, returns a Record Batch if batch size has been reached
    pub fn ingest_message(&mut self, msg: DynamicMessage) -> Result<Option<RecordBatch>> {
        self.converter.append_message(&msg)?;

        if self.converter.len() >= self.batch_size {
            Ok(Some(self.converter.records()))
        } else {
            Ok(None)
        }
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        let records = self.converter.records();
        Ok(records)
    }

    pub fn len(&self) -> usize {
        self.converter.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
