use std::path::Path;
use std::sync::Arc;

use katniss_pb2arrow::ArrowBatchProps;
use lance::arrow::RecordBatchBuffer;
use lance::dataset::{Dataset, WriteMode, WriteParams};

use katniss_pb2arrow::exports::prost_reflect::{DescriptorPool, DynamicMessage};
use katniss_pb2arrow::exports::{RecordBatch, RecordBatchReader};
use tokio::runtime::Runtime;

use crate::{arrow::ProtobufBatchIngestor, Result};

use super::BatchIngestor;

pub struct LanceFsIngestor {
    ingestor: ProtobufBatchIngestor,
    rt: Arc<Runtime>,
    filename: String,
    params: WriteParams,
    batches: Vec<RecordBatch>,
}

pub struct LanceFsIngestorProps<'a, P: AsRef<Path>> {
    pub pool: DescriptorPool,
    pub filename: P,
    pub msg_name: &'a str,
    pub arrow_record_batch_size: usize,
}

impl LanceFsIngestor {
    pub fn new<P: AsRef<Path>>(props: &ArrowBatchProps, filename: P) -> Result<Self> {
        let ingestor = ProtobufBatchIngestor::try_new(props)?;
        let filename = filename.as_ref().to_str().unwrap().to_string();

        let params = lance::dataset::WriteParams {
            max_rows_per_group: 1024 * 10,
            mode: WriteMode::Create,
            ..Default::default()
        };

        let rt = Arc::new(Runtime::new()?);

        let batches: Vec<RecordBatch> = Vec::new();

        Ok(Self {
            ingestor,
            rt,
            filename,
            params,
            batches,
        })
    }

    pub fn write(&self, batch: RecordBatch) -> Result<()> {
        let mut reader: Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(vec![batch]));
        self.rt
            .block_on(async {
                Dataset::write(&mut reader, self.filename.as_ref(), Some(self.params)).await
            })
            .unwrap(); //FIX unwrap
        Ok(())
    }
}

impl BatchIngestor for LanceFsIngestor {
    fn ingest(&mut self, packets: Vec<DynamicMessage>) -> Result<()> {
        for packet in packets {
            if let Some(batch) = self.ingestor.ingest_message(packet)? {
                self.batches.push(batch);
                // self.write(batch)?;
            }
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        let batch = self.ingestor.finish()?;
        if batch.num_rows() > 0 {
            self.batches.push(batch);
        }

        let mut reader: Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(self.batches));
        self.rt
            .block_on(async {
                Dataset::write(&mut reader, self.filename.as_ref(), Some(self.params)).await
            })
            .unwrap(); //FIX unwrap

        Ok(())
    }
}
