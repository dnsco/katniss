use std::convert::Infallible;
use std::path::Path;

use chrono::Utc;
use lance::arrow::RecordBatchBuffer;
use lance::dataset::{Dataset, WriteMode};
use object_store::ObjectStore;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinSet,
};

use katniss_pb2arrow::exports::prost_reflect::DynamicMessage;
use katniss_pb2arrow::exports::{RecordBatch, RecordBatchReader};
use katniss_pb2arrow::ArrowBatchProps;

use crate::timestuff::TemporalRotator;
use crate::{arrow::ProtobufBatchIngestor, Result};

/// Set Of Tokio Tasks that never return unless they error
pub type LoopJoinset = JoinSet<Result<Infallible>>; // (Infallible used in place of !)

/// Start a pipeline that ingests dynamic messages to Lance
/// Returns:
/// * a channel that functions as the head of the pipeline
/// * A Set of Infinite Loop Futures for:
///     - ArrowEncoding
///     - ParquetEncoding
///     - Disk access
pub async fn lance_ingestion_pipeline(
    props: ArrowBatchProps,
    object_store: Box<dyn ObjectStore>, // this should probably be some sort of lance or gcp props or somethingh
) -> Result<(UnboundedSender<DynamicMessage>, LoopJoinset)> {
    let (head, rx_msg) = unbounded_channel();
    let (mut rotator, _rx_buf) = TemporalRotator::try_new(rx_msg, &props, Utc::now())?;

    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        loop {
            rotator.process_next().await?
        }
    });

    /// SPIN UP TASK TAT READS FROM rx_buf and writes to lance
    Ok((head, tasks))
}

/// has most of the clues for working with lance but probably shouldn't be worked with
/// directly without modification
pub struct LanceFsIngestor {
    ingestor: ProtobufBatchIngestor,
    filename: String,
}

impl LanceFsIngestor {
    pub fn new<P: AsRef<Path>>(props: &ArrowBatchProps, filename: P) -> Result<Self> {
        let ingestor = ProtobufBatchIngestor::try_new(props)?;
        let filename = filename.as_ref().to_str().unwrap().to_string();

        Ok(Self { ingestor, filename })
    }

    // this function probably wants to take TemporalBuffers, which are essentially Vec<RecordBatch>
    // this could probably also return a lance Dataset or if we do the append stuff you could look
    // at the dataset that should be stored on this struct
    pub async fn write(&self, batch: RecordBatch) -> Result<()> {
        let mut reader: Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(vec![batch]));

        // todo this thing makes a new file every time,
        // this struct should probably hold a lance ```Dataset``` that it appends to
        let params = lance::dataset::WriteParams {
            max_rows_per_group: 1024 * 10,
            mode: WriteMode::Create,
            ..Default::default()
        };

        Dataset::write(&mut reader, self.filename.as_ref(), Some(params))
            .await
            .expect("THIS SHOULD BE HANDLED BETTERLOLOL");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline() {
        // make a pipeline
        // feed it some records
        // drop the pipeline (we currently drop data that hasn't ended temporal window and we don't want to do that)
        // read lance from like filesystem or something and assert it has the number of records
    }
}
