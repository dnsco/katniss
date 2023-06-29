use std::convert::Infallible;
use std::path::Path;

use chrono::Utc;
use lance::arrow::RecordBatchBuffer;
use lance::dataset::{Dataset, WriteMode};

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinSet,
};

use katniss_pb2arrow::exports::prost_reflect::DynamicMessage;
use katniss_pb2arrow::exports::RecordBatchReader;
use katniss_pb2arrow::ArrowBatchProps;

use crate::timestuff::{TemporalBuffer, TemporalRotator};
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
    // object_store: Box<dyn ObjectStore>, // this should probably be some sort of lance or gcp props or something
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
    pub async fn write(&self, buffer: TemporalBuffer) -> Result<Dataset> {
        let mut reader: Box<dyn RecordBatchReader> =
            Box::new(RecordBatchBuffer::new(buffer.batches));

        // todo this thing makes a new file every time,
        // this struct should probably hold a lance ```Dataset``` that it appends to
        let params = lance::dataset::WriteParams {
            max_rows_per_group: 1024 * 10,
            mode: WriteMode::Append,
            ..Default::default()
        };

        let dataset = Dataset::write(&mut reader, self.filename.as_ref(), Some(params))
            .await
            .expect("THIS SHOULD BE HANDLED BETTERLOLOL");

        Ok(dataset)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use katniss_test::{descriptor_pool, protos::spacecorp::Timestamp, test_util::ProtoBatch};

    use super::*;

    fn timestamp_encoding_props() -> ArrowBatchProps {
        let pool = descriptor_pool().unwrap();
        let msg_name = "eto.pb2arrow.tests.spacecorp.Timestamp";
        let arrow_props = ArrowBatchProps::try_new(pool, msg_name.to_string()).unwrap();
        arrow_props
    }

    // Alter our tests to maybe force our exploration of lance apis
    // we want to figure out how lance does gcp stuff?
    // maybe do some in memory stuff for tests
    // lance has a concept of a dataset that should be storage-agnostic
    #[tokio::test]
    async fn test_lance_ingestor() -> anyhow::Result<()> {
        let arrow_props = timestamp_encoding_props();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_micros()
            .to_string();

        let ingestor = LanceFsIngestor::new(&arrow_props, format!("test_{now}.lance"))?;

        let batch = ProtoBatch::SpaceCorp(&[
            Timestamp::default(),
            Timestamp::default(),
            Timestamp::default(),
        ])
        .arrow_batch()?;

        let buffer: TemporalBuffer = TemporalBuffer {
            begin_at: Utc::now(),
            end_at: Utc::now(),
            batches: vec![batch],
        }; // make temporal buffer here, that will record arrow record batches

        let dataset = ingestor.write(buffer).await?;
        assert_eq!(dataset.count_rows().await?, 3);

        let batch =
            ProtoBatch::SpaceCorp(&[Timestamp::default(), Timestamp::default()]).arrow_batch()?;

        let buffer: TemporalBuffer = TemporalBuffer {
            begin_at: Utc::now(),
            end_at: Utc::now(),
            batches: vec![batch],
        }; // make temporal buffer here, that will record arrow record batches

        let dataset = ingestor.write(buffer).await?;
        assert_eq!(dataset.count_rows().await?, 5);

        Ok(())
    }

    #[ignore = "This can only be integration tested and that is sad and I don't like being sad"]
    #[tokio::test]
    async fn test_pipeline() {
        // make a pipeline
        // feed it some records
        // drop the pipeline (we currently drop data that hasn't ended temporal window and we don't want to do that)
        // read lance from like filesystem or something and assert it has the number of records

        let arrow_props = timestamp_encoding_props();
        let (head, _tasks) = lance_ingestion_pipeline(arrow_props).await.unwrap();

        // we have a pipeline for sending messages about spacecorps' Timestamp message
        // how do we create and send messages in test: protubuf -> arrow -> lance?
        // how should "some sort of lance or gcp props or something" interact with the pipeline? maybe the deleted parquet code has a hint
        // what is the relationship between the pipeline and the ingestor/(owner of write())?
    }
}
