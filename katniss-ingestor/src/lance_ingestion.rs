use std::{convert::Infallible, sync::Arc};

use arrow_array::RecordBatchIterator;
use arrow_schema::Schema;
use chrono::Utc;
use lance::dataset::{Dataset, WriteMode, WriteParams};

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::{block_in_place, JoinSet},
};

use katniss_pb2arrow::exports::prost_reflect::DynamicMessage;
use katniss_pb2arrow::ArrowBatchProps;

use crate::errors::KatinssIngestorError;
use crate::temporal_rotator::{TemporalBuffer, TemporalRotator};
use crate::Result;

/// Set Of Tokio Tasks that never return unless they error
pub type LoopJoinSet = JoinSet<Result<Infallible>>; // (Infallible used in place of !)

/// Start a pipeline that ingests dynamic messages to Lance
/// Returns:
/// * a channel that functions as the head of the pipeline
/// * A Set of Infinite Loop Futures for:
///     - ArrowEncoding
///     - Disk Encoding (i.e. Lance)
pub async fn lance_ingestion_pipeline(
    props: ArrowBatchProps,
    batch_period: std::time::Duration,
    storage_uri: String, // object_store: Box<dyn ObjectStore>, // this should probably be some sort of lance or gcp props or something
) -> Result<(UnboundedSender<DynamicMessage>, LoopJoinSet)> {
    let now = Utc::now();
    let mut rotator = TemporalRotator::new(&props, now, batch_period)?;

    let (head, mut rx_msg) = unbounded_channel();
    let (tx_buffer, mut rx_buffer) = unbounded_channel();
    let ingestor = LanceIngestor::new(storage_uri, props.schema)?;

    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        loop {
            let msg = rx_msg
                .recv()
                .await
                .ok_or_else(|| KatinssIngestorError::PipelineClosed)?;

            if let Some(last_batch) =
                block_in_place(|| rotator.ingest_potentially_blocking(msg, Utc::now()))?
            {
                tx_buffer
                    .send(last_batch)
                    .map_err(|_| KatinssIngestorError::PipelineClosed)?;
            }
        }
    });

    tasks.spawn(async move {
        loop {
            let buf = rx_buffer
                .recv()
                .await
                .ok_or_else(|| KatinssIngestorError::PipelineClosed)?;

            ingestor.write(buf).await?;
        }
    });

    Ok((head, tasks))
}

pub struct LanceIngestor {
    ///object-store formatted uri i.e gcp:// or file://
    storage_uri: String,
    write_params: WriteParams,
    schema: Arc<Schema>,
}

impl LanceIngestor {
    pub fn new<P: AsRef<str>>(storage_uri: P, schema: Arc<Schema>) -> Result<Self> {
        let filename = storage_uri.as_ref().to_string();
        let write_params = WriteParams {
            max_rows_per_group: 1024 * 10,
            mode: WriteMode::Append,
            ..Default::default()
        };

        Ok(Self {
            storage_uri: filename,
            write_params,
            schema,
        })
    }

    pub async fn write(&self, buffer: TemporalBuffer) -> Result<Dataset> {
        let reader =
            RecordBatchIterator::new(buffer.batches.into_iter().map(Ok), self.schema.clone());

        let dataset =
            Dataset::write(reader, self.storage_uri.as_ref(), Some(self.write_params)).await?;

        Ok(dataset)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::AtomicI64;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use chrono::DateTime;
    use tokio::{select, spawn, task::yield_now};

    use katniss_pb2arrow::exports::prost_reflect::prost::Message;
    use katniss_test::{descriptor_pool, protos::spacecorp::Timestamp, test_util::ProtoBatch};

    use crate::temporal_rotator::timestamp_string;

    use super::*;

    fn timestamp_encoding_props() -> ArrowBatchProps {
        let pool = descriptor_pool().unwrap();
        let msg_name = "eto.pb2arrow.tests.spacecorp.Timestamp";
        ArrowBatchProps::try_new(pool, msg_name.to_string()).unwrap()
    }

    // Alter our tests to maybe force our exploration of lance apis
    // we want to figure out how lance does gcp stuff?
    // maybe do some in memory stuff for tests
    // lance has a concept of a dataset that should be storage-agnostic
    #[tokio::test]
    async fn test_lance_ingestor() -> anyhow::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_micros()
            .to_string();

        // does this batch's structure mean we can eject ArrowProps?  currently not in use.
        let batch = ProtoBatch::SpaceCorp(&[
            Timestamp::default(),
            Timestamp::default(),
            Timestamp::default(),
        ])
        .arrow_batch()?;

        let schema = batch.schema();
        let mut filename = std::env::current_dir()?;
        filename.push(format!("test_lance_ingestor_{now}.lance"));

        let ingestor =
            LanceIngestor::new(format!("file://{}", filename.to_str().unwrap()), schema)?;

        let buffer: TemporalBuffer = TemporalBuffer {
            begin_at: Utc::now(),
            end_at: Utc::now(),
            batches: vec![batch],
        }; // make temporal buffer here, that will record arrow record batches

        let dataset = ingestor.write(buffer).await?;
        assert_eq!(dataset.count_rows().await?, 3);

        let protos = &[Timestamp::default(), Timestamp::default()];
        let buffer = temporal_buffer(ProtoBatch::SpaceCorp(protos), Utc::now(), Utc::now())?;
        let dataset = ingestor.write(buffer).await?;
        assert_eq!(dataset.count_rows().await?, 5);

        let protos = &[Timestamp::default()];
        let buffer = temporal_buffer(ProtoBatch::SpaceCorp(protos), Utc::now(), Utc::now())?;
        let dataset = ingestor.write(buffer).await?;
        assert_eq!(dataset.count_rows().await?, 6);

        Ok(())
    }

    fn temporal_buffer<'a, T: Message>(
        protos: ProtoBatch<'a, T>,
        begin_at: DateTime<Utc>,
        end_at: DateTime<Utc>,
    ) -> anyhow::Result<TemporalBuffer> {
        Ok(TemporalBuffer {
            begin_at,
            end_at,
            batches: vec![protos.arrow_batch()?],
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_pipeline() -> anyhow::Result<()> {
        // make a pipeline
        // feed it some records
        // drop the pipeline (we currently drop data that hasn't ended temporal window and we don't want to do that)
        // read lance from like filesystem or something and assert it has the number of records

        let arrow_props = timestamp_encoding_props();
        let descriptor = arrow_props.descriptor.clone();
        let now = Utc::now();
        let timestamp = timestamp_string(now);

        let mut storage_path = std::env::current_dir()?;
        storage_path.push(format!("test_pipeline_{timestamp}.lance"));
        let storage_path_str = storage_path.to_str().unwrap();
        let storage_uri = format!("file://{}", storage_path_str);

        let (head, mut tasks) =
            lance_ingestion_pipeline(arrow_props, Duration::from_millis(5), storage_uri.clone())
                .await
                .unwrap();

        let sent = AtomicI64::new(0);
        spawn(async move {
            loop {
                let msg = DynamicMessage::decode(
                    descriptor.clone(),
                    &Timestamp::system_now().encode_to_vec()[..],
                )
                .unwrap();
                head.send(msg).unwrap();

                sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                yield_now().await
            }
        });

        //TODO: Make temporal rotator take a time scale (currently hardcoded to 1 minute, make it be perhaps 5ms for test?)
        // after timeframe of test has been reduced to a minute we should be writing to file system
        // we can then make write location configurable

        // Wait 10 milliseconds for pipeline to do pipeline stuff
        select! {
            () = tokio::time::sleep(Duration::from_millis(10)) => (),
            _ = tasks.join_next() => (),
        };

        assert!(Path::new(storage_path_str).is_dir());

        let mut manifest_path = storage_path.clone();
        manifest_path.push("_latest.manifest");
        block_until_file_exists(manifest_path.to_str().unwrap(), Duration::from_millis(1000));

        let dataset = Dataset::open(&storage_uri).await.unwrap();
        let row_count = dataset.count_rows().await.unwrap() as i32;
        assert!(row_count > 20); // make a better assertion after cancellation

        // The current lance Read docs don't compile, should figure out what's wrong
        // and make a PR

        Ok(())

        //TODO: Read entire lance dataset and ensure that number of rows = sent
    }

    fn block_until_file_exists(path: &str, timeout: Duration) -> bool {
        // todo: see if this can be done nicely with std lib instead of chrono
        let end_at = Utc::now() + chrono::Duration::from_std(timeout).unwrap();

        while end_at > Utc::now() {
            if Path::new(path).is_file() {
                return true;
            }
        }

        return false;
    }
}
