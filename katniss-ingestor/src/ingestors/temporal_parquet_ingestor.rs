use std::{convert::Infallible, path::Path, thread::JoinHandle};

use chrono::Utc;
use katniss_pb2arrow::{exports::DynamicMessage, ArrowBatchProps};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinSet,
};

use crate::{
    pipeline::{FileSink, ParquetConverter, TemporalRotator},
    Result,
};

pub type LoopHandle = JoinHandle<Result<Infallible>>;

/// Start a pipeline that ingests dynamic messages to parquet filesystem
/// Spins up three threads:
/// - One for ArrowEncoding
/// - One for ParquetEncoding
/// - One for Disk access
///
/// We're using threads because encoding will do a bunch of CPU work at
/// unpredictable times, so I didn't want to block an async executor.
/// If the cost of spinning up three threads and letting the OS schedule it
/// winds up being grateater than the coding work, this can be changed.
///
/// Returns:
/// - a channel that functions as the head of the pipeline
/// - an array of handles to each thread
pub async fn threaded_pipeline<P: AsRef<Path>>(
    props: ArrowBatchProps,
    directory: P,
) -> Result<(UnboundedSender<DynamicMessage>, JoinSet<Result<Infallible>>)> {
    let (head, rx_msg) = unbounded_channel();
    let (mut rotator, rx_buf) = TemporalRotator::try_new(rx_msg, &props, Utc::now())?;
    let (mut converter, rx_bytes) = ParquetConverter::new(rx_buf, props.schema);
    let mut sink = FileSink::new(rx_bytes, directory.as_ref());

    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        loop {
            rotator.process_next().await?
        }
    });
    tasks.spawn(async move {
        loop {
            converter.process_next_buffer().await?;
        }
    });

    tasks.spawn(async move {
        loop {
            sink.sink_next().await?;
        }
    });

    Ok((head, tasks))
}
