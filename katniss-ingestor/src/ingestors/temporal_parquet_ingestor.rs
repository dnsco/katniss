use std::{convert::Infallible, path::Path};

use chrono::Utc;
use object_store::local::LocalFileSystem;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinSet,
};

use crate::{
    pipeline::{ParquetConverter, StoreSink, TemporalRotator},
    Result,
};
use katniss_pb2arrow::{exports::DynamicMessage, ArrowBatchProps};

/// Set Of Tokio Tasks that never return unless they error
pub type LoopJoinset = JoinSet<Result<Infallible>>; // (Infallible used in place of !)

/// Start a pipeline that ingests dynamic messages to parquet filesystem
/// Returns:
/// * a channel that functions as the head of the pipeline
/// * A Set of Infinite Loop Futures for:
///     - ArrowEncoding
///     - ParquetEncoding
///     - Disk access
pub async fn parquet_fs_pipeline<P: AsRef<Path>>(
    props: ArrowBatchProps,
    directory: P,
) -> Result<(UnboundedSender<DynamicMessage>, LoopJoinset)> {
    let (head, rx_msg) = unbounded_channel();
    let (mut rotator, rx_buf) = TemporalRotator::try_new(rx_msg, &props, Utc::now())?;
    let (mut converter, rx_bytes) = ParquetConverter::new(rx_buf, props.schema);
    let store = LocalFileSystem::new_with_prefix(directory.as_ref())?;
    let mut sink = StoreSink::new(rx_bytes, Box::new(store));

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
