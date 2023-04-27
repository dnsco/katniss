use std::convert::Infallible;

use chrono::Utc;
use object_store::ObjectStore;
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
pub async fn parquet_object_store_pipeline(
    props: ArrowBatchProps,
    object_store: Box<dyn ObjectStore>,
) -> Result<(UnboundedSender<DynamicMessage>, LoopJoinset)> {
    let (head, rx_msg) = unbounded_channel();
    let (mut rotator, rx_buf) = TemporalRotator::try_new(rx_msg, &props, Utc::now())?;
    let (mut converter, rx_bytes) = ParquetConverter::new(rx_buf, props.schema);
    let mut sink = StoreSink::new(rx_bytes, object_store);

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
