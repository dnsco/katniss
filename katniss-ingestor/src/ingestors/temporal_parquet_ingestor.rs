use std::convert::Infallible;

use chrono::Utc;
use object_store::ObjectStore;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinSet,
};

use crate::{pipeline::TemporalRotator, Result};
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
    let (mut rotator, _rx_buf) = TemporalRotator::try_new(rx_msg, &props, Utc::now())?;

    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        loop {
            rotator.process_next().await?
        }
    });

    Ok((head, tasks))
}
