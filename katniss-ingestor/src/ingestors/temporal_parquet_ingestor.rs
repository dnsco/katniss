use std::{convert::Infallible, path::Path};

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

/// Start a pipeline that ingests dynamic messages to parquet filesystem
/// Returns:
/// * a channel that functions as the head of the pipeline
/// * A JoinSet of infinte loops (Infallible used in place of !) for
///     - ArrowEncoding
///     - ParquetEncoding
///     - Disk access
pub async fn parquet_fs_pipeline<P: AsRef<Path>>(
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
