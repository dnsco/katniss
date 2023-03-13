use std::{
    convert::Infallible,
    path::Path,
    sync::mpsc::{channel, Sender},
    thread::{self, JoinHandle},
};

use chrono::Utc;
use katniss_pb2arrow::{exports::DynamicMessage, ArrowBatchProps};

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
pub fn threaded_pipeline<P: AsRef<Path>>(
    props: ArrowBatchProps,
    directory: P,
) -> Result<(Sender<DynamicMessage>, [LoopHandle; 3])> {
    let (head, rx_msg) = channel();
    let (mut rotator, rx_buf) = TemporalRotator::try_new(rx_msg, &props, Utc::now())?;
    let (mut converter, rx_bytes) = ParquetConverter::new(rx_buf, props.schema);
    let sink = FileSink::new(rx_bytes, directory.as_ref());

    let rotator_handle = thread::spawn(move || -> Result<Infallible> {
        loop {
            rotator.process_next()?;
        }
    });

    let converter_handle = thread::spawn(move || -> Result<Infallible> {
        loop {
            converter.process_next_buffer()?;
        }
    });

    let sink_handle = thread::spawn(move || -> Result<Infallible> {
        loop {
            sink.sink_next()?;
        }
    });

    Ok((head, [rotator_handle, converter_handle, sink_handle]))
}
