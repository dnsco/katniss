use std::{
    fmt::Debug,
    sync::mpsc::{RecvError, SendError},
    time::SystemTimeError,
};

use chrono::OutOfRangeError;
use katniss_pb2arrow::KatnissArrowError;
use thiserror::Error;

use crate::temporal_rotator::TemporalBuffer;

#[derive(Error, Debug)]
pub enum KatinssIngestorError {
    #[error("Pipeline Clog: {0}")]
    BufferRecv(#[from] RecvError),

    #[error("Io Errror")]
    IoError(#[from] std::io::Error),

    #[error("Lance Error: {0}")]
    LanceError(#[from] lance::Error),

    #[error("Something: {0}")]
    NegativeDurationError(#[from] OutOfRangeError),

    #[error("Object Store Error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("Pipeline Channel Closed")]
    PipelineClosed,

    #[error("Protobuf Conversion Error: {0}")]
    Pb2ArrowArror(#[from] KatnissArrowError),

    #[error("Temporal Pipeline Clog: {0}")]
    TemporalBufferSend(#[from] SendError<TemporalBuffer>),

    #[error("Timelord Error: {0}")]
    TimeyWimeyStuff(#[from] SystemTimeError),
}
