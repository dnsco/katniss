use std::{
    fmt::Debug,
    sync::mpsc::{RecvError, SendError},
    time::SystemTimeError,
};

use katniss_pb2arrow::KatnissArrowError;
use thiserror::Error;

use crate::temporal_rotator::TemporalBuffer;

#[derive(Error, Debug)]
pub enum KatinssIngestorError {
    #[error("Protobuf Conversion Error: {0}")]
    Pb2ArrowArror(#[from] KatnissArrowError),

    #[error("Io Errror")]
    IoError(#[from] std::io::Error),

    #[error("Timelord Error: {0}")]
    TimeyWimeyStuff(#[from] SystemTimeError),

    #[error("Pipeline Clog: {0}")]
    BufferRecv(#[from] RecvError),

    #[error("Temporal Pipeline Clog: {0}")]
    TemporalBufferSend(#[from] SendError<TemporalBuffer>),

    #[error("Pipeline Channel Closed")]
    PipelineClosed,

    #[error("Object Store Error: {0}")]
    ObjectStoreError(#[from] object_store::Error),

    #[error("Lance Error: {0}")]
    LanceError(#[from] lance::Error),
}
