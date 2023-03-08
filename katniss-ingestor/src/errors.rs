use std::{
    fmt::Debug,
    sync::mpsc::{RecvError, SendError},
    time::SystemTimeError,
};

use katniss_pb2arrow::KatnissArrowError;
use thiserror::Error;

use crate::pipeline::{TemporalBuffer, TemporalBytes};

#[derive(Error, Debug)]
pub enum KatinssIngestorError {
    #[error("Protobuf Conversion Error: {0}")]
    Pb2ArrowArror(#[from] KatnissArrowError),

    #[error("Io Errror")]
    IoError(#[from] std::io::Error),

    #[error("Parquet Error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Timelord Error: {0}")]
    TimeyWimeyStuff(#[from] SystemTimeError),

    #[error("Pipeline Clog: {0}")]
    BufferRecv(#[from] RecvError),

    #[error("Temporal Pipeline Clog: {0}")]
    TemporalBufferSend(#[from] SendError<TemporalBuffer>),

    #[error("Parquet Pipeline Clog: {0}")]
    ParquetBufferSend(#[from] SendError<TemporalBytes>),
}
