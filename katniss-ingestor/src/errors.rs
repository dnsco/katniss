use std::time::SystemTimeError;

use katniss_pb2arrow::KatnissArrowError;
use thiserror::Error;

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

    #[error("Failed to lock data")]
    OtherSharedBufferReferenceHeld,

    #[error("No Schema found {0}")]
    SchemaNotFound(String),
    
    #[error("No Dictionary found {0}")]
    DictionaryNotFound(String),
}
