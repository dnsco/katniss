use std::time::SystemTimeError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProstArrowParquetError {
    #[error("Io Errror")]
    IoError(#[from] std::io::Error),

    #[error("Parquet Error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Timelord Error: {0}")]
    TimeyWimeyStuff(#[from] SystemTimeError),

    #[error("Failed to lock data")]
    MemoryFileReferenceStillHeld,
}

pub type Result<T> = core::result::Result<T, ProstArrowParquetError>;
