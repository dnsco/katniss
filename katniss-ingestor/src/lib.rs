pub mod arrow;
pub mod buffer;
pub mod errors;
pub mod full_ingestor;
pub mod parquet;

pub type Result<T> = core::result::Result<T, errors::KatinssIngestorError>;
