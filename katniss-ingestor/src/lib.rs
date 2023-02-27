pub mod arrow;
pub mod buffer;
pub mod errors;
pub mod ingestors;
pub mod parquet;

pub type Result<T> = core::result::Result<T, errors::KatinssIngestorError>;
