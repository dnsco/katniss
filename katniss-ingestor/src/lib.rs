pub mod arrow;
pub mod errors;
pub mod ingestors;
pub mod parquet;
pub mod temporal;

pub type Result<T> = core::result::Result<T, errors::KatinssIngestorError>;
