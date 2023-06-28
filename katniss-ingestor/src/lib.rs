pub mod arrow;
pub mod errors;
pub mod lance_ingestion;
pub mod timestuff;

pub type Result<T> = core::result::Result<T, errors::KatinssIngestorError>;
