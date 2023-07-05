mod arrow;
mod lance_ingestion;
mod temporal_rotator;

pub mod errors;
pub type Result<T> = core::result::Result<T, errors::KatinssIngestorError>;
pub use lance_ingestion::{lance_ingestion_pipeline, LanceIngestor};
pub use temporal_rotator::TemporalBuffer;
