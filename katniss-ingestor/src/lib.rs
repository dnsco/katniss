pub mod arrow;
pub mod buffer;
pub mod ingestors {
    pub mod buffered_ingestor;
    pub mod fs_ingestor;
}
pub mod errors;
pub mod parquet;

pub type Result<T> = core::result::Result<T, errors::KatinssIngestorError>;
