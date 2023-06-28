pub mod arrow;
pub mod errors;
pub mod ingestors;
pub mod pipeline;

pub type Result<T> = core::result::Result<T, errors::KatinssIngestorError>;
