use katniss_pb2arrow::exports::DynamicMessage;

use crate::Result;

pub mod lance_fs_ingestor;
pub mod parquet_buffered;
pub mod parquet_fs;
pub mod proto_repeated;
pub mod stub_lance;

pub trait BatchIngestor {
    fn ingest(&mut self, packets: Vec<DynamicMessage>) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<()>;
}
