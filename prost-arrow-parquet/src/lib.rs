use std::io::Write;

use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use prost_arrow::RecordBatch;

pub mod errors;
use self::errors::Result;

pub fn write_batch_inner<W: Write>(batch: RecordBatch, buf: W) -> Result<()> {
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(buf, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
