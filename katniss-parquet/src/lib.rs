use std::io::Write;

use arrow_schema::SchemaRef;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use katniss_pb2arrow::RecordBatch;

pub mod errors;
use self::errors::Result;

pub struct Writer<W: Write> {
    writer: ArrowWriter<W>,
}

impl<W: Write> Writer<W> {
    pub fn new(buf: W, schema: SchemaRef) -> Result<Self> {
        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(buf, schema, Some(props))?;
        Ok(Self { writer })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    /// Must be called to write parquet statistics.... I think
    pub fn finish(self) -> Result<()> {
        self.writer.close()?;
        Ok(())
    }
}
