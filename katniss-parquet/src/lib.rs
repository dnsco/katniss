use std::{
    fs::File,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_schema::SchemaRef;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use katniss_pb2arrow::RecordBatch;

pub mod errors;
use self::errors::Result;

/// The desire is to make a struct that holds stuff in memory until it dumps to a file
/// right now it is actually just writing to files because YOLO
/// Eventually this should hold arrow stuff in memory and be queryable and then dump at some interval
/// because it's easier we're just gonna dump after a set number
pub struct MultiBatchWriter {
    num_batches: usize,
    batches: Vec<RecordBatch>,

    factory: WriterFactory,
    writer: ArrowWriter<File>,
}

impl MultiBatchWriter {
    pub fn new(path: PathBuf, schema: SchemaRef, num_batches: usize) -> Result<Self> {
        let batches = Vec::with_capacity(num_batches);

        let factory = WriterFactory::new(path, WriterProperties::builder().build(), schema);
        let writer = factory.new_writer()?;

        Ok(Self {
            num_batches,
            batches,

            factory,
            writer,
        })
    }

    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(&batch)?;
        self.batches.push(batch);
        if self.batches.len() >= self.num_batches {
            self.finalize_and_advance()?;
        }
        Ok(())
    }

    /// Finalize current parquet file, start new file and freshen in memory buffer
    fn finalize_and_advance(&mut self) -> Result<()> {
        let writer = std::mem::replace(&mut self.writer, self.factory.new_writer()?);
        self.batches = Vec::with_capacity(self.num_batches);
        writer.close()?;
        Ok(())
    }
}

struct WriterFactory {
    path: PathBuf,
    props: WriterProperties,
    schema: SchemaRef,
}

impl WriterFactory {
    pub fn new(path: PathBuf, props: WriterProperties, schema: SchemaRef) -> Self {
        Self {
            path,
            props,
            schema,
        }
    }

    pub fn file_in_path(&self) -> Result<File> {
        let mut filename = self.path.clone();
        filename.push(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis()
                .to_string(),
        );
        filename.set_extension("parquet");
        let file = File::create(filename)?;
        Ok(file)
    }

    pub fn new_writer(&self) -> Result<ArrowWriter<File>> {
        let writer = ArrowWriter::try_new(
            self.file_in_path()?,
            self.schema.clone(),
            Some(self.props.clone()),
        )?;
        Ok(writer)
    }
}
