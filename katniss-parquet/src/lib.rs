use std::{
    fs,
    io::{self, ErrorKind, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_schema::SchemaRef;
use errors::ProstArrowParquetError;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use katniss_pb2arrow::RecordBatch;

pub mod errors;
use self::errors::Result;

/// The desire is to make a struct that holds stuff in memory until it dumps to a file
/// Eventually this should hold arrow stuff in memory and be queryable and
/// Buffers should arguably represnet a fixed time period but right now it is a fixed number of arrow batches
pub struct MultiBatchWriter {
    num_batches: usize,
    batches: Vec<RecordBatch>,

    path: PathBuf,
    props: WriterProperties,
    schema: SchemaRef,
    writer: ParquetBuffer,
}

impl MultiBatchWriter {
    pub fn new(path: PathBuf, schema: SchemaRef, num_batches: usize) -> Result<Self> {
        let batches = Vec::with_capacity(num_batches);

        let props = WriterProperties::builder().build();
        let writer = ParquetBuffer::new(&path, schema.clone(), props.clone())?;

        Ok(Self {
            num_batches,
            batches,

            path,
            schema,
            props,
            writer,
        })
    }

    /// Write an arrow RecordBatch to a parquet buffer
    /// Finalizes parquet buffer and advances if enough batches have been written
    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(&batch)?;
        self.batches.push(batch);
        if self.batches.len() >= self.num_batches {
            self.advance_buffer()?;
        }
        Ok(())
    }

    /// Write current buffer to file and start a new one for next time period
    fn advance_buffer(&mut self) -> Result<()> {
        let writer = std::mem::replace(
            &mut self.writer,
            ParquetBuffer::new(&self.path, self.schema.clone(), self.props.clone())?,
        );
        self.batches = Vec::with_capacity(self.num_batches);
        writer.finalize_and_write_file()?;
        Ok(())
    }
}

struct ParquetBuffer {
    filename: PathBuf,
    parquet_writer: ArrowWriter<SharedBuffer>,
    buffer: SharedBuffer,
}

impl ParquetBuffer {
    pub fn new(path: &PathBuf, schema: SchemaRef, props: WriterProperties) -> Result<Self> {
        let filename = timestamp_filename(&path, SystemTime::now())?;

        let buffer = SharedBuffer {
            data: Arc::new(Mutex::new(Vec::new())),
        };

        let writer = ArrowWriter::try_new(buffer.clone(), schema, Some(props))?;

        Ok(Self {
            filename,
            buffer,
            parquet_writer: writer,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.parquet_writer.write(batch)?;
        Ok(())
    }

    fn finalize_and_write_file(self) -> Result<(PathBuf, Vec<u8>)> {
        self.parquet_writer.close()?;
        let bytes = self.buffer.try_downgrade()?;

        fs::write(&self.filename, &bytes)?;
        Ok((self.filename, bytes))
    }
}

fn timestamp_filename(path: &PathBuf, start_time: SystemTime) -> Result<PathBuf> {
    let mut filename = path.to_owned();
    filename.push(
        start_time
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string(),
    );
    filename.set_extension("parquet");
    Ok(filename)
}

#[derive(Clone)]
struct SharedBuffer {
    data: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    fn try_downgrade(self) -> Result<Vec<u8>> {
        let data = Arc::try_unwrap(self.data)
            .map_err(|_| ProstArrowParquetError::OtherSharedBufferReferenceHeld)?
            .into_inner()
            .map_err(|_| ProstArrowParquetError::OtherSharedBufferReferenceHeld)?;

        Ok(data)
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut data = self
            .data
            .lock()
            .map_err(|err| io::Error::new(ErrorKind::WouldBlock, err.to_string()))?;
        data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
