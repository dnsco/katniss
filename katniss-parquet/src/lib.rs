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
/// right now it is actually just writing to files because YOLO
/// Eventually this should hold arrow stuff in memory and be queryable and then dump at some interval
/// because it's easier we're just gonna dump after a set number
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
        let writer = std::mem::replace(
            &mut self.writer,
            ParquetBuffer::new(&self.path, self.schema.clone(), self.props.clone())?,
        );
        self.batches = Vec::with_capacity(self.num_batches);
        writer.finish()?;
        Ok(())
    }
}

struct ParquetBuffer {
    file: MemoryFile,
    writer: ArrowWriter<MemoryFile>,
}

impl ParquetBuffer {
    pub fn new(path: &PathBuf, schema: SchemaRef, props: WriterProperties) -> Result<Self> {
        let file = MemoryFile {
            filename: timestamped_filename(path)?,
            data: Arc::new(Mutex::new(Vec::new())),
        };
        let writer = ArrowWriter::try_new(file.clone(), schema, Some(props))?;

        Ok(Self { file, writer })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        Ok(())
    }

    fn finish(self) -> Result<(PathBuf, Vec<u8>)> {
        self.writer.close()?;
        self.file.copy_to_file()
    }
}

fn timestamped_filename(path: &PathBuf) -> Result<PathBuf> {
    let mut filename = path.clone();
    filename.push(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string(),
    );
    filename.set_extension("parquet");
    Ok(filename)
}

#[derive(Clone)]
struct MemoryFile {
    filename: PathBuf,
    data: Arc<Mutex<Vec<u8>>>,
}

impl MemoryFile {
    fn copy_to_file(self) -> Result<(PathBuf, Vec<u8>)> {
        let data = Arc::try_unwrap(self.data)
            .map_err(|_| ProstArrowParquetError::MemoryFileReferenceStillHeld)?
            .into_inner()
            .map_err(|_| ProstArrowParquetError::MemoryFileReferenceStillHeld)?;
        let filename = self.filename;

        fs::write(&filename, &data[..])?;
        Ok((filename, data))
    }
}

impl Write for MemoryFile {
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
