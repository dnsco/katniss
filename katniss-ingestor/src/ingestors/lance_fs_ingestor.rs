use std::path::Path;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use lance::arrow::RecordBatchBuffer;
use lance::dataset::{Dataset, WriteMode, WriteParams};

use katniss_pb2arrow::{
    exports::prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor},
    RecordBatchConverter, SchemaConverter,
};
use tokio::runtime::Runtime;
use katniss_pb2arrow::exports::{RecordBatch, RecordBatchReader};

use crate::{arrow::ProtobufBatchIngestor, Result};

pub struct LanceFsIngestor {
    descriptor: MessageDescriptor,
    ingestor: ProtobufBatchIngestor,
    rt: Arc<Runtime>,
    filename: String,
    params: WriteParams,
    batches: Vec<RecordBatch>,
}

pub struct LanceFsIngestorProps<'a, P: AsRef<Path>> {
    pub pool: DescriptorPool,
    pub filename: P,
    pub msg_name: &'a str,
    pub arrow_record_batch_size: usize,
}

impl LanceFsIngestor {
    pub fn new<P: AsRef<Path>>(props: LanceFsIngestorProps<P>) -> Result<Self> {
        let converter = SchemaConverter::new(props.pool);
        let (schema_opt, dictionaries_opt) =
            converter
                .get_arrow_schema_with_dictionaries(props.msg_name, &[])?;

        let schema = SchemaRef::new(schema_opt
            .ok_or_else(|| {
                crate::errors::KatinssIngestorError::SchemaNotFound(props.msg_name.to_owned())
            })?);

        let dictionaries = dictionaries_opt.ok_or_else(|| {
            crate::errors::KatinssIngestorError::DictionaryNotFound(props.msg_name.to_owned())
        })?;

        let descriptor = converter.get_message_by_name(props.msg_name)?;

        let ingestor = ProtobufBatchIngestor::new(
            RecordBatchConverter::try_new_with_dictionaries(schema.clone(), props.arrow_record_batch_size, dictionaries)?,
            props.arrow_record_batch_size,
        );

        let filename = props.filename.as_ref().to_str().unwrap().to_string();

        let mut params = lance::dataset::WriteParams::default();
        params.max_rows_per_group = 1024 * 10;
        params.mode = WriteMode::Create;
        let rt = Arc::new(Runtime::new()?);

        let batches: Vec<RecordBatch> = Vec::new();

        Ok(Self {
            descriptor,
            ingestor,
            rt,
            filename,
            params,
            batches
        })
    }

    pub fn ingest(&mut self, packets: Vec<DynamicMessage>) -> Result<()> {
        for packet in packets {
            if let Some(batch) = self.ingestor.ingest(packet)? {
                self.batches.push(batch);
                // self.write(batch)?;
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        let batch = self.ingestor.finish()?;
        // TODO write here
        // self.write(batch.clone())?;

        let mut reader : Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(self.batches));
        self.rt.block_on(async {
            Dataset::write(&mut reader, self.filename.as_ref(), Some(self.params)).await
        }).unwrap(); //FIX unwrap

        Ok(())
    }

    /// Return a cloned reference of the Protobuf Messagedescriptor for downstream convenience
    pub fn descriptor(&self) -> MessageDescriptor {
        self.descriptor.clone()
    }

    fn write(&self, batch: RecordBatch) -> Result<()> {
        let mut reader : Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(vec![batch]));
        self.rt.block_on(async {
            Dataset::write(&mut reader, self.filename.as_ref(), Some(self.params)).await
        }).unwrap(); //FIX unwrap
        Ok(())
    }
}