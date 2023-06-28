use std::path::Path;

use crate::Result;

use itertools::Itertools;
use katniss_pb2arrow::{
    exports::prost_reflect::{bytes::Buf, MessageDescriptor},
    proto_repeated_consumer::RepeatedDynamicMessages,
    ArrowBatchProps, Result as ProtoResult,
};

use super::{lance_fs_ingestor::LanceFsIngestor, parquet_fs::ParquetFileIngestor, BatchIngestor};

pub struct RepeatedProtoIngestor<B: Buf> {
    bytes: B,
    packet_ingestor: Box<dyn BatchIngestor>,
    arrow_batch_size: usize,
    descriptor: MessageDescriptor,
}

pub enum Serialization<P: AsRef<Path>> {
    Parquet { filename: P },
    Lance { filename: P },
}

impl<B: Buf> RepeatedProtoIngestor<B> {
    pub fn new<P: AsRef<Path>>(
        bytes: B,
        arrow_props: &ArrowBatchProps,
        serialization: Serialization<P>,
    ) -> Result<Self> {
        let arrow_batch_size = arrow_props.records_per_arrow_batch;
        let descriptor = arrow_props.descriptor.clone();
        let packet_ingestor: Box<dyn BatchIngestor> = match serialization {
            Serialization::Parquet { filename } => {
                Box::new(ParquetFileIngestor::new(arrow_props, filename)?)
            }
            Serialization::Lance { filename } => {
                Box::new(LanceFsIngestor::new(arrow_props, filename)?)
            }
        };

        Ok(Self {
            bytes,
            descriptor,
            packet_ingestor,
            arrow_batch_size,
        })
    }

    pub fn ingest(mut self) -> Result<()> {
        let messages = RepeatedDynamicMessages::iterator(&mut self.bytes, self.descriptor, 1);

        for chunk in messages.chunks(self.arrow_batch_size).into_iter() {
            self.packet_ingestor
                .ingest(chunk.collect::<ProtoResult<Vec<_>>>()?)?;
        }

        self.packet_ingestor.finish()?;

        Ok(())
    }
}
