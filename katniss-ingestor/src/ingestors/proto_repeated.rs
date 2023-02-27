use std::path::Path;

use crate::{
    ingestors::fs_ingestor::{FsIngestor, FsIngestorProps},
    Result,
};

use itertools::Itertools;
use katniss_pb2arrow::{
    exports::prost_reflect::bytes::Buf, proto_repeated_consumer::RepeatedDynamicMessages,
    Result as ProtoResult,
};

pub struct LargeRepeatedProtoIngestor<B: Buf> {
    bytes: B,
    packet_ingestor: FsIngestor,
    arrow_batch_size: usize,
}

impl<B: Buf> LargeRepeatedProtoIngestor<B> {
    pub fn new<P: AsRef<Path>>(bytes: B, props: FsIngestorProps<P>) -> Result<Self> {
        let arrow_batch_size = props.arrow_record_batch_size;
        let packet_ingestor = FsIngestor::new(props)?;

        Ok(Self {
            bytes,
            packet_ingestor,
            arrow_batch_size,
        })
    }

    pub fn ingest(mut self) -> Result<()> {
        let messages = RepeatedDynamicMessages::iterator(
            &mut self.bytes,
            self.packet_ingestor.descriptor(),
            1,
        );

        for chunk in messages.chunks(self.arrow_batch_size).into_iter() {
            self.packet_ingestor
                .ingest(chunk.collect::<ProtoResult<Vec<_>>>()?)?;
        }

        self.packet_ingestor.finish()?;

        Ok(())
    }
}
