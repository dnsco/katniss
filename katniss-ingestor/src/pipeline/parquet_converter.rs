use arrow_schema::SchemaRef;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::block_in_place,
};

use super::{TemporalBuffer, TemporalBytes};
use crate::{errors::KatinssIngestorError, Result};

type ChanIn = UnboundedReceiver<TemporalBuffer>;
type ChanNext = UnboundedReceiver<TemporalBytes>;
type ChanOut = UnboundedSender<TemporalBytes>;

pub struct ParquetConverter {
    rx: ChanIn,
    tx: ChanOut,
    schema: SchemaRef,
    parquet_props: WriterProperties,
}

impl ParquetConverter {
    pub fn new(rx: ChanIn, schema: SchemaRef) -> (Self, ChanNext) {
        let (tx, rx_bytes) = unbounded_channel();

        let parquet_props = WriterProperties::builder()
            .set_max_row_group_size(1024 * 10) //Our data shape goes quadratic with larger group sizes
            .build();

        let consumer = Self {
            rx,
            tx,
            schema,
            parquet_props,
        };

        (consumer, rx_bytes)
    }

    pub fn with_parquet_writer_props(mut self, props: WriterProperties) -> Self {
        self.parquet_props = props;
        self
    }

    pub async fn process_next_buffer(&mut self) -> Result<()> {
        let buf = self
            .rx
            .recv()
            .await
            .ok_or_else(|| KatinssIngestorError::PipelineClosed)?;

        let mut writer = ArrowWriter::try_new(
            Vec::<u8>::new(), // this could probably be a with_capacity with *some* number
            self.schema.clone(),
            Some(self.parquet_props.clone()),
        )?;

        let bytes = block_in_place(|| {
            for batch in buf.batches {
                writer.write(&batch)?;
            }

            writer.into_inner()
        })?;

        self.tx
            .send(TemporalBytes {
                begin_at: buf.begin_at,
                end_at: buf.end_at,
                bytes: bytes.into(),
            })
            .map_err(|_| KatinssIngestorError::PipelineClosed)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use katniss_test::schema_converter;
    use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn it_consumes_temporal_buffers_into_parquet_bytes() -> anyhow::Result<()> {
        let (tx_buffer, rx) = unbounded_channel();
        let schema = schema_converter()?
            .get_arrow_schema("eto.pb2arrow.tests.spacecorp.Packet", &[])?
            .unwrap();

        let (mut consumer, mut rx_bytes) = ParquetConverter::new(rx, Arc::new(schema));
        tx_buffer.send(TemporalBuffer::new(Utc::now()))?;
        assert!(rx_bytes.try_recv().is_err());

        consumer.process_next_buffer().await?;

        let bytes = rx_bytes.try_recv()?;

        let reader = SerializedFileReader::new(bytes.bytes)?;
        //TODO– give initial buffer a batch to see a row group happen
        assert_eq!(0, reader.metadata().num_row_groups());
        Ok(())
    }
}
