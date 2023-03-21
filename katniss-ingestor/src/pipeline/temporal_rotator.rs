use super::TemporalBuffer;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{arrow::ProtobufBatchIngestor, errors::KatinssIngestorError, Result};
use katniss_pb2arrow::{exports::DynamicMessage, ArrowBatchProps};

type ChanIn = UnboundedReceiver<DynamicMessage>;
type ChanOut = UnboundedSender<TemporalBuffer>;
type ChanNext = UnboundedReceiver<TemporalBuffer>;

/// Rotates the in-memory buffer it is written to per a timescale,
/// Currently hardcoded to rotate every 60 seconds
/// we should probably do some more timelord stuff to have buffers line up with minutes
/// we could also make the duration configurable but YOLO
pub struct TemporalRotator {
    pub converter: ProtobufBatchIngestor,
    pub current: TemporalBuffer,
    pub rx: ChanIn,
    pub tx: ChanOut,
}

impl TemporalRotator {
    pub fn try_new(
        rx_in: ChanIn,
        props: &ArrowBatchProps,
        now: DateTime<Utc>,
    ) -> Result<(Self, ChanNext)> {
        let (tx_out, rx_next) = unbounded_channel();
        Ok((
            Self {
                converter: ProtobufBatchIngestor::try_new(props)?,
                current: TemporalBuffer::new(now),
                rx: rx_in,
                tx: tx_out,
            },
            rx_next,
        ))
    }

    pub async fn process_next(&mut self) -> Result<()> {
        let msg = self
            .rx
            .recv()
            .await
            .ok_or_else(|| KatinssIngestorError::PipelineClosed)?;
        self.ingest(msg, Utc::now())
    }

    fn ingest(&mut self, msg: DynamicMessage, now: DateTime<Utc>) -> Result<()> {
        if now > self.current.end_at {
            let batch = self.converter.finish()?;
            self.current.batches.push(batch);

            let old = std::mem::replace(&mut self.current, TemporalBuffer::new(now));
            self.tx
                .send(old)
                .map_err(|_| KatinssIngestorError::PipelineClosed)?;
        }

        if let Some(batch) = self.converter.ingest_message(msg)? {
            self.current.batches.push(batch)
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Duration;
    use katniss_pb2arrow::ArrowBatchProps;

    use katniss_test::{descriptor_pool, protos::spacecorp::Packet, test_util::to_dynamic};

    const PACKET: &str = "eto.pb2arrow.tests.spacecorp.Packet";

    #[test]
    fn it_rotates_on_a_time_period() -> anyhow::Result<()> {
        let start = Utc::now();
        let (_tx, rx_in) = unbounded_channel();
        let (mut rotator, mut rx) = TemporalRotator::try_new(
            rx_in,
            &ArrowBatchProps::try_new(descriptor_pool()?, PACKET.to_owned())?
                .with_records_per_arrow_batch(2),
            start,
        )?;

        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(1),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(2),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(5),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(10),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(20),
        )?;

        assert_eq!(2, rotator.current.batches.len()); //two completed batches of 2
        assert_eq!(1, rotator.converter.len()); //one unprocessed record
        assert!(rx.try_recv().is_err());

        // ingesting a packet more than 60 seconds in future rotates buffers
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(61),
        )?;

        let buf = rx.try_recv().unwrap();

        assert_eq!(
            vec![2, 2, 1],
            buf.batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>()
        ); // two completed batches of 2
        assert_eq!(0, rotator.current.batches.len()); // Fresh temporal buffer has no batches
        assert_eq!(1, rotator.converter.len()); // one unprocessed record

        Ok(())
    }
}
