use super::TemporalBuffer;

use chrono::{DateTime, Utc};

use crate::{arrow::ProtobufBatchIngestor, Result};
use katniss_pb2arrow::{exports::DynamicMessage, ArrowBatchProps};

/// Rotates the in-memory buffer it is written to per a timescale,
/// Currently hardcoded to rotate every 60 seconds
/// we should probably do some more timelord stuff to have buffers line up with minutes
/// we could also make the duration configurable but YOLO
pub struct TemporalRotator {
    pub converter: ProtobufBatchIngestor,
    pub current: TemporalBuffer,
}

impl TemporalRotator {
    pub fn try_new(props: &ArrowBatchProps, now: DateTime<Utc>) -> Result<Self> {
        Ok(Self {
            converter: ProtobufBatchIngestor::try_new(props)?,
            current: TemporalBuffer::new(now),
        })
    }

    /// Receives dynamic protobuf messages and sends them in to a temporal buffer
    /// Rotates the temporal buffer if time boundary has been crossed
    /// Returns the previous buffer if it has been rotated
    /// Blocking: Encoding a protobuf batch to arrow which happens ever 1024 (by default) messages
    /// or when the buffer has been rotated, theoretically blocks the thread [benchmarking needed]
    /// maybe wrap with block_in_place upstream or maybe this is overly defensive
    pub fn ingest_potentially_blocking(
        &mut self,
        msg: DynamicMessage,
        now: DateTime<Utc>,
    ) -> Result<Option<TemporalBuffer>> {
        let mut finished_batch = None;
        if now > self.current.end_at {
            let batch = self.converter.finish()?;
            self.current.batches.push(batch);

            finished_batch = Some(std::mem::replace(
                &mut self.current,
                TemporalBuffer::new(now),
            ));
        }

        if let Some(batch) = self.converter.ingest_message(msg)? {
            self.current.batches.push(batch)
        }
        Ok(finished_batch)
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

        let mut rotator = TemporalRotator::try_new(
            &ArrowBatchProps::try_new(descriptor_pool()?, PACKET.to_owned())?
                .with_records_per_arrow_batch(2),
            start,
        )?;

        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(1),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(2),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(5),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(10),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::seconds(20),
        )?;

        assert_eq!(2, rotator.current.batches.len()); //two completed batches of 2
        assert_eq!(1, rotator.converter.len()); //one unprocessed record

        // ingesting a packet more than 60 seconds in future rotates buffers
        let buf = rotator
            .ingest_potentially_blocking(
                to_dynamic(&Packet::default(), PACKET)?,
                start + Duration::seconds(61),
            )?
            .unwrap();

        assert_eq!(
            vec![2, 2, 1],
            buf.batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>()
        ); // two completed batches of 2
        assert_eq!(0, rotator.current.batches.len()); // Fresh temporal buffer has no batches
        assert_eq!(1, rotator.converter.len()); // one unprocessed record

        Ok(())
    }
}
