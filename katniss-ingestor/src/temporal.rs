use std::time::{Duration, SystemTime};

use katniss_pb2arrow::{
    exports::{DynamicMessage, RecordBatch},
    ArrowBatchProps,
};

use crate::{arrow::ProtobufBatchIngestor, Result};

pub struct TemporalBuffer {
    pub begin_at: SystemTime,
    pub end_at: SystemTime,
    pub batches: Vec<RecordBatch>,
}

impl TemporalBuffer {
    fn new(now: SystemTime) -> Self {
        let end_at = now + Duration::from_secs(60);
        Self {
            begin_at: now,
            end_at,
            batches: Vec::new(),
        }
    }
}

/// Rotates the in-memory buffer it is written to per a timescale,
/// Currently hardcoded to rotate every 60 seconds
/// we should probably do some more timelord stuff to have buffers line up with minutes
/// we could also make the duration configurable but YOLO
pub struct TemporalRotator {
    pub converter: ProtobufBatchIngestor,
    pub current: TemporalBuffer,
    pub past: Vec<TemporalBuffer>,
}

impl TemporalRotator {
    pub fn new(props: &ArrowBatchProps, now: SystemTime) -> Result<Self> {
        Ok(Self {
            converter: ProtobufBatchIngestor::new(props)?,
            current: TemporalBuffer::new(now),
            past: Default::default(),
        })
    }

    pub fn ingest(&mut self, msg: DynamicMessage, now: SystemTime) -> Result<()> {
        if now > self.current.end_at {
            let batch = self.converter.finish()?;
            self.current.batches.push(batch);

            let old = std::mem::replace(&mut self.current, TemporalBuffer::new(now));
            self.past.push(old);
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

    use std::time::{Duration, SystemTime};

    use katniss_pb2arrow::ArrowBatchProps;

    use katniss_test::{descriptor_pool, protos::spacecorp::Packet, test_util::to_dynamic};

    const PACKET: &str = "eto.pb2arrow.tests.spacecorp.Packet";

    #[test]
    fn it_rotates_on_a_time_period() -> anyhow::Result<()> {
        let start = SystemTime::now();
        let mut rotator = TemporalRotator::new(
            &ArrowBatchProps::new(descriptor_pool()?, PACKET.to_owned())?
                .with_records_per_arrow_batch(2),
            start,
        )?;

        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::from_secs(1),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::from_secs(2),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::from_secs(5),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::from_secs(10),
        )?;
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::from_secs(20),
        )?;

        assert_eq!(2, rotator.current.batches.len()); //two completed batches of 2
        assert_eq!(1, rotator.converter.len()); //one unprocessed record

        // ingesting a packet more than 60 seconds in future rotates buffers
        rotator.ingest(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::from_secs(61),
        )?;
        assert_eq!(
            vec![2, 2, 1],
            rotator.past[0]
                .batches
                .iter()
                .map(|b| b.num_rows())
                .collect::<Vec<_>>()
        ); // two completed batches of 2
        assert_eq!(0, rotator.current.batches.len()); // Fresh temporal buffer has no batches
        assert_eq!(1, rotator.converter.len()); // one unprocessed record

        Ok(())
    }
}
