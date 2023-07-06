use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{arrow::ProtobufBatchIngestor, Result};
use katniss_pb2arrow::{
    exports::{DynamicMessage, RecordBatch},
    ArrowBatchProps,
};

#[derive(Debug)]
pub struct TemporalBuffer {
    pub begin_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub batches: Vec<RecordBatch>,
}

impl TemporalBuffer {
    pub fn new(now: DateTime<Utc>, period: Duration) -> Result<Self> {
        let end_at = now + chrono::Duration::from_std(period)?;
        Ok(Self {
            begin_at: now,
            end_at,
            batches: Vec::new(),
        })
    }
}

pub fn timestamp_string(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%d-%H%M%S_utc").to_string()
}

/// Collects RecordBatches into buffers which get rotated every $batch_period of time.
pub struct TemporalRotator {
    pub converter: ProtobufBatchIngestor,
    pub current: TemporalBuffer,
    batch_period: Duration,
}

impl TemporalRotator {
    pub fn new(props: &ArrowBatchProps, now: DateTime<Utc>, period: Duration) -> Result<Self> {
        Ok(Self {
            converter: ProtobufBatchIngestor::try_new(props)?,
            current: TemporalBuffer::new(now, period)?,
            batch_period: period,
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
            let new = TemporalBuffer::new(now, self.batch_period)?;
            // constructing new before pushing as it's theoretically fallible to avoid memory leak
            self.current.batches.push(batch);
            finished_batch = Some(std::mem::replace(&mut self.current, new));
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

    use chrono::{Duration, TimeZone};
    use katniss_pb2arrow::ArrowBatchProps;

    use katniss_test::{descriptor_pool, protos::spacecorp::Packet, test_util::to_dynamic};

    const PACKET: &str = "eto.pb2arrow.tests.spacecorp.Packet";

    #[test]
    fn it_rotates_on_a_time_period() -> anyhow::Result<()> {
        let start = Utc::now();

        let mut rotator = TemporalRotator::new(
            &ArrowBatchProps::try_new(descriptor_pool()?, PACKET.to_owned())?
                .with_records_per_arrow_batch(2),
            start,
            std::time::Duration::from_millis(60),
        )?;

        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::milliseconds(1),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::milliseconds(2),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::milliseconds(5),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::milliseconds(10),
        )?;
        rotator.ingest_potentially_blocking(
            to_dynamic(&Packet::default(), PACKET)?,
            start + Duration::milliseconds(20),
        )?;

        assert_eq!(2, rotator.current.batches.len()); //two completed batches of 2
        assert_eq!(1, rotator.converter.len()); //one unprocessed record

        // ingesting a packet more than 60 milliseconds in the future rotates buffers
        let buf = rotator
            .ingest_potentially_blocking(
                to_dynamic(&Packet::default(), PACKET)?,
                start + Duration::milliseconds(61),
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

    #[test]
    fn filenames_are_pretty() -> anyhow::Result<()> {
        let now = Utc.timestamp_nanos(1678307941000000000);

        assert_eq!(timestamp_string(now), "2023-03-08-203901_utc");

        Ok(())
    }
}
