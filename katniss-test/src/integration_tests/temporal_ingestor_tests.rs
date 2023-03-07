use std::time::{Duration, SystemTime};

use katniss_ingestor::temporal::TemporalRotator;
use katniss_pb2arrow::ArrowBatchProps;

use crate::{descriptor_pool, protos::spacecorp::Packet};

use super::test_util::to_dynamic;
const PACKET: &str = "eto.pb2arrow.tests.spacecorp.Packet";

#[test]
fn feature() -> anyhow::Result<()> {
    let start = SystemTime::now();
    let mut rotator = TemporalRotator::new(
        &ArrowBatchProps::new(
            descriptor_pool()?,
            PACKET.to_owned(),
            2, //small batch size to test multi-level rotation
        )?,
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
    assert_eq!(3, rotator.past[0].batches.len()); // two completed batches of 2
    assert_eq!(0, rotator.current.batches.len()); // Fresh temporal buffer has no batches
    assert_eq!(1, rotator.converter.len()); // one unprocessed record

    Ok(())
}
