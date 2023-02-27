use anyhow::Result;
use katniss_ingestor::ingestors::{
    fs_ingestor::FsIngestorProps, proto_repeated::LargeRepeatedProtoIngestor,
};
use prost::Message;

use crate::{
    descriptor_pool,
    protos::spacecorp::{Log, Packet},
};

#[test]
fn test_log_to_parquet() -> Result<()> {
    let bytes: &[u8] = &Log {
        // TODO: Make some helpers that make different kinds of packets with data
        packets: vec![Packet::default(), Packet::default(), Packet::default()],
    }
    .encode_to_vec();

    let ingestor = LargeRepeatedProtoIngestor::new(
        bytes,
        FsIngestorProps {
            filename: "test_out.parquet",
            pool: descriptor_pool()?,
            msg_name: "eto.pb2arrow.tests.spacecorp.Packet",
            arrow_record_batch_size: 1024,
        },
    )?;

    ingestor.ingest()?;

    Ok(())
}
