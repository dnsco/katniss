use anyhow::Result;

use katniss_ingestor::ingestors::proto_repeated::{RepeatedProtoIngestor, Serialization};
use katniss_pb2arrow::ArrowBatchProps;

use prost::Message;

use crate::{
    descriptor_pool,
    protos::spacecorp::{Log, Packet},
};

#[test]
fn test_log_to_lance() -> Result<()> {
    std::fs::remove_dir_all("test_out.lance");
    let bytes: &[u8] = &Log {
        // TODO: Make some helpers that make different kinds of packets with data
        packets: vec![Packet::default(), Packet::default(), Packet::default()],
    }
    .encode_to_vec();

    let ingestor = RepeatedProtoIngestor::new(
        bytes,
        ArrowBatchProps::new(
            descriptor_pool()?,
            "eto.pb2arrow.tests.spacecorp.Packet".to_owned(),
            1024,
        )?,
        Serialization::Lance {
            filename: "test_out.lance",
        },
    )?;

    ingestor.ingest()?;
    Ok(())
}
