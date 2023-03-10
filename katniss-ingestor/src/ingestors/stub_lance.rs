use super::BatchIngestor;

pub struct StubLance;

impl BatchIngestor for StubLance {
    fn ingest(
        &mut self,
        _packets: Vec<katniss_pb2arrow::exports::DynamicMessage>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn finish(self: Box<Self>) -> crate::Result<()> {
        Ok(())
    }
}
