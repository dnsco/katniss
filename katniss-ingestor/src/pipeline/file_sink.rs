use object_store::ObjectStore;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{errors::KatinssIngestorError, pipeline::TemporalBytes, Result};

type ChanIn = UnboundedReceiver<TemporalBytes>;

pub struct StoreSink {
    rx: ChanIn,
    store: Box<dyn ObjectStore>,
}

impl StoreSink {
    pub fn new(rx: ChanIn, store: Box<dyn ObjectStore>) -> Self {
        Self { rx, store }
    }

    pub async fn sink_next(&mut self) -> Result<()> {
        let buf = self
            .rx
            .recv()
            .await
            .ok_or_else(|| KatinssIngestorError::PipelineClosed)?;
        self.store.put(&buf.path(), buf.bytes).await.unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use object_store::local::LocalFileSystem;
    use tokio::sync::mpsc::unbounded_channel;

    use crate::pipeline::TemporalBytes;
    use tempfile::tempdir;

    use super::StoreSink;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_name() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let temp_dir = tempdir()?;
        let store = LocalFileSystem::new_with_prefix(temp_dir.path())?;

        let mut sink = StoreSink::new(rx, Box::new(store));

        let data = TemporalBytes {
            begin_at: Utc::now(),
            end_at: Utc::now(),
            bytes: b"HEY GIRL".to_vec().into(),
        };

        let filename = temp_dir.path().join(data.path().to_string());

        tx.send(data)?;

        sink.sink_next().await?;
        assert_eq!(b"HEY GIRL", &std::fs::read(filename)?[..]);

        Ok(())
    }
}
