use std::{fs, path::PathBuf};

use tokio::sync::mpsc::UnboundedReceiver;

use crate::{errors::KatinssIngestorError, pipeline::TemporalBytes, Result};

type ChanIn = UnboundedReceiver<TemporalBytes>;

pub struct FileSink {
    rx: ChanIn,
    directory: PathBuf,
}

impl FileSink {
    pub fn new<P: Into<PathBuf>>(rx: UnboundedReceiver<TemporalBytes>, directory: P) -> Self {
        Self {
            rx,
            directory: directory.into(),
        }
    }

    pub async fn sink_next(&mut self) -> Result<()> {
        let buf = self
            .rx
            .recv()
            .await
            .ok_or_else(|| KatinssIngestorError::PipelineClosed)?;
        fs::write(self.filename_for(&buf), buf.bytes)?;
        Ok(())
    }

    /// Returns pretty formatted start time in directory
    fn filename_for(&self, data: &TemporalBytes) -> PathBuf {
        self.directory
            .join(data.begin_timestamp())
            .with_extension("parquet")
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use tokio::sync::mpsc::unbounded_channel;

    use crate::pipeline::TemporalBytes;
    use tempfile::tempdir;

    use super::FileSink;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_name() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let temp_dir = tempdir()?;

        let mut sink = FileSink::new(rx, temp_dir.path());
        let data = TemporalBytes {
            begin_at: Utc::now(),
            end_at: Utc::now(),
            bytes: b"HEY GIRL".to_vec(),
        };

        let filename = sink.filename_for(&data);

        tx.send(data)?;

        sink.sink_next().await?;
        assert_eq!(b"HEY GIRL", &std::fs::read(filename)?[..]);

        Ok(())
    }

    #[test]
    fn filenames_are_pretty() -> anyhow::Result<()> {
        let (_tx, rx) = unbounded_channel();

        let sink = FileSink::new(rx, "some_directory");
        let now = Utc.timestamp_nanos(1678307941000000000);

        let begin_at = now;
        let end_at = now + Duration::seconds(60);

        let data = TemporalBytes {
            begin_at,
            end_at,
            bytes: Default::default(),
        };

        let filename = sink.filename_for(&data);

        assert_eq!(
            "some_directory/2023-03-08-203901_utc.parquet",
            filename.as_os_str()
        );

        Ok(())
    }
}
