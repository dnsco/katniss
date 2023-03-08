use std::{fs, path::PathBuf, sync::mpsc::Receiver};

use crate::{pipeline::TemporalBytes, Result};

pub struct FileSink {
    rx: Receiver<TemporalBytes>,
    directory: PathBuf,
}

impl FileSink {
    pub fn new<P: Into<PathBuf>>(rx: Receiver<TemporalBytes>, directory: P) -> Self {
        Self {
            rx,
            directory: directory.into(),
        }
    }

    pub fn sink_next(&self) -> Result<()> {
        let buf = self.rx.recv()?;
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
    use std::sync::mpsc::channel;

    use chrono::{Duration, TimeZone, Utc};

    use crate::pipeline::TemporalBytes;
    use tempfile::tempdir;

    use super::FileSink;

    #[test]
    fn test_name() -> anyhow::Result<()> {
        let (tx, rx) = channel();
        let temp_dir = tempdir()?;

        let sink = FileSink::new(rx, temp_dir.path());
        let data = TemporalBytes {
            begin_at: Utc::now(),
            end_at: Utc::now(),
            bytes: b"HEY GIRL".to_vec(),
        };

        let filename = sink.filename_for(&data);

        tx.send(data)?;

        sink.sink_next()?;
        assert_eq!(b"HEY GIRL", &std::fs::read(filename)?[..]);

        Ok(())
    }

    #[test]
    fn filenames_are_pretty() -> anyhow::Result<()> {
        let (_tx, rx) = channel();

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
