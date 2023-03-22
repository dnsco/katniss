use chrono::{DateTime, Duration, Utc};

use katniss_pb2arrow::exports::{prost_reflect::bytes::Bytes, RecordBatch};
use object_store::path::Path;

#[derive(Debug)]
pub struct TemporalBuffer {
    pub begin_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub batches: Vec<RecordBatch>,
}

impl TemporalBuffer {
    pub fn new(now: DateTime<Utc>) -> Self {
        let end_at = now + Duration::seconds(60);
        Self {
            begin_at: now,
            end_at,
            batches: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct TemporalBytes {
    pub begin_at: DateTime<Utc>,
    pub end_at: DateTime<Utc>,
    pub bytes: Bytes,
}

impl TemporalBytes {
    pub fn begin_timestamp(&self) -> String {
        self.begin_at.format("%Y-%m-%d-%H%M%S_utc").to_string()
    }

    pub fn path(&self) -> Path {
        Path::from(format!("{}.parquet", self.begin_timestamp()))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn filenames_are_pretty() -> anyhow::Result<()> {
        let now = Utc.timestamp_nanos(1678307941000000000);

        let begin_at = now;
        let end_at = now + Duration::seconds(60);

        let bytes = TemporalBytes {
            begin_at,
            end_at,
            bytes: Default::default(),
        };

        assert_eq!(
            Some("2023-03-08-203901_utc.parquet"),
            bytes.path().filename()
        );

        Ok(())
    }
}
