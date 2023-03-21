use chrono::{DateTime, Duration, Utc};

use katniss_pb2arrow::exports::RecordBatch;

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
    pub bytes: Vec<u8>,
}

impl TemporalBytes {
    pub fn begin_timestamp(&self) -> String {
        self.begin_at.format("%Y-%m-%d-%H%M%S_utc").to_string()
    }
}
