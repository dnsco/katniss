use anyhow::Result;
use katniss_pb2arrow::SchemaConverter;
use prost_reflect::DescriptorPool;

pub mod test_util;
pub mod protos {
    pub const FILE_DESCRIPTOR_BYTES: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

    pub mod spacecorp {
        use std::time::{SystemTime, UNIX_EPOCH};

        include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.spacecorp.rs"));

        impl Timestamp {
            pub fn system_now() -> Self {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                Self {
                    seconds: now.as_secs() as i64,
                    nanos: now.subsec_nanos() as i32,
                }
            }
        }
    }

    pub mod v2 {
        include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.v2.rs"));
    }

    pub mod v3 {
        include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.v3.rs"));
    }
}

pub fn schema_converter() -> Result<SchemaConverter> {
    let pool = descriptor_pool()?;
    Ok(SchemaConverter::new(pool))
}

pub fn descriptor_pool() -> Result<DescriptorPool> {
    Ok(DescriptorPool::decode(protos::FILE_DESCRIPTOR_BYTES)?)
}

#[cfg(test)]
mod integration_tests;
