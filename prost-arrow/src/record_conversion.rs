use std::io::Result;

use arrow_array::builder::*;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use prost_reflect::DynamicMessage;

use self::builder_appending::append_all_fields;
use self::builder_creation::make_struct_builder;

mod builder_appending;
mod builder_creation;

/// Parse messages and return RecordBatch
pub struct RecordBatchConverter {
    pub(crate) schema: SchemaRef,
    #[allow(unused)]
    batch_size: usize,
    builder: StructBuilder, // fields align with schema
}

impl RecordBatchConverter {
    pub fn new(schema: SchemaRef, batch_size: usize) -> RecordBatchConverter {
        RecordBatchConverter {
            builder: make_struct_builder(schema.fields().clone(), batch_size),
            schema,
            batch_size,
        }
    }

    /// Append a new protobuf message to this batch
    pub fn append_message(&mut self, msg: &DynamicMessage) -> Result<()> {
        append_all_fields(self.schema.fields(), &mut self.builder, Some(msg))
    }

    /// Number of rows in this batch so far
    pub fn len(&self) -> usize {
        self.builder.len()
    }

    pub fn records(&mut self) -> core::result::Result<RecordBatch, ArrowError> {
        self.try_into()
    }
}

/// Convert RecordBatch from RecordBatchConverter.
impl TryFrom<&mut RecordBatchConverter> for RecordBatch {
    type Error = ArrowError;

    fn try_from(converter: &mut RecordBatchConverter) -> core::result::Result<Self, Self::Error> {
        let struct_array = converter.builder.finish();
        Ok(RecordBatch::from(&struct_array))
    }
}
