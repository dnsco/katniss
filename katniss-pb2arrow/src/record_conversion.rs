use arrow_array::builder::*;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use prost_reflect::DynamicMessage;

use crate::ArrowBatchProps;
use crate::KatnissArrowError;
use crate::Result;

use self::builder_appending::append_all_fields;
use self::builder_creation::BuilderFactory;

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
    pub fn try_new(props: &ArrowBatchProps) -> Result<Self> {
        let batch_size = props.arrow_record_batch_size;
        let factory = BuilderFactory::new_with_dictionary(props.dictionaries.clone());
        let builder = factory.try_from_fields(props.schema.fields().clone(), batch_size)?;
        Ok(RecordBatchConverter {
            schema: props.schema.clone(),
            batch_size,
            builder,
        })
    }

    /// Append a new protobuf message to this batch
    pub fn append_message(&mut self, msg: &DynamicMessage) -> Result<()> {
        append_all_fields(self.schema.fields(), &mut self.builder, Some(msg))
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Number of rows in this batch so far
    pub fn len(&self) -> usize {
        self.builder.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn records(&mut self) -> Result<RecordBatch> {
        RecordBatch::try_from(self).map_err(KatnissArrowError::BatchConversionError)
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
