use arrow_array::builder::*;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use prost_reflect::DynamicMessage;

use crate::schema_conversion::DictValuesContainer;
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
    pub fn try_new(schema: SchemaRef, batch_size: usize) -> Result<Self> {
        let factory = BuilderFactory::new();
        let builder = factory.try_from_fields(schema.fields().clone(), batch_size)?;
        Ok(RecordBatchConverter {
            schema,
            batch_size,
            builder,
        })
    }

    pub fn try_new_with_dictionaries(
        schema: SchemaRef,
        batch_size: usize,
        dictionaries: DictValuesContainer,
    ) -> Result<Self> {
        let factory = BuilderFactory::new_with_dictionary(dictionaries);
        let builder = factory.try_from_fields(schema.fields().clone(), batch_size)?;
        Ok(RecordBatchConverter {
            schema,
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

    pub fn records(&mut self) -> Result<RecordBatch> {
        RecordBatch::try_from(self).map_err(|e| KatnissArrowError::BatchConversionError(e))
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
