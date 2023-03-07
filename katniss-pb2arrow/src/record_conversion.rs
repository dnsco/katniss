use arrow_array::builder::*;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use prost_reflect::DynamicMessage;

use self::builder_appending::append_all_fields;
use self::builder_creation::BuilderFactory;
use crate::ArrowBatchProps;
use crate::KatnissArrowError;
use crate::Result;

mod builder_appending;
mod builder_creation;

/// Converterts records from protobuf to arrow
/// Holds records in the builder until records() is called draining builder.
pub struct RecordConverter {
    pub(crate) schema: SchemaRef,
    builder: StructBuilder, // fields align with schema
}

impl RecordConverter {
    pub fn try_new(props: &ArrowBatchProps) -> Result<Self> {
        let batch_size = props.records_per_arrow_batch;
        let factory = BuilderFactory::new_with_dictionary(props.dictionaries.clone());
        let builder = factory.try_from_fields(props.schema.fields().clone(), batch_size)?;
        Ok(Self {
            schema: props.schema.clone(),
            builder,
        })
    }

    /// Append a new protobuf message to this batch
    pub fn append_message(&mut self, msg: &DynamicMessage) -> Result<()> {
        append_all_fields(self.schema.fields(), &mut self.builder, Some(msg))
    }

    /// Returns record batch and resets the builder
    pub fn records(&mut self) -> RecordBatch {
        let struct_array = self.builder.finish();
        RecordBatch::from(&struct_array)
    }

    /// Number of rows in this batch so far
    pub fn len(&self) -> usize {
        self.builder.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl TryFrom<&ArrowBatchProps> for RecordConverter {
    type Error = KatnissArrowError;
    fn try_from(props: &ArrowBatchProps) -> Result<Self> {
        RecordConverter::try_new(props)
    }
}
