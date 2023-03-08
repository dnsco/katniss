use arrow_array::builder::*;
use arrow_array::types::Int32Type;
use arrow_schema::{DataType, Field};

use crate::errors::Result;
use crate::KatnissArrowError::{DictNotFound, BatchConversionError};
use crate::schema_conversion::DictValuesContainer;

pub struct BuilderFactory {
    dictionaries: Option<DictValuesContainer>
}

impl BuilderFactory {
    pub fn new() -> Self {
        BuilderFactory { dictionaries: None }
    }

    pub fn new_with_dictionary(dictionaries: DictValuesContainer) -> Self {
        BuilderFactory { dictionaries: Some(dictionaries) }
    }

    pub fn try_from_fields(&self, fields: Vec<Field>, capacity: usize) -> Result<StructBuilder> {
        let field_builders: Vec<Box<dyn ArrayBuilder>> = fields.iter().map(|f| self.make_builder(f, capacity)).collect::<Result<Vec<_>>>()?;
        Ok(StructBuilder::new(fields, field_builders))
    }

    /// Create the appropriate ArrayBuilder for the given field and capacity
    fn make_builder(&self, field: &Field, capacity: usize) -> Result<Box<dyn ArrayBuilder>> {
        // arrow needs generic builder methods
        let (inner_field, inner_typ, kind) = match field.data_type() {
            DataType::List(v) => (v.as_ref(), v.data_type(), ListKind::List),
            DataType::LargeList(v) => (v.as_ref(), v.data_type(), ListKind::LargeList),
            _ => (field, field.data_type(), ListKind::NotList),
        };

        match inner_typ {
            DataType::Boolean => wrap_builder(BooleanBuilder::with_capacity(capacity), kind),
            DataType::Int32 => wrap_builder(Int32Builder::with_capacity(capacity), kind),
            DataType::Int64 => wrap_builder(Int64Builder::with_capacity(capacity), kind),
            DataType::UInt32 => wrap_builder(UInt32Builder::with_capacity(capacity), kind),
            DataType::UInt64 => wrap_builder(UInt64Builder::with_capacity(capacity), kind),
            DataType::Float32 => wrap_builder(Float32Builder::with_capacity(capacity), kind),
            DataType::Float64 => wrap_builder(Float64Builder::with_capacity(capacity), kind),
            DataType::Binary => wrap_builder(BinaryBuilder::with_capacity(capacity, 1024), kind),
            DataType::LargeBinary => {
                wrap_builder(LargeBinaryBuilder::with_capacity(capacity, 1024), kind)
            }
            DataType::Utf8 => wrap_builder(StringBuilder::with_capacity(capacity, 1024), kind),
            DataType::LargeUtf8 => {
                wrap_builder(LargeStringBuilder::with_capacity(capacity, 1024), kind)
            }
            DataType::Dictionary(_, _) => {
                // Protobuf enums are int32 -> string
                let dict_builder = match self.dictionaries.as_ref() {
                    Some(d) => {
                        let dict_values = inner_field.dict_id()
                            .map(|dict_id| d.get_dict_values(dict_id))
                            .flatten()
                            .ok_or_else(|| DictNotFound)?;
                        StringDictionaryBuilder::<Int32Type>::new_with_dictionary(capacity, dict_values)
                            .map_err(|err| BatchConversionError(err))?
                    }
                    None => {
                        StringDictionaryBuilder::<Int32Type>::with_capacity(capacity, capacity, capacity)
                    }
                };
                wrap_builder(dict_builder, kind)
            }
            DataType::Struct(fields) => {
                wrap_builder(self.try_from_fields(fields.clone(), capacity)?, kind)
            }
            t => panic!("Data type {:?} is not currently supported", t),
        }
    }
}


enum ListKind {
    List,
    LargeList,
    NotList,
}

/// Return the boxed builder or wrap it in a ListBuilder then box
/// this is necessary because
fn wrap_builder<T: ArrayBuilder>(builder: T, kind: ListKind) -> Result<Box<dyn ArrayBuilder>> {
    Ok(match kind {
        ListKind::List => Box::new(ListBuilder::new(builder)),
        ListKind::LargeList => Box::new(LargeListBuilder::new(builder)),
        ListKind::NotList => Box::new(builder),
    })
}