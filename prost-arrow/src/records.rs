use std::borrow::Cow;
use std::io::ErrorKind::InvalidData;
use std::io::{Error, ErrorKind, Result};

use arrow_array::builder::*;
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, Field, SchemaRef};
use prost_reflect::{DynamicMessage, ReflectMessage, Value};

/// Parse messages and return RecordBatch
pub struct RecordBatchConverter {
    pub(crate) schema: SchemaRef,
    #[allow(unused)]
    batch_size: usize,
    builder: StructBuilder, // fields align with schema
}

impl RecordBatchConverter {
    pub fn new(schema: SchemaRef, batch_size: usize) -> RecordBatchConverter {
        let builder =
            RecordBatchConverter::make_struct_builder(schema.fields().clone(), batch_size);
        RecordBatchConverter {
            schema,
            batch_size,
            builder,
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

    fn make_struct_builder(fields: Vec<Field>, capacity: usize) -> StructBuilder {
        let mut builders = Vec::with_capacity(fields.len());
        for field in &fields {
            builders.push(RecordBatchConverter::make_builder(field, capacity));
        }
        StructBuilder::new(fields, builders)
    }

    /// Create the appropriate ArrayBuilder for the given field and capacity
    fn make_builder(field: &Field, capacity: usize) -> Box<dyn ArrayBuilder> {
        // arrow needs generic builder methods
        let (inner_typ, kind) = match field.data_type() {
            DataType::List(v) => (v.data_type(), ListKind::List),
            DataType::LargeList(v) => (v.data_type(), ListKind::LargeList),
            _ => (field.data_type(), ListKind::NotList),
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
            DataType::Struct(fields) => wrap_builder(
                RecordBatchConverter::make_struct_builder(fields.clone(), capacity),
                kind,
            ),
            t => panic!("Data type {:?} is not currently supported", t),
        }
    }

    pub fn records(&mut self) -> core::result::Result<RecordBatch, ArrowError> {
        self.try_into()
    }
}

enum ListKind {
    List,
    LargeList,
    NotList,
}

/// Return the boxed builder or wrap it in a ListBuilder then box
/// this is necessary because
fn wrap_builder<T: ArrayBuilder>(builder: T, kind: ListKind) -> Box<dyn ArrayBuilder> {
    match kind {
        ListKind::List => Box::new(ListBuilder::new(builder)),
        ListKind::LargeList => Box::new(LargeListBuilder::new(builder)),
        ListKind::NotList => Box::new(builder),
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

fn append_all_fields(
    fields: &Vec<Field>,
    builder: &mut StructBuilder,
    msg: Option<&DynamicMessage>,
) -> Result<()> {
    for (i, field) in fields.iter().enumerate() {
        append_field(i, field, msg.unwrap(), builder)?;
    }
    builder.append(true);
    Ok(())
}

/// Append a protobuf value from the same field name to the
/// i-th field builder. Assumes that the i-th field builder is the
/// ArrayBuilder for the given field
fn append_field(
    i: usize,
    f: &Field,
    msg: &DynamicMessage,
    builder: &mut StructBuilder,
) -> Result<()> {
    let name = f.name();
    let desc = msg
        .descriptor()
        .get_field_by_name(name)
        .ok_or_else(|| Error::new(InvalidData, format!("Field {name} not found")))?;
    let is_missing_field = desc.supports_presence() && !msg.has_field_by_name(name);
    match f.data_type() {
        DataType::List(_) | DataType::LargeList(_) => {
            if is_missing_field {
                append_list_value(f, builder, i, None)
            } else {
                append_list_value(
                    f,
                    builder,
                    i,
                    msg.get_field_by_name(name).unwrap().as_list(),
                )
            }
        }
        _ => {
            let value_option = if is_missing_field {
                None
            } else {
                msg.get_field_by_name(name)
            };
            append_non_list_value(f, builder, i, value_option)
        }
    }
}

fn append_non_list_value(
    f: &Field,
    builder: &mut StructBuilder,
    i: usize,
    value: Option<Cow<Value>>,
) -> Result<()> {
    if let Some(Cow::Borrowed(Value::EnumNumber(num))) = value {
        let b = builder.field_builder::<Int32Builder>(i).unwrap();
        b.append_value(*num);
        return Ok(());
    }

    /// append a value to given builder
    macro_rules! set_value {
        ($builder:expr,$i:expr,$typ:ty,$getter:ident,$value:expr) => {{
            let b: &mut $typ = $builder.field_builder::<$typ>($i).unwrap();
            match $value {
                Some(cow) => {
                    let v = cow.$getter().ok_or_else(|| {
                        let msg = format!("Could not cast {} to correct type", cow);
                        Error::new(InvalidData, msg)
                    })?;
                    b.append_value(v)
                }
                None => b.append_null(),
            }
        }};
    }

    match f.data_type() {
        DataType::Float64 => set_value!(builder, i, Float64Builder, as_f64, value),
        DataType::Float32 => set_value!(builder, i, Float32Builder, as_f32, value),
        DataType::Int64 => set_value!(builder, i, Int64Builder, as_i64, value),
        DataType::Int32 => set_value!(builder, i, Int32Builder, as_i32, value),
        DataType::UInt64 => set_value!(builder, i, UInt64Builder, as_u64, value),
        DataType::UInt32 => set_value!(builder, i, UInt32Builder, as_u32, value),
        DataType::Utf8 => set_value!(builder, i, StringBuilder, as_str, value),
        DataType::LargeUtf8 => set_value!(builder, i, LargeStringBuilder, as_str, value),
        DataType::Binary => set_value!(builder, i, BinaryBuilder, as_bytes, value),
        DataType::LargeBinary => set_value!(builder, i, LargeBinaryBuilder, as_bytes, value),
        DataType::Boolean => set_value!(builder, i, BooleanBuilder, as_bool, value),
        DataType::Struct(nested_fields) => {
            let b = builder.field_builder::<StructBuilder>(i).unwrap();
            match value {
                Some(v) => append_all_fields(&nested_fields, b, v.as_message())?,
                None => b.append_null(),
            }
        }
        _ => unimplemented!(
            "{}",
            format!("Unsupported field {} with type {}", f.name(), f.data_type())
        ),
    }
    Ok(())
}

macro_rules! set_list_val {
    // primitive inner
    ($builder:expr,$i:expr,$builder_typ:ty,$value_option:expr,$getter:ident,$value_typ:ty) => {{
        type ListBuilderType = ListBuilder<$builder_typ>;
        let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
        match $value_option {
            Some(lst) => {
                for v in lst {
                    b.values().append_value(v.$getter().unwrap());
                }
                b.append(true);
            }
            None => {
                b.values().append_null();
                b.append(false);
            }
        }
        Ok(())
    }};
}

fn append_list_value(
    f: &Field,
    builder: &mut StructBuilder,
    i: usize,
    value_option: Option<&[Value]>,
) -> Result<()> {
    let (DataType::List(inner) | DataType::LargeList(inner)) = f.data_type()  else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "append_list_value got a non-list field",
            ))

    };

    match inner.data_type() {
        DataType::Float64 => set_list_val!(builder, i, Float64Builder, value_option, as_f64, f64),
        DataType::Float32 => set_list_val!(builder, i, Float32Builder, value_option, as_f32, f32),
        DataType::Int64 => {
            set_list_val!(builder, i, Int64Builder, value_option, as_i64, i64)
        }
        DataType::Int32 => {
            set_list_val!(builder, i, Int32Builder, value_option, as_i32, i32)
        }
        DataType::UInt64 => set_list_val!(builder, i, UInt64Builder, value_option, as_u64, u64),
        DataType::UInt32 => set_list_val!(builder, i, UInt32Builder, value_option, as_u32, u32),
        DataType::Utf8 => set_list_val!(builder, i, StringBuilder, value_option, as_str, &str),
        DataType::LargeUtf8 => {
            set_list_val!(builder, i, LargeStringBuilder, value_option, as_str, &str)
        }
        DataType::Binary => set_list_val!(builder, i, BinaryBuilder, value_option, as_bytes, Bytes),
        DataType::LargeBinary => set_list_val!(
            builder,
            i,
            LargeBinaryBuilder,
            value_option,
            as_bytes,
            Bytes
        ),
        DataType::Boolean => set_list_val!(builder, i, BooleanBuilder, value_option, as_bool, bool),
        DataType::Struct(nested_fields) => {
            let b: &mut ListBuilder<StructBuilder> = builder.field_builder(i).unwrap();
            match value_option {
                Some(lst) => {
                    for v in lst {
                        append_all_fields(nested_fields, b.values(), v.as_message())?;
                    }
                    b.append(true);
                }
                None => {
                    append_all_fields(nested_fields, b.values(), None)?;
                    b.append(false);
                }
            }
            Ok(())
        }
        inner_type => unimplemented!("Unsupported inner_type {}", inner_type),
    }
}
