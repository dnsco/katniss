use std::borrow::Cow;
use std::io::ErrorKind::InvalidData;
use std::io::{Error, Result};
use std::sync::Arc;

use arrow_array::builder::*;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, SchemaRef};
use prost_reflect::{DynamicMessage, ReflectMessage, Value};

/// Parse messages and return RecordBatch
pub struct RecordBatchConverter {
    pub(crate) schema: SchemaRef,
    #[allow(unused)]
    batch_size: usize,
    builder: StructBuilder, // fields align with schema
}

macro_rules! make_list {
    ($builder:expr,$nlevels:expr) => {{
        if $nlevels == 0 {
            Box::new($builder) as Box<dyn ArrayBuilder>
        } else if $nlevels == 1 {
            Box::new(ListBuilder::new($builder)) as Box<dyn ArrayBuilder>
        } else if $nlevels == 2 {
            Box::new(ListBuilder::new(ListBuilder::new($builder))) as Box<dyn ArrayBuilder>
        } else if $nlevels == 3 {
            Box::new(ListBuilder::new(ListBuilder::new(ListBuilder::new(
                $builder,
            )))) as Box<dyn ArrayBuilder>
        } else {
            unimplemented!("matryoshka")
        }
    }};
}

macro_rules! make_builder {
    ($datatype:expr,$capacity:expr,$nlevels:expr) => {{
        match $datatype {
            DataType::Boolean => make_list!(BooleanBuilder::with_capacity($capacity), $nlevels),
            DataType::Int32 => make_list!(Int32Builder::with_capacity($capacity), $nlevels),
            DataType::Int64 => make_list!(Int64Builder::with_capacity($capacity), $nlevels),
            DataType::UInt32 => make_list!(UInt32Builder::with_capacity($capacity), $nlevels),
            DataType::UInt64 => make_list!(UInt64Builder::with_capacity($capacity), $nlevels),
            DataType::Float32 => make_list!(Float32Builder::with_capacity($capacity), $nlevels),
            DataType::Float64 => make_list!(Float64Builder::with_capacity($capacity), $nlevels),
            DataType::Binary => {
                make_list!(BinaryBuilder::with_capacity($capacity, 1024), $nlevels)
            }
            DataType::LargeBinary => {
                make_list!(LargeBinaryBuilder::with_capacity($capacity, 1024), $nlevels)
            }
            DataType::Utf8 => {
                make_list!(StringBuilder::with_capacity($capacity, 1024), $nlevels)
            }
            DataType::LargeUtf8 => {
                make_list!(LargeStringBuilder::with_capacity($capacity, 1024), $nlevels)
            }
            DataType::Struct(fields) => {
                make_list!(
                    RecordBatchConverter::make_struct_builder(fields.clone(), $capacity),
                    $nlevels
                )
            }
            t => panic!("Data type {:?} is not currently supported", t),
        }
    }};
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

    fn make_builder(field: &Field, capacity: usize) -> Box<dyn ArrayBuilder> {
        let (inner_typ, nlevels) = get_list_levels(field);
        make_builder!(inner_typ, capacity, nlevels)
    }

    pub fn records(&mut self) -> core::result::Result<RecordBatch, ArrowError> {
        self.try_into()
    }
}

/// Convert RecordBatch from RecordBatchConverter.
impl TryFrom<&mut RecordBatchConverter> for RecordBatch {
    type Error = ArrowError;

    fn try_from(converter: &mut RecordBatchConverter) -> core::result::Result<Self, Self::Error> {
        let arrays: Vec<ArrayRef> = converter
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| get_array(field, &mut converter.builder, i).unwrap())
            .collect();
        RecordBatch::try_new(SchemaRef::from(converter.schema.clone()), arrays)
    }
}

fn finish<T: ArrayBuilder>(
    builder: &mut StructBuilder,
    i: usize,
    nlevels: i32,
) -> Result<ArrayRef> {
    if nlevels == 0 {
        let b = builder.field_builder::<T>(i).unwrap();
        Ok(Arc::new(b.finish()) as ArrayRef)
    } else if nlevels == 1 {
        let b = builder.field_builder::<ListBuilder<T>>(i).unwrap();
        Ok(Arc::new(b.finish()) as ArrayRef)
    } else if nlevels == 2 {
        let b = builder
            .field_builder::<ListBuilder<ListBuilder<T>>>(i)
            .unwrap();
        Ok(Arc::new(b.finish()) as ArrayRef)
    } else if nlevels == 3 {
        let b = builder
            .field_builder::<ListBuilder<ListBuilder<ListBuilder<T>>>>(i)
            .unwrap();
        Ok(Arc::new(b.finish()) as ArrayRef)
    } else {
        Err(Error::new(
            InvalidData,
            "Dafuq you doing with this matryoshka doll",
        ))
    }
}

fn get_list_levels(f: &Field) -> (&DataType, i32) {
    match f.data_type() {
        DataType::List(values) | DataType::LargeList(values) => {
            let (inner_typ, nlevels) = get_list_levels(values);
            (inner_typ, nlevels + 1)
        }
        _ => (f.data_type(), 0),
    }
}

fn get_array(f: &Field, builder: &mut StructBuilder, i: usize) -> Result<ArrayRef> {
    let (typ, nlevels) = get_list_levels(f);
    match typ {
        DataType::Float64 => finish::<Float64Builder>(builder, i, nlevels),
        DataType::Float32 => finish::<Float32Builder>(builder, i, nlevels),
        DataType::Int64 => finish::<Int64Builder>(builder, i, nlevels),
        DataType::Int32 => finish::<Int32Builder>(builder, i, nlevels),
        DataType::UInt64 => finish::<UInt64Builder>(builder, i, nlevels),
        DataType::UInt32 => finish::<UInt32Builder>(builder, i, nlevels),
        DataType::Utf8 => finish::<StringBuilder>(builder, i, nlevels),
        DataType::LargeUtf8 => finish::<LargeStringBuilder>(builder, i, nlevels),
        DataType::Binary => finish::<BinaryBuilder>(builder, i, nlevels),
        DataType::LargeBinary => finish::<LargeBinaryBuilder>(builder, i, nlevels),
        DataType::Boolean => finish::<BooleanBuilder>(builder, i, nlevels),
        DataType::Struct(_) => finish::<StructBuilder>(builder, i, nlevels),
        _ => {
            let msg = format!("Unsupported field type {f}");
            unimplemented!("{}", msg)
        }
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

/// append a value to given builder
macro_rules! set_value {
    ($builder:expr,$i:expr,$typ:ty,$getter:ident,$value:expr) => {{
        let b: &mut $typ = $builder.field_builder::<$typ>($i).unwrap();

        match $value {
            Some(cow) => {
                dbg!(&cow);
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

macro_rules! set_list_value {
    ($builder:expr,$i:expr,$builder_typ:ty,$nlevels:expr,$value_option:expr,$getter:ident,$value_typ:ty) => {{
        if $nlevels == 1 {
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
        } else if $nlevels == 2 {
            type ListBuilderType = ListBuilder<ListBuilder<$builder_typ>>;
            let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
            match $value_option {
                Some(lst) => {
                    for v in lst {
                        b.values().values().append_value(v.$getter().unwrap());
                    }
                    b.values().append(true);
                    b.append(true)
                }
                None => {
                    b.values().values().append_null();
                    b.values().append(false);
                    b.append(false)
                }
            }
            Ok(())
        } else if $nlevels == 3 {
            type ListBuilderType = ListBuilder<ListBuilder<ListBuilder<$builder_typ>>>;
            let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
            match $value_option {
                Some(lst) => {
                    for v in lst {
                        b.values()
                            .values()
                            .values()
                            .append_value(v.$getter().unwrap());
                    }
                    b.values().values().append(true);
                    b.values().append(true);
                    b.append(true)
                }
                None => {
                    b.values().values().values().append_null();
                    b.values().values().append(false);
                    b.values().append(false);
                    b.append(false)
                }
            }
            Ok(())
        } else {
            Err(Error::new(
                InvalidData,
                "Dafuq you doing with this matryoshka doll",
            ))
        }
    }};
    ($fields:expr,$builder:expr,$i:expr,$nlevels:expr,$value_option:expr) => {{
        if $nlevels == 1 {
            type ListBuilderType = ListBuilder<StructBuilder>;
            let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
            match $value_option {
                Some(lst) => {
                    for v in lst {
                        append_all_fields($fields, b.values(), v.as_message())?;
                    }
                    b.append(true);
                }
                None => {
                    append_all_fields($fields, b.values(), None)?;
                    b.append(false);
                }
            }
            Ok(())
        } else if $nlevels == 2 {
            type ListBuilderType = ListBuilder<ListBuilder<StructBuilder>>;
            let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
            match $value_option {
                Some(lst) => {
                    for v in lst {
                        append_all_fields($fields, b.values().values(), v.as_message())?;
                    }
                    b.values().append(true);
                    b.append(true)
                }
                None => {
                    append_all_fields($fields, b.values().values(), None)?;
                    b.values().append(false);
                    b.append(false)
                }
            }
            Ok(())
        } else if $nlevels == 3 {
            type ListBuilderType = ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>;
            let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
            match $value_option {
                Some(lst) => {
                    for v in lst {
                        append_all_fields($fields, b.values().values().values(), v.as_message())?;
                    }
                    b.values().values().append(true);
                    b.values().append(true);
                    b.append(true)
                }
                None => {
                    append_all_fields($fields, b.values().values().values(), None)?;
                    b.values().values().append(false);
                    b.values().append(false);
                    b.append(false)
                }
            }
            Ok(())
        } else {
            Err(Error::new(
                InvalidData,
                "Dafuq you doing with this matryoshka doll",
            ))
        }
    }};
}

fn append_list_value(
    f: &Field,
    builder: &mut StructBuilder,
    i: usize,
    value_option: Option<&[Value]>,
) -> Result<()> {
    let (inner_typ, nlevels) = get_list_levels(f);
    match inner_typ {
        DataType::Float64 => set_list_value!(
            builder,
            i,
            Float64Builder,
            nlevels,
            value_option,
            as_f64,
            f64
        ),
        DataType::Float32 => set_list_value!(
            builder,
            i,
            Float32Builder,
            nlevels,
            value_option,
            as_f32,
            f32
        ),
        DataType::Int64 => {
            set_list_value!(builder, i, Int64Builder, nlevels, value_option, as_i64, i64)
        }
        DataType::Int32 => {
            set_list_value!(builder, i, Int32Builder, nlevels, value_option, as_i32, i32)
        }
        DataType::UInt64 => set_list_value!(
            builder,
            i,
            UInt64Builder,
            nlevels,
            value_option,
            as_u64,
            u64
        ),
        DataType::UInt32 => set_list_value!(
            builder,
            i,
            UInt32Builder,
            nlevels,
            value_option,
            as_u32,
            u32
        ),
        DataType::Utf8 => set_list_value!(
            builder,
            i,
            StringBuilder,
            nlevels,
            value_option,
            as_str,
            &str
        ),
        DataType::LargeUtf8 => set_list_value!(
            builder,
            i,
            LargeStringBuilder,
            nlevels,
            value_option,
            as_str,
            &str
        ),
        DataType::Binary => set_list_value!(
            builder,
            i,
            BinaryBuilder,
            nlevels,
            value_option,
            as_bytes,
            Bytes
        ),
        DataType::LargeBinary => set_list_value!(
            builder,
            i,
            LargeBinaryBuilder,
            nlevels,
            value_option,
            as_bytes,
            Bytes
        ),
        DataType::Boolean => set_list_value!(
            builder,
            i,
            BooleanBuilder,
            nlevels,
            value_option,
            as_bool,
            bool
        ),
        DataType::Struct(nested_fields) => {
            set_list_value!(nested_fields, builder, i, nlevels, value_option)
        }
        _ => unimplemented!("Unsupported inner_typ {}", inner_typ),
    }
}
