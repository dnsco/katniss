use arrow_array::builder::*;
use arrow_array::types::Int32Type;
use arrow_schema::{DataType, Field};
use prost_reflect::{DynamicMessage, ReflectMessage, Value};

use crate::{ProstArrowError, Result};

pub fn append_all_fields(
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
    match f.data_type() {
        DataType::List(_) | DataType::LargeList(_) => append_list_value(f, builder, i, msg),
        _ => append_non_list_value(f, builder, i, msg),
    }
}

fn append_non_list_value(
    f: &Field,
    struct_builder: &mut StructBuilder,
    i: usize,
    msg: &DynamicMessage,
) -> Result<()> {
    let field_descriptor = msg
        .descriptor()
        .get_field_by_name(f.name())
        .ok_or_else(|| ProstArrowError::DescriptorNotFound(f.name().to_owned()))?;
    let value_option = msg.get_field_by_name(f.name());
    let val = if field_descriptor.supports_presence() && !msg.has_field_by_name(f.name()) {
        None
    } else {
        value_option.as_deref()
    };

    match f.data_type() {
        DataType::Float64 => extend_builder(
            field_builder::<Float64Builder>(struct_builder, i),
            parse_val(val, Value::as_f64)?,
        ),
        DataType::Float32 => extend_builder(
            field_builder::<Float32Builder>(struct_builder, i),
            parse_val(val, Value::as_f32)?,
        ),
        DataType::Int64 => extend_builder(
            field_builder::<Int64Builder>(struct_builder, i),
            parse_val(val, Value::as_i64)?,
        ),
        DataType::Int32 => extend_builder(
            field_builder::<Int32Builder>(struct_builder, i),
            parse_val(val, Value::as_i32)?,
        ),
        DataType::UInt64 => extend_builder(
            field_builder::<UInt64Builder>(struct_builder, i),
            parse_val(val, Value::as_u64)?,
        ),
        DataType::UInt32 => extend_builder(
            field_builder::<UInt32Builder>(struct_builder, i),
            parse_val(val, Value::as_u32)?,
        ),
        DataType::Utf8 => extend_builder(
            field_builder::<StringBuilder>(struct_builder, i),
            parse_val(val, Value::as_str)?,
        ),
        DataType::LargeUtf8 => extend_builder(
            field_builder::<LargeStringBuilder>(struct_builder, i),
            parse_val(val, Value::as_str)?,
        ),
        DataType::Binary => extend_builder(
            field_builder::<BinaryBuilder>(struct_builder, i),
            parse_val(val, Value::as_bytes)?,
        ),
        DataType::LargeBinary => extend_builder(
            field_builder::<LargeBinaryBuilder>(struct_builder, i),
            parse_val(val, Value::as_bytes)?,
        ),
        DataType::Boolean => extend_builder(
            field_builder::<BooleanBuilder>(struct_builder, i),
            parse_val(val, Value::as_bool)?,
        ),
        DataType::Dictionary(_, _) => {
            let f = field_builder::<StringDictionaryBuilder<Int32Type>>(struct_builder, i);

            let kind = field_descriptor.kind();
            let enum_descriptor = kind
                .as_enum()
                .ok_or_else(|| ProstArrowError::NonEnumField)?;
            let intval = val.map(|v| v.as_i32()).flatten();
            match intval {
                Some(intval) => {
                    let enum_value = enum_descriptor
                        .get_value(intval)
                        .ok_or_else(|| ProstArrowError::NoEnumValue(intval))?;
                    f.append(enum_value.name())
                        .map_err(|err| ProstArrowError::InvalidEnumValue(err))?;
                }
                None => f.append_null(),
            };
            Ok(())
        }
        DataType::Struct(nested_fields) => {
            let b = field_builder::<StructBuilder>(struct_builder, i);
            match val {
                Some(v) => append_all_fields(&nested_fields, b, v.as_message())?,
                None => b.append_null(),
            };
            Ok(())
        }
        _ => unimplemented!(
            "{}",
            format!("Unsupported field {} with type {}", f.name(), f.data_type())
        ),
    }
}

fn append_list_value(
    f: &Field,
    struct_builder: &mut StructBuilder,
    i: usize,
    msg: &DynamicMessage,
) -> Result<()> {
    let field_descriptor = msg
        .descriptor()
        .get_field_by_name(f.name())
        .ok_or_else(|| ProstArrowError::DescriptorNotFound(f.name().to_owned()))?;

    let cow = msg.get_field_by_name(f.name()).unwrap(); // already checked by field descriptor get
    let values: Option<&[Value]> =
        if field_descriptor.supports_presence() && !msg.has_field_by_name(f.name()) {
            None
        } else {
            cow.as_list()
        };

    let (DataType::List(inner) | DataType::LargeList(inner)) = f.data_type()  else {
        return Err(ProstArrowError::NonListField)
    };

    match inner.data_type() {
        DataType::Float64 => extend_builder(
            field_builder::<ListBuilder<Float64Builder>>(struct_builder, i),
            parse_list(values, Value::as_f64),
        ),
        DataType::Float32 => extend_builder(
            field_builder::<ListBuilder<Float32Builder>>(struct_builder, i),
            parse_list(values, Value::as_f32),
        ),
        DataType::Int64 => extend_builder(
            field_builder::<ListBuilder<Int64Builder>>(struct_builder, i),
            parse_list(values, Value::as_i64),
        ),
        DataType::Int32 => extend_builder(
            field_builder::<ListBuilder<Int32Builder>>(struct_builder, i),
            parse_list(values, Value::as_i32),
        ),
        DataType::UInt64 => extend_builder(
            field_builder::<ListBuilder<UInt64Builder>>(struct_builder, i),
            parse_list(values, Value::as_u64),
        ),
        DataType::UInt32 => extend_builder(
            field_builder::<ListBuilder<UInt32Builder>>(struct_builder, i),
            parse_list(values, Value::as_u32),
        ),
        DataType::Utf8 => extend_builder(
            field_builder::<ListBuilder<StringBuilder>>(struct_builder, i),
            parse_list(values, Value::as_str),
        ),
        DataType::LargeUtf8 => extend_builder(
            field_builder::<ListBuilder<LargeStringBuilder>>(struct_builder, i),
            parse_list(values, Value::as_str),
        ),
        DataType::Binary => extend_builder(
            field_builder::<ListBuilder<BinaryBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bytes),
        ),
        DataType::LargeBinary => extend_builder(
            field_builder::<ListBuilder<LargeBinaryBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bytes),
        ),
        DataType::Boolean => extend_builder(
            field_builder::<ListBuilder<BooleanBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bool),
        ),
        DataType::Dictionary(_, _) => {
            let kind = field_descriptor.kind();
            let enum_descriptor = kind
                .as_enum()
                .ok_or_else(|| ProstArrowError::NonEnumField)?;
            let f: &mut ListBuilder<StringDictionaryBuilder<Int32Type>> =
                field_builder(struct_builder, i);
            let val_lst: Option<Vec<Option<String>>> = values.map(|vs| {
                vs.iter()
                    .map(Value::as_i32)
                    .map(|v| match v {
                        Some(v) => enum_descriptor.get_value(v).map(|v| v.name().to_string()),
                        None => panic!("parse_errorlolol"),
                    })
                    .collect::<_>()
            });

            f.extend(std::iter::once(val_lst));
            Ok(())
        }
        DataType::Struct(nested_fields) => {
            let b: &mut ListBuilder<StructBuilder> = struct_builder.field_builder(i).unwrap();
            match values {
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

fn field_builder<T: ArrayBuilder>(builder: &mut StructBuilder, i: usize) -> &mut T {
    builder.field_builder(i).unwrap()
}

fn parse_val<'val, 'ret: 'val, R, F>(value: Option<&'val Value>, getter: F) -> Result<Option<R>>
where
    F: FnOnce(&'val Value) -> Option<R> + 'ret,
{
    value
        .map(|v| getter(v).ok_or_else(|| ProstArrowError::TypeCastError(v.clone())))
        .transpose()
}

fn parse_list<'val, 'ret: 'val, F, R>(
    values: Option<&'val [Value]>,
    getter: F,
) -> Option<Vec<Option<R>>>
where
    R: std::fmt::Debug,
    F: FnMut(&'val Value) -> Option<R> + 'ret,
{
    values.map(|vs| {
        vs.iter()
            .map(getter)
            .map(|v| match v {
                Some(v) => Some(v),
                None => panic!("parse_errorlolol"),
            })
            .collect::<Vec<Option<R>>>()
    })
}

fn extend_builder<B, V>(builder: &mut B, val: V) -> Result<()>
where
    B: Extend<V>,
{
    builder.extend(std::iter::once(val));
    Ok(())
}
