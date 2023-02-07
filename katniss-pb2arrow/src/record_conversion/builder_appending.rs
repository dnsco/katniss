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
        append_field(i, field, msg, builder)?;
    }
    builder.append(msg.is_some());
    Ok(())
}

/// Append a protobuf value from the same field name to the
/// i-th field builder. Assumes that the i-th field builder is the
/// ArrayBuilder for the given field
fn append_field(
    i: usize,
    f: &Field,
    msg: Option<&DynamicMessage>,
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
    msg: Option<&DynamicMessage>,
) -> Result<()> {
    let fd_option = msg
        .map(|msg| {
            msg.descriptor()
                .get_field_by_name(f.name())
                .ok_or_else(|| ProstArrowError::DescriptorNotFound(f.name().to_owned()))
        })
        .transpose()?;

    let cow = msg.and_then(|msg| msg.get_field_by_name(f.name()));

    let has_field = msg
        .map(|msg| msg.has_field_by_name(f.name()))
        .unwrap_or(false);
    let has_presence = fd_option
        .clone()
        .map(|fd| fd.supports_presence())
        .unwrap_or(false);
    let val = if has_presence && !has_field {
        None
    } else {
        cow.as_deref()
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
            if let Some(Value::Message(_)) = val {
                //unit variant structs
                Some(true)
            } else {
                parse_val(val, Value::as_bool)?
            },
        ),
        DataType::Dictionary(_, _) => {
            let f = field_builder::<StringDictionaryBuilder<Int32Type>>(struct_builder, i);

            let intval = val.and_then(|v| v.as_i32());
            match intval {
                Some(intval) => {
                    let kind = fd_option.unwrap().kind();
                    let enum_descriptor = kind
                        .as_enum()
                        .ok_or_else(|| ProstArrowError::NonEnumField)?;

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
                None => {
                    append_all_fields(&nested_fields, b, None)?;
                }
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
    msg: Option<&DynamicMessage>,
) -> Result<()> {
    let fd_option = msg
        .map(|msg| {
            msg.descriptor()
                .get_field_by_name(f.name())
                .ok_or_else(|| ProstArrowError::DescriptorNotFound(f.name().to_owned()))
        })
        .transpose()?;

    let cow = msg.and_then(|msg| msg.get_field_by_name(f.name()));

    let has_field = msg
        .map(|msg| msg.has_field_by_name(f.name()))
        .unwrap_or(false);
    let has_presence = fd_option
        .clone()
        .map(|fd| fd.supports_presence())
        .unwrap_or(false);

    let v: Option<&Value> = if has_presence && !has_field {
        None
    } else {
        cow.as_deref()
    };

    let values = if let Some(v) = v { v.as_list() } else { None };

    let (DataType::List(inner) | DataType::LargeList(inner)) = f.data_type() else {
        return Err(ProstArrowError::NonListField)
    };

    match inner.data_type() {
        DataType::Float64 => extend_builder(
            field_builder::<ListBuilder<Float64Builder>>(struct_builder, i),
            parse_list(values, Value::as_f64)?,
        ),
        DataType::Float32 => extend_builder(
            field_builder::<ListBuilder<Float32Builder>>(struct_builder, i),
            parse_list(values, Value::as_f32)?,
        ),
        DataType::Int64 => extend_builder(
            field_builder::<ListBuilder<Int64Builder>>(struct_builder, i),
            parse_list(values, Value::as_i64)?,
        ),
        DataType::Int32 => extend_builder(
            field_builder::<ListBuilder<Int32Builder>>(struct_builder, i),
            parse_list(values, Value::as_i32)?,
        ),
        DataType::UInt64 => extend_builder(
            field_builder::<ListBuilder<UInt64Builder>>(struct_builder, i),
            parse_list(values, Value::as_u64)?,
        ),
        DataType::UInt32 => extend_builder(
            field_builder::<ListBuilder<UInt32Builder>>(struct_builder, i),
            parse_list(values, Value::as_u32)?,
        ),
        DataType::Utf8 => extend_builder(
            field_builder::<ListBuilder<StringBuilder>>(struct_builder, i),
            parse_list(values, Value::as_str)?,
        ),
        DataType::LargeUtf8 => extend_builder(
            field_builder::<ListBuilder<LargeStringBuilder>>(struct_builder, i),
            parse_list(values, Value::as_str)?,
        ),
        DataType::Binary => extend_builder(
            field_builder::<ListBuilder<BinaryBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bytes)?,
        ),
        DataType::LargeBinary => extend_builder(
            field_builder::<ListBuilder<LargeBinaryBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bytes)?,
        ),
        DataType::Boolean => extend_builder(
            field_builder::<ListBuilder<BooleanBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bool)?,
        ),
        DataType::Dictionary(_, _) => {
            let kind = fd_option.unwrap().kind();
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
            let b = field_builder::<ListBuilder<StructBuilder>>(struct_builder, i);
            match values {
                Some(lst) => {
                    for v in lst {
                        append_all_fields(nested_fields, b.values(), v.as_message())?;
                    }
                    b.append(true);
                }
                None => {
                    // I'm really curious about append_all_fields None,
                    // Must we append all child fields or can we lift the null higher?
                    // In that case append_all_fields could just take a DynamicMessage rather than an Option
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
    builder.field_builder(i).expect("schema conversion error?")
}

fn parse_val<'val, 'ret: 'val, R, F>(value: Option<&'val Value>, getter: F) -> Result<Option<R>>
where
    F: Fn(&'val Value) -> Option<R> + 'ret,
{
    value
        .map(|v| getter(v).ok_or_else(|| ProstArrowError::TypeCastError(v.clone())))
        .transpose()
}

fn parse_list<'val, 'ret: 'val, F, R>(
    values: Option<&'val [Value]>,
    getter: F,
) -> Result<Option<Vec<Option<R>>>>
where
    R: std::fmt::Debug,
    F: Fn(&'val Value) -> Option<R> + 'ret,
{
    values
        .map(|vs| {
            vs.iter()
                .map(|v| match getter(v) {
                    Some(v) => Ok(Some(v)),
                    None => Err(ProstArrowError::TypeCastError(v.clone())),
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
}

fn extend_builder<B, V>(builder: &mut B, val: V) -> Result<()>
where
    B: Extend<V>,
{
    builder.extend(std::iter::once(val));
    Ok(())
}
