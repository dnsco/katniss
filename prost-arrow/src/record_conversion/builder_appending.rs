use arrow_array::builder::*;
use arrow_schema::{DataType, Field};
use prost_reflect::{DynamicMessage, ReflectMessage, Value};
use std::io::ErrorKind::InvalidData;
use std::io::{Error, ErrorKind, Result};
use std::ops::Deref;

use super::macros::*;

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

fn field_builder<T: ArrayBuilder>(builder: &mut StructBuilder, i: usize) -> &mut T {
    builder.field_builder(i).unwrap()
}

fn set_val<'val, 'ret: 'val, T, R, F>(
    builder: &mut T,
    value: Option<&'val Value>,
    getter: F,
) -> Result<()>
where
    T: Extend<Option<R>>,
    F: FnOnce(&'val Value) -> Option<R> + 'ret,
{
    let v = value
        .map(|v| {
            getter(v).ok_or_else(|| {
                let msg = format!("Could not cast {} to correct type", v);
                Error::new(InvalidData, msg)
            })
        })
        .transpose()?;
    builder.extend(std::iter::once(v));
    Ok(())
}

fn append_non_list_value<V>(
    f: &Field,
    struct_builder: &mut StructBuilder,
    i: usize,
    value: Option<V>,
) -> Result<()>
where
    V: Deref<Target = Value> + std::fmt::Display,
{
    if let Some(Value::EnumNumber(num)) = value.as_deref() {
        let b = struct_builder.field_builder::<Int32Builder>(i).unwrap();
        b.append_value(*num);
        return Ok(());
    }
    let val = value.as_deref();

    match f.data_type() {
        DataType::Float64 => {
            let f = field_builder::<Float64Builder>(struct_builder, i);
            set_val(f, val, Value::as_f64)?
        }
        DataType::Float32 => {
            let f = field_builder::<Float32Builder>(struct_builder, i);
            set_val(f, val, Value::as_f32)?
        }
        DataType::Int64 => {
            let f = field_builder::<Int64Builder>(struct_builder, i);
            set_val(f, val, Value::as_i64)?
        }
        DataType::Int32 => {
            let f = field_builder::<Int32Builder>(struct_builder, i);
            set_val(f, val, Value::as_i32)?
        }
        DataType::UInt64 => {
            let f = field_builder::<UInt64Builder>(struct_builder, i);
            set_val(f, val, Value::as_u64)?
        }
        DataType::UInt32 => {
            let f = field_builder::<UInt32Builder>(struct_builder, i);
            set_val(f, val, Value::as_u32)?
        }
        DataType::Utf8 => {
            let f = field_builder::<StringBuilder>(struct_builder, i);
            set_val(f, val, Value::as_str)?
        }
        DataType::LargeUtf8 => {
            let f = field_builder::<LargeStringBuilder>(struct_builder, i);
            set_val(f, val, Value::as_str)?
        }
        DataType::Binary => {
            let f = field_builder::<BinaryBuilder>(struct_builder, i);
            set_val(f, val, Value::as_bytes)?
        }
        DataType::LargeBinary => {
            let f = field_builder::<LargeBinaryBuilder>(struct_builder, i);
            set_val(f, val, Value::as_bytes)?
        }
        DataType::Boolean => {
            let f = field_builder::<BooleanBuilder>(struct_builder, i);
            set_val(f, val, Value::as_bool)?
        }

        DataType::Struct(nested_fields) => {
            let b = field_builder::<StructBuilder>(struct_builder, i);
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

fn append_list_value(
    f: &Field,
    struct_builder: &mut StructBuilder,
    i: usize,
    value_option: Option<&[Value]>,
) -> Result<()> {
    let (DataType::List(inner) | DataType::LargeList(inner)) = f.data_type() else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "append_list_value got a non-list field",
        ))
    };

    match inner.data_type() {
        DataType::Float64 => {
            let builder = field_builder::<ListBuilder<Float64Builder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_f64, f64)
        }
        DataType::Float32 => {
            let builder = field_builder::<ListBuilder<Float32Builder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_f32, f32)
        }
        DataType::Int64 => {
            let builder = field_builder::<ListBuilder<Int64Builder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_i64, i64)
        }
        DataType::Int32 => {
            let builder = field_builder::<ListBuilder<Int32Builder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_i32, i32)
        }
        DataType::UInt64 => {
            let builder = field_builder::<ListBuilder<UInt64Builder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_u64, u64)
        }
        DataType::UInt32 => {
            let builder = field_builder::<ListBuilder<UInt32Builder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_u32, u32)
        }
        DataType::Utf8 => {
            let builder = field_builder::<ListBuilder<StringBuilder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_str, &str)
        }
        DataType::LargeUtf8 => {
            let builder = field_builder::<ListBuilder<LargeStringBuilder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_str, &str)
        }
        DataType::Binary => {
            let builder = field_builder::<ListBuilder<BinaryBuilder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_bytes, Bytes)
        }
        DataType::LargeBinary => {
            let builder = field_builder::<ListBuilder<LargeBinaryBuilder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_bytes, Bytes)
        }
        DataType::Boolean => {
            let builder = field_builder::<ListBuilder<BooleanBuilder>>(struct_builder, i);
            set_list_val!(builder, value_option, as_bool, bool)
        }
        DataType::Struct(nested_fields) => {
            let b: &mut ListBuilder<StructBuilder> = struct_builder.field_builder(i).unwrap();
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
