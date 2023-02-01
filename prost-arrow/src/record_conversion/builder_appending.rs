use std::io::ErrorKind::InvalidData;
use std::io::{Error, ErrorKind, Result};
use std::ops::Deref;

use arrow_array::builder::*;
use arrow_schema::{DataType, Field};
use prost_reflect::{DynamicMessage, ReflectMessage, Value};

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

fn append_non_list_value<V: Deref<Target = Value> + std::fmt::Display>(
    f: &Field,
    builder: &mut StructBuilder,
    i: usize,
    value: Option<V>,
) -> Result<()> {
    if let Some(Value::EnumNumber(num)) = value.as_deref() {
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
        DataType::Float64 => {
            set_list_val!(builder, i, Float64Builder, value_option, as_f64, f64)
        }
        DataType::Float32 => {
            set_list_val!(builder, i, Float32Builder, value_option, as_f32, f32)
        }
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
        DataType::Binary => {
            set_list_val!(builder, i, BinaryBuilder, value_option, as_bytes, Bytes)
        }
        DataType::LargeBinary => set_list_val!(
            builder,
            i,
            LargeBinaryBuilder,
            value_option,
            as_bytes,
            Bytes
        ),
        DataType::Boolean => {
            set_list_val!(builder, i, BooleanBuilder, value_option, as_bool, bool)
        }
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
