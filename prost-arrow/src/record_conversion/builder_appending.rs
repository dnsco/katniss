use arrow_array::builder::*;
use arrow_array::types::Int32Type;
use arrow_schema::ArrowError::IoError;
use arrow_schema::{DataType, Field};
use prost_reflect::{DynamicMessage, ReflectMessage, Value};
use std::io::ErrorKind::{InvalidData, NotFound};
use std::io::{Error, ErrorKind, Result};
use std::ops::Deref;

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
        DataType::List(_) | DataType::LargeList(_) => append_list_value(f, builder, i, msg),
        _ => append_non_list_value(f, builder, i, msg),
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

fn append_non_list_value(
    f: &Field,
    struct_builder: &mut StructBuilder,
    i: usize,
    msg: &DynamicMessage,
) -> Result<()> {
    let field_descriptor = msg
        .descriptor()
        .get_field_by_name(f.name())
        .ok_or_else(|| {
            Error::new(
                NotFound,
                format!("Could not find descriptor for {}", f.name()),
            )
        })?;
    let value_option = msg.get_field_by_name(f.name());
    let val = if field_descriptor.supports_presence() && !msg.has_field_by_name(f.name()) {
        None
    } else {
        value_option.as_deref()
    };

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
        DataType::Dictionary(_, _) => {
            let f = field_builder::<StringDictionaryBuilder<Int32Type>>(struct_builder, i);

            let kind = field_descriptor.kind();
            let enum_descriptor = kind
                .as_enum()
                .ok_or_else(|| Error::new(InvalidData, "field was not an enum"))?;
            let intval = val.map(|v| v.as_i32()).flatten();
            match intval {
                Some(intval) => {
                    let enum_value = enum_descriptor.get_value(intval).ok_or_else(|| {
                        Error::new(InvalidData, format!("No enum value for {intval}"))
                    })?;
                    f.append(enum_value.name())
                        .map_err(|err| Error::new(InvalidData, err.to_string()))?;
                }
                None => f.append_null(),
            }
        }
        DataType::Struct(nested_fields) => {
            let b = field_builder::<StructBuilder>(struct_builder, i);
            match val {
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

fn set_list_values<B, L>(builder: &mut B, vals: L) -> std::result::Result<(), Error>
where
    B: Extend<L>,
{
    builder.extend(std::iter::once(vals));
    Ok(())
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
        .ok_or_else(|| {
            Error::new(
                NotFound,
                format!("Could not find descriptor for {}", f.name()),
            )
        })?;
    let cow = msg.get_field_by_name(f.name()).unwrap(); // already checked by field descriptor get
    let values: Option<&[Value]> =
        if field_descriptor.supports_presence() && !msg.has_field_by_name(f.name()) {
            None
        } else {
            cow.as_list()
        };

    let (DataType::List(inner) | DataType::LargeList(inner)) = f.data_type()  else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "append_list_value got a non-list field",
        ))
    };

    match inner.data_type() {
        DataType::Float64 => set_list_values(
            field_builder::<ListBuilder<Float64Builder>>(struct_builder, i),
            parse_list(values, Value::as_f64),
        ),
        DataType::Float32 => set_list_values(
            field_builder::<ListBuilder<Float32Builder>>(struct_builder, i),
            parse_list(values, Value::as_f32),
        ),
        DataType::Int64 => set_list_values(
            field_builder::<ListBuilder<Int64Builder>>(struct_builder, i),
            parse_list(values, Value::as_i64),
        ),
        DataType::Int32 => set_list_values(
            field_builder::<ListBuilder<Int32Builder>>(struct_builder, i),
            parse_list(values, Value::as_i32),
        ),
        DataType::UInt64 => set_list_values(
            field_builder::<ListBuilder<UInt64Builder>>(struct_builder, i),
            parse_list(values, Value::as_u64),
        ),
        DataType::UInt32 => set_list_values(
            field_builder::<ListBuilder<UInt32Builder>>(struct_builder, i),
            parse_list(values, Value::as_u32),
        ),
        DataType::Utf8 => set_list_values(
            field_builder::<ListBuilder<StringBuilder>>(struct_builder, i),
            parse_list(values, Value::as_str),
        ),
        DataType::LargeUtf8 => set_list_values(
            field_builder::<ListBuilder<LargeStringBuilder>>(struct_builder, i),
            parse_list(values, Value::as_str),
        ),
        DataType::Binary => set_list_values(
            field_builder::<ListBuilder<BinaryBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bytes),
        ),
        DataType::LargeBinary => set_list_values(
            field_builder::<ListBuilder<LargeBinaryBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bytes),
        ),

        DataType::Boolean => set_list_values(
            field_builder::<ListBuilder<BooleanBuilder>>(struct_builder, i),
            parse_list(values, Value::as_bool),
        ),
        DataType::Dictionary(_, _) => {
            let kind = field_descriptor.kind();
            let enum_descriptor = kind
                .as_enum()
                .ok_or_else(|| Error::new(InvalidData, "field was not an enum"))?;
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
