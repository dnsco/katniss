//! Convert Protobuf schema and message into Apache Arrow Schema and Tables.
//!

use std::borrow::Cow;
use arrow_array::builder::*;
use arrow_array::{ArrayRef, RecordBatch};
use std::collections::HashSet;

use std::io::ErrorKind::InvalidData;
use std::io::{BufReader, Error, ErrorKind, Read, Result};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use prost_reflect::{DescriptorPool, DynamicMessage, FieldDescriptor, MessageDescriptor, ReflectMessage, Value};
use tempfile::NamedTempFile;

/// Dynamically convert protobuf messages to Arrow table or Schema.
#[derive(Debug, Clone)]
pub struct Converter {
    pub(crate) descriptor_pool: DescriptorPool,
}

/// Parse messages and return RecordBatch
pub struct RecordBatchConverter {
    pub(crate) schema: SchemaRef,
    #[allow(unused)]
    batch_size: usize,
    builder: StructBuilder, // fields align with schema
}

/// Convert prost FieldDescriptor to arrow Field
fn to_arrow(f: &FieldDescriptor) -> Result<Field> {
    let name = f.name();
    let mut data_type = match f.kind() {
        prost_reflect::Kind::Double => DataType::Float64,
        prost_reflect::Kind::Float => DataType::Float32,
        prost_reflect::Kind::Int32 => DataType::Int32,
        prost_reflect::Kind::Int64 => DataType::Int64,
        prost_reflect::Kind::Uint32 => DataType::UInt32,
        prost_reflect::Kind::Uint64 => DataType::UInt64,
        prost_reflect::Kind::Sint32 => DataType::Int32,
        prost_reflect::Kind::Sint64 => DataType::Int64,
        prost_reflect::Kind::Fixed32 => DataType::UInt32,
        prost_reflect::Kind::Fixed64 => DataType::UInt64,
        prost_reflect::Kind::Sfixed32 => DataType::Int32,
        prost_reflect::Kind::Bool => DataType::Boolean,
        prost_reflect::Kind::Sfixed64 => DataType::Int64,
        prost_reflect::Kind::String => DataType::Utf8,
        prost_reflect::Kind::Bytes => DataType::Binary,
        prost_reflect::Kind::Message(msg) => {
            DataType::Struct(msg.fields().map(|f| to_arrow(&f)).collect::<Result<_>>()?)
        }
        prost_reflect::Kind::Enum(_) => panic!("Enum is not supported yet"),
    };
    if f.is_list() {
        data_type = DataType::List(Box::new(Field::new("item", data_type, true)));
    }
    Ok(Field::new(name, data_type, true))
}

impl Converter {
    /// Compile protobuf files and build the converter.
    ///
    /// ```rust
    ///   use prost_arrow::Converter;
    /// 
    ///   let convert = Converter::compile(
    ///       &["protos/foo.proto"], &["protos"]
    ///   ).unwrap();
    /// ```
    pub fn compile(protos: &[impl AsRef<Path>], includes: &[impl AsRef<Path>]) -> Result<Self> {
        let protoc = match which::which("protoc") {
            Ok(path) => path,
            Err(e) => return Err(Error::new(ErrorKind::NotFound, e.to_string())),
        };

        let mut file_descriptor_file = NamedTempFile::new()?;

        let mut cmd = Command::new(protoc);
        cmd.arg("--include_imports")
            .arg("-o")
            .arg(file_descriptor_file.path());
        cmd.args(protos.iter().map(|p| p.as_ref().as_os_str()));
        for include_path in includes {
            cmd.arg("-I").arg(include_path.as_ref().as_os_str());
        }
        cmd.output()?;
        let mut reader = BufReader::new(file_descriptor_file.as_file_mut());
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        let pool = DescriptorPool::decode(buffer.as_slice()).unwrap();
        Ok(Self {
            descriptor_pool: pool,
        })
    }

    /// Get the arrow schema of the protobuf message, specified by the qualified message name.
    pub fn get_arrow_schema(&self, name: &str, projection: &[&str]) -> Result<Option<Schema>> {
        let msg = match self.descriptor_pool.get_message_by_name(name) {
            Some(m) => m,
            None => return Ok(None),
        };
        let schema = Schema::new(
            msg.fields()
                .map(|f| to_arrow(&f))
                .collect::<Result<Vec<_>>>()?,
        );

        if projection.is_empty() {
            Ok(Some(schema))
        } else {
            let prefix = "".to_string();
            let proj_set: HashSet<&str> = HashSet::from_iter(projection.to_vec());
            let keep: Vec<Field> = project_fields(&prefix, schema.fields(), &proj_set);
            Ok(Some(Schema::new(keep)))
        }
    }

    pub fn get_arrow_schema_by_short_name(
        &self,
        short_name: &str,
        projection: &[&str],
    ) -> Result<Option<Schema>> {
        if let Some(m) = self
            .descriptor_pool
            .all_messages()
            .find(|m| m.name() == short_name)
        {
            self.get_arrow_schema(m.full_name(), projection)
        } else {
            Ok(None)
        }
    }

    pub fn get_message_short_name(&self, short_name: &str) -> Option<MessageDescriptor> {
        if let Some(m) = self
            .descriptor_pool
            .all_messages()
            .find(|m| m.name() == short_name)
        {
            self.descriptor_pool.get_message_by_name(m.full_name())
        } else {
            None
        }
    }
}

fn project_fields(prefix: &str, fields: &Vec<Field>, projection: &HashSet<&str>) -> Vec<Field> {
    let mut keep: Vec<Field> = Vec::new();
    for f in fields {
        // make qualified name
        let qualified = format!("{}.{}", prefix, f.name());
        let name = if prefix.is_empty() {
            f.name()
        } else {
            &qualified
        };
        if projection.contains(name.as_str()) {
            keep.push(f.clone());
        } else {
            match f.data_type() {
                DataType::Struct(subfields) => {
                    let subkeep = project_fields(name, subfields, projection);
                    if subkeep.len() > 0 {
                        keep.push(Field::new(
                            f.name(),
                            DataType::Struct(subkeep),
                            f.is_nullable(),
                        ));
                    }
                }
                _ => {} // discard the field
            }
        }
    }
    keep
}

macro_rules! make_list {
    ($builder:expr,$nlevels:expr) => {
        {
            if $nlevels == 0 {
                Box::new($builder) as Box<dyn ArrayBuilder>
            } else if $nlevels == 1 {
                Box::new(ListBuilder::new($builder)) as Box<dyn ArrayBuilder>
            } else if $nlevels == 2 {
                Box::new(ListBuilder::new(ListBuilder::new($builder))) as Box<dyn ArrayBuilder>
            } else if $nlevels == 3 {
                Box::new(ListBuilder::new(ListBuilder::new(ListBuilder::new($builder)))) as Box<dyn ArrayBuilder>
            } else {
                unimplemented!("matryoshka")
            }
        }
    }
}

macro_rules! make_builder {
    ($datatype:expr,$capacity:expr,$nlevels:expr) => {
        {
            match $datatype {
                DataType::Boolean => make_list!(BooleanBuilder::with_capacity($capacity),$nlevels),
                DataType::Int32 => make_list!(Int32Builder::with_capacity($capacity),$nlevels),
                DataType::Int64 => make_list!(Int64Builder::with_capacity($capacity),$nlevels),
                DataType::UInt32 => make_list!(UInt32Builder::with_capacity($capacity),$nlevels),
                DataType::UInt64 => make_list!(UInt64Builder::with_capacity($capacity),$nlevels),
                DataType::Float32 => make_list!(Float32Builder::with_capacity($capacity),$nlevels),
                DataType::Float64 => make_list!(Float64Builder::with_capacity($capacity),$nlevels),
                DataType::Binary => make_list!(BinaryBuilder::with_capacity($capacity, 1024), $nlevels),
                DataType::LargeBinary => make_list!(LargeBinaryBuilder::with_capacity($capacity, 1024), $nlevels),
                DataType::Utf8 => make_list!(StringBuilder::with_capacity($capacity, 1024), $nlevels),
                DataType::LargeUtf8 => make_list!(LargeStringBuilder::with_capacity($capacity, 1024), $nlevels),
                DataType::Struct(fields) => {
                    make_list!(RecordBatchConverter::make_struct_builder(fields.clone(), $capacity), $nlevels)
                },
                t => panic!("Data type {:?} is not currently supported", t),
            }
        }
    }
}

impl RecordBatchConverter {
    pub fn new(schema: SchemaRef, batch_size: usize) -> RecordBatchConverter {
        let builder = RecordBatchConverter::make_struct_builder(schema.fields().clone(), batch_size);
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
        make_builder!(inner_typ,capacity,nlevels)
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

macro_rules! finish {
    ($builder:expr,$i:expr,$typ:ident,$nlevels:expr) => {
        if $nlevels == 0 {
            let b = $builder.field_builder::<$typ>($i).unwrap();
            Ok(Arc::new(b.finish()) as ArrayRef)
        } else if $nlevels == 1 {
            let b = $builder.field_builder::<ListBuilder<$typ>>($i).unwrap();
            Ok(Arc::new(b.finish()) as ArrayRef)
        } else if $nlevels == 2 {
            let b = $builder.field_builder::<ListBuilder<ListBuilder<$typ>>>($i).unwrap();
            Ok(Arc::new(b.finish()) as ArrayRef)
        } else if $nlevels == 3 {
            let b = $builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<$typ>>>>($i).unwrap();
            Ok(Arc::new(b.finish()) as ArrayRef)
        } else {
            Err(Error::new(InvalidData, "Dafuq you doing with this matryoshka doll"))
        }
    }
}

fn get_list_levels(f: &Field) -> (&DataType, i32) {
    match f.data_type() {
        DataType::List(values) | DataType::LargeList(values) => {
            let (inner_typ, nlevels) = get_list_levels(values);
            (inner_typ, nlevels + 1)
        },
        _ => (f.data_type(), 0)
    }
}


fn get_array(f: &Field, builder: &mut StructBuilder, i: usize) -> Result<ArrayRef> {
    let (typ, nlevels) = get_list_levels(f);
    match typ {
        DataType::Float64 => finish!(builder, i, Float64Builder, nlevels),
        DataType::Float32 => finish!(builder, i, Float32Builder, nlevels),
        DataType::Int64 => finish!(builder, i, Int64Builder, nlevels),
        DataType::Int32 => finish!(builder, i, Int32Builder, nlevels),
        DataType::UInt64 => finish!(builder, i, UInt64Builder, nlevels),
        DataType::UInt32 => finish!(builder, i, UInt32Builder, nlevels),
        DataType::Utf8 => finish!(builder, i, StringBuilder, nlevels),
        DataType::LargeUtf8 => finish!(builder, i, LargeStringBuilder, nlevels),
        DataType::Binary => finish!(builder, i, BinaryBuilder, nlevels),
        DataType::LargeBinary => finish!(builder, i, LargeBinaryBuilder, nlevels),
        DataType::Boolean => finish!(builder, i, BooleanBuilder, nlevels),
        DataType::Struct(_) => finish!(builder, i, StructBuilder, nlevels),
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
    ($builder:expr,$i:expr,$typ:ty,$getter:ident,$value:expr) => {
        {
            let b: &mut $typ = $builder.field_builder::<$typ>($i).unwrap();
            match $value {
                Some(cow) => {
                    let v = cow.$getter().ok_or_else(|| {
                        let msg = format!("Could not cast {} to correct type", cow);
                        Error::new(InvalidData, msg)
                    })?;
                    b.append_value(v)
                },
                None => b.append_null()
            }
        }
    };
}


fn append_non_list_value(f: &Field, builder: &mut StructBuilder, i: usize, value: Option<Cow<Value>>) -> Result<()> {
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
        },
        _ => unimplemented!("{}", format!("Unsupported field {} with type {}", f.name(), f.data_type()))
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
                append_list_value(f, builder, i, msg.get_field_by_name(name).unwrap().as_list())
            }
        },
        _ => {
            let value_option = if is_missing_field { None } else { msg.get_field_by_name(name) };
            append_non_list_value(f, builder, i, value_option)
        }
    }
}

macro_rules! set_list_value {
    ($builder:expr,$i:expr,$builder_typ:ty,$nlevels:expr,$value_option:expr,$getter:ident,$value_typ:ty) => {
        {
            if $nlevels == 1 {
                type ListBuilderType = ListBuilder<$builder_typ>;
                let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
                match $value_option {
                    Some(lst) => {
                        for v in lst {
                            b.values().append_value(v.$getter().unwrap());
                        }
                        b.append(true);
                    },
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
                    },
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
                            b.values().values().values().append_value(v.$getter().unwrap());
                        }
                        b.values().values().append(true);
                        b.values().append(true);
                        b.append(true)
                    },
                    None => {
                        b.values().values().values().append_null();
                        b.values().values().append(false);
                        b.values().append(false);
                        b.append(false)
                    }
                }
                Ok(())
            } else {
                Err(Error::new(InvalidData, "Dafuq you doing with this matryoshka doll"))
            }
        }
    };
    ($fields:expr,$builder:expr,$i:expr,$nlevels:expr,$value_option:expr) => {
        {
            if $nlevels == 1 {
                type ListBuilderType = ListBuilder<StructBuilder>;
                let b: &mut ListBuilderType = $builder.field_builder::<ListBuilderType>($i).unwrap();
                match $value_option {
                    Some(lst) => {
                        for v in lst {
                            append_all_fields($fields, b.values(), v.as_message())?;
                        }
                        b.append(true);
                    },
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
                    },
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
                    },
                    None => {
                        append_all_fields($fields, b.values().values().values(), None)?;
                        b.values().values().append(false);
                        b.values().append(false);
                        b.append(false)
                    }
                }
                Ok(())
            } else {
                Err(Error::new(InvalidData, "Dafuq you doing with this matryoshka doll"))
            }
        }
    };
}

fn append_list_value(f: &Field, builder: &mut StructBuilder, i: usize, value_option: Option<&[Value]>) -> Result<()> {
    let (inner_typ, nlevels) = get_list_levels(f);
    match inner_typ {
        DataType::Float64 => set_list_value!(builder,i,Float64Builder,nlevels,value_option,as_f64,f64),
        DataType::Float32 => set_list_value!(builder,i,Float32Builder,nlevels,value_option,as_f32,f32),
        DataType::Int64 => set_list_value!(builder,i,Int64Builder,nlevels,value_option,as_i64,i64),
        DataType::Int32 => set_list_value!(builder,i,Int32Builder,nlevels,value_option,as_i32,i32),
        DataType::UInt64 => set_list_value!(builder,i,UInt64Builder,nlevels,value_option,as_u64,u64),
        DataType::UInt32 => set_list_value!(builder,i,UInt32Builder,nlevels,value_option,as_u32,u32),
        DataType::Utf8 => set_list_value!(builder,i,StringBuilder,nlevels,value_option,as_str,&str),
        DataType::LargeUtf8 => set_list_value!(builder,i,LargeStringBuilder,nlevels,value_option,as_str,&str),
        DataType::Binary => set_list_value!(builder,i,BinaryBuilder,nlevels,value_option,as_bytes,Bytes),
        DataType::LargeBinary => set_list_value!(builder,i,LargeBinaryBuilder,nlevels,value_option,as_bytes,Bytes),
        DataType::Boolean => set_list_value!(builder,i,BooleanBuilder,nlevels,value_option,as_bool,bool),
        DataType::Struct(nested_fields) => set_list_value!(nested_fields,builder,i,nlevels,value_option),
        _ => unimplemented!("Unsupported inner_typ {}", inner_typ)
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, BooleanArray, Float64Array, StructArray, UInt64Array};
    use prost_reflect::Value;

    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn test_load_protobuf() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/protos");

        let proto = d.join("version_3.proto");
        let converter = Converter::compile(&[proto], &[d]).unwrap();
        let schema = converter
            .get_arrow_schema("eto.pb2arrow.tests.Foo", vec![].as_slice())
            .unwrap()
            .unwrap();
        let expected_schema = Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("str_val", DataType::Utf8, true),
        ]);
        assert_eq!(schema, expected_schema);
    }

    #[test]
    fn test_read_messages() {
        _run_messages_test(2, "version_2.proto", "Bar");
        _run_messages_test(3, "version_3.proto", "Bar");
    }

    fn _run_messages_test(version: i8, proto_file: &str, short_name: &str) {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("tests/protos");

        let proto = d.join(proto_file);
        let converter = Converter::compile(&[proto], &[d]).unwrap();
        let projection: &[&str] = &vec!["a", "b", "d", "s.v1"];

        let schema = converter
            .get_arrow_schema("eto.pb2arrow.tests.Bar", projection)
            .unwrap()
            .unwrap();

        let mut c = RecordBatchConverter::new(SchemaRef::from(schema.clone()), 10);
        let desc = converter.get_message_short_name(short_name).unwrap();
        let struct_desc = converter.get_message_short_name("Struct").unwrap();
        for i in 0..2 {
            let mut msg = DynamicMessage::new(desc.clone());
            let mut s = DynamicMessage::new(struct_desc.clone());
            msg.set_field_by_name("a", Value::List(vec![Value::I32(i)]));
            msg.set_field_by_name("b", Value::Bool(i % 2 == 0));
            if i % 2 == 0 {
                msg.set_field_by_name("d", Value::F64(i as f64));
                s.set_field_by_name("v1", Value::U64(i as u64));
            }
            msg.set_field_by_name("s", Value::Message(s));
            c.append_message(&msg).unwrap();
        }
        let actual = RecordBatch::try_from(&mut c).unwrap();

        let v1: Field = Field::new("v1", DataType::UInt64, true);
        let expected_schema = Schema::new(vec![
            Field::new("a", DataType::List(Box::new(Field::new("item", DataType::Int32, true))), true),
            Field::new("b", DataType::Boolean, true),
            Field::new("d", DataType::Float64, true),
            Field::new("s", DataType::Struct(vec![v1.clone()]), true),
        ]);

        let d_val: Option<f64> = if version == 2 { None } else { Some(0.) };
        let v1_val: Option<u64> = if version == 2 { None } else { Some(0) };
        let v1_data: ArrayRef = Arc::new(UInt64Array::from(vec![Some(0), v1_val]));
        let mut builder: ListBuilder<Int32Builder> = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(0);
        builder.append(true);
        builder.values().append_value(1);
        builder.append(true);
        let expected_columns: Vec<ArrayRef> = vec![
            Arc::new(builder.finish()),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
            Arc::new(Float64Array::from(vec![Some(0.), d_val])),
            Arc::new(StructArray::from(vec![(v1, v1_data)])),
        ];
        let expected =
            RecordBatch::try_new(SchemaRef::from(expected_schema), expected_columns).unwrap();
        assert_eq!(expected, actual);
    }
}
