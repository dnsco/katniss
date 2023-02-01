//! Convert Protobuf schema and message into Apache Arrow Schema and Tables.
//!

use std::collections::HashSet;

use std::io::{BufReader, Error, ErrorKind, Read, Result};
use std::path::Path;
use std::process::Command;

use arrow_schema::{DataType, Field, Schema};
use prost_reflect::{DescriptorPool, FieldDescriptor, MessageDescriptor};
use tempfile::NamedTempFile;

/// Dynamically convert protobuf messages to Arrow table or Schema.
#[derive(Debug, Clone)]
pub struct SchemaConverter {
    pub(crate) descriptor_pool: DescriptorPool,
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
        prost_reflect::Kind::Enum(_) => DataType::Int32,
    };
    if f.is_list() {
        data_type = DataType::List(Box::new(Field::new("item", data_type, true)));
    }
    Ok(Field::new(name, data_type, true))
}

impl SchemaConverter {
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
            dbg!(m.full_name());
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
