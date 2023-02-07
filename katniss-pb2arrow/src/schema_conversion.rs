//! Convert Protobuf schema and message into Apache Arrow Schema and Tables.
//!

use std::collections::HashSet;
use std::io::{BufReader, Read};
use std::path::Path;
use std::process::Command;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use prost_reflect::{DescriptorPool, FieldDescriptor, MessageDescriptor};
use tempfile::NamedTempFile;

use crate::{ProstArrowError, RecordBatchConverter, Result};

/// Dynamically convert protobuf messages to Arrow table or Schema.
#[derive(Debug, Clone)]
pub struct SchemaConverter {
    pub(crate) descriptor_pool: DescriptorPool,
}

/// Convert prost FieldDescriptor to arrow Field
fn to_arrow(f: &FieldDescriptor) -> Field {
    let name = f.name();
    let data_type = kind_to_type(f.kind());
    // OneOf fields are laid out weird. Each of the oneof's appear at the top level of the
    // message, and there's a separate oneof container that associates the oneof fields together
    // this means we can just sort of ignore the association during schema conversion for now
    // and pretend it's just separate arrow fields.
    //
    // If we're concerned about storage size (and it sounds like it won't be the case for now)
    // we can map OneOf's to an Arrow UnionType. This essentially makes the child arrays densely
    // packed to save space and relies on a separate offset array to restore at read-time.
    // However I think higher level query engines tend to not deal well with UnionTypes so
    // we should just keep the "striped" layout for now
    if f.is_list() {
        let item = Box::new(Field::new("item", data_type, true));
        Field::new(name, DataType::List(item), true)
    } else {
        Field::new(name, data_type, true)
    }
}

/// Convert protobuf data type to arrow data type
fn kind_to_type(kind: prost_reflect::Kind) -> DataType {
    match kind {
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
            let fields = msg.fields();
            if fields.len() > 0 {
                DataType::Struct(msg.fields().map(|f| to_arrow(&f)).collect())
            } else {
                DataType::Boolean
            }
        }
        prost_reflect::Kind::Enum(_) => {
            let key_type = Box::new(DataType::Int32);
            let value_type = Box::new(DataType::Utf8);
            DataType::Dictionary(key_type, value_type)
        }
    }
}

impl SchemaConverter {
    pub fn new(descriptor_pool: DescriptorPool) -> Self {
        Self { descriptor_pool }
    }
    /// Compile protobuf files and build the converter.
    ///
    /// ```rust
    ///   use katniss_pb2arrow::SchemaConverter;
    ///
    ///   let convert = SchemaConverter::compile(
    ///       &["protos/foo.proto"], &["protos"]
    ///   ).unwrap();
    /// ```
    pub fn compile(protos: &[impl AsRef<Path>], includes: &[impl AsRef<Path>]) -> Result<Self> {
        let protoc = match which::which("protoc") {
            Ok(path) => path,
            Err(e) => return Err(ProstArrowError::ProtocError(e)),
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

        let pool =
            DescriptorPool::decode(buffer.as_slice()).expect("Failed to decode descriptor pool");
        Ok(Self::new(pool))
    }

    pub fn converter_for(&self, name: &str, batch_size: usize) -> Result<RecordBatchConverter> {
        let schema = self.get_arrow_schema(name, &[])?.unwrap();
        let schema = SchemaRef::new(schema);
        Ok(RecordBatchConverter::new(schema, batch_size))
    }

    /// Get the arrow schema of the protobuf message, specified by the qualified message name.
    pub fn get_arrow_schema(&self, name: &str, projection: &[&str]) -> Result<Option<Schema>> {
        let msg = match self.descriptor_pool.get_message_by_name(name) {
            Some(m) => m,
            None => return Ok(None),
        };
        let schema = Schema::new(msg.fields().map(|f| to_arrow(&f)).collect());

        if projection.is_empty() {
            Ok(Some(schema))
        } else {
            let prefix = "".to_string();
            let proj_set: HashSet<&str> = HashSet::from_iter(projection.to_vec());
            let keep: Vec<Field> = project_fields(&prefix, schema.fields(), &proj_set);
            Ok(Some(Schema::new(keep)))
        }
    }

    pub fn get_message_by_name(&self, name: &str) -> Result<MessageDescriptor> {
        self.descriptor_pool
            .get_message_by_name(name)
            .ok_or_else(|| ProstArrowError::DescriptorNotFound(name.to_owned()))
    }

    pub fn get_arrow_schemas_by_short_name(
        &self,
        short_name: &str,
        projection: &[&str],
    ) -> Result<Vec<Option<Schema>>> {
        self.descriptor_pool
            .all_messages()
            .filter(|m| m.name() == short_name)
            .map(|m| self.get_arrow_schema(m.full_name(), projection))
            .collect()
    }

    pub fn get_messages_from_short_name(&self, short_name: &str) -> Vec<Option<MessageDescriptor>> {
        self.descriptor_pool
            .all_messages()
            .filter(|m| m.name() == short_name)
            .map(|m| self.descriptor_pool.get_message_by_name(m.full_name()))
            .collect()
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