//! Convert Protobuf schema and message into Apache Arrow Schema and Tables.
//!

use std::cell::RefCell;
use arrow_array::StringArray;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::path::Path;
use std::process::Command;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use prost_reflect::{DescriptorPool, FieldDescriptor, MessageDescriptor};
use tempfile::NamedTempFile;

use crate::{KatnissArrowError, RecordBatchConverter, Result};

/// Holds dictionary values for fields. Not threadsafe
#[derive(Debug, Clone)]
pub struct DictValuesContainer {
    /// Arrow Field.dict_id -> dictionary values
    dictionaries: HashMap<i64, StringArray>,
}

impl DictValuesContainer {
    pub fn new() -> Self {
        let dictionaries = HashMap::new();
        DictValuesContainer { dictionaries }
    }

    /// Add a new set of dictionary values and return dict_id
    pub fn add_dictionary(&mut self, dict_values: Vec<String>) -> i64 {
        // dict_id is 0 by default in Arrow Field so we start at 1 to distinguish
        let new_id = self
            .dictionaries
            .keys()
            .max()
            .unwrap_or(&(0 as i64))
            .clone()
            + 1;
        self.dictionaries
            .insert(new_id, StringArray::from_iter_values(dict_values.iter()));
        new_id
    }

    /// Get the dictionary values for the specified dict_id
    pub fn get_dict_values(&self, dict_id: i64) -> Option<&StringArray> {
        self.dictionaries.get(&dict_id)
    }
}

/// Convert PB field to Arrow field
#[derive(Debug, Clone)]
pub struct FieldConverter {
    dictionaries: DictValuesContainer,
}

impl FieldConverter {
    pub fn new() -> Self {
        let dictionaries = DictValuesContainer::new();
        FieldConverter { dictionaries }
    }

    /// Convert prost FieldDescriptor to arrow Field
    pub fn to_arrow_mut(&mut self, f: &FieldDescriptor) -> Field {
        let name = f.name();
        let data_type = self.kind_to_type(f.kind());
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
        } else if matches!(data_type, DataType::Dictionary(_, _)) {
            let enum_values = f
                .kind()
                .as_enum()
                .unwrap()
                .values()
                .map(|v| v.name().to_string())
                .collect::<Vec<_>>();
            let is_ordered = enum_values.windows(2).all(|w| w[0] <= w[1]);
            let dict_id = self.dictionaries.add_dictionary(enum_values);
            Field::new_dict(name, data_type, true, dict_id, is_ordered)
        } else {
            Field::new(name, data_type, true)
        }
    }

    /// Convert protobuf data type to arrow data type
    fn kind_to_type(&mut self, kind: prost_reflect::Kind) -> DataType {
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
                    DataType::Struct(msg.fields().map(|f| self.to_arrow_mut(&f)).collect())
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
}

/// Dynamically convert protobuf messages to Arrow table or Schema.
#[derive(Debug, Clone)]
pub struct SchemaConverter {
    pub(crate) descriptor_pool: DescriptorPool,
    /// message name -> dictionary values for the schema
    dictionary_map: RefCell<HashMap<String, DictValuesContainer>>,
}

impl SchemaConverter {
    pub fn new(descriptor_pool: DescriptorPool) -> Self {
        let dictionary_map = RefCell::new(HashMap::new());
        Self { descriptor_pool, dictionary_map }
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
            Err(e) => return Err(KatnissArrowError::ProtocError(e)),
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
        RecordBatchConverter::try_new(schema, batch_size)
    }

    /// Get the arrow schema of the protobuf message, specified by the qualified message name.
    pub fn get_arrow_schema(&self, name: &str, projection: &[&str]) -> Result<Option<Schema>> {
        let msg = match self.descriptor_pool.get_message_by_name(name) {
            Some(m) => m,
            None => return Ok(None),
        };
        let mut field_converter = FieldConverter::new();
        let schema = Schema::new(
            msg.fields()
                .map(|f| field_converter.to_arrow_mut(&f))
                .collect(),
        );
        self.dictionary_map.borrow_mut().insert(name.to_string(), field_converter.dictionaries.clone());

        if projection.is_empty() {
            Ok(Some(schema))
        } else {
            let prefix = "".to_string();
            let proj_set: HashSet<&str> = HashSet::from_iter(projection.to_vec());
            let keep: Vec<Field> = project_fields(&prefix, schema.fields(), &proj_set);
            Ok(Some(Schema::new(keep)))
        }
    }

    pub fn get_arrow_schema_with_dictionaries(&self, name: &str, projection: &[&str]) -> Result<(Option<Schema>, Option<DictValuesContainer>)> {
        let rs = self.get_arrow_schema(name, projection)?;
        let dict_values = self.dictionary_map.borrow_mut().get(name).map(|v| v.clone());
        Ok((rs, dict_values))
    }

    pub fn get_message_by_name(&self, name: &str) -> Result<MessageDescriptor> {
        self.descriptor_pool
            .get_message_by_name(name)
            .ok_or_else(|| KatnissArrowError::DescriptorNotFound(name.to_owned()))
    }

    pub fn get_arrow_schemas_by_short_name(
        &self,
        short_name: &str,
        projection: &[&str],
    ) -> Result<Vec<Option<Schema>>> {
        let descriptors = self.descriptor_pool.all_messages().filter(|m| m.name() == short_name).collect::<Vec<_>>();
        let mut schemas = Vec::with_capacity(descriptors.len());
        for m in descriptors {
            schemas.push(self.get_arrow_schema(m.full_name(), projection)?);
        }
        Ok(schemas)
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

//         let values = f.kind().as_enum().unwrap().values().map(|v| v.name().to_string()).collect::<Vec<_>>();

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_dictvaluescontainer() {
        let mut holder = DictValuesContainer::new();
        assert!(holder.get_dict_values(0).is_none());
        assert_eq!(holder.add_dictionary(vec!["a".to_string()]), 1);
        assert_eq!(
            holder
                .get_dict_values(1)
                .unwrap()
                .iter()
                .map(|v| v.unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["a".to_string()]
        );
    }
}
