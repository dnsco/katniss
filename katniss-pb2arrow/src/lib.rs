//! Convert Protobuf schema and message into Apache Arrow Schema and Tables.
//!
//!

mod errors;
mod record_conversion;
mod schema_conversion;

use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use prost_reflect::{DescriptorPool, MessageDescriptor};

pub use errors::{KatnissArrowError, Result};
pub use record_conversion::RecordConverter;
use schema_conversion::DictValuesContainer;
pub use schema_conversion::SchemaConverter;

pub mod exports {
    pub use arrow_array::{RecordBatch, RecordBatchReader};
    pub use prost_reflect;
    pub use prost_reflect::DynamicMessage;
}

/// Holds an Arrow Schema and a Protobuf Message Descriptor
/// Has all meta data necessary for converting between a bunch of protos to
/// an Arrow Record Batch
/// Dictionaries are stored seperately so that they can be mapped correctly in Arrow
pub struct ArrowBatchProps {
    pub schema: Arc<Schema>,
    pub dictionaries: Arc<DictValuesContainer>,
    pub descriptor: MessageDescriptor,
    pub records_per_arrow_batch: usize,
}

impl ArrowBatchProps {
    pub fn try_new(pool: DescriptorPool, msg_name: String) -> Result<Self> {
        let converter: SchemaConverter = SchemaConverter::new(pool);

        let (schema_opt, dictionaries_opt) =
            converter.get_arrow_schema_with_dictionaries(&msg_name, &[])?;

        let schema = SchemaRef::new(
            schema_opt.ok_or_else(|| KatnissArrowError::DescriptorNotFound(msg_name.to_owned()))?,
        );

        let dictionaries = Arc::new(
            dictionaries_opt.ok_or_else(|| crate::errors::KatnissArrowError::DictNotFound)?,
        );

        let descriptor = converter.get_message_by_name(&msg_name)?;

        Ok(Self {
            schema,
            dictionaries,
            descriptor,
            records_per_arrow_batch: 1024,
        })
    }

    pub fn with_records_per_arrow_batch(mut self, size: usize) -> Self {
        self.records_per_arrow_batch = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use super::*;

    fn converter_for(proto_file: &str) -> SchemaConverter {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("../protos/test");

        let proto = d.join(proto_file);
        SchemaConverter::compile(&[proto], &[d])
            .unwrap_or_else(|_| panic!("Failed to compile {proto_file}"))
    }

    #[test]
    fn test_load_protobuf() {
        let converter = converter_for("version_3.proto");
        let schema = converter
            .get_arrow_schema("eto.pb2arrow.tests.v3.Foo", vec![].as_slice())
            .unwrap()
            .unwrap();
        let expected_schema = Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("str_val", DataType::Utf8, true),
        ]);
        assert_eq!(schema, expected_schema);
    }

    #[test]
    fn test_enum() -> Result<()> {
        let converter = converter_for("version_3.proto");
        //    dbg!(converter.get_arrow_schema_by_short_name("MessageWithNestedEnum", &["status"]));

        let schema = converter
            .get_arrow_schema("eto.pb2arrow.tests.v3.MessageWithNestedEnum", &[])?
            .expect("no schema found");
        let schema = SchemaRef::new(schema);
        assert_eq!(
            schema
                .field_with_name("status")
                .unwrap()
                .data_type()
                .clone(),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );

        let props = ArrowBatchProps::try_new(
            converter.descriptor_pool,
            "eto.pb2arrow.tests.v3.MessageWithNestedEnum".to_string(),
        )?;

        RecordConverter::try_new(&props)?;

        Ok(())
    }

    #[test]
    fn test_read_messages() {
        // _run_messages_test(2, "version_2.proto", "eto.pb2arrow.tests.v2.Bar");
        // _run_messages_test(3, "version_3.proto", "eto.pb2arrow.tests.v3.Bar");
    }

    // fn _run_messages_test(version: i8, proto_file: &str, name: &str) {
    //     let projection = &["a", "b", "d", "s.v1"];

    //     let converter = converter_for(proto_file);
    //     let schema = converter
    //         .get_arrow_schema(name, projection)
    //         .unwrap()
    //         .unwrap();

    //     let props =
    //         ArrowBatchProps::new(converter.descriptor_pool.clone(), name.to_string(), 10).unwrap();

    //     let mut c = RecordBatchConverter::try_new(&props).unwrap();

    //     let desc = converter.get_message_by_name(name).unwrap();

    //         .get_messages_from_short_name("Struct")
    //         .remove(0)
    //         .unwrap();

    // for i in 0..2 {
    //     let mut msg = DynamicMessage::new(desc.clone());
    //     let mut s = DynamicMessage::new(struct_desc.clone());
    //     msg.set_field_by_name("a", Value::List(vec![Value::I32(i)]));
    //     msg.set_field_by_name("b", Value::Bool(i % 2 == 0));
    //     if i % 2 == 0 {
    //         msg.set_field_by_name("d", Value::F64(i as f64));
    //         s.set_field_by_name("v1", Value::U64(i as u64));
    //     }
    //     msg.set_field_by_name("s", Value::Message(s));
    //     c.append_message(&msg).unwrap();
    // }
    // let actual = c.records();

    //     let v1: Field = Field::new("v1", DataType::UInt64, true);
    //     let expected_schema = Schema::new(vec![
    //         Field::new(
    //             "a",
    //             DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
    //             true,
    //         ),
    //         Field::new("b", DataType::Boolean, true),
    //         Field::new("d", DataType::Float64, true),
    //         Field::new("s", DataType::Struct(vec![v1.clone()]), true),
    //     ]);

    //     let d_val: Option<f64> = if version == 2 { None } else { Some(0.) };
    //     let v1_val: Option<u64> = if version == 2 { None } else { Some(0) };
    //     let v1_data: ArrayRef = Arc::new(UInt64Array::from(vec![Some(0), v1_val]));
    //     let mut builder: ListBuilder<Int32Builder> = ListBuilder::new(Int32Builder::new());
    //     builder.values().append_value(0);
    //     builder.append(true);
    //     builder.values().append_value(1);
    //     builder.append(true);
    //     let expected_columns: Vec<ArrayRef> = vec![
    //         Arc::new(builder.finish()),
    //         Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
    //         Arc::new(Float64Array::from(vec![Some(0.), d_val])),
    //         Arc::new(StructArray::from(vec![(v1, v1_data)])),
    //     ];
    //     let expected =
    //         RecordBatch::try_new(SchemaRef::from(expected_schema), expected_columns).unwrap();
    //     assert_eq!(expected, actual);
    // }
}
