//! Convert Protobuf schema and message into Apache Arrow Schema and Tables.
//!
mod records;
mod schema;

pub use records::RecordBatchConverter;
pub use schema::SchemaConverter;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int32Builder, ListBuilder};
    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, RecordBatch, StructArray, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use prost_reflect::{DynamicMessage, Value};

    use std::io::Result;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn converter_for(proto_file: &str) -> SchemaConverter {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("../protos/test");

        let proto = d.join(proto_file);
        SchemaConverter::compile(&[proto], &[d])
            .expect(format!("Failed to compile {proto_file}").as_str())
    }

    #[test]
    fn test_load_protobuf() {
        let converter = converter_for("version_3.proto");
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
    fn test_enum() -> Result<()> {
        let converter = converter_for("version_3.proto");
        //    dbg!(converter.get_arrow_schema_by_short_name("MessageWithNestedEnum", &["status"]));

        let schema = converter
            .get_arrow_schema("eto.pb2arrow.tests.MessageWithNestedEnum", &[])?
            .expect("no schema found");
        let schema = SchemaRef::new(schema);

        RecordBatchConverter::new(schema, 1);

        // dbg!(schema);
        Ok(())
    }

    // #[test]
    // fn test_read_messages() {
    //     _run_messages_test(2, "version_2.proto", "Bar");
    //     _run_messages_test(3, "version_3.proto", "Bar");
    // }

    fn _run_messages_test(version: i8, proto_file: &str, short_name: &str) {
        let projection: &[&str] = &vec!["a", "b", "d", "s.v1"];

        let converter = converter_for(proto_file);
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
            Field::new(
                "a",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
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
