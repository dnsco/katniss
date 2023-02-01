use arrow_array::builder::*;
use arrow_schema::{DataType, Field};

pub fn make_struct_builder(fields: Vec<Field>, capacity: usize) -> StructBuilder {
    let field_builders = fields.iter().map(|f| make_builder(f, capacity)).collect();
    StructBuilder::new(fields, field_builders)
}

enum ListKind {
    List,
    LargeList,
    NotList,
}

/// Create the appropriate ArrayBuilder for the given field and capacity
fn make_builder(field: &Field, capacity: usize) -> Box<dyn ArrayBuilder> {
    // arrow needs generic builder methods
    let (inner_typ, kind) = match field.data_type() {
        DataType::List(v) => (v.data_type(), ListKind::List),
        DataType::LargeList(v) => (v.data_type(), ListKind::LargeList),
        _ => (field.data_type(), ListKind::NotList),
    };

    match inner_typ {
        DataType::Boolean => wrap_builder(BooleanBuilder::with_capacity(capacity), kind),
        DataType::Int32 => wrap_builder(Int32Builder::with_capacity(capacity), kind),
        DataType::Int64 => wrap_builder(Int64Builder::with_capacity(capacity), kind),
        DataType::UInt32 => wrap_builder(UInt32Builder::with_capacity(capacity), kind),
        DataType::UInt64 => wrap_builder(UInt64Builder::with_capacity(capacity), kind),
        DataType::Float32 => wrap_builder(Float32Builder::with_capacity(capacity), kind),
        DataType::Float64 => wrap_builder(Float64Builder::with_capacity(capacity), kind),
        DataType::Binary => wrap_builder(BinaryBuilder::with_capacity(capacity, 1024), kind),
        DataType::LargeBinary => {
            wrap_builder(LargeBinaryBuilder::with_capacity(capacity, 1024), kind)
        }
        DataType::Utf8 => wrap_builder(StringBuilder::with_capacity(capacity, 1024), kind),
        DataType::LargeUtf8 => {
            wrap_builder(LargeStringBuilder::with_capacity(capacity, 1024), kind)
        }
        DataType::Struct(fields) => {
            wrap_builder(make_struct_builder(fields.clone(), capacity), kind)
        }
        t => panic!("Data type {:?} is not currently supported", t),
    }
}

/// Return the boxed builder or wrap it in a ListBuilder then box
/// this is necessary because
fn wrap_builder<T: ArrayBuilder>(builder: T, kind: ListKind) -> Box<dyn ArrayBuilder> {
    match kind {
        ListKind::List => Box::new(ListBuilder::new(builder)),
        ListKind::LargeList => Box::new(LargeListBuilder::new(builder)),
        ListKind::NotList => Box::new(builder),
    }
}
