use anyhow::Result;

use crate::schema_converter;

#[test]
#[ignore]
fn test_parse_dict_field_values() -> Result<()> {
    let converter = schema_converter()?;

    let (arrow, dict_values) = converter
        .get_arrow_schema_with_dictionaries("eto.pb2arrow.tests.v3.MessageWithNestedEnum", &[])?;
    assert!(arrow.is_some());
    let values = dict_values.unwrap().get_dict_values(1).unwrap().clone();
    assert_eq!(
        vec!["PASSSING", "FAILING", "LEGACY"],
        values.iter().map(|v| v.unwrap()).collect::<Vec<_>>()
    );
    Ok(())
}
