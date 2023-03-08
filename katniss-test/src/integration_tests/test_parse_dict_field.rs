use anyhow::Result;

use super::*;
use crate::{
    protos::{
        spacecorp::{packet, ClimateStatus, JumpDriveStatus, Packet},
        v3::{
            simple_one_of_message::Inner, Foo, InnerUnitMessage, MessageWithNestedEnum,
            SimpleOneOfMessage, SomeRandomEnum, UnitContainer,
        },
    },
    schema_converter,
};
use test_util::*;

#[test]
fn test_parse_dict_field_values() -> Result<()> {
    let mut converter = schema_converter()?;

    let (arrow, dict_values) = converter.get_arrow_schema_with_dictionaries("eto.pb2arrow.tests.v3.MessageWithNestedEnum", &vec![])?;
    assert!(arrow.is_some());
    let values = dict_values.unwrap().get_dict_values(1).unwrap().clone();
    assert_eq!(vec!["PASSSING", "FAILING", "LEGACY"], values.iter().map(|v| v.unwrap()).collect::<Vec<_>>());
    Ok(())
}