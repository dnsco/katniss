use anyhow::Result;

use crate::{
    protos::{
        spacecorp::{packet, ClimateStatus, JumpDriveStatus, Packet},
        v3::{
            simple_one_of_message::Inner, Foo, InnerUnitMessage, MessageWithNestedEnum,
            SimpleOneOfMessage, SomeRandomEnum, UnitContainer,
        },
    },
    schema_converter,
    test_util::*,
};

#[test]
fn test_nested_unit_message() -> Result<()> {
    let batch = ProtoBatch::V3(&[
        UnitContainer {
            inner: Some(InnerUnitMessage {}),
        },
        UnitContainer { inner: None },
    ])
    .arrow_batch()?;
    write_batch(batch, "inner_unit")?;
    Ok(())
}

#[test]
fn test_base_unit_messages() -> Result<()> {
    let batch = ProtoBatch::V3(&[InnerUnitMessage {}]).arrow_batch()?;
    write_batch(batch, "inner_unit")?;
    Ok(())
}

#[test]
fn test_enums() -> Result<()> {
    let enum_message = MessageWithNestedEnum {
        status: SomeRandomEnum::Failing.into(),
    };

    let batch = ProtoBatch::V3(&[enum_message]).arrow_batch()?;
    write_batch(batch, "enums")?;
    Ok(())
}

#[test]
fn test_simple_oneof() -> Result<()> {
    let simple = SimpleOneOfMessage {
        words: "hullo".into(),
        inner: Some(Inner::Foo({
            Foo {
                key: 22,
                str_val: "I'm inside yr enum".into(),
            }
        })),
    };

    let batch = ProtoBatch::V3(&[simple]).arrow_batch()?;
    write_batch(batch, "simple_one_of")?;
    Ok(())
}

#[test]
fn test_nested_null_struct() -> Result<()> {
    let packet = Packet {
        msg: Some(packet::Msg::ClimateStatus(ClimateStatus::default())),
        ..Default::default()
    };

    let batch = ProtoBatch::SpaceCorp(&[packet]).arrow_batch()?;
    write_batch(batch, "nested_null_struct")?;
    Ok(())
}

#[test]
fn test_heterogenous_batch() -> Result<()> {
    let batch = ProtoBatch::SpaceCorp(&[
        Packet {
            msg: Some(packet::Msg::ClimateStatus(ClimateStatus::default())),
            ..Default::default()
        },
        Packet {
            msg: Some(packet::Msg::JumpDriveStatus(JumpDriveStatus::default())),
            ..Default::default()
        },
    ])
    .arrow_batch()?;
    write_batch(batch, "heterogenous_batch")?;
    Ok(())
}
