use arrow_schema::ArrowError;
use prost_reflect::{
    prost::{encoding::WireType, DecodeError},
    Value,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KatnissArrowError {
    #[error("file descriptor not found {0}")]
    DescriptorNotFound(String),

    #[error("couldn't cast value {0:?} to correct type")]
    TypeCastError(Value),

    #[error("Field is not an enum")]
    NonEnumField,

    #[error("No Enum Value {0}")]
    NoEnumValue(i32),

    #[error("Invalid Enum Value")]
    InvalidEnumValue(ArrowError),

    #[error("Attempted to append a list to a non-list field")]
    NonListField,

    #[error("Couldn't find protoc on path")]
    ProtocError(#[from] which::Error),

    #[error("Io Error")]
    IoError(#[from] std::io::Error),

    #[error("Batch Conversion Error: {0}")]
    BatchConversionError(ArrowError),

    #[error("Can only iterate over WireType::LengthDelimited but is {0:?}")]
    NotLengthDelimted(WireType),

    #[error("Protobuf Decode Error {0}")]
    ProtoDecodeError(#[from] DecodeError),

    #[error("Proto bytes ({0}) too big for platform")]
    TooManyBytesForPlatform(u64),

    #[error("Arrow Dictionary Field must have dict_id")]
    DictNotFound,
}

pub type Result<T> = core::result::Result<T, KatnissArrowError>;
