use arrow_schema::ArrowError;
use prost_reflect::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProstArrowError {
    #[error("file descriptor not found {0}")]
    DescriptorNotFound(String),

    #[error("couldn't cast value {0} to correct type")]
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

    #[error("Io Errror")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = core::result::Result<T, ProstArrowError>;
