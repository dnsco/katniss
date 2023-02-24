use prost_reflect::{
    bytes::Buf,
    prost::encoding::{decode_key, decode_varint, WireType},
    DynamicMessage, MessageDescriptor,
};

use crate::{KatnissArrowError, Result};

pub struct RepeatedDynamicMessages<B: Buf> {
    bytes: B,
    descriptor: MessageDescriptor,
    field_tag: u32,
}

impl<B: Buf> RepeatedDynamicMessages<B> {
    pub fn iterator(bytes: B, descriptor: MessageDescriptor, field_tag: u32) -> Self {
        Self {
            bytes,
            descriptor,
            field_tag,
        }
    }

    fn decode_next(&mut self) -> Result<Option<DynamicMessage>> {
        let buf = &mut self.bytes;

        let (tag, wire_type) = decode_key(buf)?;

        if wire_type != (WireType::LengthDelimited) {
            return Err(KatnissArrowError::NotLengthDelimted(wire_type));
        }

        let size = decode_varint(buf)?;
        let size =
            usize::try_from(size).map_err(|_| KatnissArrowError::TooManyBytesForPlatform(size))?;
        let bytes = buf.take(size);

        if tag == self.field_tag {
            let msg = DynamicMessage::decode(self.descriptor.clone(), bytes)?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }
}

impl<B: Buf> Iterator for RepeatedDynamicMessages<B> {
    type Item = Result<DynamicMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.has_remaining() {
            match self.decode_next() {
                Ok(Some(message)) => Some(Ok(message)),
                Ok(None) => self.next(),
                Err(e) => Some(Err(e)),
            }
        } else {
            None
        }
    }
}
