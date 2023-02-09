use std::{
    io::{self, ErrorKind, Write},
    sync::{Arc, Mutex},
};

use crate::{errors::KatinssIngestorError, Result};

#[derive(Clone)]
pub struct SharedBuffer {
    data: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub fn try_downgrade(self) -> Result<Vec<u8>> {
        let data = Arc::try_unwrap(self.data)
            .map_err(|_| KatinssIngestorError::OtherSharedBufferReferenceHeld)?
            .into_inner()
            .map_err(|_| KatinssIngestorError::OtherSharedBufferReferenceHeld)?;

        Ok(data)
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut data = self
            .data
            .lock()
            .map_err(|err| io::Error::new(ErrorKind::WouldBlock, err.to_string()))?;
        data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
