//! Missing adapters connecting `bytes` to `std::io`.

use bytes::{Buf, BufMut};
use std::io::{self, Write};

/// A type that implements [`BufMut`] and sends its output to an underlying
/// [`io::Write`].
///
///
pub struct BufMutWriter<W: Write> {
    buf: Vec<u8>,
    writer: W,
    error: Option<io::Error>,
}

const DEFAULT_CAPACITY: usize = 8 * 1024;

impl<W: Write> BufMutWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::with_capacity(writer, DEFAULT_CAPACITY)
    }

    pub fn with_capacity(writer: W, capacity: usize) -> Self {
        BufMutWriter {
            buf: Vec::with_capacity(capacity),
            writer,
            error: None,
        }
    }

    pub fn check(&mut self) -> io::Result<()> {
        match self.error.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        if self.error.is_none() {
            self.error = self.writer.write_all(bytes).err();
        }
    }

    fn flush_buf(&mut self) {
        if self.error.is_none() {
            self.error = self.writer.write_all(&self.buf).err();
        }
        self.buf.clear();
    }

    pub fn close(mut self) -> io::Result<()> {
        self.flush_buf();
        self.check()
    }
}

impl<W: Write> Drop for BufMutWriter<W> {
    fn drop(&mut self) {
        self.flush_buf();
    }
}

unsafe impl<W: Write> BufMut for BufMutWriter<W> {
    fn remaining_mut(&self) -> usize {
        usize::MAX
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.buf.advance_mut(cnt);
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        if self.buf.len() == self.buf.capacity() {
            self.flush_buf();
        }
        self.buf.chunk_mut()
    }

    fn put<T: Buf>(&mut self, mut src: T)
    where
        Self: Sized,
    {
        let mut size = src.remaining();
        if size > self.buf.capacity() - self.buf.len() {
            self.flush_buf();
            while size > self.buf.capacity() {
                let chunk = src.chunk();
                self.write(chunk);
                let nbytes = chunk.len();
                src.advance(nbytes);
                size = src.remaining();
            }
        }

        assert!(size <= self.buf.capacity() - self.buf.len());
        self.buf.put(src);
    }

    fn put_slice(&mut self, src: &[u8]) {
        if src.len() <= self.buf.capacity() - self.buf.len() {
            self.buf.put_slice(src);
        } else {
            self.flush_buf();
            if src.len() < self.buf.capacity() {
                self.buf.extend_from_slice(src);
            } else {
                self.write(src);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[derive(prost::Message)]
    struct MyMsg {
        #[prost(uint32, tag = "1")]
        a: u32,
        #[prost(uint32, tag = "2")]
        b: u32,
        #[prost(string, tag = "3")]
        c: String,
    }

    #[test]
    fn test_prost_small() {
        let message = MyMsg {
            a: 1,
            b: 2,
            c: "hello world".to_string(),
        };

        // without BufMutWriter
        let mut expected = vec![];
        message
            .encode(&mut expected)
            .expect("can't run out of memory");
        assert!(expected.len() > 10);

        // using BufMutWriter
        let mut dest = Vec::<u8>::new();
        let mut write_buf = BufMutWriter::new(&mut dest);
        message
            .encode(&mut write_buf)
            .expect("BufMutWriter can't run out of memory");
        write_buf.close().expect("no io::Errors from Vec<u8>");

        assert_eq!(dest, expected);
    }

    // things to test
    // - tiny capacity sizes
    // - Prost with a large string
    // - put() with large chunks
    // - put() with awkward-sized chunks, just under/over the 8K default size (using Buf chains)
    // - unusual writers
    // - writer is dropped
    // - buffer actually does effectively reduce number of writes to underlying writer
    // - panic behavior
    // - io errors are saved and delivered (not sure the API is friendly enough)
    // - io errors prevent subsequent writes
    // - check method
}
