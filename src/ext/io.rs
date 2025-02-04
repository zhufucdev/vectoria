use crate::vio::RandomAccess;
use std::io::{SeekFrom, Error};

pub(crate) trait MoveContent {
    fn move_content(
        &mut self,
        content_len: usize,
        offset: isize,
        buffer_size: usize,
    ) -> Result<(), Error>;
}

fn cut_and_paste_forward(
    fd: &mut dyn RandomAccess,
    content_len: usize,
    offset: usize,
    buffer_size: usize,
) -> Result<(), Error> {
    let mut buf = Vec::with_capacity(buffer_size);
    let begin = fd.stream_position()?;
    let mut remaining = content_len;
    loop {
        let read = if (fd.seek(SeekFrom::Current(remaining as i64))? - buffer_size as u64) < begin {
            fd.seek(SeekFrom::Start(begin))?;
            remaining
        } else {
            fd.seek_relative(-(buffer_size as i64))?;
            buffer_size
        };
        
        buf.resize(read, 0);
        fd.read_exact(&mut *buf)?;
        fd.seek_relative(offset as i64)?;
        fd.write(&buf[0..read])?;
        remaining -= read;
        if remaining <= 0 { 
            return Ok(())
        }
        fd.seek_relative(-((offset + buffer_size) as i64))?
    }
}

fn cut_and_paste_backward(
    fd: &mut dyn RandomAccess,
    content_len: usize,
    offset: usize,
    buffer_len: usize,
) -> Result<(), Error> {
    let mut buf = Vec::with_capacity(buffer_len);
    let mut remaining = content_len;
    loop {
        let read = if remaining > buffer_len {
            buffer_len
        } else {
            remaining
        };
        buf.resize(read, 0);
        fd.read_exact(&mut *buf)?;
        fd.seek_relative(-(offset as i64))?;
        fd.write(&*buf)?;
        
        remaining -= read;
        if remaining <= 0 { 
            return Ok(())
        }
    }
}

impl MoveContent for dyn RandomAccess {
    fn move_content(
        &mut self,
        content_len: usize,
        offset: isize,
        buffer_size: usize,
    ) -> Result<(), Error> {
        if offset >= 0 {
            cut_and_paste_forward(self, content_len, offset as usize, buffer_size)
        } else {
            cut_and_paste_backward(self, content_len, (-offset) as usize, buffer_size)
        }
    }
}