use crate::db::DbVector;
use crate::vio::Error;
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{BufReader, Read};

pub(crate) fn read(dim_size: u32, fd: &mut dyn Read) -> Result<DbVector, Error> {
    let mut buf_reader = BufReader::with_capacity(dim_size as usize * size_of::<f32>(), fd);
    let mut res = Vec::with_capacity(dim_size as usize);
    for _ in 0..dim_size {
        let component = buf_reader
            .read_f32::<BigEndian>()
            .map_err(|e| Error::IO(e))?;
        if component == f32::INFINITY {
            return Err(Error::EOF);
        }
        res.push(component);
    }
    Ok(res)
}
