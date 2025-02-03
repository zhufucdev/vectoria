use crate::db::{DbVector, DbVectorSlice};
use crate::vio::Error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::io::{BufReader, Read, Write};

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

pub(crate) fn write(vector: DbVectorSlice, fd: &mut dyn Write) -> Result<usize, io::Error> {
    for component in vector {
        fd.write_f32::<BigEndian>(*component)?;
    }
    Ok(vector.len() * size_of::<f32>())
}

#[cfg(test)]
mod tests {
    use crate::vio::vector::{read, write};
    use byteorder::{BigEndian, WriteBytesExt};
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn read_works() {
        let mut fd = Cursor::new(Vec::new());
        for i in 1..=32 {
            fd.write_f32::<BigEndian>(1f32 / i as f32).unwrap();
        }
        fd.seek(SeekFrom::Start(0)).unwrap();
        
        assert_eq!(
            Vec::from_iter((1..=32).map(|i| 1f32 / i as f32)),
            read(32, &mut fd).unwrap()
        )
    }
    
    #[test]
    fn write_works() {
        let v = Vec::from_iter((1..=32).map(|i| 1f32 / i as f32));
        let mut fd = Cursor::new(Vec::new());
        write(&*v, &mut fd).unwrap();
        fd.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(
            v,
            read(32, &mut fd).unwrap()
        )
    }
}
