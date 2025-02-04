use crate::ds::layer::HnswLayer;
use crate::ext::io::MoveContent;
use crate::ext::semaphore::LockAutoClear;
use crate::vio;
use crate::vio::dbheader::DbHeader;
use crate::vio::RandomAccess;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::min;
use std::collections::{HashMap, LinkedList};
use std::fmt::Formatter;
use std::io::{Seek, SeekFrom};
use std::rc::Rc;
use std::sync::Mutex;
use std::{fmt, io};

pub type DbVector = Vec<f32>;
pub type DbVectorSlice<'a> = &'a [f32];
pub type DbIndex = u32;

struct VectorHandle {
    dim_size: u32,
    data_section: u64,
    fd: Box<dyn RandomAccess>,
}

impl VectorHandle {
    fn new(header: &DbHeader, fd: Box<dyn RandomAccess>) -> VectorHandle {
        VectorHandle {
            dim_size: header.dim_size,
            data_section: header.data_section,
            fd,
        }
    }

    fn unit_size_bytes(&self) -> u64 {
        (self.dim_size * (size_of::<f32>() as u32) + size_of::<DbIndex>() as u32) as u64
    }

    fn seek_count(&mut self) -> Result<u64, Error> {
        let unit = self.unit_size_bytes();
        let available = self.fd.seek(SeekFrom::End(0)).map_err(|e| Error::IO(e))?;
        Ok((available + 1 - self.data_section) / unit)
    }

    #[allow(invalid_reference_casting)]
    fn count(&self) -> Result<u64, Error> {
        let mut_self = unsafe {
            &mut *(self as *const Self as *mut Self)
        };
        let pos = mut_self.fd.stream_position().map_err(|e| Error::IO(e))?;
        let count = mut_self.seek_count()?;
        mut_self.fd.seek(SeekFrom::Start(pos)).map_err(|e| Error::IO(e))?;
        Ok(count)
    }

    fn seek_item(&mut self, id: DbIndex) -> Result<Option<u64>, Error> {
        let unit = self.unit_size_bytes();
        let (mut head, mut tail) = (0u64, self.seek_count()? - 1);

        // employ a binary search between [head] and [tail] in fd
        loop {
            self.fd
                .seek(SeekFrom::Start(head * unit + self.data_section))
                .map_err(|e| Error::IO(e))?;
            let head_id = self.fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?;

            if head_id == id {
                return Ok(Some(head * unit + self.data_section));
            } else if head == tail {
                return Ok(None);
            }

            self.fd
                .seek(SeekFrom::Start(tail * unit + self.data_section))
                .map_err(|e| Error::IO(e))?;
            let tail_id = self.fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?;

            if tail_id == id {
                return Ok(Some(tail * unit + self.data_section));
            }

            if head_id > tail_id {
                panic!("Database is corrupted. Please report this issue.")
            }

            if id < (head_id + tail_id) / 2 {
                tail = (tail - head) / 2 + head;
            } else {
                head = (tail - head) / 2 + head;
            }
        }
    }

    fn get(&mut self, id: DbIndex) -> Result<Option<DbVector>, Error> {
        if self.seek_item(id)?.is_none() {
            return Ok(None);
        }

        Ok(Some(
            vio::vector::read(self.dim_size, &mut self.fd).map_err(|e| match e {
                vio::Error::EOF => Error::IO(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expecting {0} bytes of data, but got none",
                        self.dim_size * size_of::<f32>() as u32
                    ),
                )),
                vio::Error::IO(e) => Error::IO(e),
            })?,
        ))
    }

    fn seek_last_id(&mut self) -> Option<DbIndex> {
        match self
            .fd
            .seek(SeekFrom::End(-(self.unit_size_bytes() as i64)))
        {
            Ok(pos) => {
                if pos < self.data_section {
                    None
                } else {
                    self.fd.read_u32::<BigEndian>().ok()
                }
            },
            Err(_) => None,
        }
    }

    fn push(&mut self, vector: DbVectorSlice) -> Result<DbIndex, Error> {
        if vector.len() != self.dim_size as usize {
            return Err(Error::Dimension(self.dim_size, vector.len()));
        }

        let new_id = match self.seek_last_id() {
            None => 0,
            Some(i) => i + 1,
        };

        self.fd.seek(SeekFrom::End(0)).map_err(|e| Error::IO(e))?;
        self.fd
            .write_u32::<BigEndian>(new_id)
            .map_err(|e| Error::IO(e))?;
        vio::vector::write(vector, &mut self.fd).map_err(|e| Error::IO(e))?;
        Ok(new_id)
    }

    fn remove(&mut self, id: DbIndex) -> Result<Option<DbVector>, Error> {
        match self.seek_item(id)? {
            None => Ok(None),
            Some(pos) => {
                let vector =
                    vio::vector::read(self.dim_size, &mut self.fd).map_err(|e| match e {
                        vio::Error::EOF => Error::Parse(),
                        vio::Error::IO(e) => Error::IO(e),
                    })?;
                let available = self.fd.seek(SeekFrom::End(0)).map_err(|e| Error::IO(e))?;
                let offset = self.unit_size_bytes();
                let pos = pos - size_of::<DbIndex>() as u64;
                self.fd
                    .seek(SeekFrom::Start(pos))
                    .map_err(|e| Error::IO(e))?;
                self.fd
                    .move_content(
                        (available - pos - offset) as usize,
                        -(offset as isize),
                        min(4096, 10 * (offset as usize)),
                    )
                    .map_err(|e| Error::IO(e))?;
                Ok(Some(vector))
            }
        }
    }
}

pub struct Database {
    name: String,
    layers: LinkedList<HnswLayer>,
    loaded_vectors: Mutex<HashMap<u32, Rc<DbVector>>>,
    handle: Mutex<VectorHandle>,
}

#[derive(Debug)]
pub enum Error {
    Header(vio::dbheader::Error),
    IO(io::Error),
    Parse(),
    Dimension(u32, usize),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Header(e) => write!(f, "invalid header because {e}"),
            Error::IO(e) => write!(f, "IO error for {e}"),
            Error::Parse() => write!(f, "parse failed"),
            Error::Dimension(expected, actual) => write!(
                f,
                "dimension mismatch (expected {expected}, actual {actual})"
            ),
        }
    }
}

impl Database {
    pub fn read(name: &String, mut fd: Box<dyn RandomAccess>) -> Result<Database, Error> {
        let header = vio::dbheader::read(&mut fd).map_err(|e| Error::Header(e))?;

        let mut layers = LinkedList::new();
        loop {
            match vio::layer::read(&mut fd) {
                Ok(layer) => layers.push_back(layer),
                Err(vio::Error::IO(e)) => return Err(Error::IO(e)),
                Err(vio::Error::EOF) => break,
            }
        }
        Ok(Database {
            handle: Mutex::new(VectorHandle::new(&header, fd)),
            name: name.clone(),
            layers,
            loaded_vectors: Mutex::new(HashMap::new()),
        })
    }

    pub fn new(name: &str, dim_size: u32, mut fd: Box<dyn RandomAccess>) -> Database {
        let header = DbHeader::new(dim_size);
        header.write(&mut fd).unwrap();
        Database {
            handle: Mutex::new(VectorHandle::new(&header, fd)),
            name: String::from(name),
            layers: LinkedList::new(),
            loaded_vectors: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&mut self, id: DbIndex) -> Result<Option<Rc<DbVector>>, Error> {
        let mut handle = self.handle.lock_auto_clear_poison();
        let mut cache = self.loaded_vectors.lock_auto_clear_poison();
        match cache.get(&id) {
            None => match handle.get(id) {
                Ok(Some(v)) => {
                    let rc: Rc<DbVector> = Rc::new(v);
                    cache.insert(id, rc.clone());
                    Ok(Some(rc.clone()))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            },
            Some(v) => Ok(Some(v.clone())),
        }
    }

    pub fn flush(&self) -> Result<usize, Error> {
        unimplemented!()
    }

    pub fn push(&mut self, vector: DbVectorSlice) -> Result<DbIndex, Error> {
        let mut handle = self.handle.lock_auto_clear_poison();
        match handle.push(vector) {
            Ok(index) => {
                let mut cache = self.loaded_vectors.lock_auto_clear_poison();
                cache.insert(index, Rc::new(DbVector::from(vector)));
                Ok(index)
            }
            Err(e) => Err(e),
        }
    }

    pub fn remove(&mut self, id: DbIndex) -> Result<Option<Rc<DbVector>>, Error> {
        let mut handle = self.handle.lock_auto_clear_poison();
        match handle.remove(id) {
            Ok(Some(v)) => {
                let mut cache = self.loaded_vectors.lock_auto_clear_poison();
                cache.remove(&id);
                Ok(Some(Rc::new(v)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::Database;
    use std::io::Cursor;

    #[test]
    fn append_works() {
        let fd = Box::new(Cursor::new(Vec::new()));
        let mut db = Database::new("mem", 512, fd);
        let vector = Vec::from_iter((0..512).map(|i| i as f32));
        let victim_id = db.push(&*vector).unwrap();
        assert_eq!(victim_id, 0);

        assert_eq!(db.handle.lock().unwrap().count().unwrap(), 1);
        assert_eq!(db.get(victim_id).unwrap().unwrap(), vector.into());
    }

    #[test]
    fn index_works() {
        let fd = Box::new(Cursor::new(Vec::new()));
        let mut db = Database::new("mem", 512, fd);
        let vector = Vec::from_iter((0..512).map(|i| i as f32));
        for _ in 0..200 {
            db.push(&*vector).unwrap();
        }
        let victim_vect = Vec::from_iter((0..512).map(|_| 0f32));
        let victim_id = db.push(&*victim_vect).unwrap();
        for _ in 0..200 {
            db.push(&*vector).unwrap();
        }

        let indexed = db.get(victim_id).unwrap().unwrap();
        assert_eq!(indexed, victim_vect.into());
    }

    #[test]
    fn remove_works() {
        let fd = Box::new(Cursor::new(Vec::new()));
        let mut db = Database::new("mem", 4, fd);
        for i in 1..=200 {
            let v = vec![i as f32, i as f32, i as f32, i as f32];
            db.push(&*v).unwrap();
        }
        assert_eq!(200, db.handle.lock().unwrap().count().unwrap());

        let removed = db.remove(198).unwrap().unwrap();
        assert_eq!(removed, vec![199f32, 199f32, 199f32, 199f32].into());
    }
}
