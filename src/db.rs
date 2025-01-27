use crate::ds::layer::HnswLayer;
use crate::semaphore::LockAutoClear;
use crate::vio;
use crate::vio::header::DbHeader;
use crate::vio::RandomAccess;
use byteorder::{BigEndian, ReadBytesExt};
use std::collections::{HashMap, LinkedList};
use std::fmt::Formatter;
use std::io::{Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::{fmt, io};

pub type DbVector = Vec<f32>;

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
        (self.dim_size * (size_of::<f32>() as u32) + size_of::<u32>() as u32) as u64
    }

    fn get(&mut self, id: u32) -> Result<Option<DbVector>, Error> {
        // employ a binary search between [first_id] and [last_id]
        let unit = self.unit_size_bytes();
        self.fd.seek(SeekFrom::End(0)).map_err(|e| Error::IO(e))?;
        let count =
            (self.fd.stream_position().map_err(|e| Error::IO(e))? - self.data_section) / unit;
        let (mut head, mut tail) = (0u64, count - 1);
        loop {
            self.fd
                .seek(SeekFrom::Start(head * unit + self.data_section))
                .map_err(|e| Error::IO(e))?;
            let head_id = self.fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?;

            if head_id == id {
                break;
            } else if head == tail {
                return Ok(None);
            }

            self.fd
                .seek(SeekFrom::Start(tail * unit + self.data_section))
                .map_err(|e| Error::IO(e))?;
            let tail_id = self.fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?;

            if tail_id == id {
                break;
            }

            if id < (head_id + tail_id) / 2 {
                tail = (tail - head) / 2 + head;
            } else {
                head = (tail - head) / 2 + head;
            }
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
}

pub struct Database {
    name: String,
    layers: LinkedList<HnswLayer>,
    loaded_vectors: Mutex<HashMap<u32, Arc<DbVector>>>,
    handle: Mutex<VectorHandle>,
}

#[derive(Debug)]
pub enum Error {
    Header(vio::header::Error),
    IO(io::Error),
    Parse(),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::Header(e) => write!(f, "invalid header because {e}"),
            Error::IO(e) => write!(f, "IO error for {e}"),
            Error::Parse() => write!(f, "parse failed"),
        }
    }
}

impl Database {
    pub fn read(name: &String, mut fd: Box<dyn RandomAccess>) -> Result<Database, Error> {
        let header = vio::header::db_read(&mut fd).map_err(|e| Error::Header(e))?;

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

    pub fn new(name: &String, dim_size: u32, fd: Box<dyn RandomAccess>) -> Database {
        let header = DbHeader::new(dim_size);
        Database {
            handle: Mutex::new(VectorHandle::new(&header, fd)),
            name: name.clone(),
            layers: LinkedList::new(),
            loaded_vectors: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_vector(&mut self, id: u32) -> Result<Option<Arc<DbVector>>, Error> {
        let mut handle = self.handle.lock_auto_clear_poison();
        let mut cache = self.loaded_vectors.lock_auto_clear_poison();
        match cache.get(&id) {
            None => match handle.get(id) {
                Ok(Some(v)) => {
                    let arc = Arc::new(v);
                    cache.insert(id, arc.clone());
                    Ok(Some(arc.clone()))
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
}
