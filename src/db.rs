use crate::ds::layer::HnswLayer;
use crate::vio;
use crate::vio::RandomAccess;
use std::collections::{HashMap, LinkedList};
use std::fmt::Formatter;
use std::{fmt, io};

pub struct Database {
    name: String,
    dim_size: usize,
    layers: LinkedList<HnswLayer>,
    loaded_vectors: HashMap<usize, Vec<f32>>,
    fd: Box<dyn RandomAccess>,
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
        let header = vio::header::db_read(&mut fd).map_err(|e| Error::Header(e));
        
        let mut layers = LinkedList::new();
        loop {
            match vio::layer::read(&mut fd) {
                Ok(layer) => layers.push_back(layer),
                Err(vio::layer::Error::IO(e)) => return Err(Error::IO(e)),
                Err(vio::layer::Error::EOF) => break,
            }
        }
        Ok(Database {
            fd,
            dim_size: 0,
            name: name.clone(),
            layers,
            loaded_vectors: HashMap::new(),
        })
    }

    pub fn new(name: &String, dim_size: usize, fd: Box<dyn RandomAccess>) -> Database {
        Database {
            fd,
            dim_size,
            name: name.clone(),
            layers: LinkedList::new(),
            loaded_vectors: HashMap::new(),
        }
    }

    pub(crate) fn flush(&self) -> Result<usize, Error> {
        unimplemented!()
    }
}
