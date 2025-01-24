use crate::model::layer::HnswLayer;
use std::collections::{HashMap, LinkedList};
use std::fmt::Formatter;
use std::io::{BufReader, Read};
use std::{fmt, io};

pub struct Database {
    name: String,
    dim_size: usize,
    layers: LinkedList<HnswLayer>,
    loaded_vectors: HashMap<usize, Vec<f32>>,
}

#[derive(Debug)]
pub enum Error {
    Read(io::Error),
    Parse(),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Error::Read(e) => write!(f, "Database read error: {e}"),
            Error::Parse() => write!(f, "Database parse error")
        }
    }
}

impl Database {
    pub(crate) fn new(name: &String, dim_size: usize) -> Database {
        Database {
            name: name.clone(),
            dim_size,
            layers: LinkedList::new(),
            loaded_vectors: HashMap::new(),
        }
    }

    pub(crate) fn read<R: Read>(source: R) -> Result<Database, Error> {
        let reader = BufReader::new(source);
        // TODO: implement read
    }
}
