use std::io;
use std::io::{Read, Seek, Write};

pub(crate) mod layer;
pub(crate) mod dbheader;
pub(crate) mod vector;

pub(crate) trait RandomAccess: Read + Write + Seek {}
impl<T: Read + Write + Seek> RandomAccess for T {}

#[derive(Debug)]
pub(crate) enum Error {
    EOF,
    IO(io::Error),
}

