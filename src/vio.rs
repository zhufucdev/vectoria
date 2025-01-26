use std::io::{Read, Seek, Write};

pub(crate) mod layer;
pub(crate) mod header;

pub(crate) trait RandomAccess: Read + Write + Seek {}
impl<T: Read + Write + Seek> RandomAccess for T {}

