use crate::vio::RandomAccess;
use byteorder::{BigEndian, ReadBytesExt};
use std::fmt::Formatter;
use std::str::FromStr;
use std::{fmt, io};

#[derive(Debug)]
pub(crate) enum ParseErrorReason {
    ProductNameMismatch(String),
    StringDecodeFailed,
}

impl fmt::Display for ParseErrorReason {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ParseErrorReason::ProductNameMismatch(name) => {
                write!(f, "unknown product name ({name})")
            }
            ParseErrorReason::StringDecodeFailed => write!(f, "string decode failed"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Error {
    IO(io::Error),
    Parse(ParseErrorReason),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::IO(e) => write!(f, "IO error because {e}"),
            Error::Parse(r) => write!(f, "parse error because {r}"),
        }
    }
}

pub(crate) struct DbHeader {
    pub version: u8,
    pub dim_size: u32,
    pub data_section: u64,
}

const PRODUCT: &str = "vectoriadb;version";
type VersionNumber = u8;
pub(crate) const CURRENT_VERSION: VersionNumber = 1u8;

pub(crate) fn db_read(fd: &mut dyn RandomAccess) -> Result<DbHeader, Error> {
    let mut product_buf = [0u8; PRODUCT.len()];
    fd.read_exact(&mut product_buf).map_err(|e| Error::IO(e))?;
    let product_name = std::str::from_utf8(&product_buf)
        .map_err(|e| Error::Parse(ParseErrorReason::StringDecodeFailed))?;

    if product_name != PRODUCT {
        return Err(Error::Parse(ParseErrorReason::ProductNameMismatch(
            String::from_str(product_name).unwrap(),
        )));
    }

    let version = fd.read_u8().map_err(|e| Error::IO(e))?;
    let data_section = fd.read_u64::<BigEndian>().map_err(|e| Error::IO(e))?;
    let dim_size = fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?;
    Ok(DbHeader {
        dim_size,
        data_section,
        version,
    })
}

impl DbHeader {
    pub(crate) fn new(dim_size: u32) -> DbHeader {
        DbHeader {
            version: CURRENT_VERSION,
            dim_size,
            data_section: (PRODUCT.len() + size_of::<VersionNumber>()) as u64,
        }
    }
}
