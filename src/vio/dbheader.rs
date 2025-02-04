use crate::vio::RandomAccess;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fmt::Formatter;
use std::io::Write;
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

const PRODUCT: &str = "vectoriadb;version";
type VersionNumber = u8;
type DimSize = u32;
type DataSection = u64;
pub(crate) const CURRENT_VERSION: VersionNumber = 1u8;

pub(crate) struct DbHeader {
    pub version: VersionNumber,
    pub dim_size: DimSize,
    pub data_section: DataSection,
}

pub(crate) fn read(fd: &mut dyn RandomAccess) -> Result<DbHeader, Error> {
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
    pub(crate) fn new(dim_size: DimSize) -> DbHeader {
        DbHeader {
            version: CURRENT_VERSION,
            dim_size,
            data_section: (PRODUCT.len()
                + size_of::<VersionNumber>()
                + size_of::<DimSize>()
                + size_of::<DataSection>()) as u64,
        }
    }

    pub(crate) fn write(&self, fd: &mut dyn RandomAccess) -> Result<(), Error> {
        write!(fd, "{0}{1}", PRODUCT, self.version).map_err(|e| Error::IO(e))?;
        fd.write_u64::<BigEndian>(self.data_section).map_err(|e| Error::IO(e))?;
        fd.write_u32::<BigEndian>(self.dim_size).map_err(|e| Error::IO(e))?;
        Ok(())
    }
}
