use crate::ds::graph::{Graph, NdGraph};
use crate::ds::layer::HnswLayer;
use crate::vio::{Error, RandomAccess};
use byteorder::{BigEndian, ReadBytesExt};

pub(crate) fn read(fd: &mut dyn RandomAccess) -> Result<HnswLayer, Error> {
    let level = fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?;
    if level == 0 {
        return Err(Error::EOF);
    }

    let mut adj_list = vec![];
    loop {
        let (a, b) = (
            fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?,
            fd.read_u32::<BigEndian>().map_err(|e| Error::IO(e))?,
        );
        if a == 0 && b == 0 {
            break;
        }
        let distance = fd.read_f32::<BigEndian>().map_err(|e| Error::IO(e))?;
        adj_list.push((a, b, distance));
    }

    let graph = NdGraph::from_adj_list(adj_list);
    Ok(HnswLayer::new(graph, level))
}
