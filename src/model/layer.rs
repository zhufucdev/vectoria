use crate::model::graph::{NdGraphNode, NdGraph};

pub(crate) struct HnswLayer {
    graph: NdGraph,
    level: u32
}
