use crate::ds::graph::NdGraph;

pub(crate) struct HnswLayer {
    graph: NdGraph,
    level: u32,
}

impl HnswLayer {
    pub(crate) fn new(graph: NdGraph, level: u32) -> HnswLayer {
        HnswLayer { graph, level }
    }
    
    pub(crate) fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }
}
