use std::sync::Arc;

pub(crate) struct NdGraph {
    nodes: Vec<Arc<NdGraphNode>>,
    entry: Arc<NdGraphNode>
}

pub(crate) struct NdGraphNode {
    vct_idx: usize,
    neighbors: Vec<Arc<NdGraphNode>>,
}
