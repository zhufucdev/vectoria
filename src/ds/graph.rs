use std::cmp::max;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fmt::Formatter;

pub(crate) trait Graph<Error> {
    fn new() -> Self;
    fn with_capacity(capacity: u32) -> Self;
    fn from_adj_list(adj_list: AdjList) -> Self;

    fn len(&self) -> u32;
    fn capacity(&self) -> u32;
    fn is_empty(&self) -> bool;

    fn connect(&mut self, a: u32, b: u32, distance: f32) -> Result<(), Error>;
    fn get_neighbors(&self, query_node: u32) -> Vec<u32>;
    fn get_vertices(&self, query_node: u32) -> Vec<(u32, f32)>;
    fn get_vertice(&self, a: u32, b: u32) -> Result<Option<f32>, Error>;
}

/// # Non-directional Graph
/// Abstraction of dynamic non-directional graphs, meaning vertices
/// (the connection from one node to another) are considered the same
/// as in the other direction, and the capacity is incremented automatically.
///
/// The underlying implementation employs an adjacent matrix data structure,
/// where space complexity is proportional to the square of the node numbers,
/// and time complexity of querying is constant.
pub(crate) struct NdGraph {
    len: u32,
    capacity: u32,
    adjacent_matrix: Vec<Vec<f32>>,
}

/// # Adjacent List
/// Using a list of tuples to represent the [NdGraph] structure.
/// The first couple stands for nodes, the last being the distance.
type AdjList = Vec<(u32, u32, f32)>;

#[derive(Debug, PartialEq)]
pub(crate) enum NdgError {
    ExceedBoundary(u32, u32),
}

impl fmt::Display for NdgError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            NdgError::ExceedBoundary(e, r) => write!(
                f,
                "exceeds boundary (expected to be at least {e}, actual {r})"
            ),
        }
    }
}

impl Graph<NdgError> for NdGraph {
    fn new() -> Self {
        NdGraph {
            len: 0,
            capacity: 0,
            adjacent_matrix: Vec::new(),
        }
    }

    fn with_capacity(capacity: u32) -> NdGraph {
        NdGraph {
            len: 0,
            capacity,
            adjacent_matrix: (0..capacity)
                .map(|row| (0..=row).map(|_| f32::INFINITY).collect())
                .collect(),
        }
    }

    fn from_adj_list(adj_list: AdjList) -> NdGraph {
        let len = *adj_list
            .iter()
            .flat_map(|(a, b, _)| [a, b])
            .max()
            .unwrap_or(&0u32);

        let adj_mat = (0..len)
            .map(|row| {
                (0..=row)
                    .map(|col| {
                        match adj_list
                            .iter()
                            .find(|(a, b, _)| *a == row && *b == col || *a == col && *b == row)
                        // TODO: optimize this O(n^2) search by sorting in advance
                        {
                            None => f32::INFINITY,
                            Some((_, _, d)) => *d,
                        }
                    })
                    .collect()
            })
            .collect();

        NdGraph {
            len,
            capacity: len,
            adjacent_matrix: adj_mat,
        }
    }

    fn len(&self) -> u32 {
        self.len
    }

    fn capacity(&self) -> u32 {
        self.capacity
    }

    fn is_empty(&self) -> bool {
        self.len() <= 0
    }

    fn connect(&mut self, a: u32, b: u32, distance: f32) -> Result<(), NdgError> {
        if a >= self.len() || b >= self.len() {
            Err(NdgError::ExceedBoundary(max(a, b) + 1, self.len()))
        } else {
            let (a, b) = if a > b { (a, b) } else { (b, a) };
            self.adjacent_matrix[a as usize][b as usize] = distance;
            Ok(())
        }
    }

    fn get_neighbors(&self, query_node: u32) -> Vec<u32> {
        if query_node >= self.len() {
            return vec![];
        }
        self.adjacent_matrix[query_node as usize]
            .iter()
            .enumerate()
            .filter_map(|(node, dist)| {
                if *dist < f32::INFINITY {
                    Some(node as u32)
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_vertices(&self, query_node: u32) -> Vec<(u32, f32)> {
        if query_node >= self.len() {
            return vec![];
        }
        Vec::from_iter(
            (0..self.len())
                .map(|n| (n, self.adjacent_matrix[query_node as usize][n as usize]))
                .filter(|n| {
                    self.adjacent_matrix[query_node as usize][n.0 as usize] < f32::INFINITY
                }),
        )
    }

    fn get_vertice(&self, a: u32, b: u32) -> Result<Option<f32>, NdgError> {
        if a >= self.len() || b >= self.len() {
            Err(NdgError::ExceedBoundary(max(a, b) + 1, self.len()))
        } else {
            let (a, b) = if a > b { (a, b) } else { (b, a) };
            let dis = self.adjacent_matrix[a as usize][b as usize];
            Ok(if dis < f32::INFINITY { Some(dis) } else { None })
        }
    }
}

impl NdGraph {
    pub(crate) fn push_many(&mut self, count: u32) -> u32 {
        if self.capacity() < self.len() + count {
            let lacking = self.len() + count - self.capacity();
            for row in 0..lacking + self.capacity() {
                self.adjacent_matrix
                    .push(Vec::from_iter((0..row).map(|_| f32::INFINITY)))
            }
            self.capacity += lacking;
        }

        self.len += count;
        self.len() - 1
    }

    pub(crate) fn push_one(&mut self) -> u32 {
        self.push_many(1)
    }
}

/// # Cast Non-directional Graph
/// A derivation from [NdGraph] that implements scattered index,
/// meaning node numbers don't have to be continuous.
///
/// The underlying implementation is basically [NdGraph] and [HashMap],
/// so efficiency should be alright.
struct AnyCastNdGraph {
    graph: NdGraph,
    mapping: HashMap<u32, u32>,
}

#[derive(Debug, PartialEq)]
enum AcndgError {
    NodeNonexistence(u32)
}

impl AnyCastNdGraph {
    fn get_mapping_or_insert(&mut self, node: u32) -> u32 {
        match self.mapping.get(&node) {
            Some(m) => *m,
            None => {
                let pushed = self.graph.push_one();
                self.mapping.insert(node, pushed);
                pushed
            }
        }
    }
}

impl Graph<AcndgError> for AnyCastNdGraph {
    fn new() -> Self {
        AnyCastNdGraph {
            graph: NdGraph::new(),
            mapping: HashMap::new(),
        }
    }

    fn with_capacity(capacity: u32) -> Self {
        AnyCastNdGraph {
            graph: NdGraph::with_capacity(capacity),
            mapping: HashMap::with_capacity(capacity as usize),
        }
    }

    fn from_adj_list(adj_list: AdjList) -> Self {
        let unique_nodes = BTreeSet::from_iter(
            adj_list
                .iter()
                .map(|(a, _, _)| a)
                .chain(adj_list.iter().map(|(_, b, _)| b)),
        );
        let mut graph = Self::with_capacity(unique_nodes.len() as u32);
        for (a, b, dist) in adj_list {
            graph.connect(a, b, dist).unwrap();
        }
        graph
    }

    fn len(&self) -> u32 {
        self.graph.len()
    }

    fn capacity(&self) -> u32 {
        self.graph.capacity()
    }

    fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    fn connect(&mut self, a: u32, b: u32, distance: f32) -> Result<(), AcndgError> {
        let a = self.get_mapping_or_insert(a);
        let b = self.get_mapping_or_insert(b);
        Ok(self.graph.connect(a, b, distance).unwrap())
    }

    fn get_neighbors(&self, query_node: u32) -> Vec<u32> {
        match self.mapping.get(&query_node) {
            None => vec![],
            Some(m) => self.graph.get_neighbors(*m)
        }
    }

    fn get_vertices(&self, query_node: u32) -> Vec<(u32, f32)> {
        match self.mapping.get(&query_node) {
            None => vec![],
            Some(m) => self.graph.get_vertices(*m)
        }
    }

    fn get_vertice(&self, a: u32, b: u32) -> Result<Option<f32>, AcndgError> {
        match self.mapping.get(&a) {
            None => Err(AcndgError::NodeNonexistence(a)),
            Some(a) => {
                match self.mapping.get(&b) {
                    None => Err(AcndgError::NodeNonexistence(b)),
                    Some(b) => Ok(self.graph.get_vertice(*a, *b).unwrap())
                }
            }
        }
    }
}

impl From<NdGraph> for AnyCastNdGraph {
    fn from(value: NdGraph) -> Self {
        let mapping = HashMap::from_iter(
            value
                .adjacent_matrix
                .iter()
                .take(value.len() as usize)
                .enumerate()
                .filter_map(|(node, col)| {
                    if col.iter().any(|over| *over < f32::INFINITY) {
                        Some((node as u32, node as u32))
                    } else {
                        None
                    }
                }),
        );
        AnyCastNdGraph {
            graph: value,
            mapping,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::f32::consts::{E, PI};

    #[test]
    fn ndg_constructors_work() {
        _ = NdGraph::new();
        for size in 1..=14 {
            _ = NdGraph::with_capacity(size);
        }
    }

    #[test]
    fn ndg_insertion_works() {
        let mut graph = NdGraph::new();
        assert_eq!(graph.push_many(1000), 999);
        assert_eq!(graph.capacity(), 1000);

        graph = NdGraph::with_capacity(10);
        assert_eq!(graph.push_one(), 0);
    }

    #[test]
    fn ndg_connectivity_works() {
        let mut graph = NdGraph::with_capacity(10);
        graph.push_many(graph.capacity());
        graph.connect(0, 9, E).unwrap();
        assert_eq!(graph.get_vertice(0, 9).unwrap().unwrap(), E);
        graph.connect(9, 1, PI).unwrap();
        assert_eq!(graph.get_vertice(1, 9).unwrap().unwrap(), PI);
        // no connection
        assert!(graph.get_vertice(1, 2).unwrap().is_none());
        // out of bound
        assert_eq!(
            graph.get_vertice(10, 0),
            Err(NdgError::ExceedBoundary(11, graph.capacity))
        );
    }
    
    #[test]
    fn acndg_constructors_works() {
        _ = AnyCastNdGraph::new();
        for cap in 1..=14 {
            _ = AnyCastNdGraph::with_capacity(cap)
        }
    }
    
    #[test]
    fn acndg_connectivity_works() {
        let mut graph = AnyCastNdGraph::new();
        graph.connect(36, 69, 0.42).unwrap();
        assert_eq!(0.42, graph.get_vertice(36, 69).unwrap().unwrap());
    }
    
    #[test]
    fn acndg_many_connection_works() {
        let mut graph = AnyCastNdGraph::new();
        for i in 0..=1000 {
            graph.connect(i + 69, i + 4069, 420f32 / i as f32).unwrap()
        }
        assert_eq!(2000, graph.len());
    }
}
