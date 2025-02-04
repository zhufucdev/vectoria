use std::cmp::max;
use std::fmt;
use std::fmt::Formatter;

/// # Non-directional Graph
/// Abstraction of dynamic non-directional graphs, meaning vertices
/// (the connection from one node to another) are considered the same
/// as in the other direction.
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
pub(crate) enum Boundary {
    Capacity,
    Index,
}

impl fmt::Display for Boundary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Boundary::Capacity => write!(f, "capacity"),
            Boundary::Index => write!(f, "index"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Error {
    ExceedBoundary(Boundary, u32, u32),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::ExceedBoundary(what, e, r) => write!(
                f,
                "exceeds {what} boundary (expected to be at least {e}, actual {r})"
            ),
        }
    }
}

impl NdGraph {
    pub(crate) fn new() -> NdGraph {
        NdGraph {
            len: 0,
            capacity: 0,
            adjacent_matrix: Vec::new(),
        }
    }

    pub(crate) fn with_capacity(capacity: u32) -> NdGraph {
        NdGraph {
            len: 0,
            capacity,
            adjacent_matrix: (0..capacity)
                .map(|row| (0..=row).map(|_| f32::INFINITY).collect())
                .collect(),
        }
    }

    pub(crate) fn from_adj_list(adj_list: AdjList) -> NdGraph {
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

    pub(crate) fn get_neighbors(&self, query_node: u32) -> Vec<(u32, f32)> {
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

    pub(crate) fn insert(&mut self, count: u32) -> Result<u32, Error> {
        if self.capacity < self.len() + count {
            return Err(Error::ExceedBoundary(
                Boundary::Capacity,
                self.capacity + 1,
                self.len(),
            ));
        }

        let old_len = self.len();
        for i in 0..count {
            self.adjacent_matrix
                .push(Vec::from_iter((0..i + old_len).map(|_| f32::INFINITY)))
        }
        self.len += count;
        Ok(self.len() - 1)
    }

    pub(crate) fn insert_one(&mut self) -> Result<u32, Error> {
        self.insert(1)
    }

    pub(crate) fn connect(&mut self, a: u32, b: u32, distance: f32) -> Result<(), Error> {
        if a >= self.len() || b >= self.len() {
            Err(Error::ExceedBoundary(
                Boundary::Index,
                max(a, b) + 1,
                self.len(),
            ))
        } else {
            let (a, b) = if a > b { (a, b) } else { (b, a) };
            self.adjacent_matrix[a as usize][b as usize] = distance;
            Ok(())
        }
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.capacity
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() <= 0
    }

    pub(crate) fn distance_between(&self, a: u32, b: u32) -> Result<Option<f32>, Error> {
        if a >= self.len() || b >= self.len() {
            Err(Error::ExceedBoundary(
                Boundary::Index,
                max(a, b) + 1,
                self.len(),
            ))
        } else {
            let (a, b) = if a > b { (a, b) } else { (b, a) };
            let dis = self.adjacent_matrix[a as usize][b as usize];
            Ok(if dis < f32::INFINITY { Some(dis) } else { None })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::f32::consts::{E, PI};

    #[test]
    fn constructors_work() {
        for size in 1..=14 {
            _ = NdGraph::with_capacity(size);
        }
    }

    #[test]
    fn insertion_works() {
        let mut graph = NdGraph::with_capacity(10);

        assert_eq!(graph.insert_one().unwrap(), 0);
        // out of bound
        graph.insert(9).unwrap();
        assert_eq!(
            graph.insert_one(),
            Err(Error::ExceedBoundary(Boundary::Capacity, 11u32, 10u32))
        );
    }

    #[test]
    fn connectivity_works() {
        let mut graph = NdGraph::with_capacity(10);
        graph.insert(graph.capacity()).unwrap();
        graph.connect(0, 9, E).unwrap();
        assert_eq!(graph.distance_between(0, 9).unwrap().unwrap(), E);
        graph.connect(9, 1, PI).unwrap();
        assert_eq!(graph.distance_between(1, 9).unwrap().unwrap(), PI);
        // no connection
        assert!(graph.distance_between(1, 2).unwrap().is_none());
        // out of bound
        assert_eq!(
            graph.distance_between(10, 0),
            Err(Error::ExceedBoundary(Boundary::Index, 11, graph.capacity))
        );
    }
}
