use std::cmp::max;
use std::fmt;
use std::fmt::Formatter;

pub(crate) struct NdGraph {
    len: usize,
    capacity: usize,
    adjacent_matrix: Vec<Vec<f32>>,
}

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
    ExceedBoundary(Boundary, usize, usize),
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
    pub(crate) fn new(capacity: usize) -> NdGraph {
        NdGraph {
            len: 0,
            capacity,
            adjacent_matrix: Vec::from_iter(
                (0..capacity).map(|row| Vec::from_iter((0..=row).map(|_| f32::INFINITY))),
            ),
        }
    }

    pub(crate) fn from_adj_list(capacity: usize, adj_list: Vec<(usize, usize, f32)>) -> NdGraph {
        let adj_mat = Vec::from_iter((0..capacity).map(|row| {
            Vec::from_iter((0..=row).map(|col| {
                match adj_list
                    .iter()
                    .find(|(a, b, _)| *a == row && *b == col || *a == col && *b == row)
                {
                    None => f32::INFINITY,
                    Some((_, _, d)) => *d,
                }
            }))
        }));
        let len = *adj_list
            .iter()
            .flat_map(|(a, b, _)| [a, b])
            .max()
            .unwrap_or(&0usize);

        NdGraph {
            len,
            capacity,
            adjacent_matrix: adj_mat,
        }
    }

    pub(crate) fn get_neighbors(&self, query_node: usize) -> Vec<(usize, f32)> {
        if query_node >= self.len {
            return vec![];
        }
        Vec::from_iter(
            (0..self.len)
                .map(|n| (n, self.adjacent_matrix[query_node][n]))
                .filter(|n| self.adjacent_matrix[query_node][n.0] < f32::INFINITY),
        )
    }

    pub(crate) fn insert(&mut self, count: usize) -> Result<usize, Error> {
        if self.capacity < self.len + count {
            return Err(Error::ExceedBoundary(
                Boundary::Capacity,
                self.capacity + 1,
                self.len,
            ));
        }

        self.len += count;
        Ok(self.len - 1)
    }

    pub(crate) fn insert_one(&mut self) -> Result<usize, Error> {
        self.insert(1)
    }

    pub(crate) fn connect(&mut self, a: usize, b: usize, distance: f32) -> Result<(), Error> {
        if a >= self.len || b >= self.len {
            Err(Error::ExceedBoundary(
                Boundary::Index,
                max(a, b) + 1,
                self.len,
            ))
        } else {
            let (a, b) = if a > b { (a, b) } else { (b, a) };
            self.adjacent_matrix[a][b] = distance;
            Ok(())
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn distance_between(&self, a: usize, b: usize) -> Result<Option<f32>, Error> {
        if a >= self.len || b >= self.len {
            Err(Error::ExceedBoundary(Boundary::Index, max(a, b) + 1, self.len))
        } else {
            let (a, b) = if a > b { (a, b) } else { (b, a) };
            let dis = self.adjacent_matrix[a][b];
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
            _ = NdGraph::new(size);
        }
    }

    #[test]
    fn insertion_works() {
        let mut graph = NdGraph::new(10);

        assert_eq!(graph.insert_one().unwrap(), 0);
        // out of bound
        graph.insert(9).unwrap();
        assert_eq!(
            graph.insert_one(),
            Err(Error::ExceedBoundary(Boundary::Capacity, 11usize, 10usize))
        );
    }

    #[test]
    fn connectivity_works() {
        let mut graph = NdGraph::new(10);
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
