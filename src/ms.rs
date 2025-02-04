use crate::db;
use crate::db::Database;
use crate::ext::semaphore::LockAutoClear;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{fmt, fs, io};

pub struct ManagementSystem<H: DbHandle> {
    handle: Mutex<Arc<H>>,
    loaded_db: Mutex<HashMap<String, Arc<Database>>>,
}

#[derive(Debug)]
pub enum Error {
    NameConflict(String),
    IO(io::Error),
    Database(db::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Error::NameConflict(name) => write!(f, "conflicting name of {name}"),
            Error::IO(e) => write!(f, "IO failed because {e}"),
            Error::Database(e) => write!(f, "database failed because {e}"),
        }
    }
}

struct FsDbHandle {
    root_dir: Box<Path>,
}

impl FsDbHandle {
    fn get_underlying_file(&self, db_name: &String) -> Box<Path> {
        Box::from(self.root_dir.join(format!("{db_name}.db")))
    }
}

trait DbHandle {
    fn create(&self, name: &String, dim_size: u32) -> Result<Database, Error>;
    fn get(&self, name: &String) -> Result<Option<Database>, Error>;
}

impl DbHandle for FsDbHandle {
    fn create(&self, name: &String, dim_size: u32) -> Result<Database, Error> {
        let file = self.get_underlying_file(name);
        if fs::exists(&file).map_err(|e| Error::IO(e))? {
            Err(Error::NameConflict(name.clone()))
        } else {
            let fd = File::open(file).map_err(|e| Error::IO(e))?;
            Ok(Database::new(name, dim_size, Box::new(fd)))
        }
    }

    fn get(&self, name: &String) -> Result<Option<Database>, Error> {
        let file = self.get_underlying_file(name);
        if fs::exists(&file).unwrap_or(false) {
            let fd = File::open(file).unwrap();
            return Ok(Some(Database::read(name, Box::new(fd)).map_err(
                |e| match e {
                    db::Error::Header(e) => Error::Database(db::Error::Header(e)),
                    db::Error::IO(e) => Error::IO(e),
                    db::Error::Parse() => Error::Database(e),
                    db::Error::Dimension(_, _) => Error::Database(e),
                },
            )?));
        }
        Ok(None)
    }
}

impl ManagementSystem<FsDbHandle> {
    pub fn new_fs<P: AsRef<Path>>(root_dir: P) -> ManagementSystem<FsDbHandle> {
        ManagementSystem {
            handle: Mutex::new(Arc::from(FsDbHandle {
                root_dir: Box::from(root_dir.as_ref()),
            })),
            loaded_db: Mutex::new(HashMap::new()),
        }
    }
}

impl<H: DbHandle> ManagementSystem<H> {
    fn gc(&mut self) {
        // TODO: implement garbage collector for DBMS
    }

    pub fn create(&mut self, name: &String, dim_size: u32) -> Result<Arc<Database>, Error> {
        let created = Arc::from(
            self.handle
                .lock_auto_clear_poison()
                .create(name, dim_size)?,
        );
        self.loaded_db
            .lock_auto_clear_poison()
            .insert(name.clone(), created.clone());
        Ok(created.clone())
    }

    pub fn get(&mut self, name: &String) -> Result<Option<Arc<Database>>, Error> {
        let handle = self.handle.lock_auto_clear_poison();
        let mut cache = self.loaded_db.lock_auto_clear_poison();
        match cache.get(name) {
            None => {
                let load = handle.get(name);
                match load {
                    Ok(None) => Ok(None),
                    Ok(Some(db)) => {
                        let arc = Arc::from(db);
                        cache.insert(name.clone(), arc.clone());
                        Ok(Some(arc.clone()))
                    }
                    Err(e) => Err(e),
                }
            }
            Some(db) => Ok(Some(db.clone())),
        }
    }
}
