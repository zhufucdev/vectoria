use crate::model::db;
use crate::model::db::Database;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::{fs, io};

pub struct ManagementSystem<H: DbHandle> {
    handle: Mutex<Arc<H>>,
    loaded_db: HashMap<String, Arc<Database>>,
}

pub enum Error {
    NameConflict(String),
    IO(io::Error),
    Database(db::Error),
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
    fn create(&self, name: &String, dim_size: usize) -> Result<Database, Error>;
    fn get(&self, name: &String) -> Result<Option<Database>, Error>;
}

impl DbHandle for FsDbHandle {
    fn create(&self, name: &String, dim_size: usize) -> Result<Database, Error> {
        let file = self.get_underlying_file(name);
        if fs::exists(&file).map_err(|e| Error::IO(e))? {
            Err(Error::NameConflict(name.clone()))
        } else {
            Ok(Database::new(name, dim_size))
        }
    }

    fn get(&self, name: &String) -> Result<Option<Database>, Error> {
        let file = self.get_underlying_file(name);
        if fs::exists(&file).unwrap_or(false) {
            let fd = File::open(file).unwrap();
            let parsed = Database::read(fd);
            if parsed.is_ok() {
                return Ok(Some(parsed.unwrap()));
            }
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
            loaded_db: HashMap::new(),
        }
    }
}

impl<H: DbHandle> ManagementSystem<H> {
    fn gc() {
        // TODO: implement garbage collector for DBMS
    }

    fn get_handle(&self) -> MutexGuard<Arc<H>> {
        self.handle.lock().unwrap_or_else(|_| {
            self.handle.clear_poison();
            self.handle.lock().unwrap()
        })
    }

    pub fn create(&mut self, name: &String, dim_size: usize) -> Result<Arc<Database>, Error> {
        let created = Arc::from(self.get_handle().create(name, dim_size)?);
        self.loaded_db.insert(name.clone(), created.clone());
        Ok(created.clone())
    }

    pub fn get(&mut self, name: &String) -> Result<Option<Arc<Database>>, Error> {
        let cache = self.loaded_db.get(name);
        match cache {
            None => {
                let load = self.get_handle().get(name);
                match load {
                    Ok(None) => Ok(None),
                    Ok(Some(db)) => {
                        let arc = Arc::from(db);
                        self.loaded_db.insert(name.clone(), arc.clone());
                        Ok(Some(arc.clone()))
                    }
                    Err(e) => Err(e),
                }
            }
            Some(db) => Ok(Some(db.clone())),
        }
    }
}
