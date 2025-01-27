use std::sync::{Mutex, MutexGuard};

pub(crate) trait LockAutoClear<T> {
    fn lock_auto_clear_poison<'a>(&mut self) -> MutexGuard<T>;
}

impl<T> LockAutoClear<T> for Mutex<T> {
    fn lock_auto_clear_poison<'a>(&mut self) -> MutexGuard<T> {
        self.lock().unwrap_or_else(|_| {
            self.clear_poison();
            self.lock().unwrap()
        })
    }
}
