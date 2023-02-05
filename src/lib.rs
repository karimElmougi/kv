use std::fs::File;
use std::io;
use std::io::{BufRead, Seek, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Unable to read record: {0}")]
    Read(String),

    #[error("Unable to write record: {0}")]
    Write(String),

    #[error("Key `{0}` contains invalid characters")]
    InvalidKey(String),
}

fn write_err<E: std::error::Error>(err: E) -> Error {
    Error::Write(err.to_string())
}

fn read_err<E: std::error::Error>(err: E) -> Error {
    Error::Read(err.to_string())
}

fn line_error(line_number: usize, line: &str) -> Error {
    Error::Read(format!("Invalid data as line {line_number}: `{line}`"))
}

#[derive(Clone)]
pub struct Store<T>(Arc<Mutex<StoreInner<T>>>);

struct StoreInner<T> {
    file: File,
    _phantom: PhantomData<T>,
}

impl<T> Store<T>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    /// Opens the database at the given path.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;

        let inner = StoreInner {
            file,
            _phantom: PhantomData::default(),
        };

        Ok(Store(Arc::new(Mutex::new(inner))))
    }

    /// Sets the given key to the given value.
    pub fn set(&self, key: &str, value: &T) -> Result<(), Error> {
        let key = validate_key(key)?;
        self.write(key, Some(value))
    }

    /// Sets the given key to `None`.
    ///
    /// This appends `key,null` to the database, which in effect removes it from the database. 
    /// Previous entries are not deleted.
    pub fn unset(&self, key: &str) -> Result<(), Error> {
        let key = validate_key(key)?;
        self.write(key, Option::None)
    }

    fn write(&self, key: &str, value: Option<&T>) -> Result<(), Error> {
        let value = serde_json::to_string(&value).map_err(write_err)?;
        writeln!(self.0.lock().file, "{key},{value}").map_err(write_err)
    }

    /// Retrieves the value associated with a key.
    pub fn get(&self, key: &str) -> Result<Option<T>, Error> {
        let key = validate_key(key)?;
        self.scan(move |k, v, value: &mut Option<T>| {
            if k == key {
                *value = serde_json::from_str(v).map_err(read_err)?;
            }
            Ok(())
        })
    }

    /// Loads the entire database in memory in the form of a hash map.
    pub fn load_map(&self) -> Result<FxHashMap<String, T>, Error> {
        self.scan(|k, v, map: &mut FxHashMap<String, T>| {
            let v: Option<T> = serde_json::from_str(v).map_err(read_err)?;
            if let Some(v) = v {
                map.insert(k.to_string(), v);
            }
            Ok(())
        })
    }

    /// Scans the database and calls the given function for every line.
    fn scan<Output, F>(&self, f: F) -> Result<Output, Error>
    where
        Output: Default,
        F: Fn(&str, &str, &mut Output) -> Result<(), Error>,
    {
        let mut inner = self.0.lock();
        inner.file.rewind().map_err(read_err)?;

        let mut output = Output::default();

        let reader = io::BufReader::new(&inner.file);
        for (line_number, line) in reader.lines().enumerate() {
            let line = line.map_err(read_err)?;

            let (k, v) = split_key_value(&line, line_number)?;
            f(k, v, &mut output)?;
        }

        Ok(output)
    }
}

fn split_key_value(line: &str, line_number: usize) -> Result<(&str, &str), Error> {
    let mut split = line.split(',');
    let k = split.next().ok_or_else(|| line_error(line_number, line))?;
    let v = split.next().ok_or_else(|| line_error(line_number, line))?;

    Ok((k, v))
}

fn validate_key(key: &str) -> Result<&str, Error> {
    if key
        .chars()
        .all(|c| matches!(c, '0'..='9' | 'A'..='Z' | 'a'..='z' | ' ' | ':' | '/' | '.'))
    {
        Ok(key)
    } else {
        Err(Error::InvalidKey(key.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use rand::Rng;
    use tempfile::NamedTempFile;

    #[test]
    fn fuzz_test() {
        let f = NamedTempFile::new().unwrap();
        let store = Store::<u8>::open(f.path()).unwrap();
        let mut map = HashMap::<String, u8>::new();

        let mut rng = rand::thread_rng();
        for _ in 0..100_000 {
            let key = format!("key{}", rng.gen::<u32>());
            let value = rng.gen();

            store.set(&key, &value).unwrap();
            map.insert(key, value);
        }

        let store = store.load_map().unwrap();
        for (key, value) in map {
            assert_eq!(value, *store.get(&key).unwrap());
        }
    }

    #[test]
    fn validate_key_test() {
        assert_eq!(Ok(""), validate_key(""));
        assert_eq!(Ok("key"), validate_key("key"));
        assert_eq!(Ok("key with spaces"), validate_key("key with spaces"));
        assert!(validate_key("this,is,a,bad,key").is_err());
        assert!(validate_key("this is\nalso bad").is_err());
    }
}
