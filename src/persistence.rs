//! A minimal RDB-style snapshot format. Not compatible with real Redis's
//! RDB, but the same idea: serialize the whole keyspace to a binary file so
//! it survives a restart, and load it back on boot.
//!
//! Layout (all integers little-endian):
//!   magic:      4 bytes  b"FRDB"
//!   version:    u8
//!   entry_count: u64
//!   entries...
//!
//! Each entry:
//!   key_len: u32, key bytes
//!   has_expiry: u8 (0 or 1), [expires_at_ms: i64 if has_expiry]
//!   type_tag: u8 (0=String, 1=List, 2=Hash, 3=Set)
//!   payload (shape depends on type_tag; see write_value/read_value)

use crate::store::{Bytes, Entry, Store, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"FRDB";
const VERSION: u8 = 1;

fn write_bytes<W: Write>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)
}

fn read_len<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_bytes<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let len = read_len(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_value<W: Write>(w: &mut W, value: &Value) -> io::Result<()> {
    match value {
        Value::Str(s) => {
            w.write_all(&[0u8])?;
            write_bytes(w, s)?;
        }
        Value::List(list) => {
            w.write_all(&[1u8])?;
            w.write_all(&(list.len() as u32).to_le_bytes())?;
            for item in list {
                write_bytes(w, item)?;
            }
        }
        Value::Hash(map) => {
            w.write_all(&[2u8])?;
            w.write_all(&(map.len() as u32).to_le_bytes())?;
            for (k, v) in map {
                write_bytes(w, k)?;
                write_bytes(w, v)?;
            }
        }
        Value::Set(set) => {
            w.write_all(&[3u8])?;
            w.write_all(&(set.len() as u32).to_le_bytes())?;
            for item in set {
                write_bytes(w, item)?;
            }
        }
    }
    Ok(())
}

fn read_value<R: Read>(r: &mut R) -> io::Result<Value> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    match tag[0] {
        0 => Ok(Value::Str(read_bytes(r)?)),
        1 => {
            let count = read_len(r)?;
            let mut list: VecDeque<Bytes> = VecDeque::with_capacity(count as usize);
            for _ in 0..count {
                list.push_back(read_bytes(r)?);
            }
            Ok(Value::List(list))
        }
        2 => {
            let count = read_len(r)?;
            let mut map = HashMap::with_capacity(count as usize);
            for _ in 0..count {
                let k = read_bytes(r)?;
                let v = read_bytes(r)?;
                map.insert(k, v);
            }
            Ok(Value::Hash(map))
        }
        3 => {
            let count = read_len(r)?;
            let mut set = HashSet::with_capacity(count as usize);
            for _ in 0..count {
                set.insert(read_bytes(r)?);
            }
            Ok(Value::Set(set))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown value type tag: {}", other),
        )),
    }
}

pub fn save(store: &Store, path: &Path) -> io::Result<()> {
    let tmp_path = path.with_extension("tmp");
    {
        let file = File::create(&tmp_path)?;
        let mut w = BufWriter::new(file);
        w.write_all(MAGIC)?;
        w.write_all(&[VERSION])?;

        store.with_map(|map| -> io::Result<()> {
            w.write_all(&(map.len() as u64).to_le_bytes())?;
            for (key, entry) in map.iter() {
                write_bytes(&mut w, key.as_bytes())?;
                match entry.expires_at_ms {
                    Some(exp) => {
                        w.write_all(&[1u8])?;
                        w.write_all(&exp.to_le_bytes())?;
                    }
                    None => {
                        w.write_all(&[0u8])?;
                    }
                }
                write_value(&mut w, &entry.value)?;
            }
            Ok(())
        })?;
        w.flush()?;
    }
    // Atomic-ish: rename the finished temp file over the real path so a
    // crash mid-write never corrupts the last good snapshot.
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

pub fn load(path: &Path) -> io::Result<HashMap<String, Entry>> {
    let file = File::open(path)?;
    let mut r = BufReader::new(file);

    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a valid FRDB snapshot file",
        ));
    }
    let mut version = [0u8; 1];
    r.read_exact(&mut version)?;

    let mut count_buf = [0u8; 8];
    r.read_exact(&mut count_buf)?;
    let count = u64::from_le_bytes(count_buf);

    let mut map = HashMap::with_capacity(count as usize);
    for _ in 0..count {
        let key_bytes = read_bytes(&mut r)?;
        let key = String::from_utf8_lossy(&key_bytes).to_string();

        let mut has_expiry = [0u8; 1];
        r.read_exact(&mut has_expiry)?;
        let expires_at_ms = if has_expiry[0] == 1 {
            let mut exp_buf = [0u8; 8];
            r.read_exact(&mut exp_buf)?;
            Some(i64::from_le_bytes(exp_buf))
        } else {
            None
        };

        let value = read_value(&mut r)?;
        map.insert(
            key,
            Entry {
                value,
                expires_at_ms,
            },
        );
    }

    Ok(map)
}
