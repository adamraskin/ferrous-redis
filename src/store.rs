//! The actual key/value store. Everything lives behind a single `Mutex`
//! guarding a `HashMap`, which is the simplest correct way to share state
//! across the thread-per-connection model this server uses. It is not the
//! most scalable design (real Redis is single-threaded and lock-free by
//! virtue of that; a sharded-lock design would scale further here) but it
//! is easy to reason about and plenty fast for a learning project.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub type Bytes = Vec<u8>;

#[derive(Debug, Clone)]
pub enum Value {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    Set(HashSet<Bytes>),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Value,
    /// Absolute expiry time as milliseconds since the Unix epoch. `None`
    /// means the key never expires.
    pub expires_at_ms: Option<i64>,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub struct Store {
    inner: Mutex<HashMap<String, Entry>>,
    /// Set to true whenever the dataset changes, so the background
    /// snapshotter knows there's something worth saving.
    pub dirty: Mutex<bool>,
}

impl Store {
    pub fn new() -> Self {
        Store {
            inner: Mutex::new(HashMap::new()),
            dirty: Mutex::new(false),
        }
    }

    fn mark_dirty(&self) {
        *self.dirty.lock().unwrap() = true;
    }

    /// Removes `key` if it has an expiry in the past. Returns true if the
    /// key was (or already had been) removed for that reason.
    fn expire_if_needed(map: &mut HashMap<String, Entry>, key: &str) -> bool {
        if let Some(entry) = map.get(key) {
            if let Some(exp) = entry.expires_at_ms {
                if exp <= now_ms() {
                    map.remove(key);
                    return true;
                }
            }
        }
        false
    }

    pub fn with_map<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut HashMap<String, Entry>) -> R,
    {
        let mut map = self.inner.lock().unwrap();
        f(&mut map)
    }

    pub fn get_entry(&self, key: &str) -> Option<Entry> {
        self.with_map(|map| {
            Store::expire_if_needed(map, key);
            map.get(key).cloned()
        })
    }

    pub fn set(&self, key: String, value: Value, expires_at_ms: Option<i64>) {
        self.with_map(|map| {
            map.insert(
                key,
                Entry {
                    value,
                    expires_at_ms,
                },
            );
        });
        self.mark_dirty();
    }

    pub fn del(&self, keys: &[String]) -> usize {
        let removed = self.with_map(|map| {
            let mut count = 0;
            for k in keys {
                Store::expire_if_needed(map, k);
                if map.remove(k).is_some() {
                    count += 1;
                }
            }
            count
        });
        if removed > 0 {
            self.mark_dirty();
        }
        removed
    }

    pub fn exists(&self, keys: &[String]) -> usize {
        self.with_map(|map| {
            keys.iter()
                .filter(|k| {
                    Store::expire_if_needed(map, k);
                    map.contains_key(k.as_str())
                })
                .count()
        })
    }

    pub fn expire(&self, key: &str, ttl_ms: i64) -> bool {
        let ok = self.with_map(|map| {
            Store::expire_if_needed(map, key);
            if let Some(entry) = map.get_mut(key) {
                entry.expires_at_ms = Some(now_ms() + ttl_ms);
                true
            } else {
                false
            }
        });
        if ok {
            self.mark_dirty();
        }
        ok
    }

    pub fn persist(&self, key: &str) -> bool {
        let ok = self.with_map(|map| {
            Store::expire_if_needed(map, key);
            if let Some(entry) = map.get_mut(key) {
                if entry.expires_at_ms.is_some() {
                    entry.expires_at_ms = None;
                    return true;
                }
            }
            false
        });
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// Returns remaining TTL in milliseconds: `Some(ms)` if the key has an
    /// expiry, `None` if the key exists but never expires, or `-2` (via the
    /// caller) if the key doesn't exist at all -- that distinction is left
    /// to the command layer, matching real Redis semantics.
    pub fn ttl_ms(&self, key: &str) -> Option<Option<i64>> {
        self.with_map(|map| {
            Store::expire_if_needed(map, key);
            map.get(key).map(|entry| {
                entry
                    .expires_at_ms
                    .map(|exp| (exp - now_ms()).max(0))
            })
        })
    }

    pub fn keys_matching(&self, pattern: &str) -> Vec<String> {
        self.with_map(|map| {
            let expired: Vec<String> = map
                .iter()
                .filter(|(_, e)| e.expires_at_ms.map_or(false, |exp| exp <= now_ms()))
                .map(|(k, _)| k.clone())
                .collect();
            for k in &expired {
                map.remove(k);
            }
            map.keys()
                .filter(|k| crate::glob::glob_match(pattern, k))
                .cloned()
                .collect()
        })
    }

    pub fn dbsize(&self) -> usize {
        self.with_map(|map| {
            let expired: Vec<String> = map
                .iter()
                .filter(|(_, e)| e.expires_at_ms.map_or(false, |exp| exp <= now_ms()))
                .map(|(k, _)| k.clone())
                .collect();
            for k in &expired {
                map.remove(k);
            }
            map.len()
        })
    }

    pub fn flush_all(&self) {
        self.with_map(|map| map.clear());
        self.mark_dirty();
    }

    pub fn rename(&self, src: &str, dst: &str) -> bool {
        let ok = self.with_map(|map| {
            Store::expire_if_needed(map, src);
            if let Some(entry) = map.remove(src) {
                map.insert(dst.to_string(), entry);
                true
            } else {
                false
            }
        });
        if ok {
            self.mark_dirty();
        }
        ok
    }

    /// Sweeps the whole map removing anything past its expiry. Called
    /// periodically by the background expiry thread ("active expiry"),
    /// mirroring what real Redis does in addition to lazy/on-access expiry.
    pub fn sweep_expired(&self) -> usize {
        let removed = self.with_map(|map| {
            let expired: Vec<String> = map
                .iter()
                .filter(|(_, e)| e.expires_at_ms.map_or(false, |exp| exp <= now_ms()))
                .map(|(k, _)| k.clone())
                .collect();
            for k in &expired {
                map.remove(k);
            }
            expired.len()
        });
        if removed > 0 {
            self.mark_dirty();
        }
        removed
    }

    /// Mutates the value at `key` via `f`, inserting a fresh `default` first
    /// if the key is absent (or expired). Used by all the "modify or
    /// create" list/hash/set commands so they share one locking path.
    pub fn mutate_or_insert<F, R>(&self, key: &str, default: impl FnOnce() -> Value, f: F) -> R
    where
        F: FnOnce(&mut Value) -> R,
    {
        let result = self.with_map(|map| {
            Store::expire_if_needed(map, key);
            let entry = map.entry(key.to_string()).or_insert_with(|| Entry {
                value: default(),
                expires_at_ms: None,
            });
            f(&mut entry.value)
        });
        self.mark_dirty();
        result
    }
}
