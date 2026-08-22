use crate::resp::RespValue;
use crate::store::{now_ms, Store, Value};
use crate::ServerContext;
use std::collections::HashSet;

/// Signal sent back from the dispatcher so the connection loop knows to
/// close the socket after replying (used by QUIT).
pub enum Outcome {
    Reply(RespValue),
    ReplyAndClose(RespValue),
}

fn bytes_to_string_lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_string()
}

fn parse_i64(b: &[u8], what: &str) -> Result<i64, RespValue> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| RespValue::error(format!("ERR value is not {}", what)))
}

fn wrong_args(cmd: &str) -> RespValue {
    RespValue::error(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd.to_lowercase()
    ))
}

fn wrong_type() -> RespValue {
    RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Dispatches one already-parsed command (`args[0]` is the command name).
pub fn dispatch(store: &Store, ctx: &ServerContext, args: &[Vec<u8>]) -> Outcome {
    if args.is_empty() {
        return Outcome::Reply(RespValue::error("ERR empty command"));
    }
    let cmd = bytes_to_string_lossy(&args[0]).to_uppercase();
    let a = &args[1..];

    let reply = match cmd.as_str() {
        "PING" => {
            if a.is_empty() {
                RespValue::SimpleString("PONG".to_string())
            } else {
                RespValue::bulk(a[0].clone())
            }
        }
        "ECHO" => {
            if a.len() != 1 {
                wrong_args("echo")
            } else {
                RespValue::bulk(a[0].clone())
            }
        }
        "QUIT" => return Outcome::ReplyAndClose(RespValue::ok()),
        "SELECT" => RespValue::ok(), // single logical database, always accept
        "FLUSHALL" | "FLUSHDB" => {
            store.flush_all();
            RespValue::ok()
        }
        "DBSIZE" => RespValue::Integer(store.dbsize() as i64),
        "KEYS" => {
            if a.len() != 1 {
                wrong_args("keys")
            } else {
                let pattern = bytes_to_string_lossy(&a[0]);
                let keys = store.keys_matching(&pattern);
                RespValue::Array(Some(
                    keys.into_iter().map(RespValue::bulk).collect(),
                ))
            }
        }
        "TYPE" => {
            if a.len() != 1 {
                wrong_args("type")
            } else {
                let key = bytes_to_string_lossy(&a[0]);
                match store.get_entry(&key) {
                    Some(e) => RespValue::SimpleString(e.value.type_name().to_string()),
                    None => RespValue::SimpleString("none".to_string()),
                }
            }
        }
        "EXISTS" => {
            if a.is_empty() {
                wrong_args("exists")
            } else {
                let keys: Vec<String> = a.iter().map(|b| bytes_to_string_lossy(b)).collect();
                RespValue::Integer(store.exists(&keys) as i64)
            }
        }
        "DEL" => {
            if a.is_empty() {
                wrong_args("del")
            } else {
                let keys: Vec<String> = a.iter().map(|b| bytes_to_string_lossy(b)).collect();
                RespValue::Integer(store.del(&keys) as i64)
            }
        }
        "RENAME" => {
            if a.len() != 2 {
                wrong_args("rename")
            } else {
                let src = bytes_to_string_lossy(&a[0]);
                let dst = bytes_to_string_lossy(&a[1]);
                if store.rename(&src, &dst) {
                    RespValue::ok()
                } else {
                    RespValue::error("ERR no such key")
                }
            }
        }
        "EXPIRE" => cmd_expire(store, a, 1000),
        "PEXPIRE" => cmd_expire(store, a, 1),
        "TTL" => cmd_ttl(store, a, 1000),
        "PTTL" => cmd_ttl(store, a, 1),
        "PERSIST" => {
            if a.len() != 1 {
                wrong_args("persist")
            } else {
                let key = bytes_to_string_lossy(&a[0]);
                RespValue::Integer(if store.persist(&key) { 1 } else { 0 })
            }
        }
        "SET" => cmd_set(store, a),
        "GET" => cmd_get(store, a),
        "GETSET" => cmd_getset(store, a),
        "APPEND" => cmd_append(store, a),
        "STRLEN" => cmd_strlen(store, a),
        "INCR" => cmd_incrby(store, a, 1),
        "DECR" => cmd_incrby(store, a, -1),
        "INCRBY" => match a {
            [k, n] => match parse_i64(n, "an integer") {
                Ok(n) => cmd_incrby(store, &[k.clone()], n),
                Err(e) => e,
            },
            _ => wrong_args("incrby"),
        },
        "DECRBY" => match a {
            [k, n] => match parse_i64(n, "an integer") {
                Ok(n) => cmd_incrby(store, &[k.clone()], -n),
                Err(e) => e,
            },
            _ => wrong_args("decrby"),
        },
        "LPUSH" => cmd_push(store, a, true),
        "RPUSH" => cmd_push(store, a, false),
        "LPOP" => cmd_pop(store, a, true),
        "RPOP" => cmd_pop(store, a, false),
        "LLEN" => cmd_llen(store, a),
        "LRANGE" => cmd_lrange(store, a),
        "LINDEX" => cmd_lindex(store, a),
        "LSET" => cmd_lset(store, a),
        "HSET" => cmd_hset(store, a),
        "HGET" => cmd_hget(store, a),
        "HDEL" => cmd_hdel(store, a),
        "HGETALL" => cmd_hgetall(store, a),
        "HEXISTS" => cmd_hexists(store, a),
        "HLEN" => cmd_hlen(store, a),
        "HKEYS" => cmd_hkeys_hvals(store, a, true),
        "HVALS" => cmd_hkeys_hvals(store, a, false),
        "HMGET" => cmd_hmget(store, a),
        "SADD" => cmd_sadd(store, a),
        "SREM" => cmd_srem(store, a),
        "SMEMBERS" => cmd_smembers(store, a),
        "SISMEMBER" => cmd_sismember(store, a),
        "SCARD" => cmd_scard(store, a),
        "SAVE" => match crate::persistence::save(store, &ctx.rdb_path) {
            Ok(_) => {
                *ctx.last_save_ms.lock().unwrap() = now_ms();
                RespValue::ok()
            }
            Err(e) => RespValue::error(format!("ERR save failed: {}", e)),
        },
        "BGSAVE" => match crate::persistence::save(store, &ctx.rdb_path) {
            Ok(_) => {
                *ctx.last_save_ms.lock().unwrap() = now_ms();
                RespValue::SimpleString("Background saving started".to_string())
            }
            Err(e) => RespValue::error(format!("ERR save failed: {}", e)),
        },
        "LASTSAVE" => RespValue::Integer(*ctx.last_save_ms.lock().unwrap() / 1000),
        "DBFILENAME" | "CONFIG" => {
            // Enough of CONFIG GET/SET to keep real clients (and redis-cli's
            // startup probes) happy without implementing the full surface.
            if cmd == "CONFIG" && a.len() >= 2 && bytes_to_string_lossy(&a[0]).eq_ignore_ascii_case("get") {
                RespValue::Array(Some(vec![
                    RespValue::bulk(a[1].clone()),
                    RespValue::bulk(""),
                ]))
            } else {
                RespValue::ok()
            }
        }
        "COMMAND" => RespValue::Array(Some(Vec::new())),
        "INFO" => RespValue::bulk(format!(
            "# Server\r\nferrous_redis_version:0.1.0\r\n# Keyspace\r\ndb0:keys={}\r\n",
            store.dbsize()
        )),
        other => RespValue::error(format!(
            "ERR unknown command '{}'",
            other.to_lowercase()
        )),
    };

    Outcome::Reply(reply)
}

fn cmd_expire(store: &Store, a: &[Vec<u8>], unit_ms: i64) -> RespValue {
    match a {
        [k, secs] => {
            let key = bytes_to_string_lossy(k);
            match parse_i64(secs, "an integer") {
                Ok(n) => RespValue::Integer(if store.expire(&key, n * unit_ms) { 1 } else { 0 }),
                Err(e) => e,
            }
        }
        _ => wrong_args("expire"),
    }
}

fn cmd_ttl(store: &Store, a: &[Vec<u8>], unit_ms: i64) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match store.ttl_ms(&key) {
                None => RespValue::Integer(-2), // key doesn't exist
                Some(None) => RespValue::Integer(-1), // no expiry set
                Some(Some(ms)) => RespValue::Integer(ms / unit_ms),
            }
        }
        _ => wrong_args("ttl"),
    }
}

fn cmd_set(store: &Store, a: &[Vec<u8>]) -> RespValue {
    if a.len() < 2 {
        return wrong_args("set");
    }
    let key = bytes_to_string_lossy(&a[0]);
    let value = a[1].clone();

    let mut expires_at_ms: Option<i64> = None;
    let mut nx = false;
    let mut xx = false;

    let mut i = 2;
    while i < a.len() {
        let opt = bytes_to_string_lossy(&a[i]).to_uppercase();
        match opt.as_str() {
            "EX" => {
                if i + 1 >= a.len() {
                    return RespValue::error("ERR syntax error");
                }
                match parse_i64(&a[i + 1], "an integer") {
                    Ok(secs) => expires_at_ms = Some(now_ms() + secs * 1000),
                    Err(e) => return e,
                }
                i += 2;
            }
            "PX" => {
                if i + 1 >= a.len() {
                    return RespValue::error("ERR syntax error");
                }
                match parse_i64(&a[i + 1], "an integer") {
                    Ok(ms) => expires_at_ms = Some(now_ms() + ms),
                    Err(e) => return e,
                }
                i += 2;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            _ => return RespValue::error("ERR syntax error"),
        }
    }

    let exists = store.get_entry(&key).is_some();
    if nx && exists {
        return RespValue::nil();
    }
    if xx && !exists {
        return RespValue::nil();
    }

    store.set(key, Value::Str(value), expires_at_ms);
    RespValue::ok()
}

fn cmd_get(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match store.get_entry(&key) {
                Some(e) => match e.value {
                    Value::Str(s) => RespValue::bulk(s),
                    _ => wrong_type(),
                },
                None => RespValue::nil(),
            }
        }
        _ => wrong_args("get"),
    }
}

fn cmd_getset(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, v] => {
            let key = bytes_to_string_lossy(k);
            let old = match store.get_entry(&key) {
                Some(e) => match e.value {
                    Value::Str(s) => RespValue::bulk(s),
                    _ => return wrong_type(),
                },
                None => RespValue::nil(),
            };
            store.set(key, Value::Str(v.clone()), None);
            old
        }
        _ => wrong_args("getset"),
    }
}

fn cmd_append(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, v] => {
            let key = bytes_to_string_lossy(k);
            let mut wrong = false;
            let len = store.mutate_or_insert(
                &key,
                || Value::Str(Vec::new()),
                |val| match val {
                    Value::Str(s) => {
                        s.extend_from_slice(v);
                        s.len()
                    }
                    _ => {
                        wrong = true;
                        0
                    }
                },
            );
            if wrong {
                wrong_type()
            } else {
                RespValue::Integer(len as i64)
            }
        }
        _ => wrong_args("append"),
    }
}

fn cmd_strlen(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match store.get_entry(&key) {
                Some(e) => match e.value {
                    Value::Str(s) => RespValue::Integer(s.len() as i64),
                    _ => wrong_type(),
                },
                None => RespValue::Integer(0),
            }
        }
        _ => wrong_args("strlen"),
    }
}

fn cmd_incrby(store: &Store, a: &[Vec<u8>], delta: i64) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            let mut error: Option<RespValue> = None;
            let result = store.mutate_or_insert(
                &key,
                || Value::Str(b"0".to_vec()),
                |val| match val {
                    Value::Str(s) => {
                        let current = match std::str::from_utf8(s).ok().and_then(|s| s.parse::<i64>().ok()) {
                            Some(n) => n,
                            None => {
                                error = Some(RespValue::error(
                                    "ERR value is not an integer or out of range",
                                ));
                                return 0;
                            }
                        };
                        match current.checked_add(delta) {
                            Some(next) => {
                                *s = next.to_string().into_bytes();
                                next
                            }
                            None => {
                                error = Some(RespValue::error("ERR increment or decrement would overflow"));
                                0
                            }
                        }
                    }
                    _ => {
                        error = Some(wrong_type());
                        0
                    }
                },
            );
            error.unwrap_or(RespValue::Integer(result))
        }
        _ => wrong_args("incr/decr"),
    }
}

fn with_list<F, R>(store: &Store, key: &str, f: F) -> Result<Option<R>, RespValue>
where
    F: FnOnce(&mut std::collections::VecDeque<Vec<u8>>) -> R,
{
    match store.get_entry(key) {
        Some(e) => match e.value {
            Value::List(_) => {
                let mut out = None;
                store.mutate_or_insert(
                    key,
                    || unreachable!(),
                    |val| {
                        if let Value::List(list) = val {
                            out = Some(f(list));
                        }
                    },
                );
                Ok(out)
            }
            _ => Err(wrong_type()),
        },
        None => Ok(None),
    }
}

fn cmd_push(store: &Store, a: &[Vec<u8>], left: bool) -> RespValue {
    if a.len() < 2 {
        return wrong_args(if left { "lpush" } else { "rpush" });
    }
    let key = bytes_to_string_lossy(&a[0]);
    let values = &a[1..];

    if let Some(e) = store.get_entry(&key) {
        if !matches!(e.value, Value::List(_)) {
            return wrong_type();
        }
    }

    let len = store.mutate_or_insert(
        &key,
        || Value::List(std::collections::VecDeque::new()),
        |val| {
            if let Value::List(list) = val {
                for v in values {
                    if left {
                        list.push_front(v.clone());
                    } else {
                        list.push_back(v.clone());
                    }
                }
                list.len()
            } else {
                0
            }
        },
    );
    RespValue::Integer(len as i64)
}

fn cmd_pop(store: &Store, a: &[Vec<u8>], left: bool) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match with_list(store, &key, |list| {
                if left {
                    list.pop_front()
                } else {
                    list.pop_back()
                }
            }) {
                Ok(Some(Some(v))) => RespValue::bulk(v),
                Ok(Some(None)) | Ok(None) => RespValue::nil(),
                Err(e) => e,
            }
        }
        _ => wrong_args(if left { "lpop" } else { "rpop" }),
    }
}

fn cmd_llen(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match with_list(store, &key, |list| list.len()) {
                Ok(Some(n)) => RespValue::Integer(n as i64),
                Ok(None) => RespValue::Integer(0),
                Err(e) => e,
            }
        }
        _ => wrong_args("llen"),
    }
}

/// Normalizes Redis-style (possibly negative) start/stop indices into a
/// half-open `[start, stop)` range clamped to `[0, len]`.
fn normalize_range(start: i64, stop: i64, len: i64) -> (usize, usize) {
    let norm = |i: i64| -> i64 {
        if i < 0 {
            (len + i).max(0)
        } else {
            i
        }
    };
    let s = norm(start).min(len);
    let e = (norm(stop) + 1).clamp(0, len);
    if s >= e {
        (0, 0)
    } else {
        (s as usize, e as usize)
    }
}

fn cmd_lrange(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, start, stop] => {
            let key = bytes_to_string_lossy(k);
            let (start, stop) = match (parse_i64(start, "an integer"), parse_i64(stop, "an integer")) {
                (Ok(a), Ok(b)) => (a, b),
                (Err(e), _) | (_, Err(e)) => return e,
            };
            match with_list(store, &key, |list| {
                let (s, e) = normalize_range(start, stop, list.len() as i64);
                list.iter().skip(s).take(e - s).cloned().collect::<Vec<_>>()
            }) {
                Ok(Some(items)) => RespValue::Array(Some(items.into_iter().map(RespValue::bulk).collect())),
                Ok(None) => RespValue::Array(Some(Vec::new())),
                Err(e) => e,
            }
        }
        _ => wrong_args("lrange"),
    }
}

fn cmd_lindex(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, idx] => {
            let key = bytes_to_string_lossy(k);
            let idx = match parse_i64(idx, "an integer") {
                Ok(n) => n,
                Err(e) => return e,
            };
            match with_list(store, &key, |list| {
                let len = list.len() as i64;
                let real = if idx < 0 { len + idx } else { idx };
                if real < 0 || real >= len {
                    None
                } else {
                    list.get(real as usize).cloned()
                }
            }) {
                Ok(Some(Some(v))) => RespValue::bulk(v),
                Ok(Some(None)) | Ok(None) => RespValue::nil(),
                Err(e) => e,
            }
        }
        _ => wrong_args("lindex"),
    }
}

fn cmd_lset(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, idx, v] => {
            let key = bytes_to_string_lossy(k);
            let idx = match parse_i64(idx, "an integer") {
                Ok(n) => n,
                Err(e) => return e,
            };
            match with_list(store, &key, |list| {
                let len = list.len() as i64;
                let real = if idx < 0 { len + idx } else { idx };
                if real < 0 || real >= len {
                    false
                } else {
                    list[real as usize] = v.clone();
                    true
                }
            }) {
                Ok(Some(true)) => RespValue::ok(),
                Ok(Some(false)) | Ok(None) => RespValue::error("ERR index out of range"),
                Err(e) => e,
            }
        }
        _ => wrong_args("lset"),
    }
}

fn with_hash<F, R>(store: &Store, key: &str, f: F) -> Result<Option<R>, RespValue>
where
    F: FnOnce(&mut std::collections::HashMap<Vec<u8>, Vec<u8>>) -> R,
{
    match store.get_entry(key) {
        Some(e) => match e.value {
            Value::Hash(_) => {
                let mut out = None;
                store.mutate_or_insert(key, || unreachable!(), |val| {
                    if let Value::Hash(h) = val {
                        out = Some(f(h));
                    }
                });
                Ok(out)
            }
            _ => Err(wrong_type()),
        },
        None => Ok(None),
    }
}

fn cmd_hset(store: &Store, a: &[Vec<u8>]) -> RespValue {
    if a.len() < 3 || (a.len() - 1) % 2 != 0 {
        return wrong_args("hset");
    }
    let key = bytes_to_string_lossy(&a[0]);
    if let Some(e) = store.get_entry(&key) {
        if !matches!(e.value, Value::Hash(_)) {
            return wrong_type();
        }
    }
    let pairs = &a[1..];
    let added = store.mutate_or_insert(
        &key,
        || Value::Hash(std::collections::HashMap::new()),
        |val| {
            let mut added = 0;
            if let Value::Hash(h) = val {
                for chunk in pairs.chunks(2) {
                    if h.insert(chunk[0].clone(), chunk[1].clone()).is_none() {
                        added += 1;
                    }
                }
            }
            added
        },
    );
    RespValue::Integer(added)
}

fn cmd_hget(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, f] => {
            let key = bytes_to_string_lossy(k);
            match with_hash(store, &key, |h| h.get(f).cloned()) {
                Ok(Some(Some(v))) => RespValue::bulk(v),
                Ok(Some(None)) | Ok(None) => RespValue::nil(),
                Err(e) => e,
            }
        }
        _ => wrong_args("hget"),
    }
}

fn cmd_hdel(store: &Store, a: &[Vec<u8>]) -> RespValue {
    if a.len() < 2 {
        return wrong_args("hdel");
    }
    let key = bytes_to_string_lossy(&a[0]);
    let fields = &a[1..];
    match with_hash(store, &key, |h| {
        fields.iter().filter(|f| h.remove(f.as_slice()).is_some()).count()
    }) {
        Ok(Some(n)) => RespValue::Integer(n as i64),
        Ok(None) => RespValue::Integer(0),
        Err(e) => e,
    }
}

fn cmd_hgetall(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match with_hash(store, &key, |h| {
                let mut out = Vec::with_capacity(h.len() * 2);
                for (k, v) in h.iter() {
                    out.push(RespValue::bulk(k.clone()));
                    out.push(RespValue::bulk(v.clone()));
                }
                out
            }) {
                Ok(Some(items)) => RespValue::Array(Some(items)),
                Ok(None) => RespValue::Array(Some(Vec::new())),
                Err(e) => e,
            }
        }
        _ => wrong_args("hgetall"),
    }
}

fn cmd_hexists(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, f] => {
            let key = bytes_to_string_lossy(k);
            match with_hash(store, &key, |h| h.contains_key(f.as_slice())) {
                Ok(Some(true)) => RespValue::Integer(1),
                _ => RespValue::Integer(0),
            }
        }
        _ => wrong_args("hexists"),
    }
}

fn cmd_hlen(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match with_hash(store, &key, |h| h.len()) {
                Ok(Some(n)) => RespValue::Integer(n as i64),
                Ok(None) => RespValue::Integer(0),
                Err(e) => e,
            }
        }
        _ => wrong_args("hlen"),
    }
}

fn cmd_hkeys_hvals(store: &Store, a: &[Vec<u8>], keys: bool) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match with_hash(store, &key, |h| {
                if keys {
                    h.keys().cloned().collect::<Vec<_>>()
                } else {
                    h.values().cloned().collect::<Vec<_>>()
                }
            }) {
                Ok(Some(items)) => RespValue::Array(Some(items.into_iter().map(RespValue::bulk).collect())),
                Ok(None) => RespValue::Array(Some(Vec::new())),
                Err(e) => e,
            }
        }
        _ => wrong_args(if keys { "hkeys" } else { "hvals" }),
    }
}

fn cmd_hmget(store: &Store, a: &[Vec<u8>]) -> RespValue {
    if a.len() < 2 {
        return wrong_args("hmget");
    }
    let key = bytes_to_string_lossy(&a[0]);
    let fields = &a[1..];
    match with_hash(store, &key, |h| {
        fields
            .iter()
            .map(|f| h.get(f.as_slice()).cloned())
            .collect::<Vec<_>>()
    }) {
        Ok(Some(values)) => RespValue::Array(Some(
            values
                .into_iter()
                .map(|v| v.map(RespValue::bulk).unwrap_or(RespValue::nil()))
                .collect(),
        )),
        Ok(None) => RespValue::Array(Some(fields.iter().map(|_| RespValue::nil()).collect())),
        Err(e) => e,
    }
}

fn with_set<F, R>(store: &Store, key: &str, f: F) -> Result<Option<R>, RespValue>
where
    F: FnOnce(&mut HashSet<Vec<u8>>) -> R,
{
    match store.get_entry(key) {
        Some(e) => match e.value {
            Value::Set(_) => {
                let mut out = None;
                store.mutate_or_insert(key, || unreachable!(), |val| {
                    if let Value::Set(s) = val {
                        out = Some(f(s));
                    }
                });
                Ok(out)
            }
            _ => Err(wrong_type()),
        },
        None => Ok(None),
    }
}

fn cmd_sadd(store: &Store, a: &[Vec<u8>]) -> RespValue {
    if a.len() < 2 {
        return wrong_args("sadd");
    }
    let key = bytes_to_string_lossy(&a[0]);
    if let Some(e) = store.get_entry(&key) {
        if !matches!(e.value, Value::Set(_)) {
            return wrong_type();
        }
    }
    let members = &a[1..];
    let added = store.mutate_or_insert(
        &key,
        || Value::Set(HashSet::new()),
        |val| {
            let mut added = 0;
            if let Value::Set(s) = val {
                for m in members {
                    if s.insert(m.clone()) {
                        added += 1;
                    }
                }
            }
            added
        },
    );
    RespValue::Integer(added)
}

fn cmd_srem(store: &Store, a: &[Vec<u8>]) -> RespValue {
    if a.len() < 2 {
        return wrong_args("srem");
    }
    let key = bytes_to_string_lossy(&a[0]);
    let members = &a[1..];
    match with_set(store, &key, |s| {
        members.iter().filter(|m| s.remove(m.as_slice())).count()
    }) {
        Ok(Some(n)) => RespValue::Integer(n as i64),
        Ok(None) => RespValue::Integer(0),
        Err(e) => e,
    }
}

fn cmd_smembers(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match with_set(store, &key, |s| s.iter().cloned().collect::<Vec<_>>()) {
                Ok(Some(items)) => RespValue::Array(Some(items.into_iter().map(RespValue::bulk).collect())),
                Ok(None) => RespValue::Array(Some(Vec::new())),
                Err(e) => e,
            }
        }
        _ => wrong_args("smembers"),
    }
}

fn cmd_sismember(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k, m] => {
            let key = bytes_to_string_lossy(k);
            match with_set(store, &key, |s| s.contains(m.as_slice())) {
                Ok(Some(true)) => RespValue::Integer(1),
                _ => RespValue::Integer(0),
            }
        }
        _ => wrong_args("sismember"),
    }
}

fn cmd_scard(store: &Store, a: &[Vec<u8>]) -> RespValue {
    match a {
        [k] => {
            let key = bytes_to_string_lossy(k);
            match with_set(store, &key, |s| s.len()) {
                Ok(Some(n)) => RespValue::Integer(n as i64),
                Ok(None) => RespValue::Integer(0),
                Err(e) => e,
            }
        }
        _ => wrong_args("scard"),
    }
}
