# ferrous-redis

A Redis-compatible in-memory key/value store, built from scratch in Rust —
no external crates, just the standard library. It speaks real RESP, so
`redis-cli` and any Redis client library can talk to it directly.

```
$ ferrous-redis --port 6379
ferrous-redis listening on 0.0.0.0:6379

$ redis-cli -p 6379
127.0.0.1:6379> SET foo bar
OK
127.0.0.1:6379> GET foo
"bar"
127.0.0.1:6379> LPUSH mylist a b c
(integer) 3
```

## Why build this

Redis' external behavior (a text-ish wire protocol, a handful of data
types, key expiry) is simple to describe but touches most of what a
backend/infra engineer actually deals with day to day: concurrent network
I/O, protocol parsing, shared mutable state, and durability. Building a
clone from scratch is a compact way to get real experience with all four.

## Architecture

- **Thread-per-connection**, no async runtime. Every client connection
  gets its own OS thread (`std::thread::spawn`); there's no reactor or
  event loop to reason about. This is simpler to build and debug than an
  async design, at the cost of not scaling to tens of thousands of
  connections the way an event-loop server would — a reasonable trade-off
  for a learning project, and a conscious one worth being able to explain.
- **Shared state**: a single `Mutex<HashMap<String, Entry>>` (`store.rs`).
  Every command takes the lock, does its work, and releases it. Simple and
  correct; the obvious next step for scaling would be sharding the
  keyspace across multiple locks (e.g. hash the key into N buckets).
- **Protocol** (`resp.rs`): a hand-written RESP2 parser/encoder. Accepts
  both real RESP arrays (what `redis-cli` and client libraries send) and
  plain inline commands (so you can type commands directly over `nc`).
- **Persistence** (`persistence.rs`): a custom binary snapshot format
  (not compatible with real Redis' RDB, but the same idea). A background
  thread saves the dataset periodically if anything changed since the
  last snapshot; `SAVE`/`BGSAVE` trigger it on demand; the file is loaded
  back on startup if present.
- **Expiry**: lazy (checked on every access) plus a background sweep every
  second that proactively removes anything past its TTL, matching how
  real Redis does both lazy and active expiry.

## Supported commands

| Category | Commands |
|---|---|
| Connection | `PING`, `ECHO`, `QUIT`, `SELECT` |
| Server | `FLUSHALL`, `FLUSHDB`, `DBSIZE`, `KEYS`, `TYPE`, `SAVE`, `BGSAVE`, `LASTSAVE`, `INFO`, `CONFIG GET/SET` |
| Generic | `EXISTS`, `DEL`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `RENAME` |
| String | `SET` (`EX`/`PX`/`NX`/`XX`), `GET`, `GETSET`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| List | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET` |
| Hash | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HMGET` |
| Set | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |

`KEYS` supports `*` and `?` glob patterns (`KEYS user:*`).

## Running it

```
cargo run --release -- --port 6379 --dir ./data --save-interval 60
```

| Flag | Default | Meaning |
|---|---|---|
| `--port` | `6379` | TCP port to listen on |
| `--dir` | `.` | Directory for the snapshot file |
| `--dbfilename` | `dump.frdb` | Snapshot filename |
| `--save-interval` | `60` | Seconds between automatic snapshots (only if data changed) |

## Testing

```
cargo test
```

Unit tests cover the glob matcher; integration tests (`tests/integration.rs`)
spawn the actual compiled binary, connect over a real TCP socket, and send
hand-encoded RESP — including a test that kills the server, restarts it,
and confirms data survived via the snapshot file.

## Known limitations / what's intentionally left out

Being upfront about scope, since that's part of the point of a project
like this:

- No `AOF` (append-only file) durability, only periodic snapshots — a
  crash between snapshots loses recent writes.
- No replication, clustering, or `RDB`-format compatibility with real
  Redis.
- No `SCAN`/cursor-based iteration (`KEYS` does a full scan, fine at
  small scale, not what you'd want with millions of keys).
- No sorted sets, streams, or scripting (`EVAL`).
- The single global `Mutex` means all commands serialize behind one lock —
  fine for a learning project, a real scale-up would shard it.

## License

MIT — do whatever you want with it.
