//! ferrous-redis: a small, from-scratch, Redis-compatible server.
//!
//! Architecture, in one paragraph: a `TcpListener` accepts connections, and
//! each connection gets its own OS thread (thread-per-connection, no
//! async runtime). All threads share one `Store`, which is a `HashMap`
//! behind a `Mutex` -- see `store.rs` for why. A background thread sweeps
//! expired keys every second, and another periodically snapshots the
//! dataset to disk (`persistence.rs`) so it survives a restart.

mod commands;
mod glob;
mod persistence;
mod resp;
mod store;

use std::io::{BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use store::Store;

/// Shared, mostly-static configuration and bookkeeping that every
/// connection thread needs read access to.
pub struct ServerContext {
    pub rdb_path: PathBuf,
    pub last_save_ms: Mutex<i64>,
}

struct Config {
    port: u16,
    rdb_path: PathBuf,
    snapshot_interval_secs: u64,
}

fn parse_args() -> Config {
    let mut port = 6379u16;
    let mut dir = PathBuf::from(".");
    let mut dbfilename = String::from("dump.frdb");
    let mut snapshot_interval_secs = 60u64;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" if i + 1 < args.len() => {
                port = args[i + 1].parse().unwrap_or(port);
                i += 2;
            }
            "--dir" if i + 1 < args.len() => {
                dir = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            "--dbfilename" if i + 1 < args.len() => {
                dbfilename = args[i + 1].clone();
                i += 2;
            }
            "--save-interval" if i + 1 < args.len() => {
                snapshot_interval_secs = args[i + 1].parse().unwrap_or(snapshot_interval_secs);
                i += 2;
            }
            other => {
                eprintln!("warning: ignoring unrecognized argument '{}'", other);
                i += 1;
            }
        }
    }

    Config {
        port,
        rdb_path: dir.join(dbfilename),
        snapshot_interval_secs,
    }
}

fn handle_connection(stream: TcpStream, store: Arc<Store>, ctx: Arc<ServerContext>) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let mut writer = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to clone stream for {}: {}", peer, e);
            return;
        }
    };
    let mut reader = BufReader::new(stream);

    loop {
        let args = match resp::parse_command(&mut reader) {
            Ok(args) => args,
            Err(resp::ParseError::ConnectionClosed) => break,
            Err(resp::ParseError::Io(e)) => {
                eprintln!("connection {} read error: {}", peer, e);
                break;
            }
            Err(resp::ParseError::Protocol(msg)) => {
                let mut out = Vec::new();
                resp::RespValue::error(format!("ERR Protocol error: {}", msg)).encode(&mut out);
                let _ = writer.write_all(&out);
                break;
            }
        };

        if args.is_empty() {
            continue;
        }

        let outcome = commands::dispatch(&store, &ctx, &args);
        let (reply, should_close) = match outcome {
            commands::Outcome::Reply(r) => (r, false),
            commands::Outcome::ReplyAndClose(r) => (r, true),
        };

        let mut out = Vec::new();
        reply.encode(&mut out);
        if writer.write_all(&out).is_err() {
            break;
        }

        if should_close {
            break;
        }
    }
}

fn spawn_expiry_sweeper(store: Arc<Store>, running: Arc<AtomicBool>) {
    thread::spawn(move || {
        while running.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(1));
            store.sweep_expired();
        }
    });
}

fn spawn_snapshotter(store: Arc<Store>, ctx: Arc<ServerContext>, interval: Duration, running: Arc<AtomicBool>) {
    thread::spawn(move || {
        while running.load(Ordering::Relaxed) {
            thread::sleep(interval);
            let dirty = {
                let mut d = store.dirty.lock().unwrap();
                let was = *d;
                *d = false;
                was
            };
            if dirty {
                match persistence::save(&store, &ctx.rdb_path) {
                    Ok(_) => {
                        *ctx.last_save_ms.lock().unwrap() = store::now_ms();
                        println!("[snapshot] saved to {}", ctx.rdb_path.display());
                    }
                    Err(e) => eprintln!("[snapshot] failed: {}", e),
                }
            }
        }
    });
}

fn main() {
    let config = parse_args();
    let store = Arc::new(Store::new());

    if config.rdb_path.exists() {
        match persistence::load(&config.rdb_path) {
            Ok(map) => {
                let count = map.len();
                store.with_map(|m| *m = map);
                println!(
                    "Loaded {} keys from {}",
                    count,
                    config.rdb_path.display()
                );
            }
            Err(e) => {
                eprintln!(
                    "warning: could not load snapshot {}: {}",
                    config.rdb_path.display(),
                    e
                );
            }
        }
    }

    let ctx = Arc::new(ServerContext {
        rdb_path: config.rdb_path.clone(),
        last_save_ms: Mutex::new(store::now_ms()),
    });

    let running = Arc::new(AtomicBool::new(true));
    spawn_expiry_sweeper(store.clone(), running.clone());
    spawn_snapshotter(
        store.clone(),
        ctx.clone(),
        Duration::from_secs(config.snapshot_interval_secs),
        running.clone(),
    );

    let addr = format!("0.0.0.0:{}", config.port);
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind {}: {}", addr, e);
            std::process::exit(1);
        }
    };
    println!("ferrous-redis listening on {}", addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = store.clone();
                let ctx = ctx.clone();
                thread::spawn(move || handle_connection(stream, store, ctx));
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
}
