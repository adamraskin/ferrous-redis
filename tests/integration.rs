//! End-to-end tests: each test spawns the real compiled server as a
//! subprocess on a scratch port and talks to it over a raw TCP socket using
//! hand-encoded RESP, the same way a real client would. This exercises the
//! whole stack (parsing, dispatch, store) rather than internal functions.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

struct TestServer {
    child: Child,
    port: u16,
}

impl TestServer {
    fn start(port: u16, dir: &str) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_ferrous-redis"))
            .args(["--port", &port.to_string(), "--dir", dir, "--save-interval", "3600"])
            .spawn()
            .expect("failed to start server binary");

        // Poll until the port accepts connections instead of a fixed sleep.
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        TestServer { child, port }
    }

    fn connect(&self) -> TcpStream {
        let s = TcpStream::connect(("127.0.0.1", self.port)).expect("connect failed");
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        s
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn encode(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len());
    for p in parts {
        out += &format!("${}\r\n{}\r\n", p.len(), p);
    }
    out.into_bytes()
}

fn roundtrip(stream: &mut TcpStream, parts: &[&str]) -> String {
    stream.write_all(&encode(parts)).unwrap();
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

#[test]
fn ping_pong() {
    let server = TestServer::start(16379, "/tmp/frdb_it_ping");
    let mut c = server.connect();
    assert_eq!(roundtrip(&mut c, &["PING"]), "+PONG\r\n");
}

#[test]
fn set_get_del() {
    let server = TestServer::start(16380, "/tmp/frdb_it_setget");
    let mut c = server.connect();
    assert_eq!(roundtrip(&mut c, &["SET", "k", "v"]), "+OK\r\n");
    assert_eq!(roundtrip(&mut c, &["GET", "k"]), "$1\r\nv\r\n");
    assert_eq!(roundtrip(&mut c, &["DEL", "k"]), ":1\r\n");
    assert_eq!(roundtrip(&mut c, &["GET", "k"]), "$-1\r\n");
}

#[test]
fn incr_decr() {
    let server = TestServer::start(16381, "/tmp/frdb_it_incr");
    let mut c = server.connect();
    assert_eq!(roundtrip(&mut c, &["SET", "n", "5"]), "+OK\r\n");
    assert_eq!(roundtrip(&mut c, &["INCR", "n"]), ":6\r\n");
    assert_eq!(roundtrip(&mut c, &["DECRBY", "n", "4"]), ":2\r\n");
}

#[test]
fn list_ops() {
    let server = TestServer::start(16382, "/tmp/frdb_it_list");
    let mut c = server.connect();
    assert_eq!(roundtrip(&mut c, &["RPUSH", "l", "a", "b", "c"]), ":3\r\n");
    assert_eq!(
        roundtrip(&mut c, &["LRANGE", "l", "0", "-1"]),
        "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
    );
    assert_eq!(roundtrip(&mut c, &["LPOP", "l"]), "$1\r\na\r\n");
}

#[test]
fn wrong_type_error() {
    let server = TestServer::start(16383, "/tmp/frdb_it_wrongtype");
    let mut c = server.connect();
    roundtrip(&mut c, &["SET", "s", "x"]);
    let reply = roundtrip(&mut c, &["LPUSH", "s", "y"]);
    assert!(reply.starts_with("-WRONGTYPE"), "got: {}", reply);
}

#[test]
fn expiry() {
    let server = TestServer::start(16384, "/tmp/frdb_it_expiry");
    let mut c = server.connect();
    roundtrip(&mut c, &["SET", "temp", "x", "PX", "100"]);
    assert_eq!(roundtrip(&mut c, &["GET", "temp"]), "$1\r\nx\r\n");
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(roundtrip(&mut c, &["GET", "temp"]), "$-1\r\n");
}

#[test]
fn persistence_across_restart() {
    let dir = "/tmp/frdb_it_persist";
    {
        let server = TestServer::start(16385, dir);
        let mut c = server.connect();
        roundtrip(&mut c, &["SET", "durable", "yes"]);
        roundtrip(&mut c, &["SAVE"]);
    } // server dropped/killed here

    let server = TestServer::start(16385, dir);
    let mut c = server.connect();
    assert_eq!(roundtrip(&mut c, &["GET", "durable"]), "$3\r\nyes\r\n");
}
