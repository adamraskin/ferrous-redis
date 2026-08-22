//! Minimal implementation of the Redis Serialization Protocol (RESP2).
//!
//! Real clients (redis-cli, redis-py, etc.) send commands as RESP arrays of
//! bulk strings, e.g. `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n`. We also accept
//! "inline" commands (a plain line of whitespace-separated words) so the
//! server can be poked with `nc` or `telnet` for quick manual testing.

use std::io::{self, BufRead, Read};

/// A value the server sends back to the client.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    /// `None` represents a RESP "null bulk string" ($-1\r\n).
    BulkString(Option<Vec<u8>>),
    /// `None` represents a RESP "null array" (*-1\r\n).
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    pub fn nil() -> Self {
        RespValue::BulkString(None)
    }

    pub fn bulk(s: impl Into<Vec<u8>>) -> Self {
        RespValue::BulkString(Some(s.into()))
    }

    pub fn error(msg: impl Into<String>) -> Self {
        RespValue::Error(msg.into())
    }

    /// Serialize this value into RESP wire format, appending to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            RespValue::SimpleString(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Error(e) => {
                out.push(b'-');
                out.extend_from_slice(e.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                out.push(b':');
                out.extend_from_slice(i.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                out.extend_from_slice(b"$-1\r\n");
            }
            RespValue::BulkString(Some(bytes)) => {
                out.push(b'$');
                out.extend_from_slice(bytes.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(bytes);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Array(None) => {
                out.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Array(Some(items)) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode(out);
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum ParseError {
    Io(io::Error),
    Protocol(String),
    /// The client closed the connection cleanly before sending a full command.
    ConnectionClosed,
}

impl From<io::Error> for ParseError {
    fn from(e: io::Error) -> Self {
        ParseError::Io(e)
    }
}

/// Reads a single line ending in `\r\n` (or `\n`) and strips the terminator.
fn read_line<R: BufRead>(reader: &mut R) -> Result<String, ParseError> {
    let mut line = String::new();
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err(ParseError::ConnectionClosed);
    }
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    Ok(line)
}

fn read_exact_bytes<R: Read>(reader: &mut R, n: usize) -> Result<Vec<u8>, ParseError> {
    let mut buf = vec![0u8; n];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Consumes the trailing `\r\n` after a bulk string payload.
fn consume_crlf<R: BufRead>(reader: &mut R) -> Result<(), ParseError> {
    let mut crlf = [0u8; 2];
    reader.read_exact(&mut crlf)?;
    Ok(())
}

/// Parses one client request into a command: a vector of argument byte
/// strings (`SET foo bar` -> `["SET", "foo", "bar"]`, as raw bytes so binary
/// values round-trip correctly).
pub fn parse_command<R: BufRead>(reader: &mut R) -> Result<Vec<Vec<u8>>, ParseError> {
    // Peek the first byte to decide whether this is a RESP array or an
    // inline command.
    let first_byte = {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Err(ParseError::ConnectionClosed);
        }
        buf[0]
    };

    if first_byte == b'*' {
        parse_resp_array(reader)
    } else {
        parse_inline(reader)
    }
}

fn parse_resp_array<R: BufRead>(reader: &mut R) -> Result<Vec<Vec<u8>>, ParseError> {
    let header = read_line(reader)?;
    let count: i64 = header[1..]
        .parse()
        .map_err(|_| ParseError::Protocol(format!("invalid array length: {}", header)))?;

    if count < 0 {
        return Ok(Vec::new());
    }

    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let type_line = read_line(reader)?;
        if !type_line.starts_with('$') {
            return Err(ParseError::Protocol(format!(
                "expected bulk string, got: {}",
                type_line
            )));
        }
        let len: i64 = type_line[1..]
            .parse()
            .map_err(|_| ParseError::Protocol(format!("invalid bulk length: {}", type_line)))?;
        if len < 0 {
            args.push(Vec::new());
            continue;
        }
        let bytes = read_exact_bytes(reader, len as usize)?;
        consume_crlf(reader)?;
        args.push(bytes);
    }
    Ok(args)
}

fn parse_inline<R: BufRead>(reader: &mut R) -> Result<Vec<Vec<u8>>, ParseError> {
    let line = read_line(reader)?;
    Ok(line
        .split_whitespace()
        .map(|s| s.as_bytes().to_vec())
        .collect())
}
