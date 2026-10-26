//! JSON-RPC-controlled HTTP/HTTPS intercepting proxy.
//!
//! Accepts client connections but defers all routing decisions to an
//! external controller process via line-delimited JSON-RPC over
//! stdin/stdout; see `AGENTS.md` for the full protocol.

use std::{
    collections::HashMap,
    io::{BufRead, prelude::*},
    net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    sync::{
        LazyLock, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
        mpsc::{RecvTimeoutError, SyncSender, sync_channel},
    },
    thread,
    time::Duration,
};

use http::{
    StatusCode, Version,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
};
use log::{debug, error, info};
use serde_json::json;

// ── domain newtypes ──────────────────────────────────────────────────────

/// A TCP port number.
///
/// Wraps the raw `u16` so a caller cannot swap a port with another
/// number argument.  The wrapper is zero-cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display)]
pub struct Port(pub u16);

/// The connection id the proxy assigns to each client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display, derive_more::From)]
pub struct ConnectionId(pub u64);

/// The address family allowed for outbound connections.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::Display)]
pub enum AddressFamily {
    /// Allow both families.
    #[default]
    #[display("any")]
    Any,
    /// Allow IPv4 addresses only.
    #[display("IPv4")]
    V4,
    /// Allow IPv6 addresses only.
    #[display("IPv6")]
    V6,
}

impl AddressFamily {
    /// Return true when `ip` belongs to this family.
    ///
    /// IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`) count as IPv6.
    #[must_use]
    pub const fn allows(self, ip: IpAddr) -> bool {
        match self {
            Self::Any => true,
            Self::V4 => ip.is_ipv4(),
            Self::V6 => ip.is_ipv6(),
        }
    }
}

/// Command-line options.
#[derive(Debug, Clone, clap::Parser)]
#[command(about = "JSON-RPC-controlled HTTP/HTTPS intercepting proxy")]
struct Cli {
    /// Restrict outbound connections to IPv4.
    #[arg(long, conflicts_with = "force_ipv6")]
    force_ipv4: bool,
    /// Restrict outbound connections to IPv6.
    #[arg(long)]
    force_ipv6: bool,
}

impl Cli {
    /// The family filter selected by the flags.
    const fn address_family(&self) -> AddressFamily {
        if self.force_ipv4 {
            AddressFamily::V4
        } else if self.force_ipv6 {
            AddressFamily::V6
        } else {
            AddressFamily::Any
        }
    }
}

/// Outbound address family, set once from the command line.
static ADDRESS_FAMILY: OnceLock<AddressFamily> = OnceLock::new();

/// Read the outbound family filter.
fn address_family() -> AddressFamily {
    ADDRESS_FAMILY.get().copied().unwrap_or_default()
}

/// A failure in the server thread or in connection setup.
#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum ProxyError {
    /// The listener could not bind, or accepting a connection failed.
    #[display("listener error: {_0}")]
    Listener(#[from] std::io::Error),
    /// The `PORT` environment variable is not a valid `u16`.
    #[display("invalid PORT value: {_0}")]
    InvalidPort(#[error(ignore)] String),
}

/// A failure while parsing a CONNECT request target.
#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, derive_more::Error)]
#[error(ignore)]
pub enum TargetError {
    /// The target has no `:port` suffix.
    #[display("missing port in CONNECT target: {_0}")]
    MissingPort(String),
    /// A bracketed IPv6 host has no closing bracket.
    #[display("unclosed IPv6 bracket in CONNECT target: {_0}")]
    UnclosedBracket(String),
    /// The port text is not a valid `u16`.
    #[display("invalid port in CONNECT target: {_0}")]
    InvalidPort(String),
}

/// Split a CONNECT target into host and port.
///
/// Accepts `host:port`, `[ipv6]:port`, and an optional `https://`
/// prefix.
///
/// # Errors
///
/// Returns [`TargetError`] when the port is missing or invalid, or when
/// the bracketed host has no closing bracket.
///
/// # Examples
///
/// ```
/// use jsonrpc_httpproxy::{Port, parse_target};
///
/// assert_eq!(
///     parse_target("example.com:443").unwrap(),
///     ("example.com", Port(443))
/// );
/// assert_eq!(parse_target("[::1]:8443").unwrap(), ("::1", Port(8443)));
/// assert_eq!(
///     parse_target("https://example.com:80").unwrap(),
///     ("example.com", Port(80))
/// );
/// ```
pub fn parse_target(target: &str) -> Result<(&str, Port), TargetError> {
    let target = target.strip_prefix("https://").unwrap_or(target);
    let (host, port) = if let Some(rest) = target.strip_prefix('[') {
        let (host, rest) = rest
            .split_once(']')
            .ok_or_else(|| TargetError::UnclosedBracket(target.to_owned()))?;
        let port = rest
            .strip_prefix(':')
            .ok_or_else(|| TargetError::MissingPort(target.to_owned()))?;
        (host, port)
    } else {
        target
            .split_once(':')
            .ok_or_else(|| TargetError::MissingPort(target.to_owned()))?
    };
    let port = port
        .parse()
        .map_err(|_| TargetError::InvalidPort(port.to_owned()))?;
    Ok((host, Port(port)))
}

/// A failure while connecting to the upstream server.
#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum UpstreamError {
    /// Hostname resolution failed.
    #[display("cannot resolve upstream host: {_0}")]
    Resolve(std::io::Error),
    /// No resolved address matched the forced family.
    #[display("no {_1} address for {_0}")]
    NoMatchingAddress(#[error(ignore)] String, #[error(ignore)] AddressFamily),
    /// Every matching address refused the connection.
    #[display("cannot connect to any matching upstream address: {_0}")]
    Connect(std::io::Error),
}

/// Timeout for one upstream TCP connect attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Resolve `host` and connect an upstream socket in the allowed family.
///
/// Tries each resolved address in order and returns the first successful
/// connection.
///
/// # Errors
///
/// Returns [`UpstreamError`] when resolution fails, when no address
/// matches the family filter, or when every matching address fails.
fn connect_upstream(
    host: &str,
    port: Port,
    family: AddressFamily,
) -> Result<TcpStream, UpstreamError> {
    let addrs: Vec<SocketAddr> = (host, port.0)
        .to_socket_addrs()
        .map_err(UpstreamError::Resolve)?
        .filter(|addr| family.allows(addr.ip()))
        .collect();
    if addrs.is_empty() {
        return Err(UpstreamError::NoMatchingAddress(host.to_owned(), family));
    }
    let mut last_error = None;
    for addr in &addrs {
        match TcpStream::connect_timeout(addr, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                error!("upstream {addr} failed: {error}");
                last_error = Some(error);
            }
        }
    }
    Err(UpstreamError::Connect(
        last_error.expect("address list is non-empty"),
    ))
}
///
/// Emits a `<version> <code> <reason>` status line, one `Name: value`
/// line per header, a blank line, then the body — all with CRLF line
/// endings (`writeln!`'s `\n` plus the explicit `\r` in the format
/// strings).
///
/// # Errors
///
/// Returns an I/O error if writing the head or body fails.
///
/// # Examples
///
/// Pin the exact wire format, including the CRLF endings and the
/// lowercased header name:
///
/// ```
/// use http::{Response, StatusCode, Version};
/// use jsonrpc_httpproxy::write_http11;
///
/// let response = Response::builder()
///     .version(Version::HTTP_11)
///     .status(StatusCode::OK)
///     .header("Content-Type", "text/plain")
///     .body("hi")
///     .unwrap();
/// let mut buf: Vec<u8> = Vec::new();
/// write_http11(&mut buf, response).unwrap();
/// assert_eq!(
///     buf,
///     b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\n\r\nhi"
/// );
/// ```
pub fn write_http11(
    wtr: &mut impl std::io::Write,
    response: http::Response<impl Into<Vec<u8>>>,
) -> std::io::Result<()> {
    let (parts, body) = response.into_parts();
    let reason = parts.status.canonical_reason().unwrap_or("Unknown");
    writeln!(
        wtr,
        "{:?} {} {reason}\r",
        parts.version,
        parts.status.as_u16(),
    )?;
    for (name, value) in &parts.headers {
        writeln!(wtr, "{}: {}\r", name, value.to_str().unwrap_or(""))?;
    }
    wtr.write_all(b"\r\n")?;
    wtr.write_all(&body.into())?;
    wtr.flush()?;
    Ok(())
}

// ── connection state ─────────────────────────────────────────────────────

/// A decision the controller reaches for a pending connection.
///
/// A tiny channel handle is parked in the global map instead of the
/// full `TcpStream`.  The connection thread keeps the stream and blocks
/// on the receiver until the controller sends a decision.
#[derive(Debug, Clone)]
pub enum Decision {
    Accept { host: String, port: Port },
    AcceptFile { path: String, mimetype: String },
    Deny,
    Shutdown,
}

/// Pending connections: connection id → decision channel.
pub static GLOBAL_MAP: LazyLock<Mutex<HashMap<u64, SyncSender<Decision>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// ── HTTP response construction ───────────────────────────────────────────

/// Build a plain-text response with the given status and body.
fn plain_response(
    version: Version,
    status: StatusCode,
    body: &'static str,
) -> http::Response<&'static str> {
    http::Response::builder()
        .version(version)
        .status(status)
        .header(CONTENT_LENGTH, body.len())
        .header(CONTENT_TYPE, "text/plain")
        .body(body)
        .expect("plain-text response builder cannot fail")
}

// ── tokio-based CONNECT tunnel ───────────────────────────────────────────

/// Bidirectional tunnel between client and upstream, powered by tokio.
///
/// Writes `leftover` (bytes already read from the client past the
/// CONNECT line) to upstream first, then copies both directions
/// concurrently.  The caller connects `upstream` before it writes the
/// `200 Connection Established` response.
///
/// # Errors
///
/// Returns an error if the leftover bytes cannot be written, or if
/// spawning a copy task fails.
///
/// # Panics
///
/// Panics if a copy task panics while it is joined.
pub async fn tunnel(
    client: tokio::net::TcpStream,
    upstream: tokio::net::TcpStream,
    leftover: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::AsyncWriteExt;

    let (mut client_rd, mut client_wr) = client.into_split();
    let (mut upstream_rd, mut upstream_wr) = upstream.into_split();

    if !leftover.is_empty() {
        upstream_wr.write_all(&leftover).await?;
    }

    let c2u = tokio::spawn(async move {
        if let Err(e) = tokio::io::copy(&mut client_rd, &mut upstream_wr).await {
            error!("client → upstream copy: {e}");
        }
        let _ = upstream_wr.shutdown().await;
    });
    let u2c = tokio::spawn(async move {
        if let Err(e) = tokio::io::copy(&mut upstream_rd, &mut client_wr).await {
            error!("upstream → client copy: {e}");
        }
        let _ = client_wr.shutdown().await;
    });

    c2u.await.unwrap();
    u2c.await.unwrap();
    Ok(())
}

// ── response helpers (called from the connection thread) ─────────────────

/// Send a `502 Bad Gateway` response with the given body.
fn send_502_with(stream: &mut TcpStream, body: &'static str) {
    let res = plain_response(Version::HTTP_11, StatusCode::BAD_GATEWAY, body);
    write_http11(stream, res).ok();
}

/// Send a `502 Bad Gateway` response, used when the proxy shuts down.
pub fn send_502(stream: &mut TcpStream) {
    send_502_with(stream, "proxy shutting down");
}

/// Respond `403 Forbidden` to the client.
pub fn deny_connection(stream: &mut TcpStream) {
    let reason = StatusCode::FORBIDDEN
        .canonical_reason()
        .unwrap_or("Unknown Reason");
    let res = plain_response(Version::HTTP_10, StatusCode::FORBIDDEN, reason);
    write_http11(stream, res).ok();
}

/// Serve a local file as an HTTP 200 response, or `404` if unreadable.
///
/// # Panics
///
/// Panics if the HTTP response builder rejects the hardcoded status or
/// body.
pub fn serve_file(stream: &mut TcpStream, path: &str, mimetype: &str) {
    if let Ok(filestr) = std::fs::read(path) {
        let res = http::Response::builder()
            .version(Version::HTTP_10)
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, filestr.len())
            .header(CONTENT_TYPE, mimetype)
            .body(filestr)
            .expect("file response builder cannot fail");
        write_http11(stream, res).ok();
    } else {
        let reason = StatusCode::NOT_FOUND
            .canonical_reason()
            .unwrap_or("Unknown Reason");
        let res = plain_response(Version::HTTP_10, StatusCode::NOT_FOUND, reason);
        write_http11(stream, res).ok();
    }
}

// ── JSON-RPC error handling ──────────────────────────────────────────────

/// A JSON-RPC 2.0 error object: standard `code` plus a human message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct JsonRpcError {
    /// One of the JSON-RPC 2.0 error codes, see the constants below.
    pub code: i64,
    /// Human-readable description sent back to the controller.
    pub message: String,
}

impl JsonRpcError {
    /// A stdin line was not valid JSON (`-32700`).
    pub const PARSE_ERROR: i64 = -32700;

    /// The `jsonrpc` member was not `"2.0"` (`-32600`).
    pub const INVALID_REQUEST: i64 = -32600;

    /// Unknown method name (`-32601`).
    pub const METHOD_NOT_FOUND: i64 = -32601;

    /// Bad param type, unknown connection id, or out-of-range port
    /// (`-32602`).
    pub const INVALID_PARAMS: i64 = -32602;

    /// Reserved by JSON-RPC 2.0; currently unused (`-32603`).
    pub const INTERNAL_ERROR: i64 = -32603;

    /// Build a `METHOD_NOT_FOUND` error for an unknown method.
    #[must_use]
    pub fn method_not_found(method: &str) -> Self {
        JsonRpcError {
            code: Self::METHOD_NOT_FOUND,
            message: format!("unknown method: {method}"),
        }
    }

    /// Build an `INVALID_PARAMS` error from a message.
    ///
    /// # Examples
    ///
    /// ```
    /// use jsonrpc_httpproxy::JsonRpcError;
    ///
    /// let err = JsonRpcError::invalid_params("param 1: expected number");
    /// assert_eq!(err.code, JsonRpcError::INVALID_PARAMS);
    /// assert_eq!(err.message, "param 1: expected number");
    /// ```
    #[must_use]
    pub fn invalid_params(msg: impl Into<String>) -> Self {
        JsonRpcError {
            code: Self::INVALID_PARAMS,
            message: msg.into(),
        }
    }
}

/// Extract a `u64` from a JSON-RPC params array element.
///
/// # Errors
///
/// Returns `INVALID_PARAMS` (`-32602`) when the value is not an
/// unsigned integer — strings and negatives are rejected, not parsed.
///
/// # Examples
///
/// ```
/// use jsonrpc_httpproxy::require_u64;
/// use serde_json::json;
///
/// assert_eq!(require_u64(&json!(5), 1).unwrap(), 5);
/// assert_eq!(require_u64(&json!(0), 1).unwrap(), 0);
/// assert!(require_u64(&json!("5"), 1).is_err());
/// assert!(require_u64(&json!(-5), 1).is_err());
/// ```
pub fn require_u64(val: &serde_json::Value, pos: usize) -> Result<u64, JsonRpcError> {
    val.as_u64().ok_or_else(|| {
        JsonRpcError::invalid_params(format!("param {pos}: expected number, got {val}"))
    })
}

/// Extract a string slice from a JSON-RPC params array element.
///
/// # Errors
///
/// Returns `INVALID_PARAMS` (`-32602`) when the value is not a string.
///
/// # Examples
///
/// ```
/// use jsonrpc_httpproxy::require_str;
/// use serde_json::json;
///
/// assert_eq!(
///     require_str(&json!("example.com"), 2).unwrap(),
///     "example.com"
/// );
/// assert!(require_str(&json!(42), 2).is_err());
/// ```
pub fn require_str(val: &serde_json::Value, pos: usize) -> Result<&str, JsonRpcError> {
    val.as_str().ok_or_else(|| {
        JsonRpcError::invalid_params(format!("param {pos}: expected string, got {val}"))
    })
}

// ── JSON-RPC control loop ────────────────────────────────────────────────

/// A controller request line as parsed from stdin.
///
/// The wire format is single-line JSON-RPC 2.0 with a numeric `id` and
/// an array `params`.
///
/// # Examples
///
/// ```
/// use jsonrpc_httpproxy::JsonRPCRequest;
///
/// let req: JsonRPCRequest = serde_json::from_str(
///     r#"{"jsonrpc":"2.0","id":7,"method":"accept","params":[3,"example.com",443]}"#,
/// )
/// .unwrap();
/// assert_eq!(req.id, 7);
/// assert_eq!(req.method, "accept");
/// assert_eq!(req.params.len(), 3);
/// ```
#[derive(Debug, Clone, serde::Deserialize)]
pub struct JsonRPCRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    pub params: Vec<serde_json::Value>,
}

/// Write a JSON-RPC response (or error) object to stdout as one line.
///
/// # Panics
///
/// Panics if the response value cannot be serialized to JSON.  Because
/// `response` is built from `json!` and plain data, this cannot happen
/// in practice.
pub fn send_response(response: &serde_json::Value) {
    let mut lock = std::io::stdout().lock();
    let text = serde_json::to_string(response).expect("JSON-RPC response is always serializable");
    let _ = writeln!(lock, "{text}");
    let _ = lock.flush();
}

/// Entry point: start the server thread, then run the stdin JSON-RPC
/// control loop.
///
/// # Panics
///
/// Panics if the SIGINT handler cannot be registered, if reading a
/// stdin line fails, or if the `GLOBAL_MAP` mutex is poisoned.
pub fn main() {
    colog::init();

    let cli = <Cli as clap::Parser>::parse();
    ADDRESS_FAMILY
        .set(cli.address_family())
        .expect("address family set once");

    ctrlc::set_handler(|| {
        info!("received SIGINT, shutting down");
        std::process::exit(0);
    })
    .expect("failed to set SIGINT handler");

    std::thread::spawn(|| {
        if let Err(err) = server_thread() {
            error!("{err:?}");
            std::process::exit(1);
        }
    });

    for line in std::io::stdin().lock().lines().map(Result::unwrap) {
        let request: JsonRPCRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                error!("parse error: {e}");
                let err = json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": JsonRpcError::PARSE_ERROR, "message": format!("Parse error: {e}") },
                });
                send_response(&err);
                continue;
            }
        };

        if request.jsonrpc != "2.0" {
            let err = json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "error": { "code": JsonRpcError::INVALID_REQUEST, "message": "jsonrpc must be '2.0'" },
            });
            send_response(&err);
            continue;
        }

        debug!("  -> {request:?}");

        if request.method == "shutdown" {
            let response = json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "result": "shutting down",
            });
            send_response(&response);
            info!("shutdown via JSON-RPC, notifying pending connections");
            let mut lock = GLOBAL_MAP
                .lock()
                .expect("GLOBAL_MAP mutex is never poisoned");
            for (_, tx) in lock.drain() {
                tx.send(Decision::Shutdown).ok();
            }
            info!("all pending connections notified, exiting");
            // Wait for the connection threads to write their 502s before
            // exiting. `process::exit` would kill them mid-write, leaving
            // clients with a TCP RST instead of the graceful 502.
            thread::sleep(Duration::from_millis(500));
            std::process::exit(0);
        }

        // ── dispatch ──────────────────────────────────────────────────

        let result: Result<serde_json::Value, JsonRpcError> = (|| -> Result<_, JsonRpcError> {
            match (request.method.as_str(), request.params.as_slice()) {
                ("accept", [number, host_val, port_val, rest @ ..]) => {
                    let num = require_u64(number, 1)?;
                    let port = Port(u16::try_from(require_u64(port_val, 2)?).map_err(|_| {
                        JsonRpcError::invalid_params("param 2: port out of range (0-65535)")
                    })?);
                    let host = require_str(host_val, 1)?;
                    let mut lock = GLOBAL_MAP.lock().unwrap();
                    match lock.remove(&num) {
                        Some(tx) => {
                            tx.send(Decision::Accept {
                                host: host.to_owned(),
                                port,
                            })
                            .ok();
                            drop(lock);
                            debug!("accepted connection {num} → {host}:{port}");
                            let _ = rest;
                            Ok("accepted".into())
                        }
                        None => Err(JsonRpcError::invalid_params(format!(
                            "unknown connection id: {num}"
                        ))),
                    }
                }
                ("accept-file", [number, file, mimetype]) => {
                    let num = require_u64(number, 1)?;
                    let file = require_str(file, 2)?;
                    let mimetype = require_str(mimetype, 3)?;
                    let mut lock = GLOBAL_MAP.lock().unwrap();
                    match lock.remove(&num) {
                        Some(tx) => {
                            tx.send(Decision::AcceptFile {
                                path: file.to_owned(),
                                mimetype: mimetype.to_owned(),
                            })
                            .ok();
                            Ok("accepted-file".into())
                        }
                        None => Err(JsonRpcError::invalid_params(format!(
                            "unknown connection id: {num}"
                        ))),
                    }
                }
                ("deny", [number, ..]) => {
                    let num = require_u64(number, 1)?;
                    let mut lock = GLOBAL_MAP.lock().unwrap();
                    match lock.remove(&num) {
                        Some(tx) => {
                            tx.send(Decision::Deny).ok();
                            Ok("denied".into())
                        }
                        None => Err(JsonRpcError::invalid_params(format!(
                            "unknown connection id: {num}"
                        ))),
                    }
                }
                _ => Err(JsonRpcError::method_not_found(&request.method)),
            }
        })();

        let response = match result {
            Ok(value) => json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "result": value,
            }),
            Err(e) => json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "error": { "code": e.code, "message": e.message },
            }),
        };
        send_response(&response);
    }
}

// ── server ───────────────────────────────────────────────────────────────

/// Accept client connections and spawn a handler thread per connection.
///
/// # Errors
///
/// Returns [`ProxyError::InvalidPort`] when `PORT` is not a valid
/// `u16`, and [`ProxyError::Listener`] when binding or accepting a
/// connection fails.
pub fn server_thread() -> Result<(), ProxyError> {
    let port_text = std::env::var("PORT").unwrap_or_else(|_| "3128".into());
    let port: u16 = port_text
        .parse()
        .map_err(|_| ProxyError::InvalidPort(port_text.clone()))?;
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))?;
    info!("Server listening on 127.0.0.1:{port}");
    loop {
        let (stream, _) = listener.accept()?;
        thread::spawn(move || handle_connection(stream));
    }
}

/// Handle one client connection from accept to close.
///
/// Parses the first request, asks the controller, then acts on the
/// decision.
///
/// # Panics
///
/// Panics when the `GLOBAL_MAP` mutex is poisoned.
pub fn handle_connection(mut stream: TcpStream) {
    static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);
    let current_count = GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst);
    info!("New connection established number: {current_count}");

    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

    let mut buffer = [0; 1024];
    let size = match stream.read(&mut buffer) {
        Ok(0) => return,
        Ok(n) => n,
        Err(e) => {
            let res = plain_response(
                Version::HTTP_11,
                StatusCode::REQUEST_TIMEOUT,
                "408 Request Timeout",
            );
            write_http11(&mut stream, res).ok();
            error!("connection {current_count}: read timeout/error: {e}");
            return;
        }
    };

    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);
    let consumed = match req.parse(&buffer[..size]) {
        Ok(httparse::Status::Complete(consumed)) => consumed,
        Ok(httparse::Status::Partial) => {
            error!("connection {current_count}: partial request");
            return;
        }
        Err(e) => {
            error!("connection {current_count}: parse error: {e}");
            return;
        }
    };
    let leftover = buffer[consumed..size].to_vec();

    let Some(method) = req.method else {
        error!("connection {current_count}: missing method");
        return;
    };
    let Some(path) = req.path else {
        error!("connection {current_count}: missing request path");
        return;
    };
    let value = match method {
        "CONNECT" => match parse_target(path) {
            Ok((host, port)) => json!({
                "jsonrpc": "2.0",
                "method": "want",
                "params": [method, host, port.0, current_count]
            }),
            Err(e) => {
                error!("connection {current_count}: {e}");
                let res =
                    plain_response(Version::HTTP_11, StatusCode::BAD_REQUEST, "400 Bad Request");
                write_http11(&mut stream, res).ok();
                return;
            }
        },
        "GET" if path.starts_with("http://") => {
            json!({
                "jsonrpc": "2.0",
                "method": "gethttp",
                "params": [method, path, current_count]
            })
        }
        _ => {
            error!("connection {current_count}: unknown method {method}");
            return;
        }
    };

    let (tx, rx) = sync_channel(1);
    GLOBAL_MAP
        .lock()
        .expect("GLOBAL_MAP mutex is never poisoned")
        .insert(current_count, tx);
    // Notify the controller *after* inserting into the map. Flush explicitly:
    // stdout is block-buffered when redirected (as in the VM test), so
    // without a flush the notification would sit in the buffer and the
    // controller would never see it promptly.
    {
        let mut lock = std::io::stdout().lock();
        let _ = writeln!(lock, "{value}");
        let _ = lock.flush();
    }

    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(Decision::Accept { host, port }) => {
            let upstream = match connect_upstream(&host, port, address_family()) {
                Ok(stream) => stream,
                Err(e) => {
                    error!("connection {current_count}: {e}");
                    send_502_with(&mut stream, "upstream connection failed");
                    return;
                }
            };
            // Write the 200 only after the upstream connection succeeds,
            // so a family miss or a refused connect still gets an HTTP
            // error status instead of a dead tunnel.
            let _ = stream.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n");
            let _ = stream.flush();
            let _ = stream.set_nonblocking(true);
            let _ = upstream.set_nonblocking(true);
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    error!("connection {current_count}: runtime build failed: {e}");
                    return;
                }
            };
            if let Err(e) = rt.block_on(async {
                let stream = tokio::net::TcpStream::from_std(stream)?;
                let upstream = tokio::net::TcpStream::from_std(upstream)?;
                let _ = stream.set_nodelay(true);
                let _ = upstream.set_nodelay(true);
                tunnel(stream, upstream, leftover).await
            }) {
                error!("tunnel to {host}:{port}: {e}");
            }
        }
        Ok(Decision::AcceptFile { path, mimetype }) => {
            serve_file(&mut stream, &path, &mimetype);
        }
        Ok(Decision::Deny) => {
            deny_connection(&mut stream);
        }
        Ok(Decision::Shutdown) | Err(RecvTimeoutError::Disconnected) => {
            send_502(&mut stream);
        }
        Err(RecvTimeoutError::Timeout) => {
            let res = plain_response(
                Version::HTTP_11,
                StatusCode::GATEWAY_TIMEOUT,
                "504 Gateway Timeout",
            );
            write_http11(&mut stream, res).ok();
            error!("connection {current_count}: controller decision timed out");
        }
    }
}

// ── tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_target_accepts_host_and_port() {
        assert_eq!(
            parse_target("example.com:443").unwrap(),
            ("example.com", Port(443))
        );
    }

    #[test]
    fn parse_target_strips_https_prefix() {
        assert_eq!(
            parse_target("https://example.com:8443").unwrap(),
            ("example.com", Port(8443))
        );
    }

    #[test]
    fn parse_target_accepts_bracketed_ipv6() {
        assert_eq!(
            parse_target("[2001:db8::1]:443").unwrap(),
            ("2001:db8::1", Port(443))
        );
        assert_eq!(parse_target("[::1]:8443").unwrap(), ("::1", Port(8443)));
    }

    #[test]
    fn parse_target_rejects_missing_port() {
        assert_eq!(
            parse_target("example.com").unwrap_err(),
            TargetError::MissingPort("example.com".to_owned())
        );
        assert_eq!(
            parse_target("[::1]").unwrap_err(),
            TargetError::MissingPort("[::1]".to_owned())
        );
    }

    #[test]
    fn parse_target_rejects_unclosed_bracket() {
        assert_eq!(
            parse_target("[2001:db8::1:443").unwrap_err(),
            TargetError::UnclosedBracket("[2001:db8::1:443".to_owned())
        );
    }

    #[test]
    fn parse_target_rejects_invalid_port() {
        assert_eq!(
            parse_target("example.com:http").unwrap_err(),
            TargetError::InvalidPort("http".to_owned())
        );
        assert_eq!(
            parse_target("example.com:70000").unwrap_err(),
            TargetError::InvalidPort("70000".to_owned())
        );
    }

    #[test]
    fn family_filter_is_strict() {
        let v4: IpAddr = "192.0.2.1".parse().unwrap();
        let v6: IpAddr = "2001:db8::1".parse().unwrap();
        let mapped: IpAddr = "::ffff:192.0.2.1".parse().unwrap();

        assert!(AddressFamily::Any.allows(v4));
        assert!(AddressFamily::Any.allows(v6));
        assert!(AddressFamily::V4.allows(v4));
        assert!(!AddressFamily::V4.allows(v6));
        assert!(AddressFamily::V6.allows(v6));
        assert!(!AddressFamily::V6.allows(v4));
        assert!(!AddressFamily::V4.allows(mapped));
        assert!(AddressFamily::V6.allows(mapped));
    }
}
