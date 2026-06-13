use http::StatusCode;
use http::Version;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use log::{debug, error, info};
use serde_json::json;
use std::collections::HashMap;
use std::io::BufRead;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender};
use std::sync::{LazyLock, Mutex};
use std::thread;
use std::time::Duration;

pub fn write_http11<W: std::io::Write, T: Into<Vec<u8>>>(
    wtr: &mut W,
    response: http::Response<T>,
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

/// A tiny channel handle is parked in the global map instead of the full
/// `TcpStream`.  The connection thread keeps the stream and blocks on the
/// receiver until the controller sends a decision.
#[derive(Debug)]
enum Decision {
    Accept { host: String, port: u16 },
    AcceptFile { path: String, mimetype: String },
    Deny,
    Shutdown,
}

static GLOBAL_MAP: LazyLock<Mutex<HashMap<u64, SyncSender<Decision>>>> = LazyLock::new(|| {
    Mutex::new(HashMap::new())
});

// ── tokio-based CONNECT tunnel ───────────────────────────────────────────

/// Bidirectional tunnel between client and upstream, powered by tokio.
/// Writes `leftover` (bytes already read from the client past the CONNECT
/// line) to upstream first, then copies both directions concurrently.
async fn tunnel(
    client: tokio::net::TcpStream,
    host: &str,
    port: u16,
    leftover: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::AsyncWriteExt;

    let upstream = tokio::net::TcpStream::connect((host, port)).await?;
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

fn send_502(stream: &mut TcpStream) {
    let body = "proxy shutting down";
    let res = http::Response::builder()
        .version(Version::HTTP_11)
        .status(StatusCode::BAD_GATEWAY)
        .header(CONTENT_LENGTH, body.len())
        .header(CONTENT_TYPE, "text/plain")
        .body(body)
        .unwrap();
    write_http11(stream, res).ok();
}

fn deny_connection(stream: &mut TcpStream) {
    let reason = StatusCode::FORBIDDEN
        .canonical_reason()
        .unwrap_or("Unknown Reason");
    let res = http::Response::builder()
        .version(Version::HTTP_10)
        .status(StatusCode::FORBIDDEN)
        .header(CONTENT_LENGTH, reason.len())
        .header(CONTENT_TYPE, "text/plain")
        .body(reason)
        .unwrap();
    write_http11(stream, res).ok();
}

fn serve_file(stream: &mut TcpStream, path: &str, mimetype: &str) {
    if let Ok(filestr) = std::fs::read(path) {
        let res = http::Response::builder()
            .version(Version::HTTP_10)
            .status(StatusCode::OK)
            .header(CONTENT_LENGTH, filestr.len())
            .header(CONTENT_TYPE, mimetype)
            .body(filestr)
            .unwrap();
        write_http11(stream, res).ok();
    } else {
        let reason = StatusCode::NOT_FOUND
            .canonical_reason()
            .unwrap_or("Unknown Reason");
        let res = http::Response::builder()
            .version(Version::HTTP_10)
            .status(StatusCode::NOT_FOUND)
            .header(CONTENT_LENGTH, reason.len())
            .header(CONTENT_TYPE, "text/plain")
            .body(reason)
            .unwrap();
        write_http11(stream, res).ok();
    }
}

// ── JSON-RPC error handling ──────────────────────────────────────────────

#[derive(Debug, serde::Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

#[allow(dead_code)]
impl JsonRpcError {
    const PARSE_ERROR: i64 = -32700;
    const INVALID_REQUEST: i64 = -32600;
    const METHOD_NOT_FOUND: i64 = -32601;
    const INVALID_PARAMS: i64 = -32602;
    const INTERNAL_ERROR: i64 = -32603;

    fn method_not_found(method: &str) -> Self {
        JsonRpcError {
            code: Self::METHOD_NOT_FOUND,
            message: format!("unknown method: {method}"),
        }
    }

    fn invalid_params(msg: impl Into<String>) -> Self {
        JsonRpcError {
            code: Self::INVALID_PARAMS,
            message: msg.into(),
        }
    }
}

fn require_u64(val: &serde_json::Value, pos: usize) -> Result<u64, JsonRpcError> {
    val.as_u64().ok_or_else(|| {
        JsonRpcError::invalid_params(format!(
            "param {pos}: expected number, got {val}"
        ))
    })
}

fn require_str(val: &serde_json::Value, pos: usize) -> Result<&str, JsonRpcError> {
    val.as_str().ok_or_else(|| {
        JsonRpcError::invalid_params(format!(
            "param {pos}: expected string, got {val}"
        ))
    })
}

// ── JSON-RPC control loop ────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct JsonRPCRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    pub params: Vec<serde_json::Value>,
}

fn send_response(response: &serde_json::Value) {
    let mut lock = std::io::stdout().lock();
    let _ = writeln!(lock, "{}", serde_json::to_string(response).unwrap());
    let _ = lock.flush();
}

fn main() {
    colog::init();

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
            let mut lock = GLOBAL_MAP.lock().unwrap();
            for (_, tx) in lock.drain() {
                tx.send(Decision::Shutdown).ok();
            }
            info!("all pending connections notified, exiting");
            std::process::exit(0);
        }

        // ── dispatch ──────────────────────────────────────────────────

        let result: Result<serde_json::Value, JsonRpcError> = (|| -> Result<_, JsonRpcError> {
            match (request.method.as_str(), request.params.as_slice()) {
                ("accept", [number, host_val, port_val, rest @ ..]) => {
                    let num = require_u64(number, 1)?;
                    let port = u16::try_from(require_u64(port_val, 2)?).map_err(|_| {
                        JsonRpcError::invalid_params("param 2: port out of range (0-65535)")
                    })?;
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

fn server_thread() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = std::env::var("PORT")
        .unwrap_or("3128".into())
        .parse()
        .unwrap();
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))?;
    info!("Server listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        let stream = stream.expect("Connection failed");
        thread::spawn(move || handle_connection(stream));
    }
    unreachable!();
}

fn handle_connection(mut stream: TcpStream) {
    static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);
    let current_count = GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst);
    info!("New connection established number: {current_count}");

    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

    let mut buffer = [0; 1024];
    let size = match stream.read(&mut buffer) {
        Ok(0) => return,
        Ok(n) => n,
        Err(e) => {
            let body = "408 Request Timeout";
            let res = http::Response::builder()
                .version(Version::HTTP_11)
                .status(StatusCode::REQUEST_TIMEOUT)
                .header(CONTENT_LENGTH, body.len())
                .header(CONTENT_TYPE, "text/plain")
                .body(body)
                .unwrap();
            write_http11(&mut stream, res).ok();
            error!("connection {current_count}: read timeout/error: {e}");
            return;
        }
    };

    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);
    let res = match req.parse(&buffer[..size]) {
        Ok(r) => r,
        Err(e) => {
            error!("connection {current_count}: parse error: {e}");
            return;
        }
    };
    if res.is_partial() {
        error!("connection {current_count}: partial request");
        return;
    }
    let consumed = res.unwrap();
    let leftover = buffer[consumed..size].to_vec();

    let Some(method) = req.method else {
        error!("connection {current_count}: missing method");
        return;
    };
    let value = match method {
        "CONNECT" => {
            let mut hostport_pair = req.path.unwrap();
            if let Some(stripped) = hostport_pair.strip_prefix("https://") {
                hostport_pair = stripped;
            }
            let (host, port) = hostport_pair.split_once(':').unwrap();
            let port: u16 = port.parse().unwrap();
            json!({
                "jsonrpc": "2.0",
                "method": "want",
                "params": [method, host, port, current_count]
            })
        }
        "GET" if req.path.unwrap().starts_with("http://") => {
            json!({
                "jsonrpc": "2.0",
                "method": "gethttp",
                "params": [method, req.path.unwrap(), current_count]
            })
        }
        _ => {
            error!("connection {current_count}: unknown method {method}");
            return;
        }
    };

    let (tx, rx) = sync_channel(1);
    GLOBAL_MAP.lock().unwrap().insert(current_count, tx);
    println!("{value}"); // notify controller *after* inserting into map

    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(Decision::Accept { host, port }) => {
            let _ = stream.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n");
            let _ = stream.flush();
            stream.set_nonblocking(true).ok();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap();
            if let Err(e) = rt.block_on(async {
                let stream = tokio::net::TcpStream::from_std(stream)?;
                let _ = stream.set_nodelay(true);
                tunnel(stream, &host, port, leftover).await
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
            let body = "504 Gateway Timeout";
            let res = http::Response::builder()
                .version(Version::HTTP_11)
                .status(StatusCode::GATEWAY_TIMEOUT)
                .header(CONTENT_LENGTH, body.len())
                .header(CONTENT_TYPE, "text/plain")
                .body(body)
                .unwrap();
            write_http11(&mut stream, res).ok();
            error!("connection {current_count}: controller decision timed out");
        }
    }
}
