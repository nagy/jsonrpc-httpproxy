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
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{LazyLock, Mutex};
use std::thread;

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
/// TcpStream.  The connection thread keeps the stream and blocks on the
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
    let upstream = tokio::net::TcpStream::connect((host, port)).await?;
    let (mut client_rd, mut client_wr) = client.into_split();
    let (mut upstream_rd, mut upstream_wr) = upstream.into_split();

    use tokio::io::AsyncWriteExt;

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

// ── JSON-RPC control loop ────────────────────────────────────────────────

#[derive(Debug, serde::Deserialize)]
struct JsonRPCRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    pub params: Vec<serde_json::Value>,
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
        let request: JsonRPCRequest =
            serde_json::from_str(&line).expect("Failed to parse line as JSON");
        assert_eq!(request.jsonrpc, "2.0");
        debug!("  -> {request:?}");

        if request.method == "shutdown" {
            let response = json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "result": "shutting down",
            });
            {
                let mut lock = std::io::stdout().lock();
                writeln!(lock, "{}", serde_json::to_string(&response).unwrap()).unwrap();
                lock.flush().unwrap();
            }
            info!("shutdown via JSON-RPC, notifying pending connections");
            let mut lock = GLOBAL_MAP.lock().unwrap();
            for (_, tx) in lock.drain() {
                tx.send(Decision::Shutdown).ok();
            }
            info!("all pending connections notified, exiting");
            std::process::exit(0);
        }

        let result1 = std::panic::catch_unwind(|| -> Result<(), Box<dyn std::error::Error>> {
            let result: serde_json::Value =
                match (request.method.as_str(), request.params.as_slice()) {
                    ("accept", [number, host_val, port_val, rest @ ..]) => {
                        let num = number.as_u64().unwrap();
                        let port = port_val.as_u64().unwrap() as u16;
                        let host = host_val.as_str().unwrap();
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        if let Some(tx) = lock.remove(&num) {
                            tx.send(Decision::Accept {
                                host: host.to_owned(),
                                port,
                            })
                            .ok();
                        }
                        drop(lock);
                        debug!("accepted connection {num} → {host}:{port}");
                        let _ = rest;
                        "accepted".into()
                    }
                    ("accept-file", [number, file, mimetype]) => {
                        let num = number.as_u64().unwrap();
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        if let Some(tx) = lock.remove(&num) {
                            tx.send(Decision::AcceptFile {
                                path: file.as_str().unwrap().to_owned(),
                                mimetype: mimetype.as_str().unwrap().to_owned(),
                            })
                            .ok();
                        }
                        "accepted-file".into()
                    }
                    ("deny", [number, _args @ ..]) => {
                        let num = number.as_u64().unwrap();
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        if let Some(tx) = lock.remove(&num) {
                            tx.send(Decision::Deny).ok();
                        }
                        "denied".into()
                    }
                    _ => todo!(),
                };
            let response = json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "result": result,
            });
            {
                let mut lock = std::io::stdout().lock();
                writeln!(lock, "{}", serde_json::to_string(&response).unwrap())?;
                lock.flush()?;
            }
            Ok(())
        });
        if let Err(err) = result1 {
            eprintln!("PANIC: {err:#?}");
        }
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

    let mut buffer = [0; 1024];
    let size = match stream.read(&mut buffer) {
        Ok(0) => return,
        Ok(n) => n,
        Err(e) => {
            error!("connection {current_count}: read error: {e}");
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

    let method = match req.method {
        Some(m) => m,
        None => {
            error!("connection {current_count}: missing method");
            return;
        }
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

    match rx.recv() {
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
        Ok(Decision::Shutdown) | Err(_) => {
            send_502(&mut stream);
        }
    }
}
