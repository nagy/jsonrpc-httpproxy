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

/// Pending connection: the raw TCP stream plus any bytes the client sent
/// *after* the HTTP CONNECT request line (e.g. the TLS ClientHello) that
/// httparse didn't consume and must be forwarded to the upstream.
static GLOBAL_MAP: LazyLock<Mutex<HashMap<u64, (TcpStream, Vec<u8>)>>> = LazyLock::new(|| {
    let map = HashMap::new();
    Mutex::new(map)
});

// ── tokio-based CONNECT tunnel (replaces the old `nc` call) ──────────────

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

/// Spawn a dedicated OS thread with a single-threaded tokio runtime to run
/// the tunnel. This keeps `connect_nc_command`'s call-site contract: the
/// stream is handed off and the caller never sees it again.
fn spawn_tunnel(stream: TcpStream, host: &str, port: u16, leftover: Vec<u8>) {
    let host = host.to_owned();
    thread::spawn(move || {
        stream.set_nonblocking(true).ok();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        if let Err(e) = rt.block_on(async {
            let stream = tokio::net::TcpStream::from_std(stream)?;
            stream.set_nodelay(true)?;
            tunnel(stream, &host, port, leftover).await
        }) {
            error!("tunnel to {host}:{port}: {e}");
        }
    });
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
        let result1 = std::panic::catch_unwind(|| -> Result<(), Box<dyn std::error::Error>> {
            let result: serde_json::Value =
                match (request.method.as_str(), request.params.as_slice()) {
                    ("accept", [number, host_val, port_val, rest @ ..]) => {
                        let num = number.as_u64().unwrap();
                        let port = port_val.as_u64().unwrap() as u16;
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        let (mut stream, leftover) = lock.remove(&num).unwrap();
                        drop(lock);
                        stream.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")?;
                        stream.flush()?;
                        spawn_tunnel(stream, host_val.as_str().unwrap(), port, leftover);
                        debug!("accepted connection {num} → {host_val}:{port}");
                        // If the controller passed extra args, ignore them for now;
                        // they were used by the old `nc` invocation.
                        let _ = rest;
                        "accepted".into()
                    }
                    ("accept-file", [number, file, mimetype]) => {
                        let num = number.as_u64().unwrap();
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        let (mut stream, _leftover) = lock.remove(&num).unwrap();
                        if let Ok(filestr) = std::fs::read(file.as_str().unwrap()) {
                            let res = http::Response::builder()
                                .version(Version::HTTP_10)
                                .status(StatusCode::OK)
                                .header(CONTENT_LENGTH, filestr.len())
                                .header(CONTENT_TYPE, mimetype.as_str().unwrap())
                                .body(filestr)
                                .unwrap();
                            match write_http11(&mut stream, res) {
                                Ok(()) => {}
                                Err(err) => eprintln!("Error: {err:#?}"),
                            }
                            "accepted-file".into()
                        } else {
                            let reason_text = StatusCode::NOT_FOUND
                                .canonical_reason()
                                .unwrap_or("Unknown Reason");

                            let res = http::Response::builder()
                                .version(Version::HTTP_10)
                                .status(StatusCode::NOT_FOUND)
                                .header(CONTENT_LENGTH, reason_text.len())
                                .header(CONTENT_TYPE, "text/plain")
                                .body(reason_text)
                                .unwrap();
                            match write_http11(&mut stream, res) {
                                Ok(()) => {}
                                Err(err) => eprintln!("Error: {err:#?}"),
                            }
                            "denied".into()
                        }
                    }
                    ("deny", [number, _args @ ..]) => {
                        let num = number.as_u64().unwrap();
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        let (mut stream, _leftover) = lock.remove(&num).unwrap();

                        let reason_text = StatusCode::FORBIDDEN
                            .canonical_reason()
                            .unwrap_or("Unknown Reason");

                        let res = http::Response::builder()
                            .version(Version::HTTP_10)
                            .status(StatusCode::FORBIDDEN)
                            .header(CONTENT_LENGTH, reason_text.len())
                            .header(CONTENT_TYPE, "text/plain")
                            .body(reason_text)
                            .unwrap();
                        match write_http11(&mut stream, res) {
                            Ok(()) => {}
                            Err(err) => eprintln!("Error: {err:#?}"),
                        }
                        "denied".into()
                    }
                    _ => todo!(),
                };
            let response = json! ({
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

fn server_thread() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = std::env::var("PORT")
        .unwrap_or("3128".into())
        .parse()
        .unwrap();
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))?;
    info!("Server listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        let stream = stream.expect("Connection failed");
        thread::spawn(move || match handle_connection(stream) {
            Ok(()) => {}
            Err(err) => eprintln!("Error in thread: {err}"),
        });
    }
    unreachable!();
}

fn handle_connection(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);
    let current_count = GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst);
    info!("New connection established number: {current_count}");
    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer)?;
    if size == 0 {
        return Err("size is now zero.".into());
    }
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);
    let res = req.parse(&buffer[..size])?;
    if res.is_partial() {
        return Err("Partial request".into());
    }
    let consumed = res.unwrap();
    let leftover = buffer[consumed..size].to_vec();
    let value = match req.method.unwrap() {
        "CONNECT" => {
            let mut hostport_pair = req.path.unwrap();
            if hostport_pair.starts_with("https://") {
                hostport_pair = hostport_pair.strip_prefix("https://").unwrap();
            }
            let (host, port) = hostport_pair.split_once(':').unwrap();
            let port: u16 = port.parse()?;
            json!({
                "jsonrpc": "2.0",
                "method": "want",
                "params": [req.method, host, port, current_count]
            })
        }
        "GET" if req.path.unwrap().starts_with("http://") => {
            json!({
                "jsonrpc": "2.0",
                "method": "gethttp",
                "params": [req.method, req.path.unwrap(), current_count]
            })
        }
        _ => {
            return Err(format!("Unknown HTTP Method: {}", req.method.unwrap()).into());
        }
    };
    println!("{value}");
    GLOBAL_MAP.lock()?.insert(current_count, (stream, leftover));
    Ok(())
}
