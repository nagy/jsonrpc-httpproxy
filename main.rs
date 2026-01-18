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
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::process::Command;
use std::process::Stdio;
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

static GLOBAL_MAP: LazyLock<Mutex<HashMap<u64, TcpStream>>> = LazyLock::new(|| {
    let map = HashMap::new();
    Mutex::new(map)
});

fn connect_nc_command(stream: TcpStream, args: &[serde_json::Value]) {
    fn duplicate_raw_fd(fd: RawFd) -> OwnedFd {
        let borrowed_fd_reference: BorrowedFd<'_> = unsafe { BorrowedFd::borrow_raw(fd) };
        borrowed_fd_reference.try_clone_to_owned().unwrap()
    }
    let fd = stream.as_raw_fd();
    let stdio = unsafe { Stdio::from_raw_fd(fd) };
    let stdio2 = duplicate_raw_fd(fd);
    std::mem::forget(stream); // might be needed to not drop the connection.
    let mut cmd = Command::new("nc");
    cmd.arg("-4");
    for arg in args {
        match arg {
            serde_json::Value::String(x) => {
                cmd.arg(x);
            }
            serde_json::Value::Number(x) => {
                cmd.arg(format!("{x}"));
            }
            _ => todo!(),
        }
    }
    let mut child = cmd
        .stdin(stdio)
        .stdout(stdio2)
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    thread::spawn(move || {
        std::mem::forget(child.wait());
    });
}

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
                    ("accept", [number, args @ ..]) => {
                        let num = number.as_u64().unwrap();
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        let mut stream = lock.remove(&num).unwrap();
                        stream.write_all(b"HTTP/1.0 200 OK\r\n\r\n")?;
                        stream.flush()?;
                        connect_nc_command(stream, args);
                        "accepted".into()
                    }
                    ("accept-file", [number, file, mimetype]) => {
                        let num = number.as_u64().unwrap();
                        let mut lock = GLOBAL_MAP.lock().unwrap();
                        let mut stream = lock.remove(&num).unwrap();
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
                        let mut stream = lock.remove(&num).unwrap();

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
    // Err("Should not arrive here.".into())
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
    let res = req.parse(&buffer)?;
    if res.is_partial() {
        return Err("Partial request".into());
    }
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
    GLOBAL_MAP.lock()?.insert(current_count, stream);
    Ok(())
}
