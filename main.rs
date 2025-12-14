use log::{debug, error, info};
use serde::{Deserialize, Serialize};
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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{LazyLock, Mutex};
use std::thread;

static GLOBAL_COUNTER: AtomicUsize = AtomicUsize::new(0);

static GLOBAL_MAP: LazyLock<Mutex<HashMap<usize, TcpStream>>> = LazyLock::new(|| {
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
    for arg in args.iter() {
        match arg {
            serde_json::Value::String(x) => {
                cmd.arg(x);
            }
            serde_json::Value::Number(x) => {
                cmd.arg(format!("{}", x));
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

#[derive(Debug, Deserialize)]
pub struct JsonRPCRequest {
    pub id: i32,
    pub method: String,
    pub params: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRPCResponse {
    pub jsonrpc: &'static str,
    pub id: i32,
    pub result: serde_json::Value,
}

fn main() {
    colog::init();
    std::thread::spawn(|| {
        if let Err(err) = server_thread() {
            error!("{:?}", err);
            std::process::exit(1);
        }
    });
    for line in std::io::stdin().lock().lines() {
        let line = line.expect("Error reading line.");
        if line.trim().is_empty() {
            continue;
        }
        let request: JsonRPCRequest =
            serde_json::from_str(&line).expect("Failed to parse line as JSON");
        debug!("  -> {:?}", request);
        let result: serde_json::Value = match (request.method.as_str(), request.params.as_slice()) {
            ("add", numbers) => {
                let mut ret = 0u64;
                for p in numbers {
                    ret += p.as_u64().unwrap_or(0);
                }
                ret.into()
            }
            ("accept", [number, args @ ..]) => {
                let num = number.as_u64().unwrap() as usize;
                let mut lock = GLOBAL_MAP.lock().unwrap();
                let mut stream = lock.remove(&num).unwrap();
                stream.write_all(b"HTTP/1.0 200 OK\r\n\r\n").unwrap();
                connect_nc_command(stream, args);
                "accepted".into()
            }
            ("deny", [number, _args @ ..]) => {
                let num = number.as_u64().unwrap() as usize;
                let mut lock = GLOBAL_MAP.lock().unwrap();
                let mut stream = lock.remove(&num).unwrap();
                stream.write_all(b"HTTP/1.0 403 Denied\r\n\r\n").unwrap();
                "denied".into()
            }
            ("concat", words) => {
                let mut ret = String::new();
                for p in words {
                    ret += p.as_str().unwrap_or("");
                }
                ret.into()
            }
            ("duplicate", [s]) => {
                let s: String = s.as_str().unwrap().to_owned();
                (s.clone() + &s).into()
            }
            _ => todo!(),
        };
        let response = JsonRPCResponse {
            jsonrpc: "2.0",
            id: request.id,
            result,
        };
        println!("{}", serde_json::to_string(&response).unwrap());
        std::io::stdout().flush().unwrap();
    }
}

fn server_thread() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = 3128;
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;
    info!("Server listening on 127.0.0.1:{}", port);
    for stream in listener.incoming() {
        let stream = stream.expect("Connection failed");
        thread::spawn(move || {
            handle_connection(stream);
        });
    }
    Err("Should not arrive here.".into())
}

pub fn handle_connection(mut stream: TcpStream) {
    let current_count = GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst);
    info!("New connection established number: {}", current_count);
    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer).unwrap();
    if size == 0 {
        return;
    }
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);
    req.parse(&buffer).unwrap();
    let mut hostport_pair = req.path.unwrap();
    if hostport_pair.starts_with("https://") {
        hostport_pair = hostport_pair.strip_prefix("https://").unwrap();
    }
    let (host, port) = hostport_pair.split_once(":").unwrap();
    let port: u16 = port.parse().unwrap();
    let value = json!({
        "jsonrpc": "2.0",
        "method": "want",
        "params": [host, port, current_count]
    });
    println!("{}", serde_json::to_string(&value).unwrap());
    std::io::stdout().flush().unwrap();
    GLOBAL_MAP.lock().unwrap().insert(current_count, stream);
}
