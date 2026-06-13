# AGENTS.md — jsonrpc_httpproxy

A JSON-RPC-controlled HTTP/HTTPS intercepting proxy. It accepts client
connections but **defers all routing decisions** to an external "controller"
process via line-delimited JSON-RPC over stdin/stdout.

## Architecture

Two concurrent components run inside a single binary.
The `TcpStream` never crosses thread boundaries — the connection thread
keeps ownership of the socket for its entire lifetime.

```
┌─────────────── client connections ─────────────────────────────┐
│                                                                 │
│  server_thread (TcpListener :3128)                              │
│    │  per-connection: handle_connection()  ← long-lived thread  │
│    │    → parses CONNECT / GET                                  │
│    │    → prints "want" / "gethttp" notification                │
│    │    → parks SyncSender<Decision> in GLOBAL_MAP              │
│    │    → blocks on rx.recv()                                   │
│    │    → on Decision::Accept: tunnels via tokio inline         │
│    │    → on Decision::AcceptFile: serve_file()                 │
│    │    → on Decision::Deny: deny_connection()                  │
│    │    → on Decision::Shutdown / channel close: send_502()     │
│    │                                                            │
│  ┌─ GLOBAL_MAP: HashMap<u64, SyncSender<Decision>>   (tiny!)    │
│  │                                                            │
│  main thread (stdin JSON-RPC loop)                             │
│    ← controller sends accept / accept-file / deny / shutdown   │
│    → sends Decision through channel (no I/O on stream)         │
│    → responds on stdout with JSON-RPC result                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
        ↑ stdout (notifications + responses)
        ↓ stdin  (commands)
   ┌──────────────┐
   │  Controller  │  (external process — not in this repo)
   └──────────────┘
```

## JSON-RPC protocol

All messages are single-line JSON. The server talks JSON-RPC 2.0.

### Server → controller (stdout, notifications — no `id`)

| method     | params                                         | when                           |
|------------|------------------------------------------------|--------------------------------|
| `want`     | `["CONNECT", host, port, connection_id]`       | client sent CONNECT            |
| `gethttp`  | `["GET", "http://...", connection_id]`         | client sent GET to http:// URL |

### Controller → server (stdin, requests — have `id`)

| method       | params                                  | effect                                                   |
|------------- |-----------------------------------------|----------------------------------------------------------|
| `accept`     | `[connection_id, host, port]`          | tunnel to upstream via tokio                              |
| `accept-file`| `[connection_id, filepath, mimetype]`  | serve a local file as HTTP response                       |
| `deny`       | `[connection_id]`                      | respond HTTP 403 Forbidden                                |
| `shutdown`   | (none)                                  | send Shutdown to all pending channels, then exit          |

### Server → controller (stdout, responses — echo `id`)

Success:
```json
{"jsonrpc":"2.0","id":<n>,"result":"accepted"}
{"jsonrpc":"2.0","id":<n>,"result":"accepted-file"}
{"jsonrpc":"2.0","id":<n>,"result":"denied"}
{"jsonrpc":"2.0","id":<n>,"result":"shutting down"}
```

Error (standard JSON-RPC 2.0 error object):
```json
{"jsonrpc":"2.0","id":<n>,"error":{"code":-32602,"message":"unknown connection id: 5"}}
{"jsonrpc":"2.0","id":<n>,"error":{"code":-32601,"message":"unknown method: bogus"}}
{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error: ..."}}
{"jsonrpc":"2.0","id":<n>,"error":{"code":-32600,"message":"jsonrpc must be '2.0'"}}
```

## Key implementation details

### Channel-per-connection architecture

Instead of parking the raw `TcpStream` in a global map and transferring it
across threads, the connection thread **keeps ownership** of the socket for
its entire lifetime.  Only a tiny `SyncSender<Decision>` is stored in
`GLOBAL_MAP`:

```rust
LazyLock<Mutex<HashMap<u64, SyncSender<Decision>>>>
```

The connection thread blocks on `rx.recv()` until the controller (via the
main thread) pushes a `Decision` through the channel.  The stream never
leaves its thread — no `set_nonblocking` + `from_std` dance at decision time.

#### Decision enum

```rust
enum Decision {
    Accept { host: String, port: u16 },
    AcceptFile { path: String, mimetype: String },
    Deny,
    Shutdown,
}
```

#### Benefits over the old cross-thread-stream approach

- **Stream ownership is clean**: one thread per connection from accept to
  close, no cross-thread fd juggling.
- **Main thread does no I/O on client sockets**: it only sends decisions
  through channels — fast, non-blocking, can't panic on a dead socket.
- **Shutdown is free**: when the process drops all `SyncSender`s (or sends
  `Decision::Shutdown`), every blocked `recv()` unblocks.  Each connection
  thread sends its own 502 and exits.  No central `drain_connections()`.
- **Leftover bytes stay local**: `handle_connection` captures them after
  `httparse` and holds them in a stack variable until the tunnel starts.

### CONNECT tunnel (`tunnel`)

- Pure Rust — no external `nc` dependency.
- The `handle_connection` thread builds a single-threaded tokio runtime
  inline and calls `block_on(tunnel(...))`.  One OS thread per active tunnel.
- `TcpStream::into_split()` splits client and upstream each into read/write
  halves.  Two `tokio::spawn` tasks copy concurrently with proper TCP
  half-close via `shutdown()`.
- **`Nagle` is disabled** (`set_nodelay(true)`) for low-latency forwarding.
- **Leftover bytes** (e.g. the TLS ClientHello) are forwarded to upstream
  before the copy loop starts.

### Error handling

All JSON-RPC-level errors are reported to the controller rather than
crashing the proxy.  A lightweight `JsonRpcError` struct carries standard
JSON-RPC 2.0 error codes:

| code     | constant          | returned when                                |
|----------|-------------------|----------------------------------------------|
| `-32700` | `PARSE_ERROR`     | stdin line is not valid JSON                  |
| `-32600` | `INVALID_REQUEST` | `jsonrpc` field is not `"2.0"`                |
| `-32601` | `METHOD_NOT_FOUND`| unknown method name                          |
| `-32602` | `INVALID_PARAMS`  | wrong param type, unknown connection id      |

Two tiny helpers validate parameters:

```rust
fn require_u64(val: &Value, pos: usize) -> Result<u64, JsonRpcError>;
fn require_str(val: &Value, pos: usize) -> Result<&str, JsonRpcError>;
```

The dispatch closure returns `Result<serde_json::Value, JsonRpcError>`.  At
the boundary this is mapped to either `{"result": …}` or `{"error": …}` and
written to stdout via `send_response()`.  There is no `catch_unwind` — errors
are first-class return values.

### Response helpers

`send_502()`, `deny_connection()`, and `serve_file()` are called from the
connection thread to write HTTP responses directly to the client socket.
They use `write_http11` internally and silently ignore I/O errors (the
client may have disconnected).

`send_response()` writes a JSON-RPC response/error to stdout from the main
thread.  It also ignores I/O errors — if stdout is broken the proxy is
unusable anyway.

### HTTP response writer (`write_http11`)

Generic over `W: Write`. Handles both HTTP/1.0 and HTTP/1.1 with status
line, headers, and body.

## Configuration

| env var | default | meaning              |
|---------|---------|----------------------|
| `PORT`  | `3128`  | TCP listen port      |

Bind address is hardcoded to `127.0.0.1`.

Log level via `RUST_LOG` (uses `colog` → `env_logger`).

## Build & run

```bash
cargo build
echo '{"jsonrpc":"2.0","id":1,"method":"accept","params":[0,"example.com",443]}' \
  | ./target/debug/jsonrpc_httpproxy
```

```bash
nix-build default.nix   # or: nix build
```

## Known limitations / future work

1. ~~**No timeout on pending connections**~~ — resolved: client read has 5 s
   timeout (→ 408), controller decision has 30 s timeout (→ 504).
2. **Two threads write to stdout** (connection threads for notifications,
   main thread for responses). Line-delimited JSON usually survives
   interleaving, but a single-writer mpsc channel would be safer.
3. **SIGINT handler doesn't drain** — it calls `process::exit(0)` to avoid
   potential deadlock on the `GLOBAL_MAP` mutex.  Clients get TCP RST instead
   of a graceful 502.  The JSON-RPC `shutdown` method *does* drain gracefully.
4. **`accept-file` and `deny` throw away leftover bytes** — if the client sent
   data after its request line on a GET or a denied CONNECT, those bytes are
   silently dropped.
5. **One OS thread per connection + one tokio runtime per tunnel** — fine for
   low-volume use; a full async rewrite with a shared tokio runtime would be
   needed for hundreds of concurrent connections.
6. **Blocking `std::fs::read` in `accept-file`** — blocks the connection
   thread while reading the file.
