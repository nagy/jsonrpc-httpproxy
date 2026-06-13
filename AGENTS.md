# AGENTS.md — jsonrpc_httpproxy

A JSON-RPC-controlled HTTP/HTTPS intercepting proxy. It accepts client
connections but **defers all routing decisions** to an external "controller"
process via line-delimited JSON-RPC over stdin/stdout.

## Architecture

Two concurrent components run inside a single binary:

```
┌─────────────── client connections ───────────────┐
│                                                   │
│  server_thread (TcpListener :3128)                │
│    │  per-connection: handle_connection()         │
│    │    → parses CONNECT / GET                    │
│    │    → prints "want" / "gethttp" notification  │
│    │    → parks (TcpStream, leftover) in GLOBAL_MAP │
│    │                                              │
│  ┌─ GLOBAL_MAP: HashMap<u64, (TcpStream, Vec<u8>)> │
│  │                                              │
│  main thread (stdin JSON-RPC loop)               │
│    ← controller sends accept / accept-file / deny │
│    → responds on stdout with JSON-RPC result      │
│                                                   │
└───────────────────────────────────────────────────┘
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

### Server → controller (stdout, responses — echo `id`)

```json
{"jsonrpc":"2.0","id":<n>,"result":"accepted"}
{"jsonrpc":"2.0","id":<n>,"result":"accepted-file"}
{"jsonrpc":"2.0","id":<n>,"result":"denied"}
```

## Key implementation details

### CONNECT tunnel (`tunnel` + `spawn_tunnel`)

- Pure Rust — no external `nc` dependency.
- Uses tokio with a **single-threaded `current_thread` runtime per tunnel**,
  spawned on its own OS thread. This keeps the rest of the code synchronous
  while avoiding a global async rewrite.
- `TcpStream::into_split()` splits client and upstream each into read/write
  halves. Two `tokio::spawn` tasks copy concurrently. When one read-half hits
  EOF the corresponding write-half is shut down (proper TCP half-close).
- **`Nagle` is disabled** (`set_nodelay(true)`) on the tokio stream for
  low-latency forwarding.
- **Leftover bytes**: after `httparse` parses the CONNECT line, any remaining
  bytes in the 1024-byte read buffer (e.g. the TLS ClientHello) are stored as
  `Vec<u8>` in `GLOBAL_MAP` and forwarded to upstream before the copy loop
  starts.

### GLOBAL_MAP

```rust
LazyLock<Mutex<HashMap<u64, (TcpStream, Vec<u8>)>>>
```

Key: monotonically incrementing `AtomicU64` counter.
Value: the raw blocking `std::net::TcpStream` + leftover bytes.

The stream sits idle (blocking, not read) while waiting for the controller's
decision.

### HTTP response writer (`write_http11`)

Generic over `W: Write`. Used by `accept-file` and `deny` to send proper HTTP
responses with status line, headers, and body. Handles both HTTP/1.0 and
HTTP/1.1.

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

1. **No timeout on pending connections** — if the controller never sends
   `accept`/`deny`, the stream stays in `GLOBAL_MAP` forever and the client
   hangs indefinitely.
2. **`unwrap()` / `expect()` scattered throughout** — malformed JSON, missing
   map entries, or unexpected parameter types will panic the control thread
   (and drop all in-flight connections).
3. **Two threads write to stdout** (server thread for notifications, main
   thread for responses). Line-delimited JSON usually survives interleaving,
   but a single-writer mpsc channel would be safer.
4. **No graceful shutdown** — no signal handler, no `shutdown` JSON-RPC method.
5. **`accept-file` and `deny` throw away leftover bytes** — if the client sent
   data after its request line on a GET or a denied CONNECT, those bytes are
   silently dropped.
6. **One OS thread per connection + one tokio runtime per tunnel** — fine for
   low-volume use; a full async rewrite with a shared tokio runtime would be
   needed for hundreds of concurrent connections.
7. **Blocking `std::fs::read` in `accept-file`** — blocks the main JSON-RPC
   loop while reading the file.
