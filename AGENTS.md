# AGENTS.md — jsonrpc-httpproxy

## Purpose

A JSON-RPC-controlled HTTP/HTTPS intercepting proxy. It accepts client connections
and defers every routing decision to an external controller process through
line-delimited JSON-RPC over stdin and stdout. Edition 2021. License: MIT. One
binary, `jsonrpc-httpproxy`.

## Layout

- `main.rs` — the whole crate. The `[lib]` target points at it
  (`[lib] path = "main.rs"`), so no separate lib file is tracked.
- `test/default.nix` — NixOS VM integration test, wired into the flake as
  `checks.jsonrpc-httpproxy-vm`.
- `flake.nix` — flake-parts with crane and treefmt-nix: package, checks, VM test,
  devShell.
- `Cargo.lock` — committed. The Nix build needs it.

## Build and test

- `nix flake check` — cargo tests, doctests, clippy, treefmt check, VM test. Gate
  for a commit.
- `nix build` — release package at `result/bin/jsonrpc-httpproxy`.
- `nix build .#jsonrpc-httpproxy-doc` — static rustdoc HTML.
- `nix build .#checks.x86_64-linux.jsonrpc-httpproxy-vm` — VM test alone.
- `cargo build` — local loop.
- `nix fmt` — treefmt drives nixfmt, rustfmt (unstable options
  `imports_granularity = "Crate"`, `group_imports = "StdExternalCrate"`,
  `wrap_comments = false`) and taplo. No `rustfmt.toml` in the repo.
- Smoke test from stdin: `echo '{"jsonrpc":"2.0","id":1,"method":"accept",
  "params":[0,"example.com",443]}' | ./target/debug/jsonrpc-httpproxy`.

`checks.doctests` runs `cargo test --doc`. `checks.clippy` runs `cargo clippy
--all-targets --all-features -- --deny warnings` through `craneLib.cargoClippy`.
The crate declares no cargo features, so `--all-features` is a no-op today and
keeps the gate correct if a feature lands. Any clippy warning fails the check.

### VM test

`test/default.nix` wires the test as `checks.jsonrpc-httpproxy-vm`. Crane builds a
separate DEBUG package for it (`env.CARGO_PROFILE = "dev"` with its own
`buildDepsOnly` artifacts), so tokio's `debug_assert!`s run while the main build
stays release.

The test boots a VM, starts the proxy with its controller interface fed by `tail -f
/tmp/cmds | jsonrpc-httpproxy`, serves darkhttpd's own source tree locally as the
upstream, and runs four subtests. The client side uses `curl`. The controller side
is a Python driver, `proxy-driver.py`, a `writeText` derivation copied into the VM,
which waits for a notification in the log, appends the matching decision to
`/tmp/cmds`, and asserts the documented protocol.

1. CONNECT tunnel through `accept`: `200 Connection Established`, then a GET
   through the tunnel returns `200 OK`.
2. GET plus `accept-file`: `gethttp` notification, file served to curl.
3. `deny`: curl receives `403 Forbidden`.
4. `shutdown` drain: a pending connection receives `502 Bad Gateway`, then the
   proxy exits.

## Architecture

Two concurrent components run in one binary. The `TcpStream` never crosses a
thread boundary, because the connection thread keeps ownership of the socket for
its lifetime.

```
client connections
  server_thread (TcpListener :3128)
    per-connection handle_connection()  <- long-lived thread
      parse CONNECT / GET
      print "want" / "gethttp" notification
      park SyncSender<Decision> in GLOBAL_MAP
      block on rx.recv()
      Decision::Accept      -> tokio tunnel inline
      Decision::AcceptFile  -> serve_file()
      Decision::Deny        -> deny_connection()
      Decision::Shutdown    -> send_502()
  GLOBAL_MAP: HashMap<u64, SyncSender<Decision>>   (tiny)
  main thread (stdin JSON-RPC loop)
    controller sends accept / accept-file / deny / shutdown
    send Decision through the channel (no I/O on the stream)
    respond on stdout with a JSON-RPC result
```

### JSON-RPC protocol

All messages are single-line JSON, JSON-RPC 2.0.

Server to controller, on stdout, notifications without `id`:

| method | params | when |
|---|---|---|
| `want` | `["CONNECT", host, port, connection_id]` | client sent CONNECT |
| `gethttp` | `["GET", "http://...", connection_id]` | client sent GET to an http URL |

Controller to server, on stdin, requests with `id`:

| method | params | effect |
|---|---|---|
| `accept` | `[connection_id, host, port]` | tunnel upstream through tokio |
| `accept-file` | `[connection_id, filepath, mimetype]` | serve a local file |
| `deny` | `[connection_id]` | respond 403 Forbidden |
| `shutdown` | none | send Shutdown to every pending channel, then exit |

Server to controller, on stdout, responses echoing `id`. Success results:
`"accepted"`, `"accepted-file"`, `"denied"`, `"shutting down"`. Errors use the
standard JSON-RPC 2.0 error object with codes `-32700` (parse error), `-32600`
(`jsonrpc` not `"2.0"`), `-32601` (unknown method), `-32602` (bad param type,
unknown connection id, port out of range).

### Channel per connection

The connection thread keeps ownership of the socket for its lifetime. Only a tiny
`SyncSender<Decision>` reaches `GLOBAL_MAP`:

```rust
LazyLock<Mutex<HashMap<u64, SyncSender<Decision>>>>
```

The thread blocks on `rx.recv()` until the main thread pushes a `Decision`.

```rust
enum Decision {
    Accept { host: String, port: u16 },
    AcceptFile { path: String, mimetype: String },
    Deny,
    Shutdown,
}
```

Benefits over the old cross-thread-stream approach: one thread per connection with
no cross-thread fd juggling, a main thread that does no I/O on a client socket,
free shutdown because dropping the senders unblocks every `recv()`, and leftover
bytes that stay local because `handle_connection` holds them in a stack variable
until the tunnel starts.

### CONNECT tunnel

- Pure Rust. No external `nc`.
- `connect_upstream()` runs first, still on the connection thread. It resolves the
  host, filters addresses by the CLI family, and tries each with
  `TcpStream::connect_timeout`, 10s per address. The `200 Connection Established`
  line is written only after the connect succeeds. A family miss or a refused
  connect gets `502 Bad Gateway`.
- `tunnel()` receives an already-connected upstream socket. The
  `handle_connection` thread builds a single-threaded tokio runtime inline and
  calls `block_on(tunnel(...))`, which costs one OS thread per active tunnel.
- `TcpStream::from_std()` requires a nonblocking fd, and tokio debug-asserts this
  at registration. A blocking fd would also block the whole single-threaded
  runtime.
- `TcpStream::into_split()` splits each peer into read and write halves. Two
  `tokio::spawn` tasks copy concurrently with proper TCP half-close through
  `shutdown()`.
- `Nagle` stays disabled (`set_nodelay(true)`).
- Leftover bytes, such as the TLS ClientHello, reach upstream before the copy loop
  starts.

### Error handling

All JSON-RPC errors go to the controller instead of crashing the proxy. A
`JsonRpcError` struct carries the JSON-RPC 2.0 codes. Two helpers validate
parameters: `require_u64(val, pos)` and `require_str(val, pos)`. The dispatch
closure returns `Result<serde_json::Value, JsonRpcError>`, and the boundary maps
that to `{"result": ...}` or `{"error": ...}` and writes it through
`send_response()`.

`JsonRpcError` stays a plain `Debug + Clone + PartialEq + Eq + Serialize` struct,
not an error type, because the JSON-RPC layer reports failures as protocol error
objects and needs no `Display`. The server thread uses a separate `ProxyError`
enum with `derive_more::Display`, `Error` and `From`:

```rust
pub enum ProxyError {
    Listener(#[from] std::io::Error),
    InvalidPort(#[error(ignore)] String),
}
```

### Domain newtypes

`Port(u16)` and `ConnectionId(u64)` wrap the raw primitives at API boundaries.
`Port` carries a zero-cost `derive_more::Display`. Both derive `Debug`, `Clone`,
`Copy`, `PartialEq` and `Eq`, and `ConnectionId` adds `Hash`. They stop a caller
from swapping a port with another number argument.

### Response helpers

`send_502()`, `deny_connection()` and `serve_file()` run on the connection thread
and write HTTP responses straight to the client socket through `write_http11`.
They ignore I/O errors, because the client may have disconnected.
`send_response()` writes a JSON-RPC response to stdout from the main thread and
also ignores I/O errors. `write_http11` is generic over `W: Write` and handles
HTTP/1.0 and HTTP/1.1 with status line, headers and body.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `3128` | TCP listen port. |
| `RUST_LOG` | Info | Log level through `colog` and `env_logger`. `debug` dumps the JSON-RPC requests. |

| Flag | Meaning |
|---|---|
| `--force-ipv4` | Restrict outbound connects to IPv4. |
| `--force-ipv6` | Restrict outbound connects to IPv6. |

Bind address stays hardcoded to `127.0.0.1`. An unparsable `PORT` returns
`ProxyError::InvalidPort`, which `main()` logs before exit 1. `clap` rejects the
two family flags together. The family filter touches outbound connects alone, and
it counts an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) as IPv6.

## Rules

- Flush stdout for the `want` and `gethttp` notifications. Without the flush,
  `println!` on a redirected stdout is block-buffered, and the notification sits in
  the buffer while a controller reads line by line.
- Call `set_nonblocking(true)` on both sockets right before the handover to tokio.
  The stream still stays on its own thread, and only the fd blocking mode changes.
- Report a JSON-RPC failure through the error object. No `catch_unwind`, because
  errors stay first-class return values.
- Keep the clippy gate at `--deny warnings`.
- Do not add a `rustfmt.toml`. The rustfmt options live in `flake.nix` under
  treefmt.
- Keep `Cargo.lock` committed. The Nix build has no network.
- `git add` a new file before a build, because the flake source holds tracked files
  alone.

## Conventions

- Wrap a raw primitive in a newtype at an API boundary, as `Port` and
  `ConnectionId` do.
- Derive `Display`, `Error` and `From` with `derive_more`.
- Keep comments out of the wrapped state. rustfmt runs with `wrap_comments =
  false`.
- Log through `colog` and `env_logger`. A `debug!` line needs `RUST_LOG=debug`.
- Name a helper after the action it performs: `send_502`, `deny_connection`,
  `serve_file`, `send_response`, `write_http11`.

## Open work

1. No timeout on a pending connection. Resolved: the client read has a 5s timeout
   that answers 408, and the controller decision has a 30s timeout that answers
   504.
2. Every connection thread writes to stdout, one notification per thread plus the
   main thread responses, so N+1 writers. Lines do not tear, because every writer
   goes through the process-wide stdout lock, but notification and response
   ordering stays arbitrary, so a controller must correlate by `connection_id` and
   `id`. A single-writer mpsc channel would be safer.
3. The SIGINT handler does not drain. It calls `process::exit(0)` to avoid a
   potential deadlock on the `GLOBAL_MAP` mutex, so clients get a TCP RST instead
   of a graceful 502. The JSON-RPC `shutdown` method does drain: it sleeps 500ms
   after notifying channels, so the connection threads can write their 502s before
   `process::exit`.
4. `accept-file`, `deny` and the 504 timeout throw away leftover bytes. Data a
   client sent after its request line on a GET or a denied CONNECT drops silently.
   On `accept` those bytes reach upstream.
5. One OS thread per connection plus one tokio runtime per tunnel. Fine for
   low-volume use. Hundreds of concurrent connections need a full async rewrite
   with a shared tokio runtime.
6. Blocking work on the connection thread: `std::fs::read` in `accept-file`, plus
   `getaddrinfo` and `connect_timeout` in the `accept` path. One slow resolver
   holds only its own connection.
7. `accept` on a GET connection tunnels the raw request, because the dispatcher
   does not track CONNECT against GET. Only `accept-file` and `deny` carry meaning
   for a GET connection.
8. A poisoned `GLOBAL_MAP` mutex panics, because `handle_connection` and the
   dispatch loop call `expect` on the lock. CONNECT target parsing no longer
   panics: `parse_target()` returns `TargetError` and the client gets `400 Bad
   Request`. `server_thread()` propagates listener errors instead of panicking.
