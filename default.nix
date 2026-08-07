{
  pkgs ? import <nixpkgs> { },
  lib ? pkgs.lib,
  rustPlatform ? pkgs.rustPlatform,
}:

rustPlatform.buildRustPackage (finalAttrs: {
  pname = "jsonrpc-httpproxy";
  version = "0.0.1";
  src = lib.cleanSource ./.;

  cargoLock = {
    lockFile = ./Cargo.lock;
  };

  passthru.tests =
    let
      nixos-lib = import (pkgs.path + "/nixos/lib") { inherit (pkgs) lib; };

      # Controller-driven integration driver, uploaded into the VM. It plays
      # the controller: starts curl as the real client, waits for the
      # proxy's notification in the log, appends the JSON-RPC decision to
      # /tmp/cmds, and asserts the documented protocol end to end.
      driverScript = pkgs.writeText "proxy-driver.py" ''
        #!/usr/bin/env python3
        """Controller-driven integration test for jsonrpc-httpproxy.

        Usage: proxy-driver.py <tunnel|accept-file|deny|shutdown>
        """
        import json
        import socket
        import subprocess
        import sys
        import time

        CMDS = "/tmp/cmds"      # controller commands (tail -f -> proxy stdin)
        LOG = "/tmp/proxy.log"  # proxy stdout: notifications + responses
        PROXY = ("127.0.0.1", 3128)
        UPSTREAM = ("127.0.0.1", 8080)
        UPSTREAM_FILE = "${pkgs.darkhttpd.src}/README.md"

        def read_log():
            with open(LOG) as f:
                return f.read()

        def log_objects():
            objs = []
            for line in read_log().splitlines():
                try:
                    objs.append(json.loads(line))
                except ValueError:
                    pass  # stdout should be pure JSON-RPC; skip anything else
            return objs

        def notifications(method):
            return [o for o in log_objects() if o.get("method") == method]

        def wait_for(check, what, timeout=10):
            deadline = time.monotonic() + timeout
            while True:
                value = check()
                if value:
                    return value
                if time.monotonic() >= deadline:
                    raise AssertionError("timed out waiting for " + what)
                time.sleep(0.1)

        def wait_notification(method, since, timeout=10):
            """Wait for a notification of `method` newer than `since` (the
            number already in the log), and return it."""
            def check():
                n = notifications(method)
                return n[-1] if len(n) > since else None
            return wait_for(check, method + " notification", timeout)

        def send_command(req_id, method, params, timeout=10):
            """Append a JSON-RPC request to the controller input and wait
            for its result in the proxy log."""
            with open(CMDS, "a") as f:
                f.write(json.dumps({"jsonrpc": "2.0", "id": req_id,
                                    "method": method, "params": params}) + "\n")

            def check():
                for o in log_objects():
                    if o.get("id") == req_id and "result" in o:
                        return o["result"]
                return None
            return wait_for(check, "result for id %d" % req_id, timeout)

        def start_curl(extra_args):
            return subprocess.Popen(
                ["curl", "--silent", "--show-error"] + extra_args,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        def connect_and_notify(extra_args, method, conn_timeout=5):
            """Start curl, wait for the proxy's notification. Retries the
            connection a few times: the proxy reads the client's request in
            a single read(), so a split delivery drops it as a 'partial
            request' without any notification."""
            last = None
            for _ in range(3):
                since = len(notifications(method))
                proc = start_curl(extra_args)
                try:
                    notif = wait_notification(method, since, timeout=conn_timeout)
                    return proc, notif
                except AssertionError as e:
                    last = e
                    proc.kill()
                    proc.wait()
            raise AssertionError("proxy never sent a %s notification: %s"
                                 % (method, last))

        def finish_ok(proc, timeout=10):
            out, err = proc.communicate(timeout=timeout)
            if proc.returncode != 0:
                raise AssertionError("curl failed (%d): %s"
                                     % (proc.returncode, err.decode(errors="replace")))
            return out, err

        def test_tunnel():
            proc, notif = connect_and_notify(
                ["--proxytunnel", "-x", "http://127.0.0.1:3128", "-v",
                 "http://127.0.0.1:8080/darkhttpd.c"], "want")
            cid = notif["params"][-1]
            result = send_command(1, "accept", [cid, UPSTREAM[0], UPSTREAM[1]])
            assert result == "accepted", result
            out, err = finish_ok(proc)
            assert out.startswith(b"/*"), "unexpected tunnel body: %r" % out[:60]
            # curl's exact wording varies by version ("CONNECT tunneled, HTTP/1.1 200
            # Connection Established" vs "CONNECT tunnel established, response 200")
            assert b"Connection Established" in err or b"tunnel established" in err, err

        def test_accept_file():
            proc, notif = connect_and_notify(
                ["-x", "http://127.0.0.1:3128",
                 "http://127.0.0.1:8080/README.md"], "gethttp")
            cid = notif["params"][-1]
            result = send_command(2, "accept-file", [cid, UPSTREAM_FILE, "text/plain"])
            assert result == "accepted-file", result
            out, _ = finish_ok(proc)
            assert b"darkhttpd" in out, "unexpected file body: %r" % out[:100]

        def test_deny():
            proc, notif = connect_and_notify(
                ["--proxytunnel", "-x", "http://127.0.0.1:3128", "-o", "/dev/null",
                 "http://127.0.0.1:8080/"], "want")
            cid = notif["params"][-1]
            result = send_command(3, "deny", [cid])
            assert result == "denied", result
            _, err = proc.communicate(timeout=10)
            assert b"403" in err, err

        def test_shutdown():
            proc, notif = connect_and_notify(
                ["--proxytunnel", "-x", "http://127.0.0.1:3128", "-o", "/dev/null",
                 "http://127.0.0.1:8080/"], "want")
            result = send_command(4, "shutdown", [])
            assert result == "shutting down", result
            _, err = proc.communicate(timeout=10)
            assert b"502" in err, err
            # the proxy exits after shutdown; the listener must close
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    s = socket.create_connection(PROXY, timeout=1)
                    s.close()
                except OSError:
                    return
                time.sleep(0.1)
            raise AssertionError("proxy still listening after shutdown")

        TESTS = {
            "tunnel": test_tunnel,
            "accept-file": test_accept_file,
            "deny": test_deny,
            "shutdown": test_shutdown,
        }

        def main():
            if len(sys.argv) != 2 or sys.argv[1] not in TESTS:
                print("usage: proxy-driver.py <tunnel|accept-file|deny|shutdown>",
                      file=sys.stderr)
                return 2
            try:
                TESTS[sys.argv[1]]()
            except Exception as e:
                print("FAILED: %r" % (e,), file=sys.stderr)
                print("=== proxy log ===", file=sys.stderr)
                print(read_log(), file=sys.stderr)
                return 1
            print("OK")
            return 0

        if __name__ == "__main__":
            sys.exit(main())
      '';
    in
    {
      jsonrpc-httpproxy = nixos-lib.runTest {
        hostPkgs = pkgs;
        name = "jsonrpc-httpproxy";

        nodes.machine =
          { pkgs, ... }:
          {
            environment.systemPackages = [
              # Run the VM test against a DEBUG build so debug_assert!s
              # (e.g. tokio's check_socket_for_blocking) are exercised.
              (finalAttrs.finalPackage.overrideAttrs { cargoBuildType = "debug"; })
              pkgs.curl
              pkgs.python3
            ];
            # Local upstream HTTP server, so the test needs no internet
            # access. Serves darkhttpd's own source tree; darkhttpd.c is a
            # good non-trivial file to fetch through the proxy.
            # Note: darkhttpd 1.17 rejects --addr 127.0.0.1 combined with
            # --ipv6 (added by the module when networking.enableIPv6 is on,
            # as it is by default in the test VM). Disable IPv6.
            networking.enableIPv6 = false;
            services.darkhttpd = {
              enable = true;
              rootDir = "${pkgs.darkhttpd.src}";
              port = 8080;
            };
          };

        testScript = ''
          machine.wait_for_unit("darkhttpd.service")
          machine.wait_for_open_port(8080)

          # Controller interface: the proxy reads JSON-RPC commands from
          # stdin and writes notifications + responses to stdout. Feed
          # stdin from `tail -f` on a regular file: commands are appended
          # with `>> /tmp/cmds`, and tail (which never exits) forwards them
          # through a real pipe to the proxy. This avoids fifo semantics
          # (which proved unreliable here) and the EOF problem (a fifo read
          # EOFs when all write ends close; tail -f never EOFs).
          machine.succeed(": > /tmp/cmds")
          machine.succeed("tail -f /tmp/cmds | jsonrpc-httpproxy > /tmp/proxy.log 2>&1 &")
          machine.wait_for_open_port(3128)

          machine.copy_from_host("${driverScript}", "/tmp/proxy-driver.py")

          with subtest("CONNECT tunnel (accept)"):
              machine.succeed("python3 /tmp/proxy-driver.py tunnel")

          with subtest("GET + accept-file"):
              machine.succeed("python3 /tmp/proxy-driver.py accept-file")

          with subtest("deny"):
              machine.succeed("python3 /tmp/proxy-driver.py deny")

          with subtest("shutdown drains pending connections with 502"):
              machine.succeed("python3 /tmp/proxy-driver.py shutdown")
        '';
      };
    };

  meta = with lib; {
    license = licenses.agpl3Plus;
    maintainers = with lib.maintainers; [ nagy ];
    platforms = platforms.linux;
    mainProgram = "jsonrpc-httpproxy";
  };
})
