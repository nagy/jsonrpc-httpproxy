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

      # Controller-driven proxy test scripts, uploaded into the VM. Each
      # script drives the proxy through its JSON-RPC controller interface
      # (commands appended to /tmp/cmds, which `tail -f` feeds to the
      # proxy's stdin; notifications + responses read from /tmp/proxy.log)
      # and a real client socket.
      tunnelScript = pkgs.writeText "proxy-tunnel.sh" ''
        #!/usr/bin/env bash
        set -euo pipefail

        # The proxy reads the client's request in a single read(); a slow or
        # split delivery can produce a "partial request" drop, so retry the
        # whole exchange a few times.
        for attempt in 1 2 3; do
          exec 3<>/dev/tcp/127.0.0.1/3128
          sleep 0.2
          printf 'CONNECT 127.0.0.1:8080 HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n\r\n' >&3
          n=0; until grep -q '"method":"want"' /tmp/proxy.log || [ "$n" -ge 50 ]; do sleep 0.1; n=$((n+1)); done
          if grep -q '"method":"want"' /tmp/proxy.log; then
            break
          fi
          exec 3<&-
        done
        grep -q '"method":"want"' /tmp/proxy.log

        CID=$(grep '"method":"want"' /tmp/proxy.log | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["params"][-1])')
        echo "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"accept\",\"params\":[$CID,\"127.0.0.1\",8080]}" >> /tmp/cmds
        head -1 <&3 | grep '200 Connection Established'
        n=0; until grep -q '"result":"accepted"' /tmp/proxy.log || [ "$n" -ge 50 ]; do sleep 0.1; n=$((n+1)); done
        grep -q '"result":"accepted"' /tmp/proxy.log
        # tunnel is up: send a GET through it and check the response
        printf 'GET /darkhttpd.c HTTP/1.0\r\nHost: 127.0.0.1:8080\r\n\r\n' >&3
        head -1 <&3 | grep '200 OK'
        exec 3<&-
      '';

      acceptFileScript = pkgs.writeText "proxy-accept-file.sh" ''
        #!/usr/bin/env bash
        set -euo pipefail
        # GET through the proxy triggers a gethttp notification; curl is
        # backgrounded because the response only comes after accept-file.
        curl -s -x http://127.0.0.1:3128 http://127.0.0.1:8080/README.md > /tmp/curl.out &
        n=0; until grep -q '"method":"gethttp"' /tmp/proxy.log || [ "$n" -ge 100 ]; do sleep 0.1; n=$((n+1)); done
        CID=$(grep '"method":"gethttp"' /tmp/proxy.log | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["params"][-1])')
        echo "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"accept-file\",\"params\":[$CID,\"${pkgs.darkhttpd.src}/README.md\",\"text/plain\"]}" >> /tmp/cmds
        n=0; until grep -q '"result":"accepted-file"' /tmp/proxy.log || [ "$n" -ge 100 ]; do sleep 0.1; n=$((n+1)); done
        grep -q '"result":"accepted-file"' /tmp/proxy.log
        wait
        grep -q 'darkhttpd' /tmp/curl.out
      '';

      denyScript = pkgs.writeText "proxy-deny.sh" ''
        #!/usr/bin/env bash
        set -euo pipefail
        for attempt in 1 2 3; do
          exec 5<>/dev/tcp/127.0.0.1/3128
          sleep 0.2
          printf 'CONNECT 127.0.0.1:8080 HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n\r\n' >&5
          n=0; until grep -q '"method":"want"' /tmp/proxy.log || [ "$n" -ge 50 ]; do sleep 0.1; n=$((n+1)); done
          if grep -q '"method":"want"' /tmp/proxy.log; then
            break
          fi
          exec 5<&-
        done
        grep -q '"method":"want"' /tmp/proxy.log
        CID=$(grep '"method":"want"' /tmp/proxy.log | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["params"][-1])')
        echo "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"deny\",\"params\":[$CID]}" >> /tmp/cmds
        head -1 <&5 | grep '403 Forbidden'
        n=0; until grep -q '"result":"denied"' /tmp/proxy.log || [ "$n" -ge 50 ]; do sleep 0.1; n=$((n+1)); done
        grep -q '"result":"denied"' /tmp/proxy.log
        exec 5<&-
      '';

      shutdownScript = pkgs.writeText "proxy-shutdown.sh" ''
        #!/usr/bin/env bash
        set -euo pipefail
        # leave a connection pending so we can observe the graceful 502 drain
        exec 6<>/dev/tcp/127.0.0.1/3128
        printf 'CONNECT 127.0.0.1:8080 HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n\r\n' >&6
        n=0; until grep -q '"method":"want"' /tmp/proxy.log || [ "$n" -ge 100 ]; do sleep 0.1; n=$((n+1)); done
        echo '{"jsonrpc":"2.0","id":4,"method":"shutdown","params":[]}' >> /tmp/cmds
        head -1 <&6 | grep '502 Bad Gateway'
        n=0; until grep -q '"result":"shutting down"' /tmp/proxy.log || [ "$n" -ge 100 ]; do sleep 0.1; n=$((n+1)); done
        grep -q '"result":"shutting down"' /tmp/proxy.log
        exec 6<&-
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

          machine.copy_from_host("${tunnelScript}", "/tmp/tunnel.sh")
          machine.copy_from_host("${acceptFileScript}", "/tmp/accept-file.sh")
          machine.copy_from_host("${denyScript}", "/tmp/deny.sh")
          machine.copy_from_host("${shutdownScript}", "/tmp/shutdown.sh")

          with subtest("CONNECT tunnel (accept)"):
              machine.succeed("bash /tmp/tunnel.sh")

          with subtest("GET + accept-file"):
              machine.succeed("bash /tmp/accept-file.sh")

          with subtest("deny"):
              machine.succeed("bash /tmp/deny.sh")

          with subtest("shutdown drains pending connections with 502"):
              machine.succeed("bash /tmp/shutdown.sh")
              # the proxy exits after shutdown; the listener must close
              machine.wait_until_fails("nc -z 127.0.0.1 3128")
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
