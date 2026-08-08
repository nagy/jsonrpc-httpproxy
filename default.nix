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

  passthru.tests."jsonrpc-httpproxy" = import ./test/default.nix {
    inherit pkgs lib;
    jsonrpc-httpproxy = finalAttrs.finalPackage;
  };

  meta = with lib; {
    description = "JSON-RPC-controlled HTTP/HTTPS intercepting proxy";
    license = licenses.agpl3Plus;
    maintainers = with lib.maintainers; [ nagy ];
    platforms = platforms.linux;
    mainProgram = "jsonrpc-httpproxy";
  };
})
