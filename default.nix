{
  pkgs ? import <nixpkgs> { },
  lib ? pkgs.lib,
  rustPlatform ? pkgs.rustPlatform,
}:

rustPlatform.buildRustPackage (finalAttrs: {
  pname = "jsonrpc_httpproxy";
  version = "0.0.1";
  src = lib.cleanSource ./.;
  cargoLock = {
    lockFile = ./Cargo.lock;
  };
})
