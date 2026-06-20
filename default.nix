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

  meta = with lib; {
    license = licenses.agpl3Plus;
    maintainers = with lib.maintainers; [ nagy ];
    platforms = platforms.linux;
    mainProgram = "jsonrpc-httpproxy";
  };
})
