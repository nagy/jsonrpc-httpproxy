{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  name = "jsonrpc-httpproxy";

  nativeBuildInputs = [
    pkgs.cargo
    pkgs.rustc
    pkgs.rustfmt
    pkgs.clippy
  ];
}
