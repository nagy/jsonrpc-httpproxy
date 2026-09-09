{
  description = "JSON-RPC-controlled HTTP/HTTPS intercepting proxy (crane)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{
      flake-parts,
      nixpkgs,
      crane,
      treefmt-nix,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [ treefmt-nix.flakeModule ];
      systems = [ "x86_64-linux" ];

      perSystem =
        {
          system,
          pkgs,
          lib,
          config,
          ...
        }:
        let
          craneLib = crane.mkLib pkgs;

          # Pure Rust crate: no -sys dependencies and no build.rs, so there
          # are no native libraries to link, no pkg-config/bindgen probes,
          # and hence no libInputs or env block at all.
          libInputs = [ ];

          # Source as seen by cargo: everything tracked by git.
          src = craneLib.cleanCargoSource ./.;

          # Cargo.lock has no network access during the build; fetch deps
          # first, then feed them to cargo through the registry cache.
          cargoArtifacts = craneLib.buildDepsOnly {
            inherit src;
          };

          pkg = craneLib.buildPackage {
            inherit src cargoArtifacts;
            meta = {
              description = "JSON-RPC-controlled HTTP/HTTPS intercepting proxy";
              license = lib.licenses.agpl3Plus;
              maintainers = with lib.maintainers; [ nagy ];
              platforms = lib.platforms.linux;
              mainProgram = "jsonrpc-httpproxy";
            };
          };

          # Debug build for the NixOS VM integration test, so tokio's
          # debug_assert!s (e.g. check_socket_for_blocking) are exercised
          # while the main build stays release. The dep artifacts must be
          # rebuilt with the same profile (CARGO_PROFILE is part of the
          # artifact hash); reusing the release artifacts would silently
          # mix profiles.
          cargoArtifactsDebug = craneLib.buildDepsOnly {
            inherit src;
            env.CARGO_PROFILE = "dev";
          };
          pkg-debug = craneLib.buildPackage {
            inherit src;
            cargoArtifacts = cargoArtifactsDebug;
            env.CARGO_PROFILE = "dev";
          };

          # NixOS VM integration test: drives the controller protocol end
          # to end. It is fed the debug build on purpose (see above).
          vmTest = import ./test/default.nix {
            inherit pkgs lib;
            jsonrpc-httpproxy = pkg-debug;
          };
        in
        {
          packages.jsonrpc-httpproxy = pkg;
          packages.default = config.packages.jsonrpc-httpproxy;

          checks.default = craneLib.cargoTest {
            inherit src cargoArtifacts;
          };

          # Runs the full NixOS VM test (boots a VM, so it is slow —
          # run it alone via `nix build .#checks.x86_64-linux.jsonrpc-httpproxy-vm`).
          checks.jsonrpc-httpproxy-vm = vmTest;

          apps.default = {
            type = "app";
            program = "${pkg}/bin/jsonrpc-httpproxy";
            meta.description = "JSON-RPC-controlled HTTP/HTTPS intercepting proxy";
          };

          # rustfmt config lives here (repo has no rustfmt.toml): the
          # unstable options match the previous rustfmt.toml.
          treefmt = {
            programs.rustfmt.enable = true;
            programs.rustfmt.edition = "2024";
            programs.taplo.enable = true;
            programs.nixfmt.enable = true;
            settings.formatter.rustfmt.options = lib.mkAfter [
              "--config"
              "max_width=100,comment_width=100,wrap_comments=true,group_imports=StdExternalCrate,imports_granularity=Crate,condense_wildcard_suffixes=true,format_code_in_doc_comments=true,format_macro_matchers=true,format_macro_bodies=true,format_strings=true,use_field_init_shorthand=true"
            ];
          };

          devShells.default = craneLib.devShell {
            packages = [
              config.treefmt.build.wrapper
            ];
            # No buildInputs / LD_LIBRARY_PATH needed: pure Rust, the
            # binary only links glibc from the stdenv.
          };
        };
    };
}
