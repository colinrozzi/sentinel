{
  description = "Sentinel: supervisor + crash-notification + (eventually) HTTPS deploy for theater actor systems";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";

    theater = {
      # Tracking main (post-PR-#58/#59/#60) — supervisor.spawn signature
      # changed to (manifest, init-state: value, wasm-bytes) with auto-init.
      # Switch back to a release branch once one is cut after these merges.
      # Pinned to the packr-0.11.0 theater rev 73a4540b (PR #149: the plain-build
      # model — composition/fuse machinery removed; actors are plain cdylibs, no
      # compose step). Only used for packages.theater + the devShell now.
      url = "github:colinrozzi/theater/73a4540b";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-overlay.follows = "rust-overlay";
      inputs.crane.follows = "crane";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane, theater }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          targets = [ "wasm32-unknown-unknown" ];
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = pkgs.lib.cleanSourceWith {
          src = ./.;
          filter = path: type:
            (pkgs.lib.hasSuffix ".rs" path) ||
            (pkgs.lib.hasSuffix ".toml" path) ||
            (pkgs.lib.hasSuffix ".lock" path) ||
            (type == "directory");
        };

        # packr 0.11.0 plain-build link flags (composites retired). Just two:
        # --export-memory (the cdylib exports its own growable memory; packr-guest's
        # setup_guest!() links dlmalloc in) and --no-entry. These MUST reach the real
        # cargo invocation; crane ignores the repo .cargo/config.toml (kept for
        # devshell/plain-cargo), so pass them via CARGO_ENCODED_RUSTFLAGS, joined by
        # 0x1f (cargo's encoded delimiter). See theater docs/self-contained-actor-recipe.md.
        rustflagsSep = builtins.fromJSON "\"\\u001f\"";
        plainBuildRustflags = builtins.concatStringsSep rustflagsSep [
          "-C" "link-arg=--export-memory"
          "-C" "link-arg=--no-entry"
        ];

        commonArgs = {
          inherit src;
          pname = "sentinel";
          version = "0.1.0";
          cargoExtraArgs = "--target wasm32-unknown-unknown";
          CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
          CARGO_ENCODED_RUSTFLAGS = plainBuildRustflags;
          # Plain build: no compose step, so no theater binary / binaryen needed.
          # wasm-tools stays for the host-only-imports verify in installPhase.
          nativeBuildInputs = [ pkgs.wasm-tools ];
          doCheck = false;
        };

        # One buildPackage pass (cargoArtifacts = null), no separate buildDepsOnly:
        # the synthetic deps-only crate builds a non-cdylib and can leak libstd; a
        # single pass keeps the wasm link clean. Fast enough for this actor.
        cargoArtifacts = null;

        theaterBin = theater.packages.${system}.default;

      in {
        packages.default = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          # packr 0.11.0 plain build: crane cargo-builds each member into a
          # directly-loadable <name>.wasm (growable memory + pack:alloc exported,
          # imports host theater:simple/* only) — no compose step. Install the bare
          # wasms and verify each imports host-only. crashing_child is the test child.
          installPhaseCommand = ''
            mkdir -p $out
            dir=target/wasm32-unknown-unknown/release
            for m in sentinel crashing_child; do
              cp "$dir/$m.wasm" "$out/$m.wasm"
              bad=$(wasm-tools print "$out/$m.wasm" | grep -E '^[[:space:]]*\(import ' | grep -v 'theater:simple/' || true)
              if [ -n "$bad" ]; then
                echo "ERROR: $m.wasm has non-host imports (not self-contained):"
                echo "$bad"
                exit 1
              fi
              echo "self-contained: $m.wasm"
            done
          '';
        });

        packages.theater = theaterBin;

        packages.clippy = craneLib.cargoClippy (commonArgs // {
          inherit cargoArtifacts;
          cargoClippyExtraArgs = "--target wasm32-unknown-unknown -- -D warnings";
        });

        packages.fmt = craneLib.cargoFmt {
          inherit src;
          pname = "sentinel";
          version = "0.1.0";
        };

        devShells.default = craneLib.devShell {
          packages = [ rustToolchain theaterBin pkgs.wasm-tools pkgs.ripgrep ];
          shellHook = ''
            echo "sentinel dev environment (packr 0.11.0 plain build)"
            echo "  cargo build --release --target wasm32-unknown-unknown"
            echo "  # the bare target/.../sentinel.wasm is directly loadable — no compose"
            echo "  theater start sentinel-actor/manifest.toml"
          '';
        };
      });
}
