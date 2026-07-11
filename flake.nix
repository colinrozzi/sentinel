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
      # Pinned to the canonical packr-0.10.2 theater rev 7daab2ad (PR #141:
      # `theater build`/`theater compose` + the 0.10.x self-contained loader).
      url = "github:colinrozzi/theater/7daab2ad";
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

        # packr 0.10.2 self-contained FIXED-BASE link flags (PIC retired). Built
        # at a fixed absolute base (--global-base=0x50000, single-package slot) so
        # data needs no relocation; --no-merge-data-segments keeps the CGRF
        # __pack_types surface findable. These MUST reach the real cargo invocation;
        # crane ignores the repo .cargo/config.toml (kept for devshell/plain-cargo),
        # so pass them via CARGO_ENCODED_RUSTFLAGS. Joined by 0x1f (cargo's encoded
        # delimiter). See theater docs/self-contained-actor-recipe.md.
        picSep = builtins.fromJSON "\"\\u001f\"";
        fixedBaseRustflags = builtins.concatStringsSep picSep [
          "-C" "link-arg=--import-memory"
          "-C" "link-arg=--initial-memory=8388608"
          "-C" "link-arg=--stack-first"
          "-C" "link-arg=-zstack-size=262144"
          "-C" "link-arg=--global-base=327680"
          "-C" "link-arg=--no-entry"
          "-C" "link-arg=--no-merge-data-segments"
        ];

        commonArgs = {
          inherit src;
          pname = "sentinel";
          version = "0.1.0";
          cargoExtraArgs = "--target wasm32-unknown-unknown";
          CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
          CARGO_ENCODED_RUSTFLAGS = fixedBaseRustflags;
          # theater compose (in installPhase) needs wasm-merge (binaryen) +
          # wasm-tools on PATH; the theater binary bundles the allocator.
          nativeBuildInputs = [ theaterBin pkgs.binaryen pkgs.wasm-tools ];
          doCheck = false;
        };

        # No buildDepsOnly: crane's synthetic deps-only crate doesn't depend on
        # packr-guest, so it lacks the symbols the fixed-base link needs; one
        # buildPackage pass instead (also sidesteps a libstd leak).
        cargoArtifacts = null;

        theaterBin = theater.packages.${system}.default;

      in {
        packages.default = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          # crane builds the bare members (fixed-base, non-self-contained), then
          # `theater compose` fuses each with the bundled allocator into a
          # self-contained <name>.composite.wasm (imports host-only, memory +
          # pack:alloc internalized) and verifies it. Deploy the composite — the
          # 0.10.x loader rejects a bare member. crashing_child is the test child.
          installPhaseCommand = ''
            mkdir -p $out
            dir=target/wasm32-unknown-unknown/release
            for m in sentinel crashing_child; do
              theater compose "$dir/$m.wasm" -o "$out/$m.composite.wasm"
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
          packages = [ rustToolchain theaterBin pkgs.binaryen pkgs.wasm-tools pkgs.ripgrep ];
          shellHook = ''
            echo "sentinel dev environment (packr 0.10.2 self-contained)"
            echo "  cargo build --release --target wasm32-unknown-unknown"
            echo "  theater compose <member>.wasm -o <name>.composite.wasm"
            echo "  theater start sentinel-actor/manifest.toml"
          '';
        };
      });
}
