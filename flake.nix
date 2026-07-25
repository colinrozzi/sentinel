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

    # packr CLI (packr compose + packr verify --host-only), pinned to the 0.12.2
    # tag — the composition-consumer toolchain. Used as a build tool to fuse the
    # bare sentinel with the mesh-client component into the deployable composite.
    packr.url = "github:colinrozzi/pack/v0.12.2";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane, theater, packr }:
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

        # packr 0.11.0+ plain-build link flags (composites retired). Just two:
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
          # Plain build: no in-crate compose, so no theater binary / binaryen needed.
          # wasm-tools stays for the crashing_child host-only verify in installPhase.
          nativeBuildInputs = [ pkgs.wasm-tools ];
          doCheck = false;
        };

        # One buildPackage pass (cargoArtifacts = null), no separate buildDepsOnly:
        # the synthetic deps-only crate builds a non-cdylib and can leak libstd; a
        # single pass keeps the wasm link clean. Fast enough for this actor.
        cargoArtifacts = null;

        theaterBin = theater.packages.${system}.default;
        # packr's own package build runs a compose test that shells out to
        # wasm-merge (binaryen), which is unavailable in the sealed nix build
        # sandbox — skip its checkPhase so the CLI builds here. (Reported upstream;
        # the belt-and-suspenders is pack-dev setting doCheck=false / gating that
        # test in the packr flake.)
        packrCli = (packr.packages.${system}.packr).overrideAttrs (_: { doCheck = false; });

        # The mesh-client component (mesh v0.2.0 release), pinned by content hash.
        # Node (mesh.wasm) + client are built from the same source, so this one pin
        # is a compatible node+client pair. Exports the `mesh` interface (hash
        # 9c5ad8c4) + opt-in `mesh-control` (2be499fb); imports only
        # theater:simple/message-server-host.request (residual for theater).
        # NOTE: colinrozzi/mesh is a PRIVATE repo, so this fetchurl 404s
        # unauthenticated (nix has no GitHub auth) — the reproducible build + CI
        # are blocked until the mesh release assets are fetchable by nix (repo/asset
        # public, or a nix access-token/netrc for both local + Actions). The hash
        # below is the ACTUAL v0.2.0 release asset (sha256 3e0e2e92, 185521 bytes);
        # it composes green + host-only (verified). (pack-dev's earlier SRI was the
        # local /tmp staging artifact eca11274, which differs from the release
        # binary — same mesh interface, different bytes.)
        meshClientPkg = pkgs.fetchurl {
          url = "https://github.com/colinrozzi/mesh/releases/download/v0.2.0/mesh_client_pkg.wasm";
          hash = "sha256-Pg4ukn9qplOwT6kUcbYykYdGv48JYFzMId+gE7JeIB4=";
        };

        # crane cargo-builds both members into bare wasms. crashing_child is a
        # plain self-contained actor — asserted host-only here. sentinel is bare
        # with residual mesh.* imports (satisfied at compose time), so it is NOT
        # asserted host-only here; that assert runs on the COMPOSITE below.
        bareBuild = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          installPhaseCommand = ''
            mkdir -p $out
            dir=target/wasm32-unknown-unknown/release
            cp "$dir/crashing_child.wasm" "$out/crashing_child.wasm"
            bad=$(wasm-tools print "$out/crashing_child.wasm" | grep -E '^[[:space:]]*\(import ' | grep -v 'theater:simple/' || true)
            if [ -n "$bad" ]; then
              echo "ERROR: crashing_child.wasm has non-host imports (not self-contained):"
              echo "$bad"
              exit 1
            fi
            echo "self-contained: crashing_child.wasm"
            cp "$dir/sentinel.wasm" "$out/sentinel-bare.wasm"
          '';
        });

        # Compose manifest with absolute store paths (packr resolves component
        # paths relative to the manifest dir, but absolute store paths are
        # absolute). Links the 3 mesh functions sentinel calls to the mesh-client
        # exports; the full `mesh` interface (declared in sentinel's pack_types) is
        # hash-checked at compose time, so a drifted mesh-client fails the build.
        composeManifest = pkgs.writeText "sentinel.compose.toml" ''
          [[component]]
          name = "sentinel"
          wasm = "${bareBuild}/sentinel-bare.wasm"
          entry = true

          [[component]]
          name = "mesh-client"
          wasm = "${meshClientPkg}"

          [[link]]
          consumer = "sentinel"
          import = "mesh.submit"
          provider = "mesh-client"
          export = "submit"

          [[link]]
          consumer = "sentinel"
          import = "mesh.register"
          provider = "mesh-client"
          export = "register"

          [[link]]
          consumer = "sentinel"
          import = "mesh.delivery"
          provider = "mesh-client"
          export = "delivery"
        '';

      in {
        # The deployable artifact = sentinel COMPOSED with the mesh-client
        # component. packr compose fuses the two isolated components (multi-memory)
        # + runs the hash-check; packr verify --host-only then asserts the composite
        # imports host theater:simple/* only (mesh.* internalized;
        # message-server-host.request residual). crashing_child rides along as the
        # bare e2e test child.
        packages.default = pkgs.runCommand "sentinel-composed"
          # binaryen: `packr compose` shells out to wasm-merge to fuse the
          # multi-memory composite; it must be on PATH in the sealed sandbox.
          { nativeBuildInputs = [ packrCli pkgs.binaryen ]; } ''
            mkdir -p $out
            packr compose ${composeManifest} -o $out/sentinel.wasm
            packr verify --host-only $out/sentinel.wasm
            cp ${bareBuild}/crashing_child.wasm $out/crashing_child.wasm
            echo "composed + host-only verified: sentinel.wasm ($(stat -c%s $out/sentinel.wasm) bytes)"
          '';

        # The bare (pre-compose) build, exposed for debugging / the runtime e2e.
        packages.bare = bareBuild;

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
          packages = [ rustToolchain theaterBin packrCli pkgs.wasm-tools pkgs.ripgrep ];
          shellHook = ''
            echo "sentinel dev environment (packr 0.12.x — composed with mesh-client)"
            echo "  cargo build --release --target wasm32-unknown-unknown"
            echo "  packr compose deploy/sentinel.compose.toml -o sentinel_composed.wasm"
            echo "  packr verify --host-only sentinel_composed.wasm"
            echo "  theater start sentinel-actor/manifest.toml"
          '';
        };
      });
}
