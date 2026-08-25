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
      # Bumped 2026-08-21 from the packr-0.11-era rev 73a4540b to the current fleet
      # release. The old pin (theater 0.3.9, packr-0.11 host) cannot instantiate a
      # packr-0.20 Pack module that imports `theater:simple/supervisor` ("Failed to
      # instantiate Pack module"); 0.3.17 wires supervisor into the packr-0.20 host, which
      # is what the RSM control plane (control/) needs. release-20260812 == theater 0.3.17.
      url = "github:colinrozzi/theater/release-20260812-e8affc4";
      # NOTE: do NOT `follows` our nixpkgs/rust-overlay/crane here — theater 0.3.17 needs
      # the toolchain pinned in ITS own flake. Forcing our (older) nixpkgs made its crane
      # deps build fail; letting it use its own inputs builds cleanly.
    };

    # packr CLI (packr compose + packr verify --host-only), pinned to the 0.12.2
    # tag — the composition-consumer toolchain. Used as a build tool to fuse the
    # bare sentinel with the mesh-client component into the deployable composite.
    # 0.12.4: strips internalized interfaces from the composite __pack_types
    # required-metadata (the load-blocker fix — the composite now declares
    # host-only so theater stops demanding a phantom mesh handler); also carries
    # the 0.12.3 flake fixes (doCheck=false + binaryen-wrapped), so no
    # overrideAttrs / binaryen workarounds are needed.
    packr.url = "github:colinrozzi/pack/v0.12.4";

    # --- RSM control plane (control/) toolchain ---
    # packr 0.20 CLI: composes the RSM control composites (sentinel-system ⊕ mesh core ⊕
    # control-sm, and the client composite). Separate from the legacy 0.12.4 above, which
    # composes the old mesh-client sentinel.
    packr20.url = "github:colinrozzi/pack/v0.20.0";
    # mesh SOURCE (flake = false → we don't resolve mesh's own local-path inputs; we build
    # its node + mesh-system wasms with OUR toolchain). Pinned to main @ fb109cc (the RSM
    # substrate: node.pact + the generic mesh-system entry).
    mesh = {
      url = "github:colinrozzi/mesh/fb109ccf759c5ac9cf80b74c1f99754964fe2767";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane, theater, packr, packr20, mesh }:
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
        # packr 0.12.3+ sets doCheck=false in its own flake and wraps the binary
        # with binaryen on PATH, so the CLI builds in the sealed sandbox and packr
        # compose finds wasm-merge itself — no overrideAttrs / binaryen workaround.
        packrCli = packr.packages.${system}.packr;

        # =====================================================================
        # RSM control plane (control/) — reproducible build
        # =====================================================================
        packr20Cli = packr20.packages.${system}.packr;

        # Build one standalone packr-0.20 wasm cdylib (its own Cargo.lock, vendored +
        # offline). `.pact` files MUST be in the sandbox (pact!(from "...") reads them at
        # compile time). Mirrors mesh's own buildWasm.
        buildWasm = { pname, wasmSrc, crate, lock, wasmName }:
          let
            vendorDir = craneLib.vendorCargoDeps { cargoLock = lock; };
            filtered = pkgs.lib.cleanSourceWith {
              src = wasmSrc;
              filter = path: type:
                (pkgs.lib.hasSuffix ".rs" path) ||
                (pkgs.lib.hasSuffix ".toml" path) ||
                (pkgs.lib.hasSuffix ".lock" path) ||
                (pkgs.lib.hasSuffix ".pact" path) ||
                (type == "directory");
            };
          in pkgs.stdenv.mkDerivation {
            name = "${pname}-wasm";
            src = filtered;
            nativeBuildInputs = [ rustToolchain ];
            buildPhase = ''
              export CARGO_HOME=$(mktemp -d)
              export CARGO_TARGET_DIR=$PWD/_target
              cp ${vendorDir}/config.toml $CARGO_HOME/config.toml
              cargo build --release --offline --target wasm32-unknown-unknown --manifest-path ${crate}/Cargo.toml
            '';
            installPhase = ''
              mkdir -p $out
              cp _target/wasm32-unknown-unknown/release/${wasmName} $out/
            '';
          };

        # The four components. control-* build from ./control (so ../control.pact is in the
        # sandbox); mesh core + generic system build from the mesh source input.
        controlSm = buildWasm {
          pname = "control-sm"; wasmSrc = ./control; crate = "control-sm";
          lock = ./control/control-sm/Cargo.lock; wasmName = "control_sm.wasm";
        };
        sentinelSystem = buildWasm {
          pname = "sentinel-system"; wasmSrc = ./control; crate = "sentinel-system";
          lock = ./control/sentinel-system/Cargo.lock; wasmName = "sentinel_system.wasm";
        };
        meshCore = buildWasm {
          pname = "mesh-core"; wasmSrc = mesh; crate = ".";
          lock = mesh + "/Cargo.lock"; wasmName = "mesh.wasm";
        };
        meshSystemWasm = buildWasm {
          pname = "mesh-system"; wasmSrc = mesh; crate = "mesh-system";
          lock = mesh + "/mesh-system/Cargo.lock"; wasmName = "mesh_system.wasm";
        };

        # Compose manifests (built wasm store paths). node→SM links are the 4 state-machine
        # fns; system→node links differ: the sentinel side imports 7 node fns (drops
        # subscribe/current-members/event-status), the generic client imports all 10.
        smLinks = provider: ''
          [[link]]
          consumer = "mesh"
          import   = "state-machine.initial-state"
          provider = "control-sm"
          export   = "initial-state"
          [[link]]
          consumer = "mesh"
          import   = "state-machine.validate"
          provider = "control-sm"
          export   = "validate"
          [[link]]
          consumer = "mesh"
          import   = "state-machine.apply"
          provider = "control-sm"
          export   = "apply"
          [[link]]
          consumer = "mesh"
          import   = "state-machine.members"
          provider = "control-sm"
          export   = "members"
        '';
        nodeLink = consumer: fn: ''
          [[link]]
          consumer = "${consumer}"
          import   = "node.${fn}"
          provider = "mesh"
          export   = "${fn}"
        '';
        sentinelNodeFns = [ "init" "on-connect" "on-bytes" "on-close" "tick" "author" "current-state" ];
        clientNodeFns = sentinelNodeFns ++ [ "subscribe" "current-members" "event-status" ];

        sentinelComposeManifest = pkgs.writeText "sentinel.compose.toml" (''
          [[component]]
          name  = "sentinel-system"
          wasm  = "${sentinelSystem}/sentinel_system.wasm"
          entry = true
          [[component]]
          name = "mesh"
          wasm = "${meshCore}/mesh.wasm"
          [[component]]
          name = "control-sm"
          wasm = "${controlSm}/control_sm.wasm"
        '' + (pkgs.lib.concatMapStrings (nodeLink "sentinel-system") sentinelNodeFns) + (smLinks "control-sm"));

        clientComposeManifest = pkgs.writeText "control-client.compose.toml" (''
          [[component]]
          name  = "mesh-system"
          wasm  = "${meshSystemWasm}/mesh_system.wasm"
          entry = true
          [[component]]
          name = "mesh"
          wasm = "${meshCore}/mesh.wasm"
          [[component]]
          name = "control-sm"
          wasm = "${controlSm}/control_sm.wasm"
        '' + (pkgs.lib.concatMapStrings (nodeLink "mesh-system") clientNodeFns) + (smLinks "control-sm"));

        # The mesh-client component (mesh v0.3.0 release), pinned by content hash.
        # Node (mesh.wasm) + client are built from the same source, so this one pin
        # is a compatible node+client pair. v0.3.0 = ephemeral membership +
        # finality-anchored retention (the substrate for the sentinelctl control
        # loop); the `mesh` interface added `is-ready` (route it first in
        # handle-send). Exports `mesh` + opt-in `mesh-control` (the control
        # envelope). Imports only theater:simple/message-server-host.request
        # (residual for theater). colinrozzi/mesh is public — fetchurl needs no auth.
        meshClientPkg = pkgs.fetchurl {
          url = "https://github.com/colinrozzi/mesh/releases/download/v0.3.0/mesh_client_pkg.wasm";
          hash = "sha256-W4q2sjCAIRkKQ9rMlR4yZuw8+YKCeiKK7RrrKFdH9pM=";
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
        # absolute). Links the mesh + mesh-control functions sentinel calls to the
        # mesh-client exports (export names are the hyphenated pact names verbatim);
        # the full `mesh` + `mesh-control` interfaces (declared in sentinel's
        # pack_types) are hash-checked at compose, so a drifted mesh-client fails
        # the build. Both interfaces internalize -> the composite is host-only.
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

          [[link]]
          consumer = "sentinel"
          import = "mesh.is-ready"
          provider = "mesh-client"
          export = "is-ready"

          [[link]]
          consumer = "sentinel"
          import = "mesh-control.control-kind"
          provider = "mesh-client"
          export = "control-kind"

          [[link]]
          consumer = "sentinel"
          import = "mesh-control.decode-command"
          provider = "mesh-client"
          export = "decode-command"

          [[link]]
          consumer = "sentinel"
          import = "mesh-control.encode-response"
          provider = "mesh-client"
          export = "encode-response"
        '';

      in {
        # The deployable artifact = sentinel COMPOSED with the mesh-client
        # component. packr compose fuses the two isolated components (multi-memory)
        # + runs the hash-check; packr verify --host-only then asserts the composite
        # imports host theater:simple/* only (mesh.* internalized;
        # message-server-host.request residual). crashing_child rides along as the
        # bare e2e test child.
        packages.default = pkgs.runCommand "sentinel-composed"
          # packrCli (0.12.4) is wrapped with binaryen on its PATH, so packr
          # compose finds wasm-merge itself — no separate binaryen input needed.
          { nativeBuildInputs = [ packrCli ]; } ''
            mkdir -p $out
            packr compose ${composeManifest} -o $out/sentinel.wasm
            packr verify --host-only $out/sentinel.wasm
            cp ${bareBuild}/crashing_child.wasm $out/crashing_child.wasm
            echo "composed + host-only verified: sentinel.wasm ($(stat -c%s $out/sentinel.wasm) bytes)"
          '';

        # The bare (pre-compose) build, exposed for debugging / the runtime e2e.
        packages.bare = bareBuild;

        # --- RSM control plane composites (packr 0.20, host-only) ---
        # Sentinel side: the supervisor gaining a mesh face (sentinel-system ⊕ core ⊕
        # control-sm). Client side: a generic participant a manager runs to drive it.
        # Runs on theater >=0.3.17 (the bumped pin) — the packr-0.20 host wires supervisor.
        packages.control-sentinel = pkgs.runCommand "sentinel-control-composed"
          { nativeBuildInputs = [ packr20Cli pkgs.binaryen ]; } ''
            mkdir -p $out
            packr compose ${sentinelComposeManifest} --output $out/sentinel-control.wasm
            packr verify --host-only $out/sentinel-control.wasm
            echo "composed + host-only: sentinel-control.wasm ($(stat -c%s $out/sentinel-control.wasm) bytes)"
          '';
        packages.control-client = pkgs.runCommand "control-client-composed"
          { nativeBuildInputs = [ packr20Cli pkgs.binaryen ]; } ''
            mkdir -p $out
            packr compose ${clientComposeManifest} --output $out/control-client.wasm
            packr verify --host-only $out/control-client.wasm
            echo "composed + host-only: control-client.wasm ($(stat -c%s $out/control-client.wasm) bytes)"
          '';
        packages.control-sm = controlSm;
        packages.sentinel-system = sentinelSystem;

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
