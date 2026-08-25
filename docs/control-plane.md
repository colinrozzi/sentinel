# Sentinel control plane (RSM) — design

Status: KICKOFF (2026-08-19). Colin's direction: build the control plane **fresh**
on the current RSM / DESIGN-dx mesh architecture (not on chat, not on the old
mesh-control-envelope path), and publish **two artifacts** so that *actors can talk
to sentinel nodes and manage actors remotely*:

1. **Sentinel side** — what a sentinel runs to be *manageable over mesh*.
2. **Client side** — what an actor composes to *reach a sentinel and manage actors*.

This is **sentinel the supervisor gaining a mesh face**.

## The one-actor-three-layers shape (from mesh `docs/DESIGN-dx.md`)

A mesh participant is one composed actor = **system ⊕ core ⊕ SM**:

- **SM** (`control-sm`, this repo) — the **rules**. `validate`/`apply`/`members`/
  `initial-state`. Membership + `command_allow` authz + a `(author, corr_id)`
  command/response journal. Pure, confluent, structure-blind. Safety.
- **core** (`mesh.wasm`, from the mesh release) — the pure engine: DAG · fold ·
  finality · gossip. Consumed, no I/O. Exports the `node` interface (`node.pact`).
- **system** (per side) — the entry actor + the **sole I/O shell**. Owns every
  theater handler (init / tcp on-connect·on-bytes·on-close / timer tick /
  handle-send) and host import (tcp / timer / message-server / log). Drives the core
  via `node.pact`, performs the effects the core returns. Behavior + external edge.

The SM is **shared by both sides** — every member of a control mesh folds the same
rules. The two sides differ only in their **system**.

## Sentinel side — a CUSTOM effectful system

Chat's system is a near-empty `react` (just gossip). The sentinel side is the
opposite: **a finalized `command` must cause a real supervisor action.** Its `react`:

```
on a finalized command (I'm the responsible sentinel):
    perform the supervisor op   (list / start / stop / get_chain)
    author a `response` event   (corr_id-matched)
```

That effect touches the live supervisor (spawn/stop children, read chain buffers) —
exactly what DESIGN-dx puts *in the system*. So the sentinel-side system is
`sentinel-system` = the generic mesh-system's drive loop **plus** sentinel's existing
command handlers (`cmd_list`/`cmd_start`/`cmd_stop`/`cmd_get_chain` already live in
`sentinel-actor/src/lib.rs`). It needs sentinel's **supervisor host imports**, so it
lives with the supervisor: **sentinel-the-actor gains a mesh face** rather than
spawning a separate node.

Compose: `sentinel-system (entry) ⊕ mesh core ⊕ control-sm`
(custom manifest — `mkComposite` hardcodes the *generic* mesh-system as entry, so the
sentinel side needs its own compose manifest, not `mkComposite`).

## Client side — mostly generic + a driver

A managing actor is a **member** of the sentinel's control mesh, so it runs its own
participant (`generic mesh-system ⊕ mesh core ⊕ control-sm`, i.e. `mkComposite` with
`sm = control-sm`) and **drives** it over message-server: `author` a `command`,
`subscribe` to the finalized stream, match the `response` by `corr_id`. The published
"client side" = that composite + a thin **client library** (encode a command / decode
a response / correlate) any actor pulls in — `sentinelctl` reborn as a reusable
component rather than a one-off CLI.

## Command set (v1)

`list` · `start {name, package}` · `stop {name}` · `get_chain {name}`.
(Dropped `health` — `list` already carries the per-child status. Future: `follow_chain`
— a streaming subscription to a child's chain events, a natural fit for the finalized
stream.) `verb` is a plain string in `command-body`; per-verb args are encoded in
`args: list<u8>`. See `control/control.pact`.

## Consistency with the lifecycle model (`docs/mesh-supervision-spec.md`)

- The control mesh is a **command channel** → **ephemeral profile**: its log is a
  transcript, the truth is sentinel's config + live process table. So the sentinel-side
  node is **disposable** — fresh genesis per sentinel lifetime is fine.
- Sentinel is the **genesis root** of its *own* control mesh (it never joins), so
  fresh-identity-per-boot works today with no key-verification machinery — the
  pubkey-pinned-admission blocker only applies to joiners (the managing clients).
- **One control mesh per sentinel**; managing actors join it, pre-authorized by pubkey
  in `join_allow` / `command_allow` (seeded from the genesis event = config).

## Repo layout

```
control/
  control.pact          # the protocol (shared contract, both sides)
  control-sm/           # the rules (shared SM; folded by every member)
  sentinel-system/      # sentinel-side effectful system (entry) — CUSTOM        [to build]
  sentinel.compose.toml # sentinel side: sentinel-system ⊕ core ⊕ control-sm     [to build]
  control-client/       # client-side driver library                            [to build]
  sentinelctl/          # LEGACY (old mesh-control-envelope client) — superseded
```

## Build (nix)

No Rust/packr toolchain on PATH; build through nix (proven: `nix shell nixpkgs#rustc`
pulls from cache). SM + systems are `packr-guest = "0.20"` cdylibs → wasm; the sentinel
side composes with `packr compose` (packr 0.20, from the mesh/pack flake) against the
mesh-release `mesh.wasm` core. The mesh flake exposes `lib.mkComposite`/`buildWasm` for
the client (generic-entry) side; the sentinel side uses a custom compose manifest.

## Status / next

- [x] Protocol (`control.pact`) + rules (`control-sm`) — **compiled + 8/8 tests green**
      via nix (packr-guest 0.20 resolves, `pact!` macro works).
- [x] `control-sm` refinement: **Genesis auto-includes its author as a member**, so the
      sentinel-system never needs its own pubkey to seed membership.
- [x] `sentinel-system` — the merged sentinel-side entry (drive loop + genesis authoring
      + the react→supervisor-op bridge polling the SM journal + the full supervisor port:
      children / spawn / rate-limit / chain rings / child handlers). **Compiles + links to
      wasm** (347KB) with the correct theater exports.
- [x] `control/sentinel.compose.toml` — custom compose (sentinel-system ⊕ core ⊕
      control-sm), **composed via packr 0.20 (host-only) + smoked**: spawns, authors the
      control genesis, listens.
- [x] `control/control-client.compose.toml` — client composite (generic mesh-system ⊕
      core ⊕ control-sm), composed.
- [x] **End-to-end 2-participant round-trip GREEN** (on theater 0.3.17): manager joins
      the sentinel's control mesh → authors `Command(list)` → sentinel reacts (real
      `cmd_list`) → authors `Response` → manager reads `{"ok":true,"children":[]}`.
      Requires theater **0.3.17** (packr-0.20 + supervisor); the repo's pinned theater
      (`73a4540b`, packr-0.11 era) is too old — **bump the flake theater input**.
- [x] **Reproducible nix wiring** — `flake.nix` adds `packr20` (pack v0.20.0) + `mesh`
      (source @ fb109cc) inputs, a `buildWasm` helper, and outputs
      `packages.{control-sentinel,control-client,control-sm,sentinel-system}`. Both
      composites build via `nix build .#control-sentinel` / `.#control-client`, verify
      host-only in-derivation, and **round-trip green on the nix-built artifacts**.
- [x] **Theater pin bumped** `73a4540b` → `release-20260812-e8affc4` (0.3.17) + lock
      regenerated (in-container regen works). NOTE: `nix build .#theater` OOMs locally
      (wasmtime compile exceeds container memory) — the pin is correct; the fleet/CI
      builds the binary. Dropped the `inputs.*.follows` on theater (0.3.17 needs its own
      toolchain).
- [ ] PR the control-plane branch; a `start`/`stop`/`get_chain` round-trip with a real
      supervised child; then the Colin-gated gc-root cutover to replace the live sentinel.
```
