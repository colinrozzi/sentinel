# sentinel

Multi-child supervisor + (eventually) HTTPS-triggered deploy for theater actor systems.

Runs as the top-level process for a set of actors: spawns each configured child manifest, watches them, respawns any that crash (subject to a per-child crash-loop rate limiter), and emits a periodic heartbeat to the journal. Phase 2 will add an HTTPS endpoint that accepts authenticated deploy prompts from GitHub Actions to fetch + swap binaries.

## Status: Phase A — clean multi-child supervisor

What works:
- Spawns a **set** of configured children on init (name -> child record), not just one
- After spawning each child, calls `subscribe-to-child` so its chain events flow (chain delivery is opt-in on theater f852aec3+)
- Accumulates each child's chain events into its own in-memory ring buffer (cap 500/child, oldest dropped past cap)
- On `handle-child-error` / `handle-child-exit`: demuxes by child-id, logs a crash summary, and respawns just that child
- `handle-child-external-stop` does **not** respawn (intentional shutdown)
- Per-child crash-loop rate limiter: at most 5 restarts per 60s per child; past that, that child is left dead and logged (operator must restart the sentinel to unblock). Other children are unaffected.
- Periodic heartbeat line `[sentinel] heartbeat children=N blocked=M t_ms=...` via the `timer` handler (`set-interval` -> `handle-tick`)

What's coming in phase 2:
- HTTPS endpoint (`tcp` listen + `[handler.server_tls]`, bearer-token auth) that accepts deploy prompts
- Pull binary from a GitHub release artifact (URL + SHA256 in the request), verify checksum
- Blue-green swap: spawn the new version, health-gate it, then stop the old child

## Out-of-band notification (ticket #43)

The original phase-1 crash-email path went through the inbox — which silently fails to alert when inbox itself is down (the dev-agent mailbox is on the same inbox). That path is gone. The Phase A replacement keeps the critical alert **off the box**:

- Sentinel emits a periodic `[sentinel] heartbeat ...` line plus structured `[sentinel] crash ...` / `[sentinel] crash loop ...` lines to journald — no inbox dependency.
- An external, off-box watcher (SSH/health-probe) tails those: crash lines drive a detail alert; **absence** of the heartbeat past a threshold is a dead-man's-switch for a wedged/dead sentinel. Escalation to a human happens from where the watcher runs, which can reach the outside world independently of the box.

Rich chain diagnostics to a dev mailbox remain a best-effort, deferred nicety layered on top — never the critical path.

## Config

`sentinel-actor/manifest.toml` ships with `initial_state = "{}"` (placeholder). Replace with a real config. Shape:

```json
{
  "children": [
    { "name": "inbox-ui",   "manifest": "/abs/path/to/rendered/inbox-ui.manifest.toml" },
    { "name": "tickets-ui", "manifest": "/abs/path/to/rendered/tickets-ui.manifest.toml" }
  ],
  "heartbeat_ms": 30000
}
```

Each child manifest is **rendered at deploy time** (package pin + secret substitution already applied) and spawned by path. In-sentinel template rendering + package pinning is deferred to the Phase 2 deploy path, where dynamic package pins belong. `heartbeat_ms` is optional (defaults to 30000).

## Run locally

```sh
nix build .#default
nix build .#theater -o result-theater
./result-theater/bin/theater spawn sentinel-actor/manifest.toml
```

## Demo / test: two deliberately-crashing children

`tests/crashing-child/` ships a minimal actor that calls `runtime.shutdown(Some(bytes))` in its init, so the supervising sentinel sees a `handle-child-exit` and runs the crash flow. `tests/two-crashing-children.json` supervises **two** instances of it to exercise per-child rate-limit + chain accounting independence.

```sh
nix build .#default
# point sentinel-actor/manifest.toml's initial_state at the contents of
# tests/two-crashing-children.json (adjust the absolute manifest paths first)
./result-theater/bin/theater spawn sentinel-actor/manifest.toml
```

Expect each child (`crash-a`, `crash-b`) to independently run 5 respawn cycles and then trip its own rate limiter on the 6th — proving the accounting is per-child, not global. Watch the journal for per-child `[sentinel] crash child=... name=crash-a ...`, `[sentinel] crash loop for crash-a ...` / `... for crash-b ...`, and the `[sentinel] heartbeat children=2 blocked=...` line climbing to `blocked=2`. See `tests/README.md`.

## Architecture

```
sentinel (singleton, lives as long as the supervised set)
  ├── supervisor.spawn + subscribe-to-child → each child actor (inbox-ui, tickets-ui, frontdoor, ...)
  ├── supervisor-handlers callbacks (all demuxed by child-id):
  │     handle-child-event       → accumulate that child's chain (in memory, capped)
  │     handle-child-error       → log + respawn that child (per-child rate limit)
  │     handle-child-exit        → log + respawn that child (per-child rate limit)
  │     handle-child-external-stop → no respawn (intentional stop)
  └── timer.handle-tick          → emit heartbeat (dead-man's-switch for the off-box watcher)

(phase 2)
  └── tcp listen :NNN + server_tls → on POST /deploy {child, url, sha256}: fetch, verify, blue-green swap
```

## Security model (phase 2)

The HTTPS deploy endpoint will be the only inbound surface. Bearer-token auth keyed off a secret in the sentinel's config. GitHub Actions holds the token as a repo secret. No SSH key shared with CI; no shell access to the VPS from the action.

Verification: the deploy request includes a SHA256 the sentinel checks against the downloaded artifact. A GitHub compromise lets an attacker push a release, but signed tags + a checksum review narrow the window further.
