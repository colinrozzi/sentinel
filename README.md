# sentinel

Supervisor + (eventually) HTTPS-triggered deploy for theater actor systems.

Runs as the top-level process for an actor system: spawns the configured child manifest, watches it, respawns on crash (subject to a crash-loop rate limiter). Phase 2 will add an HTTPS endpoint that accepts authenticated deploy prompts from GitHub Actions to fetch + swap binaries.

## Status: slim supervisor

What works:
- Spawns a configured child manifest on init
- Accumulates child chain events into an in-memory ring buffer (cap 500, oldest dropped past cap)
- On `handle-child-error` / `handle-child-exit`: logs a crash summary and respawns the child
- `handle-child-external-stop` does **not** respawn (intentional shutdown)
- Crash-loop rate limiter: at most 5 restarts per 60s; past that, the sentinel logs and stops respawning (operator must restart the sentinel to unblock)

What's coming in phase 2:
- HTTPS endpoint (bearer-token auth) that accepts deploy prompts
- Pull binary from GitHub release artifact (URL + SHA256 in the request)
- Verify checksum, swap the child manifest's `package` path atomically, restart child via supervisor

## Deferred: out-of-band crash notification

Phase 1 originally shipped a crash-email path via the inbox HTTP API. We pulled it: sentinel-supervising-inbox would silently fail to alert when inbox is the thing that's down (the dev-agent mailbox is also hosted on the same inbox). The chain buffer is still accumulated — it's lightweight and can re-attach to whatever transport we settle on. Open design: direct-to-gmail-MX bypass, retry queue, alternate transport (file/syslog/exec), etc. Operators currently rely on the systemd log for crash awareness.

## Run locally

```sh
nix build .#default
nix build .#theater -o result-theater
./result-theater/bin/theater spawn sentinel-actor/manifest.toml
```

`sentinel-actor/manifest.toml` ships with `initial_state = "{}"` (placeholder). Replace with a real config before running. Shape:

```json
{
  "child_manifest": "/abs/path/to/child/manifest.toml"
}
```

State persists across restarts via `theater:simple/store` at `./.store/sentinel/` (repo-local).

## Demo / test: deliberately-crashing child

`tests/crashing-child/` ships a minimal actor that calls `runtime.shutdown(Some(bytes))` in its init, so the supervising sentinel sees a `handle-child-exit` with a non-empty result and runs the crash flow.

```sh
nix build .#default
# point sentinel-actor/manifest.toml's initial_state at:
#   { "child_manifest": "/abs/path/to/sentinel/tests/crashing-child/manifest.toml" }
./result-theater/bin/theater spawn sentinel-actor/manifest.toml
```

The child crashes immediately on init; expect 5 respawn cycles, then the rate limiter trips on the 6th and the child stays dead. Watch the runtime log for `[sentinel] crash ...` and `[sentinel] crash loop ...` lines.

## Architecture

```
sentinel (singleton, lives as long as the supervised system)
  ├── supervisor.spawn → child actor system (e.g. inbox's acceptor, tickets' acceptor)
  └── supervisor-handlers callbacks:
        handle-child-event       → accumulate chain (in memory, capped)
        handle-child-error       → log + respawn (rate-limited)
        handle-child-exit        → log + respawn (rate-limited)
        handle-child-external-stop → no respawn (intentional stop)

(phase 2)
  └── tcp listen :NNN → on POST /deploy {url, sha256}: fetch, verify, swap, restart
```

## Security model (phase 2)

The HTTPS deploy endpoint will be the only inbound surface. Bearer-token auth keyed off a secret stored in the sentinel's config. GitHub Actions has the token as a repo secret. No SSH key shared with CI; no shell access to the VPS from the action.

Verification: the deploy request includes a SHA256 the sentinel checks against the downloaded artifact. A GitHub compromise lets an attacker push a release, but signed tags + a manual checksum review of the action's output narrow the window further.
