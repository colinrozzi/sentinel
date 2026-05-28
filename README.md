# sentinel

Supervisor + bearer-token TCP+JSON deploy gateway for theater actor systems.

Runs as the top-level process for an actor system: spawns the configured child manifest, watches it, respawns on crash (subject to a crash-loop rate limiter). Phase 2 exposes a small command surface over TCP for deploying new package URLs and inspecting the supervised child.

## What works

- Spawns a configured child manifest on init
- Accumulates child chain events into an in-memory ring buffer (cap 500, oldest dropped past cap)
- On `handle-child-error` / `handle-child-exit`: logs a crash summary and respawns the child
- `handle-child-external-stop` does **not** respawn (intentional shutdown)
- Crash-loop rate limiter: at most 5 restarts per 60s; past that, the sentinel logs and stops respawning until either a `start` command lands or the sentinel itself is restarted
- TCP+JSON command surface (bearer-token auth):
  - `start { package }` — swap the child's package URL/path and respawn
  - `stop` — gracefully stop the current child (no respawn)
  - `list` — current child id + package
  - `get_chain` — in-memory chain ring buffer
  - `health` — overall status snapshot

## Configuration

`sentinel-actor/manifest.toml`'s `initial_state` is a JSON document:

```json
{
  "child_manifest_template": "name = \"tickets\"\nversion = \"0.1.0\"\npackage = \"__PACKAGE__\"\n\n[[handler]]\ntype = \"runtime\"\n...",
  "default_package": "https://github.com/colinrozzi/tickets/releases/download/release-XXX/tickets.wasm",
  "listen_addr": "0.0.0.0:8443",
  "bearer_token": "<shared secret>"
}
```

The `child_manifest_template` is the child's full `manifest.toml` body with the `package = "..."` line replaced by `package = "__PACKAGE__"`. Sentinel keeps it as a template and substitutes the current package URL in at every spawn. The supervisor never reads a separate file from disk — it composes the manifest TOML in memory and hands it to `supervisor.spawn` (which accepts either a path or inline content).

## Protocol

One JSON object per connection, one JSON response, then close. All requests carry a `token` field matching the configured bearer token; mismatch returns `{"ok": false, "error": "unauthorized"}`.

```
$ printf '{"token":"...","cmd":"health"}\n' | nc 127.0.0.1 8443
{"ok":true,"child_id":"<uuid>","restart_blocked":false,"recent_restarts":0,"chain_size":0,"chain_truncated":false,"listen_addr":"0.0.0.0:8443"}

$ printf '{"token":"...","cmd":"start","package":"https://.../new.wasm"}\n' | nc 127.0.0.1 8443
{"ok":true,"child_id":"<new-uuid>","current_package":"https://.../new.wasm"}

$ printf '{"token":"...","cmd":"stop"}\n' | nc 127.0.0.1 8443
{"ok":true,"stopped_child_id":"<uuid>"}

$ printf '{"token":"...","cmd":"list"}\n' | nc 127.0.0.1 8443
{"ok":true,"child_id":"<uuid>","current_package":"<url>"}

$ printf '{"token":"...","cmd":"get_chain"}\n' | nc 127.0.0.1 8443
{"ok":true,"chain":["..."],"chain_truncated":false}
```

A successful `start` resets the crash-loop block and clears the restart-history window — operator intent of `start` is "the previous problem has been addressed, give the child another shot."

No TLS in v1. Acceptable for VPS-internal traffic and the early scale we're operating at; add TLS termination via a reverse proxy or upgrade the listener once the threat model demands it.

## Deferred: out-of-band crash notification

Phase 1 originally shipped a crash-email path via the inbox HTTP API. We pulled it: sentinel-supervising-inbox would silently fail to alert when inbox is the thing that's down (the dev-agent mailbox is also hosted on the same inbox). The chain buffer is still accumulated — it's lightweight and can re-attach to whatever transport we settle on. Open design: direct-to-gmail-MX bypass, retry queue, alternate transport (file/syslog/exec), etc. Operators currently rely on the systemd log for crash awareness.

## Run locally

```sh
nix build .#default
nix build .#theater -o result-theater
./result-theater/bin/theater spawn sentinel-actor/manifest.toml
```

Edit `sentinel-actor/manifest.toml`'s `initial_state` to a real config first (see Configuration above). State persists across restarts via `theater:simple/store` at `./.store/sentinel/` (repo-local).

## Demo / test: deliberately-crashing child

`tests/crashing-child/` ships a minimal actor that calls `runtime.shutdown(Some(bytes))` in its init, so the supervising sentinel sees a `handle-child-exit` with a non-empty result and runs the crash flow. To use it with the new config schema, embed the crashing-child manifest body into `child_manifest_template` with the package field replaced by `__PACKAGE__`, and set `default_package` to the on-disk wasm path produced by `nix build`.

Expect 5 respawn cycles, then the rate limiter trips on the 6th and the child stays dead. Watch the runtime log for `[sentinel] crash ...` and `[sentinel] crash loop ...` lines.

## Architecture

```
sentinel (singleton, lives as long as the supervised system)
  ├── tcp.listen :listen_addr  →  handle-connection → JSON dispatch
  │     {start, stop, list, get_chain, health}
  ├── supervisor.spawn(render(template, current_package)) → child actor
  └── supervisor-handlers callbacks:
        handle-child-event       → accumulate chain (in memory, capped)
        handle-child-error       → log + respawn (rate-limited)
        handle-child-exit        → log + respawn (rate-limited)
        handle-child-external-stop → no respawn (intentional stop)
```

## Security model

The TCP command surface is the only inbound network surface. Bearer-token auth is required on every request; comparison is constant-time. No TLS in v1 — assume the listener is reachable only from trusted networks (loopback or VPS-internal). GitHub Actions, when wired up, will hold the token as a repo secret and the deploy step will POST the `start` command.

Package verification: theater fetches https:// package URLs and assumes a 2xx is enough — no checksum or signature validation today. A GitHub compromise would let an attacker push a malicious release that sentinel would happily deploy. SHA256-pinning is a future theater-side ask; until then, restrict who can publish releases on the upstream repo.
