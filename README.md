# sentinel

Supervisor + crash-notification + (eventually) HTTPS-triggered deploy for theater actor systems.

Runs as the top-level process for an actor system: it spawns the configured child manifest, watches it, and on crash collects the child's chain, emails it to the corresponding dev agent's mailbox, and respawns. Eventually a small HTTPS endpoint accepts authenticated deploy prompts from GitHub Actions to fetch + swap binaries.

## Status: phase 1 — crash → email → respawn

What works:
- Spawns a configured child manifest on init
- Accumulates child chain events into an in-memory ring buffer (cap 500, oldest dropped past cap)
- On `handle-child-error` / `handle-child-exit`: emails the configured dev address via the inbox HTTP API with the chain snapshot, then respawns the child
- `handle-child-external-stop` does **not** respawn (intentional shutdown)
- Crash-loop rate limiter: at most 5 restarts per 60s; past that, the sentinel logs and stops respawning (operator must restart the sentinel to unblock)

What's coming in phase 2:
- HTTPS endpoint (bearer-token auth) that accepts deploy prompts
- Pull binary from GitHub release artifact (URL + SHA256 in the request)
- Verify checksum, swap the child manifest's `package` path atomically, restart child via supervisor

Phase 3+:
- Status query endpoint (current child id, uptime, last crash, etc.)
- Configurable rate-limit / chain-cap (currently hard-coded)
- Attachment-based chain delivery once the inbox supports it (today the chain ships inline in the email body)

## Run locally

```sh
nix build .#default
nix build .#theater -o result-theater
./result-theater/bin/theater start sentinel-actor/manifest.toml
```

`sentinel-actor/manifest.toml` ships with `initial_state = "{}"` (placeholder). Replace with a real config before running. Shape:

```json
{
  "child_manifest": "/abs/path/to/child/manifest.toml",
  "dev_email": "inbox-dev@colinrozzi.com",
  "inbox_api": "mail.colinrozzi.com:443",
  "inbox_token": "<bearer token>"
}
```

State persists across restarts via `theater:simple/store` at `./.store/sentinel/` (repo-local).

## Demo / test: deliberately-crashing child

`tests/crashing-child/` ships a minimal actor that calls `runtime.shutdown(Some(bytes))` in its init, so the supervising sentinel sees a `handle-child-exit` with a non-empty result and runs the crash flow.

```sh
nix build .#default
# point sentinel-actor/manifest.toml's initial_state at:
#   {
#     "child_manifest": "/abs/path/to/sentinel/tests/crashing-child/manifest.toml",
#     "dev_email": "sentinel-dev@colinrozzi.com",
#     "inbox_api": "mail.colinrozzi.com:443",
#     "inbox_token": "<token>"
#   }
./result-theater/bin/theater start sentinel-actor/manifest.toml
```

The child crashes immediately on init; expect to see a crash email land in `dev_email`'s mailbox, then a respawn. After 5 crashes within 60s the rate limiter trips and the child stays dead.

## Architecture

```
sentinel (singleton, lives as long as the supervised system)
  ├── supervisor.spawn → child actor system (e.g. inbox's acceptor, tickets' acceptor)
  └── supervisor-handlers callbacks:
        handle-child-event       → accumulate chain
        handle-child-error       → email + respawn
        handle-child-exit        → email + respawn
        handle-child-external-stop → no respawn (intentional stop)

(phase 2)
  └── tcp listen :NNN → on POST /deploy {url, sha256}: fetch, verify, swap, restart
```

## How it talks to the inbox

The sentinel doesn't run its own mail stack. It POSTs to inbox's `/v1/mailboxes/<dev>/send` (or `/messages` if we want to bypass outbound delivery and just put the notification straight into the dev's inbox). Bearer token + inbox API endpoint live in the sentinel's `initial_state` config.

## Security model (phase 2)

The HTTPS deploy endpoint is the only inbound surface. Bearer-token auth keyed off a secret stored in the sentinel's config. GitHub Actions has the token as a repo secret. No SSH key shared with CI; no shell access to the VPS from the action.

Verification: the deploy request includes a SHA256 the sentinel checks against the downloaded artifact. A GitHub compromise lets an attacker push a release, but signed tags + a manual checksum review of the action's output narrows the window further.
