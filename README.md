# sentinel

Supervisor + crash-notification + (eventually) HTTPS-triggered deploy for theater actor systems.

Runs as the top-level process for an actor system: it spawns the configured child manifest, watches it, and on crash collects the child's chain, emails it to the corresponding dev agent's mailbox, and respawns. Eventually a small HTTPS endpoint accepts authenticated deploy prompts from GitHub Actions to fetch + swap binaries.

## Status: phase 0 — skeleton only

What works:
- Spawns a configured child manifest on init
- Logs lifecycle events (`handle-child-event`, `handle-child-error`, `handle-child-exit`, `handle-child-external-stop`)

What's coming in phase 1:
- Accumulate the child's chain in memory via `handle-child-event`
- On error / exit-with-error: serialize the chain, email it to the configured dev address via the inbox API, respawn the child

What's coming in phase 2:
- HTTPS endpoint (bearer-token auth) that accepts deploy prompts
- Pull binary from GitHub release artifact (URL + SHA256 in the request)
- Verify checksum, swap the child manifest's `package` path atomically, restart child via supervisor

Phase 3+:
- Per-child rate limiting on respawn (currently unbounded restart-loops are possible)
- Crash-chain truncation rules (chains can grow large)
- Status query endpoint (current child id, uptime, last crash, etc.)

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
