# sentinel

Supervisor + bearer-token TCP+JSON deploy gateway for theater actor systems.

Runs as the top-level process for an actor system. Spawns N configured child actors at init, watches them, respawns each on crash (subject to a per-child crash-loop rate limiter). Phase 2 added the TCP+JSON command surface. Phase 3 generalised from a single child to N statically-registered children, each independently rate-limited and individually targetable by `name`. Phase 3.1 added per-child chain ring buffers on top of theater 0.3.18's `handle-child-event` carrying child-id.

## What works

- Spawns every configured child on init; init hard-fails if any one of them fails to spawn (operator needs to know on day 1)
- Pre-spawn template validation: init scans each child's `manifest_template` for `__KEY__` placeholders and rejects the config if any KEY has no matching `secrets` entry (and isn't the built-in `__PACKAGE__`). Catches operator typos before the literal placeholder string gets persisted as a child's initial state
- On `handle-child-error` / `handle-child-exit` for a known child-id: logs a crash summary and respawns *that* child
- Per-child rate limiter: at most 5 restarts per 60s per child; past that, the affected child is flagged `restart_blocked` and not respawned until either a `start` command for that name lands or the sentinel process restarts. Independent across children — one runaway child can't block another's respawns
- `handle-child-external-stop` does **not** respawn (intentional shutdown)
- A crash or chain event whose child-id matches no tracked child (most commonly a stale event during respawn) is logged and ignored
- Per-child chain ring buffer (cap 500, oldest dropped past cap). Reset after each crash (the contents belonged to the run that just ended) and on a successful `start`. Reachable per-child via `get_chain { name }`
- TCP+JSON command surface (bearer-token auth):
  - `start { name, package }` — swap the named child's package URL/path and respawn
  - `stop { name }` — gracefully stop the named child (no respawn)
  - `list` — array of all children with current id, package, and `restart_blocked`
  - `get_chain { name }` — chain ring buffer for the named child
  - `health` — listener address + per-child status array (id, restart state, current package, chain size)

## Configuration

`sentinel-actor/manifest.toml`'s `initial_state` is a JSON document:

```json
{
  "listen_addr": "0.0.0.0:8444",
  "bearer_token": "<shared secret>",
  "children": {
    "tickets-acceptor": {
      "manifest_template": "name = \"tickets\"\nversion = \"0.1.0\"\npackage = \"__PACKAGE__\"\n\n[[handler]]\ntype = \"runtime\"\n...\ninitial_state = \"__API_TOKEN__\"\n",
      "default_package": "https://github.com/colinrozzi/tickets/releases/download/release-XXX/tickets_acceptor.wasm",
      "secrets": {
        "INBOX_TOKEN": "<inbox HTTP API bearer for the child>",
        "API_TOKEN":   "<the child's own HTTP API token>"
      }
    },
    "inbox-acceptor": {
      "manifest_template": "name = \"inbox\"\nversion = \"0.1.0\"\npackage = \"__PACKAGE__\"\n\n[[handler]]\ntype = \"runtime\"\n...\n",
      "default_package": "https://github.com/colinrozzi/inbox/releases/download/release-XXX/inbox_acceptor.wasm",
      "secrets": {
        "BEARER_TOKEN": "<inbox API bearer>",
        "DKIM_KEY":     "<PEM-encoded DKIM private key>"
      }
    }
  }
}
```

The `children` map is the operator's source of truth for which actor systems sentinel supervises. The map key (e.g. `"tickets-acceptor"`) is the operator-chosen *name* that TCP commands target.

Per child:

- `manifest_template` — the child's full `manifest.toml` body, with placeholders for any values that vary per deploy
- `default_package` — initial wasm artifact URL/path; the first spawn substitutes this into `__PACKAGE__`. Subsequent `start { name, package: ... }` commands rewrite it
- `secrets` — `{"KEY": "value"}` map; every `__KEY__` placeholder in the template is substituted with the corresponding value at every spawn

Placeholders:

- `__PACKAGE__` — the wasm artifact URL
- `__KEY__` for each entry in the child's `secrets` map

Sentinel renders each child's manifest TOML at spawn time, writes it to its own content store under a per-child label (`child-manifest-<name>`), and hands theater a `store://sentinel/child-manifest-<name>` URI. theater's `resolve_reference` supports only `store://`, `http(s)://`, and bare filesystem paths — inline manifest content is not supported, so the store hop is mandatory.

Secret values live only in sentinel's RAM — never persisted to disk on sentinel's side. Substitution order per child is *all secrets first, then `__PACKAGE__`* — don't reuse `__PACKAGE__` as a literal substring inside a secret value.

Secret values are inserted **verbatim**. If a value contains TOML-special characters (`"`, `\`, etc.) they must be pre-escaped in the JSON config so that the resulting `field = "<value>"` line parses correctly. For hex tokens / random secrets this isn't an issue.

## Protocol

One JSON object per connection, one JSON response, then close. All requests carry a `token` field matching the configured bearer token; mismatch returns `{"ok": false, "error": "unauthorized"}`.

```
$ printf '{"token":"...","cmd":"health"}\n' | nc 127.0.0.1 8444
{"ok":true,"listen_addr":"0.0.0.0:8444",
 "children":[
   {"name":"tickets-acceptor","child_id":"<uuid>","restart_blocked":false,"recent_restarts":0,"current_package":"<url>","chain_size":0,"chain_truncated":false},
   {"name":"inbox-acceptor",  "child_id":"<uuid>","restart_blocked":false,"recent_restarts":0,"current_package":"<url>","chain_size":0,"chain_truncated":false}
 ]}

$ printf '{"token":"...","cmd":"list"}\n' | nc 127.0.0.1 8444
{"ok":true,"children":[
   {"name":"tickets-acceptor","child_id":"<uuid>","current_package":"<url>","restart_blocked":false},
   {"name":"inbox-acceptor",  "child_id":"<uuid>","current_package":"<url>","restart_blocked":false}
 ]}

$ printf '{"token":"...","cmd":"start","name":"tickets-acceptor","package":"https://.../new.wasm"}\n' | nc 127.0.0.1 8444
{"ok":true,"name":"tickets-acceptor","child_id":"<new-uuid>","current_package":"https://.../new.wasm"}

$ printf '{"token":"...","cmd":"stop","name":"tickets-acceptor"}\n' | nc 127.0.0.1 8444
{"ok":true,"name":"tickets-acceptor","stopped_child_id":"<uuid>"}

$ printf '{"token":"...","cmd":"get_chain","name":"tickets-acceptor"}\n' | nc 127.0.0.1 8444
{"ok":true,"chain":["..."],"chain_truncated":false}
```

`start { name, package }` against an unknown `name` returns `{"ok":false,"error":"unknown child name: <name>"}`. Static registration — Phase 3 does not support dynamic child registration; the `children` map at init is the complete set.

A successful `start` resets the *per-child* crash-loop block and restart-history window — operator intent of `start` for that child is "the previous problem has been addressed, give it another shot." Other children are unaffected.

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

`tests/crashing-child/` ships a minimal actor that calls `runtime.shutdown(Some(bytes))` in its init, so the supervising sentinel sees a `handle-child-exit` with a non-empty result and runs the crash flow. To exercise per-child rate-limit independence, register it as *two* children in the config — one will trip its own limiter while the other (separately registered crashing-child or a healthy child) is unaffected.

Example minimal config for the rate-limiter exercise — two crashing children under the same `default_package`:

```json
{
  "listen_addr": "0.0.0.0:8444",
  "bearer_token": "test",
  "children": {
    "crash-a": {
      "manifest_template": "name = \"crashing-child\"\nversion = \"0.1.0\"\npackage = \"__PACKAGE__\"\n\n[[handler]]\ntype = \"runtime\"\n",
      "default_package": "/absolute/path/to/result/crashing_child.wasm",
      "secrets": {}
    },
    "crash-b": {
      "manifest_template": "name = \"crashing-child\"\nversion = \"0.1.0\"\npackage = \"__PACKAGE__\"\n\n[[handler]]\ntype = \"runtime\"\n",
      "default_package": "/absolute/path/to/result/crashing_child.wasm",
      "secrets": {}
    }
  }
}
```

Expect each child to cycle 5 respawns, then its own limiter trips on the 6th and that child stays dead while the other continues independently. `health` shows `restart_blocked: true` for the tripped child and a non-zero `recent_restarts` count for the other.

## Architecture

```
sentinel (top-level, lives as long as the supervised system)
  ├── tcp.listen :listen_addr  →  handle-connection → JSON dispatch
  │     {start, stop, list, get_chain, health}
  ├── for each configured child (statically registered at init):
  │     supervisor.spawn(store://sentinel/child-manifest-<name>) → child actor
  └── supervisor-handlers callbacks (theater 0.3.18+ — all carry child-id):
        handle-child-event       → append to that child's ring (cap 500)
        handle-child-error       → log + per-child respawn (rate-limited)
        handle-child-exit        → log + per-child respawn (rate-limited)
        handle-child-external-stop → no respawn (intentional stop)
```

## Security model

The TCP command surface is the only inbound network surface. Bearer-token auth is required on every request; comparison is constant-time. No TLS in v1 — assume the listener is reachable only from trusted networks (loopback or VPS-internal). GitHub Actions, when wired up, will hold the token as a repo secret and the deploy step will POST the `start` command.

Package verification: theater fetches https:// package URLs and assumes a 2xx is enough — no checksum or signature validation today. A GitHub compromise would let an attacker push a malicious release that sentinel would happily deploy. SHA256-pinning is a future theater-side ask; until then, restrict who can publish releases on the upstream repo.
