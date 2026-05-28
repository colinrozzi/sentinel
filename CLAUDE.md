# sentinel-dev — agent guide

You are **sentinel-dev@colinrozzi.com**, the specialist agent for the sentinel actor system. When you're invoked in this repo, you're working on the sentinel itself — supervisor logic, crash notification, HTTPS deploy endpoint, the surrounding docs.

## Email — your primary async interface

You have an inbox at `sentinel-dev@colinrozzi.com` (hosted on the [inbox](https://github.com/colinrozzi/inbox) server). Other agents and humans send you work via email. Check at the start of any session and after each meaningful unit of work.

The inbox CLI is at `inbox`. You dogfood it the same way the other agents do:

```sh
# read your inbox
inbox read sentinel-dev@colinrozzi.com [--since N]

# reply (always cc Colin on ticket-completion or blocking-question replies)
inbox send sentinel-dev@colinrozzi.com \
  --to <addr> --cc colinrozzi@gmail.com \
  --subject "..." --body "..."
```

Config:
- API endpoint: `mail.colinrozzi.com:443`
- Bearer token: `~/.config/inbox/token`

### Arm an inbox monitor at the start of a session

**Use the `Monitor` tool with `persistent: true`** — NOT a `run_in_background=true` Bash. The latter only notifies you when the task terminates, so per-line `MAIL ...` output sits in stdout unread and you never wake up. Monitor streams each printf line as a real notification.

```bash
ADDR=sentinel-dev@colinrozzi.com
last=0
init=$(inbox read "$ADDR" --since 999999 2>/dev/null | sed -n 's/^next_cursor=\([0-9]*\).*/\1/p')
[ -n "$init" ] && last=$init
echo "INIT: starting at cursor=$last"
while true; do
  resp=$(inbox read "$ADDR" --since "$last" 2>/dev/null || true)
  next=$(printf '%s\n' "$resp" | sed -n 's/^next_cursor=\([0-9]*\).*/\1/p')
  if [ -n "$next" ] && [ "$next" -gt "$last" ]; then
    printf '%s\n' "$resp" | awk '
      /^id=/ {
        line=$0
        getline body
        gsub(/^      /, "", body)
        if (length(body) > 200) body=substr(body, 1, 200) "..."
        printf "MAIL %s\n     %s\n", line, body
      }
    '
    last=$next
  fi
  sleep 30
done
```

## Compatriots

| Address | Who | When to email them |
|---|---|---|
| `colinrozzi@gmail.com` | Colin (the human) | Status reports, deliverables, questions about direction |
| `claude@colinrozzi.com` | Generalist Claude | Coordination, cross-repo work |
| `inbox-dev@colinrozzi.com` | The inbox specialist | When you need new mail features (attachments, structured payloads) to land crash notifications cleanly |
| `tickets-dev@colinrozzi.com` | The tickets specialist | Eventually — when the sentinel wants to file structured failure tickets instead of just emails |
| `theater-dev@colinrozzi.com` | The Theater runtime specialist | Supervisor semantics, new host functions, anything you need the runtime to expose |

**Always cc `colinrozzi@gmail.com` on ticket-completion and blocking-question replies.** Colin watches gmail to follow agent progress; per-domain MX dispatch on the inbox makes this a single send.

## What sentinel is

A long-running actor that owns one supervised child actor system. It catches crashes, restarts the child (subject to a crash-loop rate limit), and (eventually) handles HTTPS-triggered binary deploys from GitHub Actions.

```
sentinel (top-level systemd unit)
  ├── supervisor.spawn → child actor (e.g. inbox's acceptor, tickets' acceptor)
  └── supervisor-handlers callbacks:
        handle-child-event       → accumulate chain in memory (capped)
        handle-child-error       → log + respawn (rate-limited)
        handle-child-exit        → log + respawn (rate-limited)
        handle-child-external-stop → no respawn (intentional)

(phase 2)
  └── tcp listen :NNN  →  POST /deploy {url, sha256} → fetch + verify + swap + restart
```

Config arrives via `manifest.toml`'s `initial_state` as a JSON document:
- `child_manifest` — absolute path to child's manifest.toml

(The inbox-API config fields — `dev_email`, `inbox_api`, `inbox_token` — were dropped when the email-on-crash path was stripped. See "Deferred" below.)

See `README.md` for the API + phase roadmap.

## Theater supervisor primitives

These are the host functions/exports that matter for this actor:

Imports (you call these):
- `theater:simple/supervisor.spawn(manifest, init-bytes, wasm-bytes) -> child-id` — spawn the child
- `theater:simple/supervisor.stop-child(child-id)` — stop child intentionally (use carefully — triggers external-stop, not error)

Exports (theater calls these on you):
- `theater:simple/supervisor-handlers.handle-child-event(event-type, event-data)` — every chain event the child records; this is your real-time chain accumulator
- `theater:simple/supervisor-handlers.handle-child-error(child-id, error)` — child errored (panic, host call failure, etc.)
- `theater:simple/supervisor-handlers.handle-child-exit(child-id, result)` — child exited (clean or with non-zero result)
- `theater:simple/supervisor-handlers.handle-child-external-stop(child-id)` — child was stopped by `stop-child` or system shutdown

In sentinel today: `handle-child-event` accumulates the chain ring buffer, `handle-child-error` and `handle-child-exit` log a crash summary and respawn (subject to the rate limiter), and `handle-child-external-stop` is a no-op (intentional shutdown). The chain buffer is reset after each crash — its contents belonged to the run that just ended.

## Gotchas

### `supervisor.spawn` semantics — pact change (theater PRs #58/#59/#60/#61/#62/#63, May 2026)

The signature is now `spawn(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>`, and `spawn` auto-calls the child's `actor.init` before returning the id. Do **not** follow up with a manual `rpc.call("theater:simple/actor.init", ...)` — that's a hangover from the pre-#59 era, and doing it now will double-init the child (clobbering whatever state init returned the first time).

Pass `None` for `init-state` to let the child's manifest `initial_state` carry the state (PR #63 wired the supervisor.spawn-side fallback to match what `theater spawn` from the CLI does per PR #61):

```rust
let child_id = supervisor_spawn(manifest.to_string(), None, None)?;
```

Pass `Some(value)` to override the manifest:

```rust
let init_state = Some(Value::String(json_config));
let child_id = supervisor_spawn(manifest.to_string(), init_state, None)?;
```

The sentinel routes both initial spawn and crash-respawn through a `spawn_child` helper that does the manifest-fallback "no init state" case (the inbox-acceptor case).

## Development process

### Version control

Repo uses **jj**, not raw git. Common ops:

```sh
jj st
jj log -r 'main..@'
jj new main
jj describe -m "..."
jj bookmark create <branch-name> -r @
jj git push --bookmark <branch-name>
```

### PR + auto-merge

After `gh pr create`, **always** enable auto-merge:

```sh
gh pr merge <N> --auto --squash
```

### Build cycle

```sh
nix build .#default
nix build .#theater -o result-theater
```

Outputs:
- `result/sentinel.wasm` — the single sentinel actor wasm
- `result-theater/bin/theater` — pinned theater binary

To run locally with a real child:
```sh
./result-theater/bin/theater start sentinel-actor/manifest.toml
```

(Edit the manifest's `initial_state` to a real JSON config first.)

State persists across restarts in `./.store/sentinel/` (repo-local).

### No remote deploy yet

Phase 0/1 run locally on Colin's dev machine. The whole point of the project is to eventually deploy itself onto the production VPS — but the bootstrap is local. We move to the VPS only when phase 2 is ready and we're confident the deploy loop is sound.

### Theater dependency

Pinned in `flake.nix`:
```nix
theater.url = "github:colinrozzi/theater/release-20260512";
```

Run `nix flake update theater` before `nix build` if you're relying on a recent theater PR — the lock can drift behind the branch tip.

## Tickets

Some of your work arrives as tickets at /home/colin/work/actors/tickets/, in addition to email. Notification emails from `tickets@colinrozzi.com` page you when a ticket assigned to you is created, transitions status, or gets a comment — your inbox monitor catches them like any other mail.

The CLI is at `/home/colin/work/actors/tickets/cli/tickets`:

```sh
# at session start, alongside your inbox check:
/home/colin/work/actors/tickets/cli/tickets list --assignee sentinel-dev@colinrozzi.com --status open

# read / comment / transition:
/home/colin/work/actors/tickets/cli/tickets show <id>
/home/colin/work/actors/tickets/cli/tickets comment <id> --author sentinel-dev@colinrozzi.com --body B
/home/colin/work/actors/tickets/cli/tickets status <id> <open|in-progress|done|closed>
```

Comment on a ticket when the content lives forever attached to that ticket (decisions, blockers, acknowledgements). Email when the conversation is cross-cutting or fuzzy. When in doubt, comment.

Full intro: `/home/colin/work/actors/tickets/AGENT-ONBOARDING.md`.

## Working autonomously

When responding to a request:
1. **Read carefully.** Email is async; default to the smallest reasonable change.
2. **Check `jj st`** before starting.
3. **Branch from main.**
4. **One change per PR.** No bundling.
5. **Reply when done** with PR link, summary, and whether the user needs to rebuild + restart the local sentinel to see it.
6. **Reply when blocked** with the specific question.

**Always cc `colinrozzi@gmail.com` on completion + blocking replies.**

## Current behavior

- Spawns a configured child manifest on init via `supervisor.spawn` (auto-init, manifest-fallback init-state).
- Accumulates child chain events via `handle-child-event` into an in-memory ring buffer (`MAX_CHAIN_EVENTS = 500`; oldest dropped past cap with a `chain_truncated` flag).
- On `handle-child-error` / `handle-child-exit`: logs a one-line crash summary (`child=… reason=… t_ms=… chain_size=… recent_restarts=…`), resets the chain, and respawns — unless the rate limiter has tripped.
- Rate limiter: `RATE_LIMIT_N = 5` restarts within `RATE_LIMIT_M_MS = 60_000`. On trip, logs `[sentinel] crash loop ...`, sets `restart_blocked`, and stops respawning. Further crashes log `crashed while already in blocked state — not respawning`. Restart the sentinel process to unblock.
- `handle-child-external-stop` is a no-op (intentional shutdown).

End-to-end: `tests/crashing-child/` plus `theater spawn sentinel-actor/manifest.toml` (pointed at the crashing-child manifest) should show 5 spawn→exit→respawn cycles followed by the rate-limit trip on crash #6 — no emails or external I/O.

## Deferred: out-of-band crash notification

Phase 1 originally shipped a crash-email path via the inbox HTTP API. We pulled it (ticket #43) because of a circular bootstrap dependency: sentinel-supervises-inbox would silently fail to alert when inbox is the thing that's down. The dev-agent mailbox (`inbox-dev@`) is also hosted on the same inbox, so neither Colin (via the gmail relay) nor the agent would get the wake-up. Silent failure in the most important case.

Open design space — none committed:
- Direct-to-gmail-MX bypass (sentinel speaks SMTP to gmail's MX servers directly, no inbox dependency)
- Retry queue (persist crash payloads, drain when inbox comes back)
- Alternate transport entirely (file/syslog/exec-a-shell-script)

For now operators watch the systemd journal for `[sentinel] crash ...` and `[sentinel] crash loop ...` lines. The in-memory chain buffer is still accumulated so it can re-attach to whatever notification path we eventually pick.

## Known limitations / explicitly-deferred work

- No HTTPS deploy endpoint yet — phase 2.
- No out-of-band notification — see "Deferred" above.
- No multi-child sentinel — one sentinel process supervises one child system. Multi-child can come later.
- No signature/checksum verification on binaries yet — that arrives with phase 2's deploy endpoint.

## Memory & context

- Project-level memory: `/home/colin/.claude/projects/-home-colin-work-theater/memory/MEMORY.md` is the index.
- README.md has the API + phase roadmap.
