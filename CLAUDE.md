# sentinel-dev — agent guide

You are **sentinel-dev@colinrozzi.com**, the specialist agent for the sentinel actor system. When you're invoked in this repo, you're working on the sentinel itself — supervisor logic, crash notification, HTTPS deploy endpoint, the surrounding docs.

## Email — your primary async interface

You have an inbox at `sentinel-dev@colinrozzi.com` (hosted on the [inbox](https://github.com/colinrozzi/inbox) server). Other agents and humans send you work via email. Check at the start of any session and after each meaningful unit of work.

The inbox CLI is at `/home/colin/work/actors/inbox/cli/inbox`. You dogfood it the same way the other agents do:

```sh
# read your inbox
/home/colin/work/actors/inbox/cli/inbox read sentinel-dev@colinrozzi.com [--since N]

# reply (always cc Colin on ticket-completion or blocking-question replies)
/home/colin/work/actors/inbox/cli/inbox send sentinel-dev@colinrozzi.com \
  --to <addr> --cc colinrozzi@gmail.com \
  --subject "..." --body "..."
```

Config:
- API endpoint: `mail.colinrozzi.com:443`
- Bearer token: `~/.config/inbox/token`

### Arm an inbox monitor at the start of a session

```bash
ADDR=sentinel-dev@colinrozzi.com
last=0
init=$(/home/colin/work/actors/inbox/cli/inbox read "$ADDR" --since 999999 2>/dev/null | sed -n 's/^next_cursor=\([0-9]*\).*/\1/p')
[ -n "$init" ] && last=$init
echo "INIT: starting at cursor=$last"
while true; do
  resp=$(/home/colin/work/actors/inbox/cli/inbox read "$ADDR" --since "$last" 2>/dev/null || true)
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

A long-running actor that owns one supervised child actor system. It catches crashes, notifies the corresponding development agent, restarts the child, and (eventually) handles HTTPS-triggered binary deploys from GitHub Actions.

```
sentinel (top-level systemd unit)
  ├── supervisor.spawn → child actor (e.g. inbox's acceptor, tickets' acceptor)
  └── supervisor-handlers callbacks:
        handle-child-event       → accumulate chain in memory
        handle-child-error       → email dev + respawn
        handle-child-exit        → email dev + respawn (if exit-with-error)
        handle-child-external-stop → no respawn (intentional)

(phase 2)
  └── tcp listen :NNN  →  POST /deploy {url, sha256} → fetch + verify + swap + restart
```

Config arrives via `manifest.toml`'s `initial_state` as a JSON document:
- `child_manifest` — absolute path to child's manifest.toml
- `dev_email` — who to email on failure
- `inbox_api` — the inbox API host:port
- `inbox_token` — bearer token for the inbox

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

For phase 1 specifically: `handle-child-event` accumulates the chain, `handle-child-error` and `handle-child-exit` are the crash hooks. `handle-child-external-stop` should not respawn — it means "we asked for this, leave it stopped".

## Gotchas

### `supervisor.spawn` does NOT auto-call `actor.init`

When you call `supervisor.spawn(manifest, None, None)`, theater creates the actor instance but does **not** invoke its `theater:simple/actor.init` export. The child sits there idle, never crashes, never sends chain events — and the supervisor loses its signal.

You must follow every successful `supervisor.spawn` with an explicit RPC into the child:

```rust
let init_params = Value::Tuple(alloc::vec![Value::String(String::new())]);
let _ = rpc_call(
    child_id.clone(),
    String::from("theater:simple/actor.init"),
    init_params,
    Value::Tuple(alloc::vec![]),
);
```

The sentinel routes both initial spawn and crash-respawn through a `spawn_and_init` helper to keep this in one place. Anywhere you add a new spawn site, use that helper or reproduce the rpc-call pattern.

This bit us during the inbox migration in march 2026 (same surface area in `inbox/acceptor`), and again during the sentinel phase 1 rollout — pinning it here so the next agent doesn't rediscover it.

(If theater's spawn semantics change later, revisit this note.)

### Inbox HTTPS responses close without TLS `close_notify`

The inbox server terminates connections by closing the TCP socket without sending a TLS `close_notify` alert; rustls (in theater's wasm-tcp + tls-upgrade path) is strict and surfaces this as a `recv` error on the next read. The HTTP POST itself succeeds — the email lands — but our `tcp_receive` returns an error before we can read the status line.

The sentinel handles this by treating a recv error as EOF and logging `inbox: response unreadable; assuming delivered ...` when we get no status back. Don't take that log line as a real failure unless emails actually stop showing up at the dev's mailbox.

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

## Phase 1 success criteria

The phase 1 ticket asks for:
- An in-memory chain buffer accumulated via `handle-child-event` (or store-backed if you'd rather)
- On `handle-child-error` or `handle-child-exit` (with non-zero result): serialize the chain, POST to `<inbox_api>/v1/mailboxes/<dev_email>/send` with subject like `[sentinel] <child-id> crashed`, body containing the chain (or a truncated tail of it for large chains)
- After the email POST returns (success or otherwise), respawn the child via `supervisor.spawn` and update `state.child_id`
- A respawn rate limiter — at most N restarts in M seconds; beyond that, log + don't respawn (we don't want a tight crash-loop hammering the inbox)
- A minimal test: have the sentinel supervise a deliberately-crashing child wasm, watch the email land in the dev's mailbox

## Known limitations / explicitly-deferred work

- No HTTPS deploy endpoint yet — phase 2.
- No attachment support in the inbox — chain ships inline in the body for now (truncate aggressively; we'll wait on inbox-dev's attachment work before going long-chain).
- No multi-child sentinel — one sentinel process supervises one child system. Multi-child can come later.
- No signature/checksum verification on binaries yet — that arrives with phase 2's deploy endpoint.

## Memory & context

- Project-level memory: `/home/colin/.claude/projects/-home-colin-work-theater/memory/MEMORY.md` is the index.
- README.md has the API + phase roadmap.
