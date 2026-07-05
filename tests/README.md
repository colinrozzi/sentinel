# sentinel tests

## Two-children crash-loop test

Proves the Phase A refactor keeps **per-child** accounting: two supervised
children crash-loop and rate-limit independently, and the heartbeat reports the
supervised set.

### Actors

- `crashing-child/` — a minimal actor that calls `runtime.shutdown(Some(bytes))`
  in its init. The supervising sentinel sees `handle-child-exit` and runs its
  crash flow. We spawn two instances of it under different names.

### Run

```sh
nix build .#default            # builds sentinel.wasm + crashing_child.wasm
nix build .#theater -o result-theater

# 1. edit tests/two-crashing-children.json so the two manifest paths point at
#    THIS checkout's tests/crashing-child/manifest.toml (absolute path).
# 2. put that JSON into sentinel-actor/manifest.toml's initial_state
#    (single-line; keep it a JSON string).
./result-theater/bin/theater spawn sentinel-actor/manifest.toml
```

### What to expect in the journal

Both children crash immediately on init, so each runs its own respawn cycle:

```
[sentinel] spawned + subscribed crash-a -> child <id>
[sentinel] spawned + subscribed crash-b -> child <id>
[sentinel] init complete — supervising 2 children, heartbeat every 5000ms
[sentinel] crash child=<id> name=crash-a reason=exit t_ms=... recent_restarts=0
[sentinel] spawned + subscribed crash-a -> child <id'>
[sentinel] crash child=<id> name=crash-b reason=exit t_ms=... recent_restarts=0
...
[sentinel] heartbeat children=2 blocked=0 t_ms=...
...
[sentinel] crash loop for crash-a (6 crashes in 60000ms) — not respawning
[sentinel] crash loop for crash-b (6 crashes in 60000ms) — not respawning
[sentinel] heartbeat children=2 blocked=2 t_ms=...
```

Key assertions:
- Each child independently reaches 5 restarts before its own rate limiter trips
  (per-child, not a shared global counter — a global limiter would trip at 5
  total, ~half the restarts seen here).
- The `blocked=` count in the heartbeat climbs from 0 to 2 as each child trips.
- A crash of `crash-a` never touches `crash-b`'s counters, and vice versa.

### Healthy-child path

The healthy path (a child that stays up, accumulates a chain, and keeps the
heartbeat's `children=` count without ever incrementing `blocked=`) is exercised
at integration time by the real UI children (inbox-ui, tickets-ui), which bind a
loopback `/healthz`. No synthetic stable-child is kept in-tree for that.
