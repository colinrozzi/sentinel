# Mesh-node supervision spec (sentinel)

Status: DRAFT v2 (2026-08-18). **RE-SCOPED per Colin's call (mail id=486):**
silent-wedge handling moves from EXTERNAL probing (sentinel polls a node health
signal) to INTERNAL fail-loud (each node self-watchdogs and panics on no-progress).
Sentinel stays crash-only. Executes with the chat prod stand-up.

## Goal

Detection + auto-restart + crash-notify for prod mesh nodes (chat rooms, weft
volumes, additional control identities), with **silent wedges converted to LOUD
crashes at the source** so the existing crash-path catches them.

## Division of responsibility (the re-scope)

- **sentinel (this repo): LOUD deaths only.** Wire each prod mesh system as a
  supervised child; the shipped crash-supervision (respawn, per-child rate limiter,
  chain buffer) handles it — *including* wedge-crashes, which are now loud. **No
  liveness-probe in the heartbeat.**
- **mesh-dev (mesh node): the in-node progress watchdog.** A dead-man's-switch timer
  the main loop pets each cycle; if the loop wedges/deadlocks and doesn't pet within
  K seconds (frontier not advancing, gossip loop stuck, WANT runaway, poisoned
  store), the watchdog PANICS the process. It runs *independent* of the stuck loop,
  so it's robust even against deadlocks. This converts the smtp-acceptor silent-wedge
  class into a loud death.

## Why this is cleaner

1. **Same pattern, pushed down.** It's the dead-man's-switch sentinel already runs on
   ITSELF (the off-box heartbeat), moved DOWN into each node. Consistent.
2. **No cross-team coupling.** mesh-dev no longer exposes a liveness signal for
   sentinel to poll (the earlier `health()` ask is scratched). Each node owns its own
   liveness.
3. **The node is best-positioned to make the call.** It has full internal visibility
   (is the tick loop running, is ingest draining, is WANT bounded), which dissolves
   the idle-vs-wedged ambiguity an *external* poller would have to guess at. An
   idle-but-healthy node still pets (its loop runs); only a genuinely stuck loop
   fails to pet. No false-positive-restart-of-an-idle-node risk on the supervisor side.

## Model (unchanged)

Each prod mesh system = a sentinel-supervised child actor (its own mesh composite),
declared via a mesh-node manifest template in sentinel's `children` config. Reuses
the shipped multi-child supervisor verbatim: `handle-child-error`/`handle-child-exit`
-> respawn that child; per-child rate limiter (5/60s -> `restart_blocked`); per-child
chain ring buffer. Because mesh nodes are **replicated**, a respawned node re-syncs
from peers -> detection, not state recovery, is the job.

## Failure classes (all loud; all arrive as `handle-child-error`/`exit`)

1. **Ordinary crash** (panic / host-call failure / exit): existing path.
2. **SOFT wedge** — handlers *return* normally but the node stops making progress
   (frontier stalls, WANT runaway, poisoned store). The in-guest tick watchdog pets-
   or-panics -> loud crash. **mesh-dev ships it** (their `DESIGN-liveness.md`).
3. **HARD wedge** — a handler infinite-loops / deadlocks and never returns. No
   in-guest code can catch this. **theater already covers it** (mail id=489, verified
   in code, live on packr >=0.10.6): a background epoch ticker + `set_epoch_deadline`
   traps any guest call that doesn't return within K seconds; the trap Err routes
   through `handle_actor_error` -> `ActorResult::Error` to the supervisor
   (`handle-child-error` fires) + graceful stop. So a hard loop becomes a supervised
   crash with no in-guest cooperation needed. K today is a hard-coded const:
   `actor.init` = ~60s, all other calls (tick/handle-*/decode) = ~300s. theater-dev
   offered a per-manifest epoch-deadline knob if a mesh node wants a tighter ceiling
   (a hard-looped handler pegs a core for up to K before the trap; 300s is loose for
   a mesh node — a tighter K means faster restart). **The K value is mesh-dev's call;
   supervisor-side preference: tighter, so a lost replica recovers in ~30-60s not 5min.**

Sentinel does not distinguish any of these — all three arrive as
`handle-child-error`/`handle-child-exit` and take the existing crash-path. Nothing
new on the supervisor side.

## Crash-notify (unchanged)

Off-box #43 path: a journal watcher tails `[sentinel] crash ...` /
`[sentinel] crash loop ...`; heartbeat-absence past a threshold is the dead-man's-
switch for a fully-dead sentinel. A watchdog-panic surfaces as an ordinary crash
line. **No inbox-dependent alerting** (the mail spine is supervised here — alerting
through it fails exactly when it's the thing down). *Nice-to-have (mesh-dev's call):*
have the panic message name the pet-condition that fired (`frontier_stalled` /
`gossip_stuck` / `want_runaway` / `store_poisoned`) so the crash line carries the
wedge reason for diagnostics.

## Architectural note

- **Control node** (sentinel's own, composed-in): stays composed-in short-term
  (members=1, low-churn). **Bonus from this re-scope:** the in-node watchdog applies
  to it too, so a control-node wedge now panics -> crashes the sentinel process
  (composed-in) -> systemd restart. That *partially closes the composed-in blind spot*
  I flagged earlier (a silent in-composite stall is now a loud process crash).
- **Business nodes** (chat / weft): separate supervised children from day one.

## Node lifecycle: identity, persistence, and the two supervision profiles

Settled with Colin (design session 2026-08-18/19). What a node must preserve across
a restart is **not uniform** — it depends on what the node's log *is*.

**The diagnostic:** *if the history's referent is reconstituted from somewhere else,
the mesh is a command channel (ephemeral); if the history IS the referent, it's a
datastore (durable).*

**Profile A — ephemeral (command-channel mesh; e.g. the control plane).** The log is
a transcript of imperatives + results (`list`/`stop X`/response) — not a system of
record. The authoritative "what is running" lives in sentinel's children config
(durable) + the live theater process table, both **reconstituted from config on every
boot**. So sentinel death legitimately wipes the control-plane state, and that's fine
— it was never the source of truth. Consequences:
- **No persistence needed.** No `node_seed` to save, no durable event store.
- **Fresh genesis per lifetime works TODAY** with zero new machinery. The
  identity-verification blocker (a fresh key can't be pre-listed in `join_allow`)
  applies only to *joining* nodes; the control node is a **genesis root** — it never
  joins, it roots its own mesh — so it can regenerate its identity every boot. Clients
  re-join fresh each lifetime; `command_allow`/`join_allow` re-seed from the genesis
  event (config).
- **Composed-in is correct** for it (not a liability): a node whose meaning resets
  with sentinel *should* share sentinel's lifetime.
- **Kill-and-restart is completely free** — nothing to preserve, nothing to recover.
- Note: the children's *own* application data (inbox's mail store, etc.) persists in
  the children's own stores, independent of the control mesh — the control log never
  held it.

**Profile B — durable (datastore mesh; e.g. chat rooms, weft volumes).** The log **is**
the content; losing it is real data loss. Protect it by **replication, not persistence
per se**: run **≥2 members** so a dead node re-syncs its state from a live peer on
restart. Consequences:
- **Never the sole replica of anything durable.** A solo datastore node that dies with
  no peer to re-sync from *is* data loss — replicate to ≥2 and restart becomes recovery.
- **Identity:** stable/hardcoded for now (fresh-identity-per-restart for a *joiner*
  needs a join-credential/attestation verification scheme that isn't built — parked;
  the unlock is "build key-verification → joiner nodes get disposable too").
- **The one discipline that replaces "persist the store":** *catch up before you
  author.* An accidental self-fork happens only when a node authors off a stale/empty
  head before syncing. Enforcing catch-up-before-author lets a replicated node recover
  from peers **without** persisting; persistence drops to a speed optimization. (So the
  earlier "hardcoded identity ⟹ must persist" was too strong: hardcoded + sole-replica
  ⟹ persist-or-reset; hardcoded + replicated ⟹ catch-up-before-author, persist optional.)

**On forks generally (design stance):** a self-fork is an **SM concern, not a
substrate-level ban**. The substrate already admits forks (partial-order DAG); RSM
membership is already fork-tolerant (folds all finalized directly, no ancestry walk).
Two SM-layer tools cover the cases: *merge* a fork = apply-confluence (free for CRDT-ish
SMs like chat); *reset* the mesh to genesis = an authored `Reset` kind the SM applies as
supersede/truncate, gated on a trusted key (fine for a single-operator fleet). Neither
needs the substrate to police forks. **Open (non-blocking):** whether we want the
operator `Reset` verb now or just confluent SMs — different costs (the former needs a
who-may-reset trust decision), decided when a data mesh actually needs it.

## Ticket breakdown (simplified by the re-scope)

- **T1 (sentinel)** — mesh-node child manifest template + wire a mesh system as a
  supervised child (config schema + spawn path). *Gated on chat being a persistent
  prod service.* This is now essentially ALL of sentinel's net-new work. **Two child
  profiles** (above): an **ephemeral** profile (no store, fresh genesis — the control
  node) and a **durable/replicated** profile (≥2 members, catch-up-before-author,
  restart recovers from peers — chat/weft). Not a uniform "persist the store."
- **T2 (mesh-dev)** — in-node progress watchdog (dead-man's-switch -> panic on
  no-progress). The net-new that makes wedges loud. *Replaces the old liveness-signal
  ticket.*
- **T3 (ops, optional)** — the off-box watcher already keys on `crash`; wedges now
  surface AS crashes, so no `wedge` line is needed. Optionally fold the pet-condition
  into the panic message for richer crash diagnostics.

Dropped vs v1: the liveness-probe-in-heartbeat (T3), the idle-vs-wedged predicate +
false-positive guard (T4), and the `wedge` journal line (T5) — all obviated by
fail-loud-at-the-source. Both remaining tickets ship WITH the chat prod stand-up.
