// The sentinel control-plane protocol — the payload schema both sides build against.
// ONE source of truth: the SM and the client generate their `Msg` type from this file
// via `pact!(from "…/control.pact")` (packr 0.20) — no shared Rust crate. The node is
// generic over the payload `p`; it stores/gossips/hashes these as opaque bytes and
// decodes them to `Msg` only at the fold, handing the SM a typed value.
//
// A mesh is per-conversation; a sentinel's control mesh is one such mesh, rooted at the
// sentinel's genesis. Membership + allow-lists arrive as the genesis EVENT (config),
// not as node config — the SM owns membership (there is no node-core membership layer).
//
// Kinds:
//   genesis      — seeds the control mesh: member set + who may join + who may command.
//                  Authored once, by the sentinel (the genesis root).
//   join-request — a pre-authorized actor (in join_allow) admits itself as a member.
//   depart       — a member leaves (causal self-depart; the no-kick invariant).
//   command      — an authorized member asks the sentinel to act. `verb` + `args`:
//                    "list"       args: (empty)
//                    "start"      args: encodes { name, package }
//                    "stop"       args: encodes { name }
//                    "get_chain"  args: encodes { name }
//                  (`verb` is a free string; the SM is verb-agnostic — it enforces
//                  membership + command_allow + corr_id freshness, not the verb set.
//                  Future: "follow_chain".)
//   response     — the sentinel's result for a command, matched by (cmd-author, corr-id).

record genesis-cfg {
    members: list<list<u8>>,       // 32-byte pubkeys — the initial member set (the sentinel)
    join-allow: list<list<u8>>,    // pubkeys allowed to self-admit via join-request (the managers)
    command-allow: list<list<u8>>, // pubkeys allowed to author commands
}

record command-body {
    corr-id: u64,                  // unique per (author) — the journal key + response correlator
    verb: string,                  // "list" | "start" | "stop" | "get_chain" | …
    args: list<u8>,                // verb-specific, opaque to the SM
}

record response-body {
    corr-id: u64,                  // the command's corr-id
    cmd-author: list<u8>,          // the command's author (with corr-id, the journal key)
    result: list<u8>,              // verb-specific result bytes, opaque to the SM
}

variant msg {
    genesis(genesis-cfg),
    join-request,
    depart,
    command(command-body),
    response(response-body),
}

world control {}
