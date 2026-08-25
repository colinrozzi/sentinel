//! `control-sm` — the sentinel control plane as an RSM `state-machine` component.
//!
//! RSM Interface 1 (mesh `docs/DESIGN-dx.md`): membership + `command_allow` authz + a
//! command/response journal keyed by `(author, corr_id)`. Pure, deterministic,
//! structure-blind — it sees only `(id, author, timestamp, payload, state)`, never the DAG.
//!
//! **This is the "rules" layer, shared by BOTH sides of the control plane** — the
//! sentinel (genesis root + responder) and every managing client fold this same SM. The
//! two sides differ only in their *system* (behavior/I/O), never in the rules.
//!
//! Admission-final under the **no-kick invariant** (membership shrinks only via causal
//! self-`depart`, never a concurrent kick) — so no command is ever concurrent with its
//! author's removal, everything is conflict-free, and a member finalizes on admission.
//! Config (members + allow-lists) arrives as a **genesis event**, not node config.
//!
//! The payload arrives **typed** as `Msg`, generated from the shared `control.pact` via
//! `pact!(from …)` — no protocol crate, no `decode` in the SM (the node is generic over
//! the payload `p`). State is opaque bytes (serde-json). Rules mirror the proven mesh
//! `tests/networks/control` SM, authored fresh here on packr 0.20.

#![cfg_attr(not(test), no_std)]
extern crate alloc;

use alloc::collections::BTreeSet;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use packr_guest::export;
use serde::{Deserialize, Serialize};

#[cfg(not(test))]
packr_guest::setup_guest!();

// `Msg` + its payload records (GenesisCfg / CommandBody / ResponseBody), generated from
// the shared `control.pact` (one source of truth — resolved relative to the crate root).
packr_guest::pact!(from "../control.pact");

packr_guest::pack_types! {
    exports {
        state-machine {
            initial-state: func() -> list<u8>,
            validate: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: msg, state: list<u8>) -> result<bool, string>,
            apply: func(id: list<u8>, author: list<u8>, timestamp: u64, payload: msg, state: list<u8>) -> list<u8>,
            members: func(state: list<u8>) -> list<list<u8>>,
        }
    }
}

// ===== state =====

#[derive(Serialize, Deserialize, Default, Clone)]
struct ControlState {
    members: BTreeSet<Vec<u8>>,
    join_allow: BTreeSet<Vec<u8>>,
    command_allow: BTreeSet<Vec<u8>>,
    journal: Vec<Entry>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Entry {
    author: Vec<u8>, // the command's author (with corr_id, the journal key)
    corr_id: u64,
    verb: String,
    args: Vec<u8>,
    response: Option<Vec<u8>>,
}

impl ControlState {
    fn decode(bytes: &[u8]) -> ControlState {
        if bytes.is_empty() {
            return ControlState::default();
        }
        serde_json::from_slice(bytes).unwrap_or_default()
    }
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
    fn entry(&self, author: &[u8], corr_id: u64) -> Option<&Entry> {
        self.journal.iter().find(|e| e.author == author && e.corr_id == corr_id)
    }
}

// ===== core logic (host-testable) — payload arrives TYPED as `Msg`, no decode =====

fn do_validate(author: &[u8], msg: &Msg, state: &[u8]) -> Result<bool, String> {
    let s = ControlState::decode(state);
    match msg {
        Msg::Genesis(_) => {
            if s.members.is_empty() && s.join_allow.is_empty() && s.command_allow.is_empty() {
                Ok(true)
            } else {
                Err("genesis after the control mesh already exists".to_string())
            }
        }
        Msg::JoinRequest => {
            if s.join_allow.contains(author) {
                Ok(true)
            } else {
                Err("join-request: author not in join_allow".to_string())
            }
        }
        Msg::Depart => {
            if s.members.contains(author) {
                Ok(true)
            } else {
                Err("depart: author is not a member".to_string())
            }
        }
        Msg::Command(CommandBody { corr_id, .. }) => {
            if !s.members.contains(author) {
                Err("command: author is not a member".to_string())
            } else if !s.command_allow.contains(author) {
                Err("command: author not in command_allow".to_string())
            } else if s.entry(author, *corr_id).is_some() {
                Err("command: corr_id already used by this author".to_string())
            } else {
                Ok(true)
            }
        }
        Msg::Response(ResponseBody { corr_id, cmd_author, .. }) => {
            if !s.members.contains(author) {
                return Err("response: author is not a member".to_string());
            }
            match s.entry(cmd_author, *corr_id) {
                Some(e) if e.response.is_none() => Ok(true),
                Some(_) => Err("response: already answered".to_string()),
                None => Err("response: no matching command".to_string()),
            }
        }
    }
}

fn do_apply(author: &[u8], msg: Msg, state: &[u8]) -> Vec<u8> {
    let mut s = ControlState::decode(state);
    match msg {
        Msg::Genesis(GenesisCfg { members, join_allow, command_allow }) => {
            s.members = members.into_iter().collect();
            // The founder (genesis author = the sentinel, the genesis root) is a member
            // by definition — so the sentinel-system never needs to know its own pubkey
            // to seed the member set; the node signs the Genesis and apply adds it here.
            s.members.insert(author.to_vec());
            s.join_allow = join_allow.into_iter().collect();
            s.command_allow = command_allow.into_iter().collect();
        }
        Msg::JoinRequest => {
            s.members.insert(author.to_vec());
        }
        Msg::Depart => {
            s.members.remove(author);
        }
        Msg::Command(CommandBody { corr_id, verb, args }) => {
            s.journal.push(Entry { author: author.to_vec(), corr_id, verb, args, response: None });
        }
        Msg::Response(ResponseBody { corr_id, cmd_author, result }) => {
            if let Some(e) = s.journal.iter_mut().find(|e| e.author == cmd_author && e.corr_id == corr_id) {
                e.response = Some(result);
            }
        }
    }
    s.encode()
}

// ===== the interface =====

#[export(name = "initial-state")]
fn initial_state() -> Vec<u8> {
    ControlState::default().encode()
}

#[export]
fn validate(_id: Vec<u8>, author: Vec<u8>, _timestamp: u64, payload: Msg, state: Vec<u8>) -> Result<bool, String> {
    do_validate(&author, &payload, &state)
}

#[export]
fn apply(_id: Vec<u8>, author: Vec<u8>, _timestamp: u64, payload: Msg, state: Vec<u8>) -> Vec<u8> {
    do_apply(&author, payload, &state)
}

#[export]
fn members(state: Vec<u8>) -> Vec<Vec<u8>> {
    ControlState::decode(&state).members.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(n: u8) -> Vec<u8> {
        alloc::vec![n; 32]
    }

    /// Fold (author, msg) through validate+apply, applying only what validates.
    fn fold(seed: &[u8], evs: &[(Vec<u8>, Msg)]) -> Vec<u8> {
        let mut s = seed.to_vec();
        for (author, msg) in evs {
            if do_validate(author, msg, &s).is_ok() {
                s = do_apply(author, msg.clone(), &s);
            }
        }
        s
    }

    fn sentinel() -> Vec<u8> {
        k(1)
    }
    fn manager() -> Vec<u8> {
        k(2)
    }
    fn stranger() -> Vec<u8> {
        k(9)
    }

    fn genesis() -> Msg {
        Msg::Genesis(GenesisCfg {
            members: alloc::vec![sentinel()],
            join_allow: alloc::vec![manager()],
            command_allow: alloc::vec![manager()],
        })
    }

    #[test]
    fn genesis_seeds_membership_and_allowlists() {
        let s = fold(b"", &[(sentinel(), genesis())]);
        let st = ControlState::decode(&s);
        assert!(st.members.contains(&sentinel()));
        assert!(st.join_allow.contains(&manager()) && st.command_allow.contains(&manager()));
    }

    #[test]
    fn genesis_author_is_a_member_even_if_absent_from_the_list() {
        // Founder is auto-membered — the sentinel-system need not know its own pubkey.
        let g = Msg::Genesis(GenesisCfg {
            members: alloc::vec![],
            join_allow: alloc::vec![manager()],
            command_allow: alloc::vec![manager()],
        });
        let s = fold(b"", &[(sentinel(), g)]);
        assert!(ControlState::decode(&s).members.contains(&sentinel()));
    }

    #[test]
    fn second_genesis_is_rejected() {
        let s = fold(b"", &[(sentinel(), genesis())]);
        assert!(do_validate(&sentinel(), &genesis(), &s).is_err());
    }

    #[test]
    fn join_request_gated_by_join_allow() {
        let s = fold(b"", &[(sentinel(), genesis())]);
        assert!(do_validate(&manager(), &Msg::JoinRequest, &s).is_ok());
        assert!(do_validate(&stranger(), &Msg::JoinRequest, &s).is_err());
        let s2 = fold(&s, &[(manager(), Msg::JoinRequest)]);
        assert!(ControlState::decode(&s2).members.contains(&manager()));
    }

    #[test]
    fn command_requires_membership_and_command_allow_and_fresh_corrid() {
        let s = fold(b"", &[(sentinel(), genesis()), (manager(), Msg::JoinRequest)]);
        let cmd = |c| Msg::Command(CommandBody { corr_id: c, verb: "list".into(), args: alloc::vec![] });
        assert!(do_validate(&manager(), &cmd(1), &s).is_ok());
        assert!(do_validate(&stranger(), &cmd(1), &s).is_err());
        let s2 = fold(&s, &[(manager(), cmd(1))]);
        assert!(do_validate(&manager(), &cmd(1), &s2).is_err(), "dup corr_id");
        assert!(do_validate(&manager(), &cmd(2), &s2).is_ok(), "fresh corr_id");
    }

    #[test]
    fn start_stop_get_chain_are_valid_verbs() {
        // The SM is verb-agnostic; the v1 verbs journal like any command.
        let s = fold(b"", &[(sentinel(), genesis()), (manager(), Msg::JoinRequest)]);
        let mk = |c, v: &str, a: Vec<u8>| Msg::Command(CommandBody { corr_id: c, verb: v.into(), args: a });
        let s2 = fold(&s, &[
            (manager(), mk(1, "start", b"{\"name\":\"frontdoor\",\"package\":\"url\"}".to_vec())),
            (manager(), mk(2, "stop", b"{\"name\":\"frontdoor\"}".to_vec())),
            (manager(), mk(3, "get_chain", b"{\"name\":\"frontdoor\"}".to_vec())),
        ]);
        let st = ControlState::decode(&s2);
        assert_eq!(st.journal.len(), 3);
        assert_eq!(st.entry(&manager(), 1).unwrap().verb, "start");
    }

    #[test]
    fn response_answers_a_pending_command_once() {
        let cmd = Msg::Command(CommandBody { corr_id: 7, verb: "list".into(), args: alloc::vec![] });
        let s = fold(b"", &[(sentinel(), genesis()), (manager(), Msg::JoinRequest), (manager(), cmd)]);
        let resp = |r: &[u8]| Msg::Response(ResponseBody { corr_id: 7, cmd_author: manager(), result: r.to_vec() });
        assert!(do_validate(&sentinel(), &resp(b"ok"), &s).is_ok());
        let s2 = fold(&s, &[(sentinel(), resp(b"ok"))]);
        let st = ControlState::decode(&s2);
        assert_eq!(st.entry(&manager(), 7).unwrap().response.as_deref(), Some(&b"ok"[..]));
        assert!(do_validate(&sentinel(), &resp(b"again"), &s2).is_err(), "already answered");
        assert!(do_validate(&sentinel(), &Msg::Response(ResponseBody { corr_id: 99, cmd_author: manager(), result: alloc::vec![] }), &s2).is_err());
    }

    #[test]
    fn depart_removes_self() {
        let s = fold(b"", &[(sentinel(), genesis()), (manager(), Msg::JoinRequest)]);
        let s2 = fold(&s, &[(manager(), Msg::Depart)]);
        assert!(!ControlState::decode(&s2).members.contains(&manager()));
    }
}
