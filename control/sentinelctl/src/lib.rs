//! sentinelctl — the manager's ephemeral-node control CLI, as a one-shot composed
//! actor. It joins the control mesh, sends ONE control command to sentinel, reads
//! the response, and DEPARTS cleanly. Like `git push`: connect, transact, disconnect.
//!
//! Composition: this actor (entry) + the mesh-client component (mesh + mesh-control
//! interfaces), fused by `packr compose`. mesh.* / mesh-control.* are internalized;
//! `message-server-host` stays residual for theater to supply.
//!
//! Lifecycle (slice 1 = `list`):
//!   - init: register with the message server (to receive deliveries), build a mesh
//!     InitConfig (node_seed = the manager control keypair; dial = sentinel's control
//!     node), spawn the ephemeral node child, arm a timeout as a safety net.
//!   - handle-send / is-ready (v0.3 sync signal — node admitted + caught up): now the
//!     node is live, so `register` for delivery and `submit(encode-command(list))`.
//!   - handle-send / delivery of a Response for our corr_id: log the result, then
//!     `depart` cleanly (clean Depart is LOAD-BEARING — it removes the ephemeral node
//!     instantly, so churn never stalls sentinel's control mesh; the evict-timeout
//!     only bites when a node vanishes without departing).
//!   - handle-tick (timeout): if we never completed, log the timeout and depart anyway.
//!
//! The driver (`theater spawn`) reads the RESULT/ERROR line from the actor's log and
//! stops the spawn once done — the actor departs first, then idles until stopped.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use packr_guest::{export, import, import_from, pack_types, GraphValue, Value};

packr_guest::setup_guest!();

pack_types! {
    imports {
        // The complete mesh interface (v0.3.0) — declared in full for the
        // hash-checked link; only the fns we call are bound via #[import_from].
        mesh {
            submit: func(node: string, payload: list<u8>) -> result<list<u8>, string>,
            introduce: func(node: string, member: list<u8>) -> result<list<u8>, string>,
            depart: func(node: string) -> result<list<u8>, string>,
            register: func(node: string, app-id: string) -> result<bool, string>,
            delivery: func(msg: list<u8>) -> option<tuple<list<u8>, list<u8>>>,
            is-ready: func(msg: list<u8>) -> bool,
            node-config: func(seed: string, listen: string, members: list<string>, dial: list<tuple<string, string>>) -> string,
        }
        // The complete mesh-control interface — the app-level control envelope.
        mesh-control {
            encode-command: func(corr-id: u64, target: list<u8>, cmd: string, args: list<u8>) -> list<u8>,
            encode-response: func(corr-id: u64, target: list<u8>, result: list<u8>) -> list<u8>,
            encode-lifecycle: func(event: u8, actor-id: string, ts: u64, data: list<u8>) -> list<u8>,
            control-kind: func(bytes: list<u8>) -> option<u8>,
            decode-command: func(bytes: list<u8>) -> option<tuple<u64, list<u8>, string, list<u8>>>,
            decode-response: func(bytes: list<u8>) -> option<tuple<u64, list<u8>, list<u8>>>,
            decode-lifecycle: func(bytes: list<u8>) -> option<tuple<u8, string, u64, list<u8>>>,
        }
        theater:simple/runtime {
            log: func(msg: string),
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
        }
        theater:simple/timer {
            set-interval: func(name: string, interval-ms: u64) -> result<string, string>,
        }
        theater:simple/message-server-host {
            register: func() -> result<_, string>,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<actor-state, string>,
        theater:simple/timer.handle-tick: func(state: actor-state, timer-name: string) -> result<actor-state, string>,
        // packr delivers `params: tuple<list<u8>>` POSITIONALLY → fn(state, msg).
        theater:simple/message-server-client.handle-send: func(state: actor-state, params: tuple<list<u8>>) -> result<actor-state, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);
// `self` is a WIT keyword the pack_types! tokenizer can't express — #[import] only.
#[import(module = "theater:simple/runtime", name = "self")]
fn runtime_self() -> String;
#[import(module = "theater:simple/supervisor", name = "spawn")]
fn supervisor_spawn(manifest: String, init_state: Option<Value>, wasm_bytes: Option<Vec<u8>>) -> Result<String, String>;
#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;
#[import(module = "theater:simple/message-server-host", name = "register")]
fn message_server_register() -> Result<(), String>;

// ---- composed mesh + mesh-control bindings (satisfied by the mesh-client component) ----
#[import_from("mesh", name = "node-config")]
fn mesh_node_config(seed: String, listen: String, members: Vec<String>, dial: Vec<(String, String)>) -> String;
#[import_from("mesh", name = "register")]
fn mesh_register(node: String, app_id: String) -> Result<bool, String>;
#[import_from("mesh", name = "submit")]
fn mesh_submit(node: String, payload: Vec<u8>) -> Result<Vec<u8>, String>;
#[import_from("mesh", name = "depart")]
fn mesh_depart(node: String) -> Result<Vec<u8>, String>;
#[import_from("mesh", name = "delivery")]
fn mesh_delivery(msg: Vec<u8>) -> Option<(Vec<u8>, Vec<u8>)>;
#[import_from("mesh", name = "is-ready")]
fn mesh_is_ready(msg: Vec<u8>) -> bool;
#[import_from("mesh-control", name = "encode-command")]
fn ctl_encode_command(corr_id: u64, target: Vec<u8>, cmd: String, args: Vec<u8>) -> Vec<u8>;
#[import_from("mesh-control", name = "control-kind")]
fn ctl_kind(bytes: Vec<u8>) -> Option<u8>;
#[import_from("mesh-control", name = "decode-response")]
fn ctl_decode_response(bytes: Vec<u8>) -> Option<(u64, Vec<u8>, Vec<u8>)>;

// phase: 0 spawned/waiting-ready, 1 submitted, 2 done
#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct CtlState {
    pub my_id: String,
    pub node_id: String,
    pub target_hex: String,
    pub cmd: String,
    pub args_json: String,
    pub corr_id: u64,
    pub phase: u8,
}

#[derive(serde::Deserialize)]
struct CtlConfig {
    /// mesh node child manifest (points at mesh.wasm).
    node_manifest: String,
    /// The manager control keypair seed → deterministic pubkey (STABLE identity).
    node_seed: String,
    /// Throwaway loopback listen for the ephemeral node.
    node_listen: String,
    /// Sentinel's control node: pubkey (hex) + dial address. Used for dial AND target.
    sentinel_pubkey: String,
    sentinel_addr: String,
    /// The control verb (slice 1: "list") + opaque JSON args.
    #[serde(default = "default_cmd")]
    cmd: String,
    #[serde(default)]
    args_json: String,
    #[serde(default = "default_corr")]
    corr_id: u64,
    /// Safety-net timeout (ms) before we give up + depart.
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}
fn default_cmd() -> String { "list".to_string() }
fn default_corr() -> u64 { 1 }
fn default_timeout() -> u64 { 15000 }

const APP_ID: &str = "sentinel-control";

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(CtlState, ()), String> {
    let cfg: CtlConfig = match state {
        Value::String(s) if !s.is_empty() => serde_json::from_str(&s).map_err(|e| format!("parse config: {}", e))?,
        _ => return Err("sentinelctl: missing config".to_string()),
    };
    log("[sentinelctl] init".to_string());

    let my_id = runtime_self();
    if let Err(e) = message_server_register() {
        log(format!("[sentinelctl] message-server register failed: {}", e));
    }

    // Build the ephemeral node's InitConfig: our stable manager identity dialing
    // sentinel's control node as the single peer.
    // members = the GENESIS set, BYTE-IDENTICAL on every node (mesh-dev): sentinel's
    // control node is the sole founding member; sentinelctl JOINS, so it does NOT
    // list itself. An empty members would mean {self} → a forked mesh with split
    // finality. dial + join_allow (on sentinel's side) are what differ, not members.
    let node_init = mesh_node_config(
        cfg.node_seed.clone(),
        cfg.node_listen.clone(),
        alloc::vec![cfg.sentinel_pubkey.clone()],
        alloc::vec![(cfg.sentinel_pubkey.clone(), cfg.sentinel_addr.clone())],
    );
    let node_id = supervisor_spawn(cfg.node_manifest.clone(), Some(Value::String(node_init)), None)
        .map_err(|e| format!("spawn node: {}", e))?;
    log(format!("[sentinelctl] spawned ephemeral node {}", node_id));

    // Safety net: if we never see is-ready + a response, give up and depart.
    if let Err(e) = timer_set_interval("timeout".to_string(), cfg.timeout_ms) {
        log(format!("[sentinelctl] set timeout failed: {}", e));
    }

    Ok((
        CtlState {
            my_id,
            node_id,
            target_hex: cfg.sentinel_pubkey,
            cmd: cfg.cmd,
            args_json: cfg.args_json,
            corr_id: cfg.corr_id,
            phase: 0,
        },
        (),
    ))
}

#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: CtlState, msg: Vec<u8>) -> Result<(CtlState, ()), String> {
    // Route: is-ready first (the one-shot sync signal), else a committed delivery.
    if mesh_is_ready(msg.clone()) {
        if state.phase != 0 {
            return Ok((state, ()));
        }
        log("[sentinelctl] node READY — registering + submitting command".to_string());
        if let Err(e) = mesh_register(state.node_id.clone(), APP_ID.to_string()) {
            log(format!("[sentinelctl] register failed: {}", e));
        }
        let target = match hex_to_bytes(&state.target_hex) {
            Some(t) => t,
            None => return Err(format!("bad sentinel pubkey hex: {}", state.target_hex)),
        };
        let payload = ctl_encode_command(
            state.corr_id,
            target,
            state.cmd.clone(),
            state.args_json.clone().into_bytes(),
        );
        match mesh_submit(state.node_id.clone(), payload) {
            Ok(_) => log(format!("[sentinelctl] submitted cmd={} corr={}", state.cmd, state.corr_id)),
            Err(e) => log(format!("[sentinelctl] submit failed: {}", e)),
        }
        return Ok((CtlState { phase: 1, ..state }, ()));
    }

    if let Some((_from, body)) = mesh_delivery(msg) {
        if let Some(2) = ctl_kind(body.clone()) {
            if let Some((corr, _tgt, result)) = ctl_decode_response(body) {
                if corr == state.corr_id {
                    // The result IS what sentinel's TCP `list` returns.
                    log(format!("[sentinelctl] RESULT corr={} {}", corr, String::from_utf8_lossy(&result)));
                    let _ = mesh_depart(state.node_id.clone());
                    log("[sentinelctl] departed — done".to_string());
                    return Ok((CtlState { phase: 2, ..state }, ()));
                }
            }
        }
    }
    Ok((state, ()))
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: CtlState, _timer: String) -> Result<(CtlState, ()), String> {
    if state.phase == 2 {
        return Ok((state, ()));
    }
    log(format!("[sentinelctl] TIMEOUT (phase={}) — departing", state.phase));
    let _ = mesh_depart(state.node_id.clone());
    Ok((CtlState { phase: 2, ..state }, ()))
}

fn hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let hi = hex_nib(b[i])?;
        let lo = hex_nib(b[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}
fn hex_nib(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
