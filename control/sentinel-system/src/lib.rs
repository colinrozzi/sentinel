//! `sentinel-system` — the sentinel-side RSM system entry.
//!
//! **Sentinel the supervisor gaining a mesh face.** The entry half of the
//! `system ⊕ node ⊕ SM` composite (mesh `docs/DESIGN-dx.md`). It:
//!   1. owns ALL host I/O — tcp (gossip sockets), timer, supervisor, store;
//!   2. drives the composed pure `node` core (`node.pact`) — shuttles inbound gossip to
//!      it and performs the effects it returns (`[kind][id][payload]` blobs);
//!   3. authors the **control-plane genesis** (members + join/command allow-lists) so the
//!      sentinel roots its own control mesh (the genesis root — it never joins);
//!   4. **reacts** to finalized `command` events by running the REAL supervisor op
//!      (list / start / stop / get_chain — the same primitives sentinel always had) and
//!      authoring a `response`.
//!
//! Rules (membership, authz, the corr_id journal) live in the composed `control-sm`; this
//! is the *behavior + I/O* half. Reads finalized commands from the SM's journal each tick
//! (poll `node.current-state`), never a separate delivery channel.

#![no_std]
extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use packr_guest::{export, import, import_from, pack_types, GraphValue, Value};
use serde::{Deserialize, Serialize};

packr_guest::setup_guest!();

// `Msg` + records (GenesisCfg / CommandBody / ResponseBody) — the control protocol,
// generated from the shared `control.pact` (same source the SM uses).
packr_guest::pact!(from "../control.pact");

// ============================================================================
// Tunables (ported from sentinel-actor)
// ============================================================================

const MAX_CHAIN_EVENTS: usize = 500;
const MAX_EVENT_PAYLOAD_BYTES: usize = 256;
const RATE_LIMIT_N: usize = 5;
const RATE_LIMIT_M_MS: u64 = 60_000;
const DEFAULT_HEARTBEAT_MS: u64 = 30_000;
const DEFAULT_TICK_MS: u64 = 2_000;
const HEARTBEAT_TIMER_NAME: &str = "heartbeat";
const TICK_TIMER_NAME: &str = "tick";
const PACKAGE_PLACEHOLDER: &str = "__PACKAGE__";
const STORE_ID: &str = "sentinel";
const CHILD_MANIFEST_LABEL_PREFIX: &str = "child-manifest-";

// ============================================================================
// State
// ============================================================================

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct ChildState {
    pub name: String,
    pub manifest_template: String,
    pub current_package: String,
    pub secret_names: Vec<String>,
    pub secret_values: Vec<String>,
    pub subscribe: bool,
    pub child_id: String,
    pub chain: Vec<String>,
    pub chain_truncated: bool,
    pub restart_times: Vec<u64>,
    pub restart_blocked: bool,
}

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SysState {
    /// Handle for the gossip listener (the control mesh's tcp socket).
    pub gossip_listener_id: String,
    /// The composed node core's opaque state (node.pact) — persisted, never inspected.
    pub node: Vec<u8>,
    /// Supervised children.
    pub children: Vec<ChildState>,
}

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
            stop-child: func(child-id: string) -> result<_, string>,
            subscribe-to-child: func(child-id: string) -> result<_, string>,
        }
        theater:simple/timer {
            now: func() -> u64,
            set-interval: func(name: string, interval-ms: u64) -> result<string, string>,
        }
        theater:simple/tcp {
            listen: func(address: string) -> result<string, string>,
            connect: func(address: string) -> result<string, string>,
            activate: func(connection-id: string) -> result<_, string>,
            set-active: func(connection-id: string, mode: string) -> result<_, string>,
            send: func(connection-id: string, data: list<u8>) -> result<u64, string>,
            close: func(connection-id: string) -> result<_, string>,
        }
        theater:simple/store {
            store-at-label: func(store-id: string, label: string, content: list<u8>) -> result<string, string>,
        }
        // The composed pure core (node.pact / DESIGN-dx.md Interface 0). Full interface
        // declared for the hash-checked link; we bind the subset we drive.
        node {
            init: func(config: string, now: u64) -> result<tuple<list<u8>, list<u8>>, string>,
            on-connect: func(state: list<u8>, conn: string, dialed: bool, peer: string) -> tuple<list<u8>, list<list<u8>>>,
            on-bytes: func(state: list<u8>, conn: string, data: list<u8>, now: u64) -> tuple<list<u8>, list<list<u8>>>,
            on-close: func(state: list<u8>, conn: string) -> list<u8>,
            tick: func(state: list<u8>) -> tuple<list<u8>, list<list<u8>>>,
            author: func(state: list<u8>, payload: list<u8>, now: u64) -> tuple<list<u8>, bool, list<u8>, list<list<u8>>>,
            subscribe: func(state: list<u8>, app-id: string) -> tuple<list<u8>, list<list<u8>>>,
            current-state: func(state: list<u8>) -> list<u8>,
            current-members: func(state: list<u8>) -> list<list<u8>>,
            event-status: func(state: list<u8>, id: list<u8>) -> u8,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<sys-state, string>,
        theater:simple/tcp-client.handle-connection: func(state: sys-state, connection-id: string) -> result<sys-state, string>,
        theater:simple/tcp-client.on-data: func(state: sys-state, connection-id: string, data: list<u8>) -> result<sys-state, string>,
        theater:simple/tcp-client.on-close: func(state: sys-state, connection-id: string, reason: string) -> result<sys-state, string>,
        theater:simple/timer.handle-tick: func(state: sys-state, name: string) -> result<sys-state, string>,
        theater:simple/supervisor-handlers.handle-child-error: func(state: sys-state, child-id: string, error: value) -> result<sys-state, string>,
        theater:simple/supervisor-handlers.handle-child-exit: func(state: sys-state, child-id: string, result: value) -> result<sys-state, string>,
        theater:simple/supervisor-handlers.handle-child-external-stop: func(state: sys-state, child-id: string) -> result<sys-state, string>,
        theater:simple/supervisor-handlers.handle-child-event: func(state: sys-state, child-id: string, event-type: string, event-data: list<u8>) -> result<sys-state, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);
#[import(module = "theater:simple/timer", name = "now")]
fn now() -> u64;
#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;
#[import(module = "theater:simple/tcp", name = "listen")]
fn tcp_listen(address: String) -> Result<String, String>;
#[import(module = "theater:simple/tcp", name = "connect")]
fn tcp_connect(address: String) -> Result<String, String>;
#[import(module = "theater:simple/tcp", name = "activate")]
fn tcp_activate(conn_id: String) -> Result<(), String>;
#[import(module = "theater:simple/tcp", name = "set-active")]
fn tcp_set_active(conn_id: String, mode: String) -> Result<(), String>;
#[import(module = "theater:simple/tcp", name = "send")]
fn tcp_send(conn_id: String, data: Vec<u8>) -> Result<u64, String>;
#[import(module = "theater:simple/tcp", name = "close")]
fn tcp_close(conn_id: String) -> Result<(), String>;
#[import(module = "theater:simple/store", name = "store-at-label")]
fn store_at_label(store_id: String, label: String, content: Vec<u8>) -> Result<String, String>;
#[import(module = "theater:simple/supervisor", name = "spawn")]
fn supervisor_spawn(manifest: String, init_state: Option<Value>, wasm_bytes: Option<Vec<u8>>) -> Result<String, String>;
#[import(module = "theater:simple/supervisor", name = "stop-child")]
fn supervisor_stop_child(child_id: String) -> Result<(), String>;
#[import(module = "theater:simple/supervisor", name = "subscribe-to-child")]
fn supervisor_subscribe_to_child(child_id: String) -> Result<(), String>;

#[import_from("node", name = "init")]
fn node_init(config: String, now: u64) -> Result<(Vec<u8>, Vec<u8>), String>;
#[import_from("node", name = "on-connect")]
fn node_on_connect(state: Vec<u8>, conn: String, dialed: bool, peer: String) -> (Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "on-bytes")]
fn node_on_bytes(state: Vec<u8>, conn: String, data: Vec<u8>, now: u64) -> (Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "on-close")]
fn node_on_close(state: Vec<u8>, conn: String) -> Vec<u8>;
#[import_from("node", name = "tick")]
fn node_tick(state: Vec<u8>) -> (Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "author")]
#[allow(clippy::type_complexity)]
fn node_author(state: Vec<u8>, payload: Vec<u8>, now: u64) -> (Vec<u8>, bool, Vec<u8>, Vec<Vec<u8>>);
#[import_from("node", name = "current-state")]
fn node_current_state(state: Vec<u8>) -> Vec<u8>;

// ============================================================================
// Config (initial_state)
// ============================================================================

#[derive(Deserialize)]
struct Config {
    /// Seed string the node hashes into its ed25519 identity.
    node_seed: String,
    /// Gossip listen address for the control mesh (e.g. "127.0.0.1:9500").
    #[serde(default)]
    listen_addr: Option<String>,
    /// Peers to outbound-dial on init (other control-mesh participants).
    #[serde(default)]
    dial: Vec<PeerEntry>,
    /// Control genesis: hex pubkeys allowed to self-admit as members.
    #[serde(default)]
    join_allow: Vec<String>,
    /// Control genesis: hex pubkeys allowed to author commands.
    #[serde(default)]
    command_allow: Vec<String>,
    /// Children to supervise, keyed by operator-chosen name.
    #[serde(default)]
    children: BTreeMap<String, ChildCfg>,
    #[serde(default)]
    tick_ms: Option<u64>,
    #[serde(default)]
    heartbeat_ms: Option<u64>,
}

#[derive(Deserialize)]
struct ChildCfg {
    manifest_template: String,
    default_package: String,
    #[serde(default)]
    secrets: BTreeMap<String, String>,
    #[serde(default = "default_subscribe")]
    subscribe: bool,
}
fn default_subscribe() -> bool {
    true
}

#[derive(Deserialize, Serialize)]
struct PeerEntry {
    pubkey: String,
    address: String,
}

/// The node's own JSON config (node_seed / listen_addr / dial) — membership is NOT here
/// (it left the core; the control-SM owns it via the genesis event we author).
#[derive(Serialize)]
struct NodeConfig<'a> {
    node_seed: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    listen_addr: Option<&'a str>,
    dial: &'a [PeerEntry],
}

// ============================================================================
// Reading the SM journal (control-sm's ControlState, serde-json)
// ============================================================================

#[derive(Deserialize)]
struct JournalView {
    #[serde(default)]
    journal: Vec<JournalEntry>,
}

#[derive(Deserialize)]
struct JournalEntry {
    author: Vec<u8>,
    corr_id: u64,
    verb: String,
    args: Vec<u8>,
    #[serde(default)]
    response: Option<Vec<u8>>,
}

// ============================================================================
// Command arg / response shapes
// ============================================================================

#[derive(Deserialize)]
struct NameArgs {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    package: Option<String>,
}

#[derive(Serialize)]
struct ListResp<'a> {
    ok: bool,
    children: Vec<ListChild<'a>>,
}
#[derive(Serialize)]
struct ListChild<'a> {
    name: &'a str,
    child_id: &'a str,
    current_package: &'a str,
    restart_blocked: bool,
}
#[derive(Serialize)]
struct GetChainResp<'a> {
    ok: bool,
    chain: &'a [String],
    chain_truncated: bool,
}
#[derive(Serialize)]
struct StartResp<'a> {
    ok: bool,
    name: &'a str,
    child_id: &'a str,
    current_package: &'a str,
}
#[derive(Serialize)]
struct StopResp<'a> {
    ok: bool,
    name: &'a str,
    stopped_child_id: &'a str,
}
#[derive(Serialize)]
struct ErrResp<'a> {
    ok: bool,
    error: &'a str,
}

// ============================================================================
// Effect + init-plan codecs (mirror the node's encoders — from mesh-system)
// ============================================================================

/// Perform each effect: `[kind:u8][id-len:u16 BE][id][payload]` (0=tcp send, 2=tcp close).
/// kind 1 (message-server to app) is unused — the sentinel reacts internally, no app.
fn perform(effects: Vec<Vec<u8>>) {
    for e in effects {
        if e.len() < 3 {
            continue;
        }
        let kind = e[0];
        let id_len = u16::from_be_bytes([e[1], e[2]]) as usize;
        if e.len() < 3 + id_len {
            continue;
        }
        let id = String::from_utf8_lossy(&e[3..3 + id_len]).into_owned();
        let payload = e[3 + id_len..].to_vec();
        match kind {
            0 => {
                let _ = tcp_send(id, payload);
            }
            2 => {
                let _ = tcp_close(id);
            }
            _ => {}
        }
    }
}

fn decode_init_plan(b: &[u8]) -> (String, u64, Vec<(String, String)>) {
    let mut dials = Vec::new();
    let rd_str = |b: &[u8], p: &mut usize| -> String {
        if *p + 2 > b.len() {
            return String::new();
        }
        let n = u16::from_be_bytes([b[*p], b[*p + 1]]) as usize;
        *p += 2;
        if *p + n > b.len() {
            return String::new();
        }
        let s = String::from_utf8_lossy(&b[*p..*p + n]).into_owned();
        *p += n;
        s
    };
    let mut p = 0usize;
    let listen = rd_str(b, &mut p);
    let tick = if p + 8 <= b.len() {
        let t = u64::from_be_bytes(b[p..p + 8].try_into().unwrap());
        p += 8;
        t
    } else {
        DEFAULT_TICK_MS
    };
    let ndials = if p + 2 <= b.len() {
        let n = u16::from_be_bytes([b[p], b[p + 1]]) as usize;
        p += 2;
        n
    } else {
        0
    };
    for _ in 0..ndials {
        let pk = rd_str(b, &mut p);
        let addr = rd_str(b, &mut p);
        dials.push((pk, addr));
    }
    (listen, tick, dials)
}

// ============================================================================
// Lifecycle: init (drive node, author genesis, spawn children)
// ============================================================================

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(SysState, ()), String> {
    log("[sentinel] init".to_string());
    let raw = match state {
        Value::String(s) if !s.is_empty() => s,
        _ => return Err("sentinel: initial_state must be a JSON config string".to_string()),
    };
    let cfg: Config =
        serde_json::from_str(&raw).map_err(|e| format!("sentinel: bad initial_state JSON: {}", e))?;

    // Validate children templates before we spawn anything.
    for (name, child) in &cfg.children {
        if name.is_empty() {
            return Err("sentinel: child name must be non-empty".to_string());
        }
        if !child.manifest_template.contains(PACKAGE_PLACEHOLDER) {
            return Err(format!(
                "sentinel: child '{}' manifest_template must contain {}",
                name, PACKAGE_PLACEHOLDER
            ));
        }
        if child.default_package.is_empty() {
            return Err(format!("sentinel: child '{}' default_package must be non-empty", name));
        }
        let mut known: Vec<&str> = Vec::with_capacity(child.secrets.len() + 1);
        known.push("PACKAGE");
        for k in child.secrets.keys() {
            known.push(k.as_str());
        }
        for placeholder in find_template_placeholders(&child.manifest_template) {
            if !known.iter().any(|k| *k == placeholder) {
                return Err(format!(
                    "sentinel: child '{}' references __{}__ but no such secret (only __PACKAGE__ is built-in)",
                    name, placeholder
                ));
            }
        }
    }

    // Drive the node up.
    let node_cfg = serde_json::to_string(&NodeConfig {
        node_seed: &cfg.node_seed,
        listen_addr: cfg.listen_addr.as_deref(),
        dial: &cfg.dial,
    })
    .map_err(|e| format!("sentinel: node config encode: {}", e))?;
    let (mut node, plan) = node_init(node_cfg, now())?;
    let (listen_addr, tick_ms, dials) = decode_init_plan(&plan);

    let gossip_listener_id =
        tcp_listen(listen_addr.clone()).map_err(|e| format!("sentinel: gossip listen({}): {}", listen_addr, e))?;
    log(format!("[sentinel] control mesh listening on {} (id={})", listen_addr, gossip_listener_id));

    let tick_ms = cfg.tick_ms.unwrap_or(tick_ms);
    if let Err(e) = timer_set_interval(TICK_TIMER_NAME.to_string(), tick_ms) {
        log(format!("[sentinel] set-interval(tick) failed: {}", e));
    }
    let heartbeat_ms = cfg.heartbeat_ms.unwrap_or(DEFAULT_HEARTBEAT_MS);
    if let Err(e) = timer_set_interval(HEARTBEAT_TIMER_NAME.to_string(), heartbeat_ms) {
        log(format!("[sentinel] set-interval(heartbeat) failed: {}", e));
    }

    for (pk, addr) in dials {
        match tcp_connect(addr.clone()) {
            Ok(conn_id) => {
                let _ = tcp_activate(conn_id.clone());
                let _ = tcp_set_active(conn_id.clone(), "active".to_string());
                let (n2, out) = node_on_connect(node, conn_id, true, pk);
                node = n2;
                perform(out);
            }
            Err(e) => log(format!("[sentinel] dial {} failed: {}", addr, e)),
        }
    }

    // Author the control-plane genesis: the sentinel roots its own control mesh. The
    // founder is auto-membered by the SM (no self-pubkey needed here); managers are
    // pre-authorized by pubkey in join/command allow-lists (from config).
    let genesis = Msg::Genesis(GenesisCfg {
        members: Vec::new(),
        join_allow: decode_hex_list(&cfg.join_allow),
        command_allow: decode_hex_list(&cfg.command_allow),
    });
    let (n2, ok, data, out) = node_author(node, encode_msg(genesis), now());
    node = n2;
    perform(out);
    if !ok {
        log(format!("[sentinel] control genesis REJECTED: {}", String::from_utf8_lossy(&data)));
    } else {
        log("[sentinel] control genesis authored".to_string());
    }

    // Build + spawn the supervised children (hard-fail init if any won't spawn).
    let mut children: Vec<ChildState> = cfg
        .children
        .into_iter()
        .map(|(name, c)| {
            let (secret_names, secret_values): (Vec<String>, Vec<String>) = c.secrets.into_iter().unzip();
            ChildState {
                name,
                manifest_template: c.manifest_template,
                current_package: c.default_package,
                secret_names,
                secret_values,
                subscribe: c.subscribe,
                child_id: String::new(),
                chain: Vec::new(),
                chain_truncated: false,
                restart_times: Vec::new(),
                restart_blocked: false,
            }
        })
        .collect();
    for child in children.iter_mut() {
        match spawn_child(child) {
            Ok(id) => child.child_id = id,
            Err(e) => return Err(format!("sentinel: spawn child '{}' failed: {}", child.name, e)),
        }
    }

    Ok((SysState { gossip_listener_id, node, children }, ()))
}

// ============================================================================
// Gossip handlers (drive the node's connection machine)
// ============================================================================

#[export(name = "theater:simple/tcp-client.handle-connection")]
fn handle_connection(state: SysState, conn_id: String) -> Result<(SysState, ()), String> {
    let SysState { gossip_listener_id, node, children } = state;
    if tcp_activate(conn_id.clone()).is_err() || tcp_set_active(conn_id.clone(), "active".to_string()).is_err() {
        let _ = tcp_close(conn_id);
        return Ok((SysState { gossip_listener_id, node, children }, ()));
    }
    let (node, out) = node_on_connect(node, conn_id, false, String::new());
    perform(out);
    Ok((SysState { gossip_listener_id, node, children }, ()))
}

#[export(name = "theater:simple/tcp-client.on-data")]
fn on_data(state: SysState, conn_id: String, data: Vec<u8>) -> Result<(SysState, ()), String> {
    let SysState { gossip_listener_id, node, children } = state;
    let (node, out) = node_on_bytes(node, conn_id, data, now());
    perform(out);
    Ok((SysState { gossip_listener_id, node, children }, ()))
}

#[export(name = "theater:simple/tcp-client.on-close")]
fn on_close(state: SysState, conn_id: String, _reason: String) -> Result<(SysState, ()), String> {
    let SysState { gossip_listener_id, node, children } = state;
    let node = node_on_close(node, conn_id);
    Ok((SysState { gossip_listener_id, node, children }, ()))
}

// ============================================================================
// Tick: drive the node, then REACT to finalized commands
// ============================================================================

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: SysState, name: String) -> Result<(SysState, ()), String> {
    let SysState { gossip_listener_id, mut node, mut children } = state;
    if name == TICK_TIMER_NAME {
        let (n2, out) = node_tick(node);
        node = n2;
        perform(out);

        // React: every unanswered command in the SM journal is ours to answer.
        let jv: JournalView = serde_json::from_slice(&node_current_state(node.clone())).unwrap_or(JournalView { journal: Vec::new() });
        let pending: Vec<(Vec<u8>, u64, String, Vec<u8>)> = jv
            .journal
            .into_iter()
            .filter(|e| e.response.is_none())
            .map(|e| (e.author, e.corr_id, e.verb, e.args))
            .collect();
        for (cmd_author, corr_id, verb, args) in pending {
            let result = dispatch(&mut children, &verb, &args);
            let resp = Msg::Response(ResponseBody { corr_id, cmd_author: cmd_author.clone(), result });
            let (n2, ok, data, out) = node_author(node, encode_msg(resp), now());
            node = n2;
            perform(out);
            if !ok {
                log(format!(
                    "[sentinel] control: response for corr_id={} rejected: {}",
                    corr_id,
                    String::from_utf8_lossy(&data)
                ));
            } else {
                log(format!("[sentinel] control: answered {} corr_id={}", verb, corr_id));
            }
        }
    } else if name == HEARTBEAT_TIMER_NAME {
        let blocked = children.iter().filter(|c| c.restart_blocked).count();
        log(format!("[sentinel] heartbeat children={} blocked={} t_ms={}", children.len(), blocked, now()));
    }
    Ok((SysState { gossip_listener_id, node, children }, ()))
}

/// Route a control verb into the supervisor primitives. Returns the result bytes
/// (JSON) that become the `response`'s `result`.
fn dispatch(children: &mut Vec<ChildState>, verb: &str, args: &[u8]) -> Vec<u8> {
    match verb {
        "list" => cmd_list(children),
        "get_chain" => match parse_args(args) {
            Ok(a) => cmd_get_chain(children, a.name),
            Err(e) => error_response(&e),
        },
        "start" => match parse_args(args) {
            Ok(a) => cmd_start(children, a.name, a.package),
            Err(e) => error_response(&e),
        },
        "stop" => match parse_args(args) {
            Ok(a) => cmd_stop(children, a.name),
            Err(e) => error_response(&e),
        },
        other => error_response(&format!("unknown control command: {}", other)),
    }
}

fn parse_args(args: &[u8]) -> Result<NameArgs, String> {
    if args.is_empty() {
        return Ok(NameArgs { name: None, package: None });
    }
    serde_json::from_slice(args).map_err(|e| format!("bad args JSON: {}", e))
}

// ============================================================================
// Supervisor-handlers (the crash-supervision half — ported from sentinel-actor)
// ============================================================================

#[export(name = "theater:simple/supervisor-handlers.handle-child-event")]
fn handle_child_event(state: SysState, child_id: String, event_type: String, event_data: Vec<u8>) -> Result<(SysState, ()), String> {
    let SysState { gossip_listener_id, node, mut children } = state;
    match children.iter().position(|c| c.child_id == child_id) {
        Some(idx) => {
            let payload = summarize_payload(&event_data);
            let child = &mut children[idx];
            child.chain.push(format!("{} {}", event_type, payload));
            if child.chain.len() > MAX_CHAIN_EVENTS {
                let drop = child.chain.len() - MAX_CHAIN_EVENTS;
                child.chain.drain(0..drop);
                child.chain_truncated = true;
            }
        }
        None => log(format!("[sentinel] chain event for unknown child_id={} type={} — ignoring", child_id, event_type)),
    }
    Ok((SysState { gossip_listener_id, node, children }, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-error")]
fn handle_child_error(state: SysState, child_id: String, _error: Value) -> Result<(SysState, ()), String> {
    log(format!("[sentinel] child {} errored", child_id));
    let SysState { gossip_listener_id, node, mut children } = state;
    on_crash(&mut children, &child_id, "error");
    Ok((SysState { gossip_listener_id, node, children }, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-exit")]
fn handle_child_exit(state: SysState, child_id: String, _result: Value) -> Result<(SysState, ()), String> {
    log(format!("[sentinel] child {} exited", child_id));
    let SysState { gossip_listener_id, node, mut children } = state;
    on_crash(&mut children, &child_id, "exit");
    Ok((SysState { gossip_listener_id, node, children }, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-external-stop")]
fn handle_child_external_stop(state: SysState, child_id: String) -> Result<(SysState, ()), String> {
    log(format!("[sentinel] child {} stopped externally — no respawn", child_id));
    Ok((state, ()))
}

// ============================================================================
// Supervisor primitives (ported from sentinel-actor)
// ============================================================================

fn cmd_list(children: &[ChildState]) -> Vec<u8> {
    let list: Vec<ListChild> = children
        .iter()
        .map(|c| ListChild {
            name: &c.name,
            child_id: &c.child_id,
            current_package: &c.current_package,
            restart_blocked: c.restart_blocked,
        })
        .collect();
    encode(&ListResp { ok: true, children: list })
}

fn cmd_get_chain(children: &[ChildState], name: Option<String>) -> Vec<u8> {
    let Some(name) = name else {
        return error_response("get_chain requires `name`");
    };
    match children.iter().find(|c| c.name == name) {
        Some(c) => encode(&GetChainResp { ok: true, chain: &c.chain, chain_truncated: c.chain_truncated }),
        None => error_response(&format!("unknown child name: {}", name)),
    }
}

fn cmd_start(children: &mut Vec<ChildState>, name: Option<String>, package: Option<String>) -> Vec<u8> {
    let Some(name) = name else {
        return error_response("start requires `name`");
    };
    let Some(new_package) = package else {
        return error_response("start requires `package`");
    };
    if new_package.is_empty() {
        return error_response("start `package` must be non-empty");
    }
    let idx = match children.iter().position(|c| c.name == name) {
        Some(i) => i,
        None => return error_response(&format!("unknown child name: {}", name)),
    };
    let child = &mut children[idx];
    child.restart_blocked = false;
    child.restart_times.clear();
    child.chain.clear();
    child.chain_truncated = false;
    if !child.child_id.is_empty() {
        let _ = supervisor_stop_child(child.child_id.clone());
    }
    child.current_package = new_package;
    match spawn_child(child) {
        Ok(new_id) => {
            child.child_id = new_id;
            encode(&StartResp {
                ok: true,
                name: &child.name,
                child_id: &child.child_id,
                current_package: &child.current_package,
            })
        }
        Err(e) => error_response(&format!("spawn failed: {}", e)),
    }
}

fn cmd_stop(children: &mut Vec<ChildState>, name: Option<String>) -> Vec<u8> {
    let Some(name) = name else {
        return error_response("stop requires `name`");
    };
    let idx = match children.iter().position(|c| c.name == name) {
        Some(i) => i,
        None => return error_response(&format!("unknown child name: {}", name)),
    };
    let child = &mut children[idx];
    if child.child_id.is_empty() {
        return error_response("no child running for that name");
    }
    let stopped = child.child_id.clone();
    if let Err(e) = supervisor_stop_child(stopped.clone()) {
        return error_response(&format!("stop failed: {}", e));
    }
    let name_ref = child.name.clone();
    encode(&StopResp { ok: true, name: &name_ref, stopped_child_id: &stopped })
}

fn on_crash(children: &mut Vec<ChildState>, child_id: &str, reason: &str) {
    let now_ms = now();
    let idx = match children.iter().position(|c| c.child_id == child_id) {
        Some(i) => i,
        None => {
            log(format!("[sentinel] crash for unknown child_id={} reason={} — ignoring", child_id, reason));
            return;
        }
    };
    let child = &mut children[idx];
    child.restart_times.retain(|t| now_ms.saturating_sub(*t) <= RATE_LIMIT_M_MS);
    let recent = child.restart_times.len();
    log(format!(
        "[sentinel] crash name={} child={} reason={} t_ms={} chain_size={}{} recent_restarts={}",
        child.name,
        child_id,
        reason,
        now_ms,
        child.chain.len(),
        if child.chain_truncated { " (TRUNCATED)" } else { "" },
        recent,
    ));
    child.chain.clear();
    child.chain_truncated = false;
    if child.restart_blocked {
        log(format!("[sentinel] {} ({}): crashed while blocked — not respawning", child.name, child_id));
        return;
    }
    if recent >= RATE_LIMIT_N {
        log(format!("[sentinel] crash loop on {} ({} in {}ms) — not respawning", child.name, recent + 1, RATE_LIMIT_M_MS));
        child.restart_blocked = true;
        return;
    }
    child.restart_times.push(now_ms);
    match spawn_child(child) {
        Ok(new_id) => child.child_id = new_id,
        Err(e) => log(format!("[sentinel] respawn {} failed: {}", child.name, e)),
    }
}

fn spawn_child(child: &ChildState) -> Result<String, String> {
    let mut manifest_toml = child.manifest_template.clone();
    for (name, value) in child.secret_names.iter().zip(child.secret_values.iter()) {
        manifest_toml = manifest_toml.replace(&format!("__{}__", name), value);
    }
    manifest_toml = manifest_toml.replace(PACKAGE_PLACEHOLDER, &child.current_package);
    let label = format!("{}{}", CHILD_MANIFEST_LABEL_PREFIX, child.name);
    store_at_label(STORE_ID.to_string(), label.clone(), manifest_toml.into_bytes())
        .map_err(|e| format!("store-at-label: {}", e))?;
    let manifest_uri = format!("store://{}/{}", STORE_ID, label);
    let child_id = supervisor_spawn(manifest_uri.clone(), None, None)?;
    if child.subscribe {
        if let Err(e) = supervisor_subscribe_to_child(child_id.clone()) {
            log(format!("[sentinel] subscribe-to-child failed name={} child={}: {}", child.name, child_id, e));
        }
    }
    log(format!("[sentinel] spawned name={} as {} (package={})", child.name, child_id, child.current_package));
    Ok(child_id)
}

// ============================================================================
// Helpers
// ============================================================================

fn encode_msg(msg: Msg) -> Vec<u8> {
    packr_guest::encode(&Value::from(msg)).unwrap_or_default()
}

fn decode_hex_list(hexes: &[String]) -> Vec<Vec<u8>> {
    hexes.iter().filter_map(|h| decode_hex(h)).collect()
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let b = s.as_bytes();
    let nib = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    let mut i = 0;
    while i < b.len() {
        out.push((nib(b[i])? << 4) | nib(b[i + 1])?);
        i += 2;
    }
    Some(out)
}

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_else(|e| error_response(&format!("encode failed: {}", e)))
}

fn error_response(msg: &str) -> Vec<u8> {
    serde_json::to_vec(&ErrResp { ok: false, error: msg })
        .unwrap_or_else(|_| b"{\"ok\":false,\"error\":\"encode failure\"}".to_vec())
}

fn find_template_placeholders(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < template.len() {
        let Some(start_rel) = template[i..].find("__") else {
            break;
        };
        let start = i + start_rel;
        let after = start + 2;
        if after >= template.len() {
            break;
        }
        let Some(end_rel) = template[after..].find("__") else {
            break;
        };
        let end = after + end_rel;
        let name = &template[after..end];
        if !name.is_empty() {
            out.push(name.to_string());
        }
        i = end + 2;
    }
    out
}

fn summarize_payload(data: &[u8]) -> String {
    if data.is_empty() {
        return "(empty)".to_string();
    }
    let slice = if data.len() > MAX_EVENT_PAYLOAD_BYTES { &data[..MAX_EVENT_PAYLOAD_BYTES] } else { data };
    let mut s = match core::str::from_utf8(slice) {
        Ok(text) if text.chars().all(|c| !c.is_control() || c == '\n' || c == '\t') => text.to_string(),
        _ => {
            let mut hex = String::with_capacity(slice.len() * 2 + 4);
            hex.push_str("hex:");
            for b in slice {
                hex.push_str(&format!("{:02x}", b));
            }
            hex
        }
    };
    s = s.replace(['\n', '\r'], " ");
    if data.len() > MAX_EVENT_PAYLOAD_BYTES {
        s.push('…');
    }
    s
}
