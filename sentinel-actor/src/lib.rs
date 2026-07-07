//! Sentinel actor — Phase 3.3 multi-actor supervisor with per-child chain
//! rings + pre-spawn template-placeholder validation.
//!
//! Supervises N child actors registered statically via the operator's JSON
//! config. Per-child:
//!   - manifest template + current package + secrets (substituted at spawn)
//!   - theater-assigned child id, refreshed on every respawn
//!   - independent rate limiter (N restarts / M ms before respawn-blocked)
//!   - independent chain ring buffer (cap MAX_CHAIN_EVENTS, cleared on crash —
//!     the contents belong to the child run that just ended)
//!
//! Global state:
//!   - TCP+JSON command surface (bearer-token auth); every per-child command
//!     (`start`/`stop`/`get_chain`) targets a child by `name`. `list`/`health`
//!     are global.
//!
//! Phase 3.1 ships per-child chain rings on top of theater 0.3.18 /
//! theater-handler-supervisor 0.3.12, where `handle-child-event` now carries
//! `child-id` as the first argument.
//!
//! Phase 3.2 bumps packr-guest 0.5.5 → 0.6.0 to match theater 0.3.18's
//! packr-abi 0.6.0 (the new 0x15 compact-primitive-list node kind would
//! otherwise hit "failed to convert parameter" on `handle-child-event`'s
//! `event-data: list<u8>` payload). Phase 3.2 also moves the bearer token
//! out of state into the content store — when theater logs a wasm error,
//! it prints the input tuple verbatim, which used to include the bearer
//! token field of `SentinelState`'s `Value::Record` representation. The
//! token now lives only in the store under BEARER_TOKEN_LABEL.
//!
//! Phase 3.4 (theater 0.3.25, PR #108): supervisor chain subscription is
//! opt-in. `spawn_child` calls `supervisor.subscribe-to-child` after every
//! spawn so the per-child chain rings keep filling; lifecycle handlers
//! (error/exit/external-stop) ride the always-on ActorResult channel and
//! need no subscription.
//!
//! Phase 3.3 adds pre-spawn validation: every `__KEY__` placeholder in a
//! child's `manifest_template` must resolve to either the built-in
//! `__PACKAGE__` or a `secrets` entry. Catches operator typos before they
//! ever reach `spawn_child` — otherwise the literal `__KEY__` string would
//! get persisted as the child's initial_state and (in the inbox case) leak
//! a useless bearer-like string into the inbox content store.
//!
//! Notification path is intentionally absent — the phase 1 email-on-crash
//! went through inbox, which is exactly the thing that'd be down when we'd
//! want to notify. See CLAUDE.md "Deferred" for the design space.

#![no_std]
extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use packr_guest::{export, import, pack_types, GraphValue, Value};
use serde::{Deserialize, Serialize};

packr_guest::setup_guest!();

// ============================================================================
// Tunables
// ============================================================================

/// Maximum chain events kept in memory. Oldest get dropped past this cap.
const MAX_CHAIN_EVENTS: usize = 500;
/// Per-event payload bytes summarized in the chain entry. Keeps lines tight.
const MAX_EVENT_PAYLOAD_BYTES: usize = 256;
/// Crash-loop window: at most N restarts in M ms per child before we give up
/// on that child specifically (rate-limiter is per-child — a runaway one
/// child can't block another's respawns).
const RATE_LIMIT_N: usize = 5;
const RATE_LIMIT_M_MS: u64 = 60_000;

/// Heartbeat cadence if the config doesn't override it.
const DEFAULT_HEARTBEAT_MS: u64 = 30_000;
/// Name the heartbeat interval is registered under (echoed back to handle-tick).
const HEARTBEAT_TIMER_NAME: &str = "heartbeat";

/// Timer that fires the one-shot mesh Register once the node child is reachable
/// (the message-server router doesn't resolve the node until after spawn
/// returns, so Register can't run from inside init).
const MESH_ARM_TIMER_NAME: &str = "mesh-arm";
/// Cadence of the mesh-arm timer. It keeps firing but no-ops after the first
/// successful Register (idempotent via `mesh_armed`).
const MESH_ARM_INTERVAL_MS: u64 = 1_000;

/// Max bytes accepted in a single inbound command line.
const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Per-call tcp.receive cap. We loop until newline / EOF / MAX_REQUEST_BYTES.
const RECV_CHUNK: u32 = 4096;
/// Placeholder substituted with the current package URL when rendering a
/// child manifest TOML at spawn time.
const PACKAGE_PLACEHOLDER: &str = "__PACKAGE__";
/// Content-store ID that sentinel's manifest declares — must match
/// sentinel-actor/manifest.toml's `[[handler]] type = "store"` `store_id`.
const STORE_ID: &str = "sentinel";
/// Per-child label prefix under which sentinel writes each rendered child
/// manifest TOML before handing theater a `store://` URI. Concurrent spawns
/// of different children don't trample each other because the suffix is the
/// child's name.
const CHILD_MANIFEST_LABEL_PREFIX: &str = "child-manifest-";
/// Stable store label holding the deploy bearer token. Init writes; every
/// TCP request reads + ct_eq's the inbound `token` field. Held out of
/// `SentinelState` so it never appears in the `Value::Record` representation
/// theater prints as part of wasm-error input formatting.
const BEARER_TOKEN_LABEL: &str = "bearer-token";

// ============================================================================
// Mesh wire codec (vendored from mesh/mesh-api/src/lib.rs)
// ============================================================================
//
// The app<->node control envelope carried over theater's message-server. This
// is a byte-for-byte copy of the four functions sentinel needs; mesh-api is the
// source of truth. Vendored (not a dep) because sentinel builds via nix+crane
// whose cleanSource is sentinel's dir only, so a cross-repo path dep won't
// build. theater-dev endorsed vendoring for round one and will publish mesh-api
// as a crate before round two touches the wire format. Keep in sync if the mesh
// command/ack/delivery encoding ever changes.
mod mesh {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    /// A 32-byte event hash (Submit ack) or ed25519 pubkey (delivery author).
    pub type Bytes32 = [u8; 32];

    const CMD_SUBMIT: u8 = 0x01; // body = payload bytes
    const CMD_REGISTER: u8 = 0x04; // body = app actor-id (utf8) for delivery

    /// `0x01 || payload` — ask the node to author + submit a payload.
    pub fn encode_submit(payload: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + payload.len());
        v.push(CMD_SUBMIT);
        v.extend_from_slice(payload);
        v
    }

    /// `0x04 || utf8(app_id)` — tell the node which app-id to deliver to. The
    /// node's delivery target starts EMPTY, so this MUST precede any Submit or
    /// handle-send never fires.
    pub fn encode_register(app_id: &str) -> Vec<u8> {
        let mut v = Vec::with_capacity(1 + app_id.len());
        v.push(CMD_REGISTER);
        v.extend_from_slice(app_id.as_bytes());
        v
    }

    /// Decode a request ack: `1 || hash[32]` on success, `0 || utf8` on error.
    pub fn decode_ack(bytes: &[u8]) -> Result<Bytes32, String> {
        match bytes.split_first() {
            Some((1, h)) if h.len() == 32 => {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(h);
                Ok(hash)
            }
            Some((0, e)) => Err(String::from_utf8_lossy(e).to_string()),
            _ => Err("malformed ack".to_string()),
        }
    }

    /// Decode a delivery (node -> app via send): `from[32] || body`.
    pub fn decode_delivery(bytes: &[u8]) -> Option<(Bytes32, Vec<u8>)> {
        if bytes.len() < 32 {
            return None;
        }
        let mut from = [0u8; 32];
        from.copy_from_slice(&bytes[..32]);
        Some((from, bytes[32..].to_vec()))
    }

    /// Short hex of the first 6 bytes, for log lines.
    pub fn short_hex(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(12);
        for &b in bytes.iter().take(6) {
            s.push_str(&alloc::format!("{:02x}", b));
        }
        s
    }

    /// Full lowercase hex of all bytes (for returning an event hash to a client).
    pub fn hex(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push_str(&alloc::format!("{:02x}", b));
        }
        s
    }
}

// ============================================================================
// State
// ============================================================================

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct ChildState {
    /// Operator-supplied stable handle for this child. TCP commands target
    /// children by name; theater's child-id is opaque and rotates on respawn.
    pub name: String,
    /// Child manifest TOML body with PACKAGE_PLACEHOLDER and `__SECRET__`
    /// placeholders. Sentinel substitutes both at every spawn.
    pub manifest_template: String,
    /// The package URL / path that supervisor.spawn should resolve next.
    /// Mutated by the `start` command.
    pub current_package: String,
    /// Parallel name/value vectors for the secrets map. Indexed pair-wise.
    /// RAM-only — never written to disk on sentinel's side.
    pub secret_names: Vec<String>,
    pub secret_values: Vec<String>,
    /// Whether sentinel subscribes to this child's chain events. False for
    /// high-traffic singletons (e.g. frontdoor on public :443) to avoid the
    /// chain-amplification wedge — see `ChildCfg::subscribe`. Supervision
    /// (crash/exit/external-stop) is unaffected; only the chain ring stays empty.
    pub subscribe: bool,
    /// theater-assigned child id from the most recent successful spawn.
    /// Empty until init's first spawn lands.
    pub child_id: String,
    /// Per-child chain ring buffer — one line per chain event,
    /// "<event_type> <truncated_payload>". Bounded at MAX_CHAIN_EVENTS;
    /// oldest dropped past cap with `chain_truncated` set. Reset after every
    /// crash — the contents belonged to the run that just ended.
    pub chain: Vec<String>,
    pub chain_truncated: bool,
    /// Recent restart timestamps in ms-since-epoch. Trimmed to the
    /// RATE_LIMIT_M_MS window before each rate-limit check.
    pub restart_times: Vec<u64>,
    /// True once the per-child rate limiter has tripped. A successful `start`
    /// command for this child clears it.
    pub restart_blocked: bool,
}

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SentinelState {
    /// TCP address sentinel listens on for inbound commands.
    pub listen_addr: String,
    /// Listener handle returned by `tcp.listen` at init.
    pub listener_id: String,
    /// One entry per configured child. Insertion order = config-map iteration
    /// order (BTreeMap, so alphabetical).
    pub children: Vec<ChildState>,
    // bearer_token intentionally NOT here — see BEARER_TOKEN_LABEL.
    /// Mesh node child id, if a `mesh` block was configured. Empty otherwise.
    /// The node is NOT in `children` (it's infrastructure, not a supervised
    /// app child); lifecycle events for it fall through the "unknown child"
    /// path and are ignored (round one does not respawn it).
    pub mesh_node_id: String,
    /// True once we've sent the mesh Register for this node (one-shot, on the
    /// first mesh-arm tick). Idempotent guard so the repeating timer no-ops.
    pub mesh_armed: bool,
}

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
            self: func() -> string,
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
            stop-child: func(child-id: string) -> result<_, string>,
            subscribe-to-child: func(child-id: string) -> result<_, string>,
        }
        theater:simple/message-server-host {
            register: func() -> result<_, string>,
            request: func(actor-id: string, msg: list<u8>) -> result<list<u8>, string>,
        }
        theater:simple/timer {
            now: func() -> u64,
            set-interval: func(name: string, interval-ms: u64) -> result<string, string>,
        }
        theater:simple/tcp {
            listen: func(address: string) -> result<string, string>,
            activate: func(connection-id: string) -> result<_, string>,
            send: func(connection-id: string, data: list<u8>) -> result<u64, string>,
            receive: func(connection-id: string, max-bytes: u32) -> result<list<u8>, string>,
            close: func(connection-id: string) -> result<_, string>,
        }
        theater:simple/store {
            store-at-label: func(store-id: string, label: string, content: list<u8>) -> result<string, string>,
            get-by-label: func(store-id: string, label: string) -> result<option<string>, string>,
            get: func(store-id: string, content-ref: string) -> result<list<u8>, string>,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-error: func(state: sentinel-state, child-id: string, error: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-exit: func(state: sentinel-state, child-id: string, result: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-external-stop: func(state: sentinel-state, child-id: string) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-event: func(state: sentinel-state, child-id: string, event-type: string, event-data: list<u8>) -> result<sentinel-state, string>,
        theater:simple/tcp-client.handle-connection: func(state: sentinel-state, connection-id: string) -> result<sentinel-state, string>,
        theater:simple/timer.handle-tick: func(state: sentinel-state, name: string) -> result<sentinel-state, string>,
        // Mesh delivery callback: the node calls message-server send(app, bytes)
        // with a finalized payload; theater routes it here. The runtime passes
        // Value::Tuple([list<u8>]), which packr unpacks as a single positional
        // list<u8> param (same convention as handle-child-event's event-data) —
        // NOT tuple<list<u8>> (that would expect a tuple nested in the tuple).
        theater:simple/message-server-client.handle-send: func(state: sentinel-state, data: list<u8>) -> result<sentinel-state, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);

#[import(module = "theater:simple/runtime", name = "self")]
fn runtime_self() -> String;

#[import(module = "theater:simple/message-server-host", name = "register")]
fn message_server_register() -> Result<(), String>;

#[import(module = "theater:simple/message-server-host", name = "request")]
fn message_server_request(actor_id: String, msg: Vec<u8>) -> Result<Vec<u8>, String>;

#[import(module = "theater:simple/supervisor", name = "spawn")]
fn supervisor_spawn(
    manifest: String,
    init_state: Option<Value>,
    wasm_bytes: Option<Vec<u8>>,
) -> Result<String, String>;

#[import(module = "theater:simple/supervisor", name = "stop-child")]
fn supervisor_stop_child(child_id: String) -> Result<(), String>;

#[import(module = "theater:simple/supervisor", name = "subscribe-to-child")]
fn supervisor_subscribe_to_child(child_id: String) -> Result<(), String>;

#[import(module = "theater:simple/timer", name = "now")]
fn timer_now() -> u64;

#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;

#[import(module = "theater:simple/tcp", name = "listen")]
fn tcp_listen(address: String) -> Result<String, String>;

#[import(module = "theater:simple/tcp", name = "activate")]
fn tcp_activate(connection_id: String) -> Result<(), String>;

#[import(module = "theater:simple/tcp", name = "send")]
fn tcp_send(connection_id: String, data: Vec<u8>) -> Result<u64, String>;

#[import(module = "theater:simple/tcp", name = "receive")]
fn tcp_receive(connection_id: String, max_bytes: u32) -> Result<Vec<u8>, String>;

#[import(module = "theater:simple/tcp", name = "close")]
fn tcp_close(connection_id: String) -> Result<(), String>;

#[import(module = "theater:simple/store", name = "store-at-label")]
fn store_at_label(store_id: String, label: String, content: Vec<u8>) -> Result<String, String>;

#[import(module = "theater:simple/store", name = "get-by-label")]
fn store_get_by_label(store_id: String, label: String) -> Result<Option<String>, String>;

#[import(module = "theater:simple/store", name = "get")]
fn store_get(store_id: String, content_ref: String) -> Result<Vec<u8>, String>;

// ============================================================================
// Config — what the operator hands us via initial_state
// ============================================================================

#[derive(Deserialize)]
struct ChildCfg {
    /// Child manifest TOML body with `__PACKAGE__` placeholder + any `__KEY__`
    /// placeholders for secrets.
    manifest_template: String,
    /// Initial package URL or path that fills `__PACKAGE__` until a `start`
    /// command rewrites it.
    default_package: String,
    /// Optional secret values for substitution. `{"API_TOKEN": "..."}` replaces
    /// every `__API_TOKEN__` placeholder. Substitution is deterministic but
    /// not topological — secret values containing other placeholder names are
    /// left as-is on subsequent passes.
    #[serde(default)]
    secrets: BTreeMap<String, String>,
    /// Whether sentinel subscribes to this child's chain events (chain-event
    /// delivery is opt-in on theater f852aec3+). Default true. Set false for a
    /// high-traffic singleton like frontdoor on public :443: a subscribed
    /// supervisor records every one of the child's per-connection chain events
    /// as its own chain entry, and under internet scanner storms that
    /// amplification saturates theater's event-notification channel — the
    /// "no available capacity" wedge that stalled the earlier phase-2 attempt.
    /// Lifecycle handlers (error/exit/external-stop) are always-on regardless
    /// of subscription, so an unsubscribed child is still supervised and
    /// respawned; only its per-event chain ring stays empty (which for a :443
    /// scanner magnet is 99% junk anyway).
    #[serde(default = "default_subscribe")]
    subscribe: bool,
}

fn default_subscribe() -> bool {
    true
}

#[derive(Deserialize)]
struct Config {
    /// TCP listen address, e.g. "0.0.0.0:8444".
    listen_addr: String,
    /// Shared secret for the deploy endpoint. Compared byte-for-byte against
    /// each request's `token` field.
    bearer_token: String,
    /// Children to supervise, keyed by operator-chosen name. Names appear in
    /// TCP requests (`start`/`stop` target by name) and in log lines.
    children: BTreeMap<String, ChildCfg>,
    /// Heartbeat cadence in ms; defaults to DEFAULT_HEARTBEAT_MS. The heartbeat
    /// is the off-box watcher's dead-man's-switch (see handle-tick) — the #43
    /// notification path that does not route through inbox.
    #[serde(default)]
    heartbeat_ms: Option<u64>,
    /// Optional mesh membership. When present, sentinel spawns a mesh node child
    /// and becomes a mesh member (round one: a thin translation layer). Absent =
    /// no mesh, existing behavior unchanged.
    #[serde(default)]
    mesh: Option<MeshCfg>,
}

/// Config for the mesh node sentinel spawns + drives over the message-server.
#[derive(Deserialize)]
struct MeshCfg {
    /// Path or `store://`/`https://` reference to the mesh node's manifest.
    node_manifest: String,
    /// Seed string the node SHA256's into its ed25519 identity.
    node_seed: String,
    /// Node listen address; None -> the node's default (127.0.0.1:9447).
    #[serde(default)]
    node_listen: Option<String>,
    /// Other members' hex pubkeys. Empty = single-node bootstrap (the node
    /// self-includes and self-finalizes) — the round-one PoC shape.
    #[serde(default)]
    members: Vec<String>,
    /// Peers to outbound-dial on init (subset of members). Empty for single-node.
    #[serde(default)]
    dial: Vec<PeerEntry>,
}

#[derive(Deserialize, Serialize)]
struct PeerEntry {
    pubkey: String,
    address: String,
}

// ============================================================================
// Lifecycle
// ============================================================================

#[export(name = "theater:simple/actor.init")]
fn init(state: Value) -> Result<(SentinelState, ()), String> {
    log(String::from("[sentinel] init"));

    let raw = match state {
        Value::String(s) if !s.is_empty() => s,
        _ => return Err(String::from("sentinel: initial_state must be a JSON config string")),
    };
    let cfg: Config = serde_json::from_str(&raw)
        .map_err(|e| format!("sentinel: bad initial_state JSON: {}", e))?;

    if cfg.bearer_token.is_empty() {
        return Err(String::from("sentinel: bearer_token must be non-empty"));
    }
    if cfg.children.is_empty() && cfg.mesh.is_none() {
        return Err(String::from(
            "sentinel: nothing to do — configure `children`, a `mesh` block, or both",
        ));
    }
    let heartbeat_ms = cfg.heartbeat_ms.unwrap_or(DEFAULT_HEARTBEAT_MS);
    for (name, child) in &cfg.children {
        if name.is_empty() {
            return Err(String::from("sentinel: child name must be non-empty"));
        }
        if !child.manifest_template.contains(PACKAGE_PLACEHOLDER) {
            return Err(format!(
                "sentinel: child '{}' manifest_template must contain placeholder {}",
                name, PACKAGE_PLACEHOLDER
            ));
        }
        if child.default_package.is_empty() {
            return Err(format!(
                "sentinel: child '{}' default_package must be non-empty",
                name
            ));
        }
        // Every __X__ placeholder in the template must resolve to either
        // PACKAGE (always substituted) or a secret entry. Without this check
        // a typo like `__BEARRER_TOKEN__` (or a missing secrets entry) would
        // leave the literal placeholder in the rendered TOML, the child
        // would get `__BEARRER_TOKEN__` as its initial_state, and (in the
        // inbox case) the bad bearer would get persisted to the inbox store
        // — recovery means wiping the store. Catch it before init returns.
        let mut known: Vec<&str> = Vec::with_capacity(child.secrets.len() + 1);
        known.push("PACKAGE");
        for k in child.secrets.keys() {
            known.push(k.as_str());
        }
        for placeholder in find_template_placeholders(&child.manifest_template) {
            if !known.iter().any(|k| *k == placeholder) {
                return Err(format!(
                    "sentinel: child '{}' template references __{}__ but no secret with that name (only __PACKAGE__ is built-in)",
                    name, placeholder
                ));
            }
        }
    }

    let listener_id = tcp_listen(cfg.listen_addr.clone())
        .map_err(|e| format!("sentinel: tcp.listen({}) failed: {}", cfg.listen_addr, e))?;
    log(format!(
        "[sentinel] listening on {} (id={})",
        cfg.listen_addr, listener_id
    ));

    // Write bearer token to the content store under a stable label so it
    // never sits in `SentinelState`. theater logs the wasm input tuple
    // verbatim on conversion error; keeping the token off the Record means
    // those error lines no longer leak it.
    store_at_label(
        String::from(STORE_ID),
        String::from(BEARER_TOKEN_LABEL),
        cfg.bearer_token.into_bytes(),
    )
    .map_err(|e| format!("sentinel: failed to persist bearer token: {}", e))?;

    let mut children: Vec<ChildState> = cfg
        .children
        .into_iter()
        .map(|(name, child_cfg)| {
            let (secret_names, secret_values): (Vec<String>, Vec<String>) =
                child_cfg.secrets.into_iter().unzip();
            ChildState {
                name,
                manifest_template: child_cfg.manifest_template,
                current_package: child_cfg.default_package,
                secret_names,
                secret_values,
                subscribe: child_cfg.subscribe,
                child_id: String::new(),
                chain: Vec::new(),
                chain_truncated: false,
                restart_times: Vec::new(),
                restart_blocked: false,
            }
        })
        .collect();

    // Hard-fail init if any configured child fails to spawn — operator needs
    // to know immediately if a child's manifest or package URL is broken.
    for child in children.iter_mut() {
        match spawn_child(child) {
            Ok(child_id) => child.child_id = child_id,
            Err(e) => {
                return Err(format!(
                    "sentinel: spawn child '{}' failed: {}",
                    child.name, e
                ));
            }
        }
    }

    // Arm the heartbeat — the off-box watcher tails these lines and treats
    // their ABSENCE past a threshold as the dead-man's-switch for a wedged or
    // dead sentinel. Non-fatal: supervision works without it, and a missing
    // heartbeat is itself the alert.
    match timer_set_interval(HEARTBEAT_TIMER_NAME.to_string(), heartbeat_ms) {
        Ok(_) => log(format!("[sentinel] heartbeat armed every {}ms", heartbeat_ms)),
        Err(e) => log(format!("[sentinel] set-interval(heartbeat) failed: {}", e)),
    }

    // Optional mesh membership (round one): spawn a node child and become a mesh
    // member — sentinel stays a thin translation layer (TCP mesh_submit -> node,
    // node delivery -> handle-send log). The Register that tells the node our
    // app-id is deferred to the first mesh-arm tick: the node isn't reachable via
    // the message-server router until spawn returns. The node is INFRASTRUCTURE,
    // not in `children`; a crash of it falls through the unknown-child path (round
    // one does not respawn it).
    let mesh_node_id = match cfg.mesh {
        Some(mesh) => {
            // Declaring the message-server handler + exporting handle-send is
            // enough for theater to route deliveries here; this explicit register
            // is belt-and-suspenders (matches the mesh example-app).
            if let Err(e) = message_server_register() {
                log(format!("[sentinel] mesh: message-server register failed: {}", e));
            }
            let node_init = build_node_init(&mesh);
            match supervisor_spawn(mesh.node_manifest.clone(), Some(Value::String(node_init)), None) {
                Ok(id) => {
                    log(format!("[sentinel] mesh: spawned node {}", id));
                    if let Err(e) =
                        timer_set_interval(MESH_ARM_TIMER_NAME.to_string(), MESH_ARM_INTERVAL_MS)
                    {
                        log(format!("[sentinel] mesh: set-interval(mesh-arm) failed: {}", e));
                    }
                    id
                }
                Err(e) => {
                    log(format!(
                        "[sentinel] mesh: spawn node failed: {} — mesh disabled this run",
                        e
                    ));
                    String::new()
                }
            }
        }
        None => String::new(),
    };

    Ok((
        SentinelState {
            listen_addr: cfg.listen_addr,
            listener_id,
            children,
            mesh_node_id,
            mesh_armed: false,
        },
        (),
    ))
}

/// Build the mesh node's `InitConfig` JSON from the mesh config. Mirrors the
/// mesh example-app's `build_node_init` (raw format! — the PoC seed/addresses
/// are simple strings needing no escaping).
fn build_node_init(m: &MeshCfg) -> String {
    let members = serde_json::to_string(&m.members).unwrap_or_else(|_| "[]".to_string());
    let dial = serde_json::to_string(&m.dial).unwrap_or_else(|_| "[]".to_string());
    match &m.node_listen {
        Some(listen) if !listen.is_empty() => format!(
            r#"{{"node_seed":"{}","listen_addr":"{}","members":{},"dial":{}}}"#,
            m.node_seed, listen, members, dial
        ),
        _ => format!(
            r#"{{"node_seed":"{}","members":{},"dial":{}}}"#,
            m.node_seed, members, dial
        ),
    }
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: SentinelState, name: String) -> Result<(SentinelState, ()), String> {
    let mut state = state;
    if name == HEARTBEAT_TIMER_NAME {
        let now_ms = timer_now();
        let blocked = state.children.iter().filter(|c| c.restart_blocked).count();
        log(format!(
            "[sentinel] heartbeat children={} blocked={} t_ms={}",
            state.children.len(),
            blocked,
            now_ms
        ));
    } else if name == MESH_ARM_TIMER_NAME && !state.mesh_armed && !state.mesh_node_id.is_empty() {
        // One-shot: tell the node our app-id so it delivers finalized payloads
        // to us. MUST land before any Submit or handle-send never fires. The
        // node is reachable now (we're past init). Idempotent via `mesh_armed`.
        let my_id = runtime_self();
        let cmd = mesh::encode_register(&my_id);
        match message_server_request(state.mesh_node_id.clone(), cmd) {
            Ok(reply) => match mesh::decode_ack(&reply) {
                Ok(_) => {
                    log(format!("[sentinel] mesh: registered app-id {} for delivery", my_id));
                    state.mesh_armed = true;
                }
                Err(e) => log(format!("[sentinel] mesh: register rejected: {}", e)),
            },
            Err(e) => log(format!("[sentinel] mesh: register request failed: {}", e)),
        }
    }
    Ok((state, ()))
}

/// Mesh delivery callback: the node calls message-server `send(app, bytes)` with
/// a finalized payload; theater routes it here. Round one just decodes + logs —
/// this is the PoC success signal. (Round two maps delivered mesh commands onto
/// sentinel's primitives.) Kept a pure translation per Colin's constraint.
#[export(name = "theater:simple/message-server-client.handle-send")]
fn handle_send(state: SentinelState, data: Vec<u8>) -> Result<(SentinelState, ()), String> {
    match mesh::decode_delivery(&data) {
        Some((from, body)) => log(format!(
            "[sentinel] mesh RECEIVED from {}: {}",
            mesh::short_hex(&from),
            String::from_utf8_lossy(&body)
        )),
        None => log(format!(
            "[sentinel] mesh: malformed delivery ({} bytes)",
            data.len()
        )),
    }
    Ok((state, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-event")]
fn handle_child_event(
    state: SentinelState,
    child_id: String,
    event_type: String,
    event_data: Vec<u8>,
) -> Result<(SentinelState, ()), String> {
    let mut state = state;
    let idx = match state.children.iter().position(|c| c.child_id == child_id) {
        Some(i) => i,
        None => {
            // Stale event during respawn, or an event for a child theater
            // tracks but we don't (shouldn't happen — static registration).
            // Log + drop; don't accumulate unattributable events.
            log(format!(
                "[sentinel] chain event for unknown child_id={} type={} — ignoring",
                child_id, event_type
            ));
            return Ok((state, ()));
        }
    };
    let payload = summarize_payload(&event_data);
    let child = &mut state.children[idx];
    child.chain.push(format!("{} {}", event_type, payload));
    if child.chain.len() > MAX_CHAIN_EVENTS {
        let drop = child.chain.len() - MAX_CHAIN_EVENTS;
        child.chain.drain(0..drop);
        child.chain_truncated = true;
    }
    Ok((state, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-error")]
fn handle_child_error(
    state: SentinelState,
    child_id: String,
    _error: Value,
) -> Result<(SentinelState, ()), String> {
    log(format!("[sentinel] child {} errored", child_id));
    Ok((on_crash(state, &child_id, "error"), ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-exit")]
fn handle_child_exit(
    state: SentinelState,
    child_id: String,
    _result: Value,
) -> Result<(SentinelState, ()), String> {
    log(format!("[sentinel] child {} exited", child_id));
    Ok((on_crash(state, &child_id, "exit"), ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-external-stop")]
fn handle_child_external_stop(
    state: SentinelState,
    child_id: String,
) -> Result<(SentinelState, ()), String> {
    log(format!(
        "[sentinel] child {} stopped externally — no respawn",
        child_id
    ));
    Ok((state, ()))
}

// ============================================================================
// TCP command surface
// ============================================================================

#[derive(Deserialize)]
struct Request {
    token: String,
    cmd: String,
    /// Used by `start`/`stop`. The operator-chosen child name.
    #[serde(default)]
    name: Option<String>,
    /// Used by `start`. The new package URL/path; the named child's manifest
    /// template gets rendered with this value before the spawn.
    #[serde(default)]
    package: Option<String>,
    /// Used by `mesh_submit`. The payload string to submit to the mesh.
    #[serde(default)]
    payload: Option<String>,
}

#[derive(Serialize)]
struct HealthResp<'a> {
    ok: bool,
    listen_addr: &'a str,
    children: Vec<HealthChild<'a>>,
}

#[derive(Serialize)]
struct HealthChild<'a> {
    name: &'a str,
    child_id: &'a str,
    restart_blocked: bool,
    recent_restarts: usize,
    current_package: &'a str,
    chain_size: usize,
    chain_truncated: bool,
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

#[export(name = "theater:simple/tcp-client.handle-connection")]
fn handle_connection(
    state: SentinelState,
    connection_id: String,
) -> Result<(SentinelState, ()), String> {
    // Single-failing-connection must not kill the listener: catch internal
    // errors, log + close, carry on. (Same defensive pattern as inbox-acceptor.)
    let mut state = state;
    if let Err(e) = handle_connection_inner(&mut state, &connection_id) {
        log(format!(
            "[sentinel] handle-connection failed (conn={}): {}",
            connection_id, e
        ));
        let _ = tcp_close(connection_id);
    }
    Ok((state, ()))
}

fn handle_connection_inner(state: &mut SentinelState, conn_id: &str) -> Result<(), String> {
    // accept() returned a PENDING connection per tcp.pact — activate it first.
    tcp_activate(conn_id.to_string()).map_err(|e| format!("activate: {}", e))?;
    let request_bytes = receive_line(conn_id)?;
    let response_bytes = dispatch_request(state, &request_bytes);
    tcp_send(conn_id.to_string(), response_bytes).map_err(|e| format!("send: {}", e))?;
    tcp_close(conn_id.to_string()).map_err(|e| format!("close: {}", e))?;
    Ok(())
}

fn receive_line(conn_id: &str) -> Result<Vec<u8>, String> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if buf.len() >= MAX_REQUEST_BYTES {
            return Err(format!("request exceeds {} bytes", MAX_REQUEST_BYTES));
        }
        let chunk =
            tcp_receive(conn_id.to_string(), RECV_CHUNK).map_err(|e| format!("receive: {}", e))?;
        if chunk.is_empty() {
            // EOF from peer.
            break;
        }
        buf.extend_from_slice(&chunk);
        if buf.iter().any(|&b| b == b'\n') {
            break;
        }
    }
    if buf.is_empty() {
        return Err(String::from("empty request"));
    }
    Ok(buf)
}

fn dispatch_request(state: &mut SentinelState, request_bytes: &[u8]) -> Vec<u8> {
    let req: Request = match serde_json::from_slice(request_bytes) {
        Ok(r) => r,
        Err(e) => return error_response(&format!("bad request JSON: {}", e)),
    };
    // Bearer token lives in the content store (off `SentinelState`) so it
    // never appears in wasm-error input formatting. Two store calls per
    // request (get-by-label → get); fine because TCP volume is operator-
    // scale, not request-scale.
    let bearer_bytes = match load_bearer_token() {
        Ok(b) => b,
        Err(e) => {
            log(format!("[sentinel] bearer lookup failed: {}", e));
            return error_response("internal: bearer unavailable");
        }
    };
    if !ct_eq(req.token.as_bytes(), &bearer_bytes) {
        // Don't leak which header failed; one error for any auth failure.
        return error_response("unauthorized");
    }
    match req.cmd.as_str() {
        "health" => cmd_health(state),
        "list" => cmd_list(state),
        "get_chain" => cmd_get_chain(state, req.name),
        "start" => cmd_start(state, req.name, req.package),
        "stop" => cmd_stop(state, req.name),
        "mesh_submit" => cmd_mesh_submit(state, req.payload),
        other => error_response(&format!("unknown cmd: {}", other)),
    }
}

fn cmd_health(state: &SentinelState) -> Vec<u8> {
    let now_ms = timer_now();
    let children: Vec<HealthChild> = state
        .children
        .iter()
        .map(|c| {
            let recent = c
                .restart_times
                .iter()
                .filter(|t| now_ms.saturating_sub(**t) <= RATE_LIMIT_M_MS)
                .count();
            HealthChild {
                name: &c.name,
                child_id: &c.child_id,
                restart_blocked: c.restart_blocked,
                recent_restarts: recent,
                current_package: &c.current_package,
                chain_size: c.chain.len(),
                chain_truncated: c.chain_truncated,
            }
        })
        .collect();
    encode(&HealthResp {
        ok: true,
        listen_addr: &state.listen_addr,
        children,
    })
}

fn cmd_list(state: &SentinelState) -> Vec<u8> {
    let children: Vec<ListChild> = state
        .children
        .iter()
        .map(|c| ListChild {
            name: &c.name,
            child_id: &c.child_id,
            current_package: &c.current_package,
            restart_blocked: c.restart_blocked,
        })
        .collect();
    encode(&ListResp { ok: true, children })
}

fn cmd_get_chain(state: &SentinelState, name: Option<String>) -> Vec<u8> {
    let Some(name) = name else {
        return error_response("get_chain requires `name` field");
    };
    let child = match state.children.iter().find(|c| c.name == name) {
        Some(c) => c,
        None => return error_response(&format!("unknown child name: {}", name)),
    };
    encode(&GetChainResp {
        ok: true,
        chain: &child.chain,
        chain_truncated: child.chain_truncated,
    })
}

fn cmd_start(
    state: &mut SentinelState,
    name: Option<String>,
    package: Option<String>,
) -> Vec<u8> {
    let Some(name) = name else {
        return error_response("start requires `name` field");
    };
    let Some(new_package) = package else {
        return error_response("start requires `package` field");
    };
    if new_package.is_empty() {
        return error_response("start `package` must be non-empty");
    }
    let idx = match state.children.iter().position(|c| c.name == name) {
        Some(i) => i,
        None => return error_response(&format!("unknown child name: {}", name)),
    };
    let child = &mut state.children[idx];

    // Operator intent: `start` resets the per-child crash-loop block. Without
    // this a recovered child could never come up after the limiter trips,
    // even when the operator has fixed the underlying issue.
    child.restart_blocked = false;
    child.restart_times.clear();
    // The old chain belongs to the previous run; clear it so the new run
    // starts with a fresh buffer (parallel to on_crash's reset).
    child.chain.clear();
    child.chain_truncated = false;

    // Stop the current child if it's still alive. Tolerate stop failure
    // (already dead, etc.) — external-stop is a no-op so no unwanted respawn.
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

fn cmd_stop(state: &mut SentinelState, name: Option<String>) -> Vec<u8> {
    let Some(name) = name else {
        return error_response("stop requires `name` field");
    };
    let child = match state.children.iter().find(|c| c.name == name) {
        Some(c) => c,
        None => return error_response(&format!("unknown child name: {}", name)),
    };
    if child.child_id.is_empty() {
        return error_response("no child running for that name");
    }
    let stopped = child.child_id.clone();
    if let Err(e) = supervisor_stop_child(stopped.clone()) {
        return error_response(&format!("stop failed: {}", e));
    }
    encode(&StopResp {
        ok: true,
        name: &child.name,
        stopped_child_id: &stopped,
    })
}

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    match serde_json::to_vec(value) {
        Ok(mut v) => {
            v.push(b'\n');
            v
        }
        Err(e) => error_response(&format!("encode failed: {}", e)),
    }
}

#[derive(Serialize)]
struct MeshSubmitResp<'a> {
    ok: bool,
    /// Hex of the finalized event hash the node acked.
    hash: &'a str,
}

/// `mesh_submit {payload}` — translate a TCP command into a mesh Submit. Pure
/// translation per Colin's thin-layer constraint: encode -> request the node ->
/// decode the ack -> return the event hash. No mesh logic in sentinel.
fn cmd_mesh_submit(state: &SentinelState, payload: Option<String>) -> Vec<u8> {
    if state.mesh_node_id.is_empty() {
        return error_response("mesh not configured");
    }
    if !state.mesh_armed {
        return error_response("mesh not armed yet (Register pending) — retry shortly");
    }
    let cmd = mesh::encode_submit(payload.unwrap_or_default().as_bytes());
    match message_server_request(state.mesh_node_id.clone(), cmd) {
        Ok(reply) => match mesh::decode_ack(&reply) {
            Ok(hash) => {
                let hex = mesh::hex(&hash);
                match serde_json::to_vec(&MeshSubmitResp { ok: true, hash: &hex }) {
                    Ok(mut v) => {
                        v.push(b'\n');
                        v
                    }
                    Err(e) => error_response(&format!("encode mesh resp: {}", e)),
                }
            }
            Err(e) => error_response(&format!("mesh submit rejected: {}", e)),
        },
        Err(e) => error_response(&format!("mesh request failed: {}", e)),
    }
}

fn error_response(msg: &str) -> Vec<u8> {
    // Best-effort — if even error encoding fails we send a hardcoded line.
    match serde_json::to_vec(&ErrResp { ok: false, error: msg }) {
        Ok(mut v) => {
            v.push(b'\n');
            v
        }
        Err(_) => b"{\"ok\":false,\"error\":\"encode failure\"}\n".to_vec(),
    }
}

/// Read the deploy bearer token from the content store. Two host calls —
/// resolve label to content-ref, fetch content. Called on every TCP request.
fn load_bearer_token() -> Result<Vec<u8>, String> {
    let content_ref = store_get_by_label(
        String::from(STORE_ID),
        String::from(BEARER_TOKEN_LABEL),
    )
    .map_err(|e| format!("get-by-label: {}", e))?
    .ok_or_else(|| String::from("bearer token label missing"))?;
    store_get(String::from(STORE_ID), content_ref).map_err(|e| format!("get: {}", e))
}

/// Constant-time byte comparison so token-equality leak is bounded by length.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ============================================================================
// Crash handling
// ============================================================================

/// Common crash path for error + exit. Looks up the ChildState that owned the
/// crashed child-id, applies the per-child rate limiter, respawns it
/// (or flags it `restart_blocked`). A crash event for an unknown id (most
/// likely a stale event during respawn) is logged and ignored.
fn on_crash(mut state: SentinelState, child_id: &str, reason: &str) -> SentinelState {
    let now_ms = timer_now();

    let idx = match state.children.iter().position(|c| c.child_id == child_id) {
        Some(i) => i,
        None => {
            log(format!(
                "[sentinel] crash for unknown child_id={} reason={} — ignoring",
                child_id, reason
            ));
            return state;
        }
    };

    let child = &mut state.children[idx];
    child
        .restart_times
        .retain(|t| now_ms.saturating_sub(*t) <= RATE_LIMIT_M_MS);
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

    // The chain snapshot belongs to the run that just ended — drop it before
    // the next spawn so the new run starts with a clean buffer.
    child.chain.clear();
    child.chain_truncated = false;

    if child.restart_blocked {
        log(format!(
            "[sentinel] {} ({}): crashed while already in blocked state — not respawning",
            child.name, child_id
        ));
        return state;
    }
    if recent >= RATE_LIMIT_N {
        log(format!(
            "[sentinel] crash loop on {} ({} crashes in {}ms) — not respawning",
            child.name,
            recent + 1,
            RATE_LIMIT_M_MS,
        ));
        child.restart_blocked = true;
        return state;
    }

    child.restart_times.push(now_ms);
    match spawn_child(child) {
        Ok(new_id) => {
            child.child_id = new_id;
        }
        Err(e) => {
            // Treat spawn failure as another crash — but we can't recurse into
            // on_crash without risking infinite loops. Log and leave child_id
            // pointing at the dead one; the next crash event re-enters and
            // tries again.
            log(format!("[sentinel] respawn {} failed: {}", child.name, e));
        }
    }
    state
}

/// Render the child manifest TOML by substituting any configured secrets and
/// the current package URL into the template, write it to sentinel's content
/// store under a per-child label, and hand theater a `store://` URI.
/// theater's resolve_reference only understands store:// / http(s):// / a
/// bare filesystem path — there is no inline-content support.
///
/// Substitution order: secrets first, then `__PACKAGE__`. A secret value
/// containing `__PACKAGE__` would therefore also get the package
/// substitution applied — operators should not pick `__PACKAGE__` as a
/// literal substring of an unrelated secret value.
fn spawn_child(child: &ChildState) -> Result<String, String> {
    let mut manifest_toml = child.manifest_template.clone();
    for (name, value) in child.secret_names.iter().zip(child.secret_values.iter()) {
        let placeholder = format!("__{}__", name);
        manifest_toml = manifest_toml.replace(&placeholder, value);
    }
    manifest_toml = manifest_toml.replace(PACKAGE_PLACEHOLDER, &child.current_package);

    let label = format!("{}{}", CHILD_MANIFEST_LABEL_PREFIX, child.name);
    store_at_label(
        String::from(STORE_ID),
        label.clone(),
        manifest_toml.into_bytes(),
    )
    .map_err(|e| format!("store-at-label failed: {}", e))?;

    let manifest_uri = format!("store://{}/{}", STORE_ID, label);
    let child_id = supervisor_spawn(manifest_uri.clone(), None, None)?;
    // theater 0.3.25 (PR #108): chain-event delivery is opt-in — spawn no
    // longer attaches a parent subscriber, so without this call
    // handle-child-event never fires and the chain ring stays empty.
    // Non-fatal on failure: the child is already running, and losing chain
    // visibility is better than killing the (re)spawn path over it.
    //
    // Skipped entirely when child.subscribe is false (high-traffic singletons
    // like frontdoor): not subscribing keeps the child's per-connection chain
    // firehose from bubbling into sentinel — the amplification wedge. Lifecycle
    // supervision is unaffected (always-on channel).
    if child.subscribe {
        if let Err(e) = supervisor_subscribe_to_child(child_id.clone()) {
            log(format!(
                "[sentinel] subscribe-to-child failed name={} child={}: {} — chain ring will stay empty for this run",
                child.name, child_id, e
            ));
        }
    } else {
        log(format!(
            "[sentinel] not subscribing to {} (subscribe=false) — chain ring stays empty by design",
            child.name
        ));
    }
    log(format!(
        "[sentinel] spawned name={} as {} (package={}, secrets={}, manifest={})",
        child.name,
        child_id,
        child.current_package,
        child.secret_names.len(),
        manifest_uri,
    ));
    Ok(child_id)
}

// ============================================================================
// Helpers
// ============================================================================

/// Scan `template` for placeholders of the shape `__NAME__` (any non-empty
/// non-`__`-containing name between two `__` delimiters). Used by init to
/// verify every placeholder maps to a known secret before spawn.
///
/// Scan is non-greedy left-to-right and mirrors the substring-replace
/// behavior `spawn_child` uses for substitution — so a placeholder that
/// won't actually substitute (e.g. an empty `____`) is not reported. The
/// `__PACKAGE__` builtin is reported; the caller filters it out.
fn find_template_placeholders(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < template.len() {
        let Some(start_rel) = template[i..].find("__") else { break };
        let start = i + start_rel;
        let after = start + 2;
        if after >= template.len() { break }
        let Some(end_rel) = template[after..].find("__") else { break };
        let end = after + end_rel;
        let name = &template[after..end];
        if !name.is_empty() {
            out.push(name.to_string());
        }
        i = end + 2;
    }
    out
}

/// Render `data` as a short, mostly-printable summary for a chain line.
/// UTF-8 if it parses, otherwise hex. Truncated to MAX_EVENT_PAYLOAD_BYTES.
fn summarize_payload(data: &[u8]) -> String {
    if data.is_empty() {
        return String::from("(empty)");
    }
    let slice = if data.len() > MAX_EVENT_PAYLOAD_BYTES {
        &data[..MAX_EVENT_PAYLOAD_BYTES]
    } else {
        data
    };
    let mut s = match core::str::from_utf8(slice) {
        Ok(text) if text.chars().all(|c| !c.is_control() || c == '\n' || c == '\t') => {
            text.to_string()
        }
        _ => {
            let mut hex = String::with_capacity(slice.len() * 2 + 4);
            hex.push_str("hex:");
            for b in slice {
                hex.push_str(&format!("{:02x}", b));
            }
            hex
        }
    };
    s = s.replace('\n', " ").replace('\r', " ");
    if data.len() > MAX_EVENT_PAYLOAD_BYTES {
        s.push_str("…");
    }
    s
}
