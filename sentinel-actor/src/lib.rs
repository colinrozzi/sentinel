//! Sentinel actor.
//!
//! Phase 1 (existing): supervises one child actor. Accumulates a per-event
//! chain summary in memory; on child error / exit, logs a crash line and
//! respawns (subject to a crash-loop rate limiter). An external stop never
//! respawns.
//!
//! Phase 2 (new): exposes a bearer-token-authenticated TCP+JSON command
//! surface (start, stop, list, get_chain, health) for managing the supervised
//! child. The child's manifest is held in memory as a template with a
//! `__PACKAGE__` placeholder; "start" swaps the package URL/path and respawns.
//!
//! The rendered child manifest TOML is written to sentinel's own content store
//! (label `child-manifest-current`) at every spawn and theater is handed a
//! `store://sentinel/child-manifest-current` URI. theater's resolve_reference
//! supports only store://, http(s)://, and bare-filesystem-paths — inline
//! manifest content is not supported, so the store hop is mandatory.
//!
//! Notification is intentionally absent right now — the original phase 1
//! email-on-crash path went through the inbox, which is exactly what
//! sentinel-supervises-inbox would have down at the moment we'd want to
//! notify. Circular bootstrap dependency. See CLAUDE.md "Deferred" for the
//! design space.

#![no_std]
extern crate alloc;

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
/// Crash-loop window: at most N restarts in M ms before we give up.
const RATE_LIMIT_N: usize = 5;
const RATE_LIMIT_M_MS: u64 = 60_000;

/// Max bytes accepted in a single inbound command line.
const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Per-call tcp.receive cap. We loop until newline / EOF / MAX_REQUEST_BYTES.
const RECV_CHUNK: u32 = 4096;
/// Placeholder substituted with the current package URL when rendering the
/// child manifest TOML at spawn time.
const PACKAGE_PLACEHOLDER: &str = "__PACKAGE__";
/// Content-store ID that sentinel's manifest declares — must match
/// sentinel-actor/manifest.toml's `[[handler]] type = "store"` `store_id`.
const STORE_ID: &str = "sentinel";
/// Stable label under which sentinel writes the rendered child manifest TOML
/// before handing theater a `store://` URI. Overwritten on each spawn.
const CHILD_MANIFEST_LABEL: &str = "child-manifest-current";

// ============================================================================
// State
// ============================================================================

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SentinelState {
    /// Child manifest TOML body containing PACKAGE_PLACEHOLDER. Sentinel
    /// substitutes the current_package URL into this template at every spawn.
    pub manifest_template: String,
    /// The package URL / path that supervises.spawn should resolve. Mutated
    /// by the `start` command.
    pub current_package: String,
    /// TCP address sentinel listens on for inbound commands.
    pub listen_addr: String,
    /// Shared secret required on every inbound command. Compared byte-for-byte.
    pub bearer_token: String,
    /// Listener handle returned by `tcp.listen` at init.
    pub listener_id: String,
    pub child_id: String,
    /// Operator-supplied secrets substituted into the manifest template
    /// at every spawn. Parallel `secret_names`/`secret_values` Vecs of equal
    /// length — a `{name="API_TOKEN", value="..."}` pair becomes a `__API_TOKEN__`
    /// placeholder substitution. RAM-only; never written to disk on sentinel's side.
    pub secret_names: Vec<String>,
    pub secret_values: Vec<String>,
    /// One line per recorded chain event: "<event_type> <truncated_payload>".
    pub chain: Vec<String>,
    /// True if we've dropped older events from `chain` to stay under the cap.
    pub chain_truncated: bool,
    /// Recent restart timestamps in ms-since-epoch; old entries trimmed before
    /// the rate-limit check.
    pub restart_times: Vec<u64>,
    /// Once true, the rate limiter has fired and we no longer respawn.
    /// Operator intervention required to unblock (`start` command or restart
    /// the sentinel process).
    pub restart_blocked: bool,
}

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
            stop-child: func(child-id: string) -> result<_, string>,
        }
        theater:simple/timer {
            now: func() -> u64,
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
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-error: func(state: sentinel-state, child-id: string, error: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-exit: func(state: sentinel-state, child-id: string, result: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-external-stop: func(state: sentinel-state, child-id: string) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-event: func(state: sentinel-state, event-type: string, event-data: list<u8>) -> result<sentinel-state, string>,
        theater:simple/tcp-client.handle-connection: func(state: sentinel-state, connection-id: string) -> result<sentinel-state, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);

#[import(module = "theater:simple/supervisor", name = "spawn")]
fn supervisor_spawn(
    manifest: String,
    init_state: Option<Value>,
    wasm_bytes: Option<Vec<u8>>,
) -> Result<String, String>;

#[import(module = "theater:simple/supervisor", name = "stop-child")]
fn supervisor_stop_child(child_id: String) -> Result<(), String>;

#[import(module = "theater:simple/timer", name = "now")]
fn timer_now() -> u64;

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

// ============================================================================
// Config — what the operator hands us via initial_state
// ============================================================================

#[derive(Deserialize)]
struct Config {
    /// Child manifest TOML body with `__PACKAGE__` placeholder for the package URL.
    child_manifest_template: String,
    /// Initial package URL or path that fills the placeholder until `start` rewrites it.
    default_package: String,
    /// TCP listen address, e.g. "0.0.0.0:8443".
    listen_addr: String,
    /// Shared secret for the deploy endpoint. Compared byte-for-byte against
    /// each request's `token` field.
    bearer_token: String,
    /// Optional secret values for substitution into the manifest template.
    /// `{"API_TOKEN": "..."}` replaces every `__API_TOKEN__` placeholder. Order
    /// of substitution is deterministic but not topological — secret values
    /// containing other placeholder names are left as-is on subsequent passes.
    #[serde(default)]
    secrets: alloc::collections::BTreeMap<String, String>,
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

    if !cfg.child_manifest_template.contains(PACKAGE_PLACEHOLDER) {
        return Err(format!(
            "sentinel: child_manifest_template must contain placeholder {}",
            PACKAGE_PLACEHOLDER
        ));
    }
    if cfg.bearer_token.is_empty() {
        return Err(String::from("sentinel: bearer_token must be non-empty"));
    }

    let listener_id = tcp_listen(cfg.listen_addr.clone())
        .map_err(|e| format!("sentinel: tcp.listen({}) failed: {}", cfg.listen_addr, e))?;
    log(format!(
        "[sentinel] listening on {} (id={})",
        cfg.listen_addr, listener_id
    ));

    let (secret_names, secret_values): (Vec<String>, Vec<String>) =
        cfg.secrets.into_iter().unzip();

    let mut state = SentinelState {
        manifest_template: cfg.child_manifest_template,
        current_package: cfg.default_package,
        listen_addr: cfg.listen_addr,
        bearer_token: cfg.bearer_token,
        listener_id,
        child_id: String::new(),
        secret_names,
        secret_values,
        chain: Vec::new(),
        chain_truncated: false,
        restart_times: Vec::new(),
        restart_blocked: false,
    };

    let child_id = spawn_child(&state)
        .map_err(|e| format!("sentinel: spawn child failed: {}", e))?;
    state.child_id = child_id;

    Ok((state, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-event")]
fn handle_child_event(
    state: SentinelState,
    event_type: String,
    event_data: Vec<u8>,
) -> Result<(SentinelState, ()), String> {
    let mut state = state;
    let payload = summarize_payload(&event_data);
    state.chain.push(format!("{} {}", event_type, payload));
    if state.chain.len() > MAX_CHAIN_EVENTS {
        let drop = state.chain.len() - MAX_CHAIN_EVENTS;
        state.chain.drain(0..drop);
        state.chain_truncated = true;
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
    /// Used by `start`. The new package URL/path; the manifest template gets
    /// rendered with this value before the spawn.
    #[serde(default)]
    package: Option<String>,
}

#[derive(Serialize)]
struct HealthResp<'a> {
    ok: bool,
    child_id: &'a str,
    restart_blocked: bool,
    recent_restarts: usize,
    chain_size: usize,
    chain_truncated: bool,
    listen_addr: &'a str,
}

#[derive(Serialize)]
struct ListResp<'a> {
    ok: bool,
    child_id: &'a str,
    current_package: &'a str,
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
    child_id: &'a str,
    current_package: &'a str,
}

#[derive(Serialize)]
struct StopResp<'a> {
    ok: bool,
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
    tcp_activate(conn_id.to_string())
        .map_err(|e| format!("activate: {}", e))?;
    let request_bytes = receive_line(conn_id)?;
    let response_bytes = dispatch_request(state, &request_bytes);
    tcp_send(conn_id.to_string(), response_bytes)
        .map_err(|e| format!("send: {}", e))?;
    tcp_close(conn_id.to_string())
        .map_err(|e| format!("close: {}", e))?;
    Ok(())
}

fn receive_line(conn_id: &str) -> Result<Vec<u8>, String> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        if buf.len() >= MAX_REQUEST_BYTES {
            return Err(format!("request exceeds {} bytes", MAX_REQUEST_BYTES));
        }
        let chunk = tcp_receive(conn_id.to_string(), RECV_CHUNK)
            .map_err(|e| format!("receive: {}", e))?;
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
    if !ct_eq(req.token.as_bytes(), state.bearer_token.as_bytes()) {
        // Don't leak which header failed; one error for any auth failure.
        return error_response("unauthorized");
    }
    match req.cmd.as_str() {
        "health" => cmd_health(state),
        "list" => cmd_list(state),
        "get_chain" => cmd_get_chain(state),
        "start" => cmd_start(state, req.package),
        "stop" => cmd_stop(state),
        other => error_response(&format!("unknown cmd: {}", other)),
    }
}

fn cmd_health(state: &SentinelState) -> Vec<u8> {
    let now_ms = timer_now();
    let recent = state
        .restart_times
        .iter()
        .filter(|t| now_ms.saturating_sub(**t) <= RATE_LIMIT_M_MS)
        .count();
    encode(&HealthResp {
        ok: true,
        child_id: &state.child_id,
        restart_blocked: state.restart_blocked,
        recent_restarts: recent,
        chain_size: state.chain.len(),
        chain_truncated: state.chain_truncated,
        listen_addr: &state.listen_addr,
    })
}

fn cmd_list(state: &SentinelState) -> Vec<u8> {
    encode(&ListResp {
        ok: true,
        child_id: &state.child_id,
        current_package: &state.current_package,
    })
}

fn cmd_get_chain(state: &SentinelState) -> Vec<u8> {
    encode(&GetChainResp {
        ok: true,
        chain: &state.chain,
        chain_truncated: state.chain_truncated,
    })
}

fn cmd_start(state: &mut SentinelState, package: Option<String>) -> Vec<u8> {
    let Some(new_package) = package else {
        return error_response("start requires `package` field");
    };
    if new_package.is_empty() {
        return error_response("start `package` must be non-empty");
    }

    // Operator intent: a `start` resets the crash-loop block. Without this
    // a recovered child could never come up after the limiter trips, even
    // when the operator has fixed the underlying issue.
    state.restart_blocked = false;
    state.restart_times.clear();

    // Stop the current child if it's still alive. We tolerate stop failure
    // (it may already be dead from a prior crash). External-stop handler
    // is a no-op so this won't trigger an unwanted respawn.
    if !state.child_id.is_empty() {
        let _ = supervisor_stop_child(state.child_id.clone());
    }

    state.current_package = new_package;
    match spawn_child(state) {
        Ok(new_id) => {
            state.child_id = new_id.clone();
            state.chain.clear();
            state.chain_truncated = false;
            encode(&StartResp {
                ok: true,
                child_id: &state.child_id,
                current_package: &state.current_package,
            })
        }
        Err(e) => error_response(&format!("spawn failed: {}", e)),
    }
}

fn cmd_stop(state: &mut SentinelState) -> Vec<u8> {
    if state.child_id.is_empty() {
        return error_response("no child to stop");
    }
    let stopped = state.child_id.clone();
    if let Err(e) = supervisor_stop_child(stopped.clone()) {
        return error_response(&format!("stop failed: {}", e));
    }
    encode(&StopResp {
        ok: true,
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

/// Common crash path for error + exit. Logs a summary, runs the rate limiter,
/// respawns (or marks the child blocked), and resets the chain buffer.
fn on_crash(mut state: SentinelState, child_id: &str, reason: &str) -> SentinelState {
    let now_ms = timer_now();
    state
        .restart_times
        .retain(|t| now_ms.saturating_sub(*t) <= RATE_LIMIT_M_MS);
    let recent = state.restart_times.len();

    log(format!(
        "[sentinel] crash child={} reason={} t_ms={} chain_size={}{} recent_restarts={}",
        child_id,
        reason,
        now_ms,
        state.chain.len(),
        if state.chain_truncated { " (TRUNCATED)" } else { "" },
        recent,
    ));

    // Reset the chain — this snapshot belongs to the run that just ended.
    state.chain.clear();
    state.chain_truncated = false;

    if state.restart_blocked {
        log(format!(
            "[sentinel] {} crashed while already in blocked state — not respawning",
            child_id
        ));
        return state;
    }
    if recent >= RATE_LIMIT_N {
        log(format!(
            "[sentinel] crash loop ({} crashes in {}ms) — not respawning {}",
            recent + 1,
            RATE_LIMIT_M_MS,
            child_id,
        ));
        state.restart_blocked = true;
        return state;
    }

    state.restart_times.push(now_ms);
    match spawn_child(&state) {
        Ok(new_id) => {
            state.child_id = new_id;
        }
        Err(e) => {
            // Treat spawn failure as another crash — but we can't recurse into
            // on_crash without risking infinite loops, so just log and leave
            // state.child_id pointing at the dead one. Next crash event will
            // re-enter on_crash and try again.
            log(format!("[sentinel] respawn failed: {}", e));
        }
    }
    state
}

/// Render the child manifest TOML by substituting the current package URL
/// and any configured secrets into the template, write it to sentinel's
/// content store, and hand theater a `store://` URI. theater's
/// resolve_reference (crates/theater/src/utils/mod.rs) only understands
/// store:// / http(s):// / filesystem-path — there is no inline-content
/// support, so the rendered TOML *must* be written somewhere theater can
/// fetch it. Sentinel's own store is the cheapest landing pad.
///
/// Substitution order: secrets first, then `__PACKAGE__`. A secret value
/// containing `__PACKAGE__` would therefore *also* get the package
/// substitution applied to it — which is the right behavior for the
/// edge case where someone uses the package URL inside a secret-typed
/// field, but it does mean operators should not pick `__PACKAGE__` as
/// a literal substring of an otherwise unrelated secret value.
fn spawn_child(state: &SentinelState) -> Result<String, String> {
    let mut manifest_toml = state.manifest_template.clone();
    for (name, value) in state.secret_names.iter().zip(state.secret_values.iter()) {
        let placeholder = format!("__{}__", name);
        manifest_toml = manifest_toml.replace(&placeholder, value);
    }
    manifest_toml = manifest_toml.replace(PACKAGE_PLACEHOLDER, &state.current_package);

    store_at_label(
        String::from(STORE_ID),
        String::from(CHILD_MANIFEST_LABEL),
        manifest_toml.into_bytes(),
    )
    .map_err(|e| format!("store-at-label failed: {}", e))?;

    let manifest_uri = format!("store://{}/{}", STORE_ID, CHILD_MANIFEST_LABEL);
    let child_id = supervisor_spawn(manifest_uri.clone(), None, None)?;
    log(format!(
        "[sentinel] spawned child {} (package={}, secrets={}, manifest={})",
        child_id,
        state.current_package,
        state.secret_names.len(),
        manifest_uri,
    ));
    Ok(child_id)
}

// ============================================================================
// Helpers
// ============================================================================

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
