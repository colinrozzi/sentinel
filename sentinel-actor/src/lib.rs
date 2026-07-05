//! Sentinel actor — Phase A: clean multi-child supervision.
//!
//! Supervises a SET of children (name -> child record), not just one. On every
//! subscribed child chain event, records a short summary into that child's own
//! ring buffer. On child error / exit, logs a crash line and respawns the
//! offending child (subject to a per-child crash-loop rate limiter). An
//! external stop never respawns. A periodic timer tick emits a heartbeat line
//! to the journal so an off-box watcher can detect a wedged/dead sentinel
//! (dead-man's-switch) — this is the notification path that does NOT route
//! through the inbox (ticket #43).
//!
//! Contract notes for theater f852aec3 (#132), confirmed against the runtime
//! source + theater-dev:
//!   - supervisor.spawn is 3-arg (manifest, init-state, wasm-bytes); passing
//!     None/None lets the child manifest's initial_state carry its config.
//!   - chain events are OPT-IN: after spawning a child we must call
//!     supervisor.subscribe-to-child(child-id) or handle-child-event never
//!     fires for it. Lifecycle handlers (error/exit/external-stop) are
//!     always-on and independent of subscription.
//!   - handle-child-event now carries child-id as its first param — that is
//!     how a multi-child supervisor attributes each event to the right child.
//!     It is dispatched by name and is NOT in the shipped supervisor.wit, so
//!     it is hand-declared below.
//!
//! Phase B will layer an HTTPS deploy endpoint (tcp listen + server_tls) on
//! top of this.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use packr_guest::{export, import, pack_types, GraphValue, Value};
use serde::Deserialize;

packr_guest::setup_guest!();

// ============================================================================
// Tunables
// ============================================================================

/// Maximum chain events kept in memory per child. Oldest dropped past this cap.
const MAX_CHAIN_EVENTS: usize = 500;
/// Per-event payload bytes summarized in the chain entry. Keeps lines tight.
const MAX_EVENT_PAYLOAD_BYTES: usize = 256;
/// Crash-loop window: at most N restarts in M ms per child before we give up.
const RATE_LIMIT_N: usize = 5;
const RATE_LIMIT_M_MS: u64 = 60_000;
/// Heartbeat cadence if the config does not override it.
const DEFAULT_HEARTBEAT_MS: u64 = 30_000;
/// Name we register the heartbeat interval under (echoed back to handle-tick).
const HEARTBEAT_TIMER_NAME: &str = "heartbeat";

// ============================================================================
// State
// ============================================================================

/// One supervised child. `child_id` changes on every respawn; the rest is
/// stable identity + the accounting we keep for it.
#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct ChildRecord {
    /// Stable human name (e.g. "inbox-ui"). Used for logging + config identity.
    pub name: String,
    /// Absolute path to this child's (already-rendered) manifest.toml.
    pub manifest: String,
    /// Current live child id from the last spawn. Empty if spawn failed.
    pub child_id: String,
    /// One line per recorded chain event: "<event_type> <truncated_payload>".
    pub chain: Vec<String>,
    /// True once we've dropped older events from `chain` to stay under the cap.
    pub chain_truncated: bool,
    /// Recent restart timestamps (ms-since-epoch); trimmed to the window before
    /// the rate-limit check.
    pub restart_times: Vec<u64>,
    /// Once true, this child's rate limiter has fired and we no longer respawn
    /// it. Operator intervention required (restart the sentinel) to unblock.
    pub restart_blocked: bool,
}

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SentinelState {
    /// The supervised set. Small N (a handful) — linear scan by child_id is fine.
    pub children: Vec<ChildRecord>,
    /// Heartbeat cadence in ms (informational; the interval is armed in init).
    pub heartbeat_ms: u64,
}

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
            subscribe-to-child: func(child-id: string) -> result<_, string>,
        }
        theater:simple/timer {
            now: func() -> u64,
            set-interval: func(name: string, interval-ms: u64) -> result<string, string>,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<sentinel-state, string>,
        // handle-child-event carries child-id first (multi-child attribution).
        // Not in the shipped supervisor.wit — dispatched by name — so declared here.
        theater:simple/supervisor-handlers.handle-child-event: func(state: sentinel-state, child-id: string, event-type: string, event-data: list<u8>) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-error: func(state: sentinel-state, child-id: string, error: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-exit: func(state: sentinel-state, child-id: string, result: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-external-stop: func(state: sentinel-state, child-id: string) -> result<sentinel-state, string>,
        theater:simple/timer.handle-tick: func(state: sentinel-state, name: string) -> result<sentinel-state, string>,
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

#[import(module = "theater:simple/supervisor", name = "subscribe-to-child")]
fn supervisor_subscribe_to_child(child_id: String) -> Result<(), String>;

#[import(module = "theater:simple/timer", name = "now")]
fn timer_now() -> u64;

#[import(module = "theater:simple/timer", name = "set-interval")]
fn timer_set_interval(name: String, interval_ms: u64) -> Result<String, String>;

// ============================================================================
// Config — what the operator hands us via initial_state
// ============================================================================

#[derive(Deserialize)]
struct ChildConfig {
    /// Stable human name for this child.
    name: String,
    /// Absolute path to the child's already-rendered manifest.toml. (Package
    /// pin + secret substitution happen at deploy time and land in this file;
    /// in-sentinel template rendering is deferred to the Phase 2 deploy path.)
    manifest: String,
}

#[derive(Deserialize)]
struct Config {
    /// The children to supervise. Must be non-empty.
    children: Vec<ChildConfig>,
    /// Heartbeat cadence in ms; defaults to DEFAULT_HEARTBEAT_MS.
    #[serde(default)]
    heartbeat_ms: Option<u64>,
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
    if cfg.children.is_empty() {
        return Err(String::from("sentinel: config.children must be non-empty"));
    }
    let heartbeat_ms = cfg.heartbeat_ms.unwrap_or(DEFAULT_HEARTBEAT_MS);

    let mut children = Vec::with_capacity(cfg.children.len());
    for cc in cfg.children {
        // A spawn failure at init is fatal — the operator asked for this child
        // and something is wrong (bad path, bad manifest). Fail loudly rather
        // than come up half-supervising.
        let child_id = spawn_and_subscribe(&cc.manifest, &cc.name)
            .map_err(|e| format!("sentinel: spawn child {} failed: {}", cc.name, e))?;
        children.push(ChildRecord {
            name: cc.name,
            manifest: cc.manifest,
            child_id,
            chain: Vec::new(),
            chain_truncated: false,
            restart_times: Vec::new(),
            restart_blocked: false,
        });
    }

    // Arm the heartbeat. Non-fatal if it fails — supervision still works, we
    // just lose the dead-man's-switch signal (and the watcher will notice the
    // absence of heartbeats, which is itself the alert).
    if let Err(e) = timer_set_interval(HEARTBEAT_TIMER_NAME.to_string(), heartbeat_ms) {
        log(format!("[sentinel] set-interval(heartbeat) failed: {}", e));
    }

    log(format!(
        "[sentinel] init complete — supervising {} children, heartbeat every {}ms",
        children.len(),
        heartbeat_ms
    ));

    Ok((SentinelState { children, heartbeat_ms }, ()))
}

#[export(name = "theater:simple/timer.handle-tick")]
fn handle_tick(state: SentinelState, name: String) -> Result<(SentinelState, ()), String> {
    if name == HEARTBEAT_TIMER_NAME {
        let now_ms = timer_now();
        let blocked = state.children.iter().filter(|c| c.restart_blocked).count();
        log(format!(
            "[sentinel] heartbeat children={} blocked={} t_ms={}",
            state.children.len(),
            blocked,
            now_ms
        ));
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
    // The #[export] macro does not preserve a `mut` param binding, so rebind.
    let mut state = state;
    let payload = summarize_payload(&event_data);
    if let Some(rec) = state.children.iter_mut().find(|c| c.child_id == child_id) {
        rec.chain.push(format!("{} {}", event_type, payload));
        if rec.chain.len() > MAX_CHAIN_EVENTS {
            let drop = rec.chain.len() - MAX_CHAIN_EVENTS;
            rec.chain.drain(0..drop);
            rec.chain_truncated = true;
        }
    }
    // Unknown child_id (e.g. a straggler event for a since-respawned id) — drop it.
    Ok((state, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-error")]
fn handle_child_error(
    state: SentinelState,
    child_id: String,
    _error: Value,
) -> Result<(SentinelState, ()), String> {
    let mut state = state;
    on_crash(&mut state, &child_id, "error");
    Ok((state, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-exit")]
fn handle_child_exit(
    state: SentinelState,
    child_id: String,
    _result: Value,
) -> Result<(SentinelState, ()), String> {
    let mut state = state;
    on_crash(&mut state, &child_id, "exit");
    Ok((state, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-external-stop")]
fn handle_child_external_stop(
    state: SentinelState,
    child_id: String,
) -> Result<(SentinelState, ()), String> {
    let name = state
        .children
        .iter()
        .find(|c| c.child_id == child_id)
        .map(|c| c.name.as_str())
        .unwrap_or("?");
    log(format!(
        "[sentinel] child {} ({}) stopped externally — no respawn",
        child_id, name
    ));
    Ok((state, ()))
}

// ============================================================================
// Crash handling
// ============================================================================

/// Common crash path for error + exit, scoped to the child identified by
/// `child_id`. Logs a summary, runs that child's rate limiter, respawns it (or
/// marks it blocked), and resets its chain buffer. Other children are untouched.
fn on_crash(state: &mut SentinelState, child_id: &str, reason: &str) {
    let now_ms = timer_now();

    let idx = match state.children.iter().position(|c| c.child_id == child_id) {
        Some(i) => i,
        None => {
            log(format!(
                "[sentinel] crash for unknown child {} (reason={}) — ignoring",
                child_id, reason
            ));
            return;
        }
    };

    // Trim the rate-limit window, then read the counters we need.
    state.children[idx]
        .restart_times
        .retain(|t| now_ms.saturating_sub(*t) <= RATE_LIMIT_M_MS);
    let recent = state.children[idx].restart_times.len();
    let name = state.children[idx].name.clone();
    let chain_size = state.children[idx].chain.len();
    let truncated = state.children[idx].chain_truncated;

    log(format!(
        "[sentinel] crash child={} name={} reason={} t_ms={} chain_size={}{} recent_restarts={}",
        child_id,
        name,
        reason,
        now_ms,
        chain_size,
        if truncated { " (TRUNCATED)" } else { "" },
        recent,
    ));

    // The chain we just summarized belonged to the run that ended; reset it.
    state.children[idx].chain.clear();
    state.children[idx].chain_truncated = false;

    if state.children[idx].restart_blocked {
        log(format!(
            "[sentinel] {} ({}) crashed while already blocked — not respawning",
            name, child_id
        ));
        return;
    }
    if recent >= RATE_LIMIT_N {
        log(format!(
            "[sentinel] crash loop for {} ({} crashes in {}ms) — not respawning",
            name,
            recent + 1,
            RATE_LIMIT_M_MS
        ));
        state.children[idx].restart_blocked = true;
        return;
    }

    // Respawn this child.
    state.children[idx].restart_times.push(now_ms);
    let manifest = state.children[idx].manifest.clone();
    match spawn_and_subscribe(&manifest, &name) {
        Ok(new_id) => {
            state.children[idx].child_id = new_id;
        }
        Err(e) => {
            // Leave child_id pointing at the dead one; the next lifecycle event
            // (or lack of one) is the operator's cue. We do not recurse.
            log(format!("[sentinel] respawn {} failed: {}", name, e));
        }
    }
}

/// Spawn `manifest` and subscribe to its chain events. Post-f852aec3,
/// subscribe-to-child is REQUIRED or handle-child-event never fires for the
/// child. Subscription failure is non-fatal — supervision still works, we just
/// lose per-event chain visibility for that child.
fn spawn_and_subscribe(manifest: &str, name: &str) -> Result<String, String> {
    let child_id = supervisor_spawn(manifest.to_string(), None, None)?;
    if let Err(e) = supervisor_subscribe_to_child(child_id.clone()) {
        log(format!(
            "[sentinel] subscribe-to-child {} ({}) failed: {} — chain visibility off for this child",
            child_id, name, e
        ));
    }
    log(format!("[sentinel] spawned + subscribed {} -> child {}", name, child_id));
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
