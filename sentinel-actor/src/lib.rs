//! Sentinel actor.
//!
//! Watches one supervised child. Accumulates a per-event chain summary in
//! memory; on child error / exit, logs a crash line and respawns (subject to
//! a crash-loop rate limiter). An external stop never respawns.
//!
//! Notification is intentionally absent right now — the original phase 1
//! email-on-crash path went through the inbox, which is exactly what
//! sentinel-supervises-inbox would have down at the moment we'd want to
//! notify. Circular bootstrap dependency. See CLAUDE.md "Deferred" for the
//! design space; phase 2 (HTTPS deploy endpoint) is the next ticket.

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

/// Maximum chain events kept in memory. Oldest get dropped past this cap.
const MAX_CHAIN_EVENTS: usize = 500;
/// Per-event payload bytes summarized in the chain entry. Keeps lines tight.
const MAX_EVENT_PAYLOAD_BYTES: usize = 256;
/// Crash-loop window: at most N restarts in M ms before we give up.
const RATE_LIMIT_N: usize = 5;
const RATE_LIMIT_M_MS: u64 = 60_000;

// ============================================================================
// State
// ============================================================================

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SentinelState {
    pub child_manifest: String,
    pub child_id: String,
    /// One line per recorded chain event: "<event_type> <truncated_payload>".
    pub chain: Vec<String>,
    /// True if we've dropped older events from `chain` to stay under the cap.
    pub chain_truncated: bool,
    /// Recent restart timestamps in ms-since-epoch; old entries trimmed before
    /// the rate-limit check.
    pub restart_times: Vec<u64>,
    /// Once true, the rate limiter has fired and we no longer respawn.
    /// Operator intervention required to unblock (restart the sentinel).
    pub restart_blocked: bool,
}

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-state: option<value>, wasm-bytes: option<list<u8>>) -> result<string, string>,
        }
        theater:simple/timer {
            now: func() -> u64,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-error: func(state: sentinel-state, child-id: string, error: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-exit: func(state: sentinel-state, child-id: string, result: value) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-external-stop: func(state: sentinel-state, child-id: string) -> result<sentinel-state, string>,
        theater:simple/supervisor-handlers.handle-child-event: func(state: sentinel-state, event-type: string, event-data: list<u8>) -> result<sentinel-state, string>,
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

#[import(module = "theater:simple/timer", name = "now")]
fn timer_now() -> u64;

// ============================================================================
// Config — what the operator hands us via initial_state
// ============================================================================

#[derive(Deserialize)]
struct Config {
    /// Absolute path to the child actor's manifest.toml.
    child_manifest: String,
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

    let child_id = spawn_child(&cfg.child_manifest)
        .map_err(|e| format!("sentinel: spawn child failed: {}", e))?;

    Ok((
        SentinelState {
            child_manifest: cfg.child_manifest,
            child_id,
            chain: Vec::new(),
            chain_truncated: false,
            restart_times: Vec::new(),
            restart_blocked: false,
        },
        (),
    ))
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
    match spawn_child(&state.child_manifest) {
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

/// Spawn the child from `manifest`. Post theater PRs #58–#63, supervisor.spawn
/// auto-calls the child's `actor.init` before returning the id, and passing
/// `None` for init-state lets the child's manifest `initial_state` carry the
/// state (see CLAUDE.md "Gotchas").
fn spawn_child(manifest: &str) -> Result<String, String> {
    let child_id = supervisor_spawn(manifest.to_string(), None, None)?;
    log(format!("[sentinel] spawned child {}", child_id));
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
