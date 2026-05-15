//! Sentinel actor.
//!
//! Skeleton — phase 1 will flesh out crash → email-with-chain → respawn.
//!
//! Today sentinel:
//! - on init: parses config from initial_state, spawns the configured child manifest
//! - handle-child-event: logs the event type (chain accumulation is phase 1)
//! - handle-child-error / handle-child-exit: logs the failure (email + respawn is phase 1)
//! - handle-child-external-stop: logs (sentinel intends an external stop to mean "shut down cleanly")
//!
//! Phase 1 will add:
//! - in-memory chain buffer accumulated via handle-child-event
//! - on error/exit-with-error: serialize chain, POST to inbox /v1/mailboxes/<dev>/send
//! - respawn the child after notifying
//!
//! Phase 2 will add:
//! - HTTPS endpoint authenticated by bearer token
//! - on POST /deploy with {url, sha256}: fetch artifact, verify, stop child, swap manifest, respawn

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use packr_guest::{export, import, pack_types, GraphValue, Value};
use serde::Deserialize;

packr_guest::setup_guest!();

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SentinelState {
    pub child_manifest: String,
    pub dev_email: String,
    pub inbox_api: String,
    pub inbox_token: String,
    pub child_id: String,
}

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
        }
        theater:simple/supervisor {
            spawn: func(manifest: string, init-bytes: option<list<u8>>, wasm-bytes: option<list<u8>>) -> result<string, string>,
            stop-child: func(child-id: string) -> result<_, string>,
        }
        theater:simple/store {
            get: func(store-id: string, content-ref: string) -> result<list<u8>, string>,
            get-by-label: func(store-id: string, label: string) -> result<option<string>, string>,
            store-at-label: func(store-id: string, label: string, content: list<u8>) -> result<string, string>,
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
    init_bytes: Option<Vec<u8>>,
    wasm_bytes: Option<Vec<u8>>,
) -> Result<String, String>;

#[import(module = "theater:simple/supervisor", name = "stop-child")]
fn supervisor_stop_child(child_id: String) -> Result<(), String>;

// ============================================================================
// Config — what the operator hands us via initial_state
// ============================================================================

#[derive(Deserialize)]
struct Config {
    /// Absolute path to the child actor's manifest.toml.
    child_manifest: String,
    /// Email address to notify on child failure (e.g. "inbox-dev@colinrozzi.com").
    dev_email: String,
    /// Inbox API endpoint, "host:port".
    inbox_api: String,
    /// Bearer token for the inbox API.
    inbox_token: String,
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

    let child_id = supervisor_spawn(cfg.child_manifest.clone(), None, None)
        .map_err(|e| format!("sentinel: spawn child failed: {}", e))?;
    log(format!("[sentinel] spawned child {}", child_id));

    Ok((
        SentinelState {
            child_manifest: cfg.child_manifest,
            dev_email: cfg.dev_email,
            inbox_api: cfg.inbox_api,
            inbox_token: cfg.inbox_token,
            child_id,
        },
        (),
    ))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-error")]
fn handle_child_error(
    state: SentinelState,
    child_id: String,
    _error: Value,
) -> Result<(SentinelState, ()), String> {
    log(format!("[sentinel] child {} errored — TODO email + respawn", child_id));
    // phase 1: decode the wit-actor-error record from _error, email dev with chain, respawn
    Ok((state, ()))
}

#[export(name = "theater:simple/supervisor-handlers.handle-child-exit")]
fn handle_child_exit(
    state: SentinelState,
    child_id: String,
    _result: Value,
) -> Result<(SentinelState, ()), String> {
    log(format!("[sentinel] child {} exited — TODO email + respawn", child_id));
    // phase 1: same as error path (exit-with-error is still a death we want to notify on)
    Ok((state, ()))
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

#[export(name = "theater:simple/supervisor-handlers.handle-child-event")]
fn handle_child_event(
    state: SentinelState,
    event_type: String,
    _event_data: Vec<u8>,
) -> Result<(SentinelState, ()), String> {
    // phase 1: accumulate into in-memory chain buffer; phase 1's crash email
    // ships this buffer as the attachment / inline body.
    log(format!("[sentinel] child event: {}", event_type));
    Ok((state, ()))
}
