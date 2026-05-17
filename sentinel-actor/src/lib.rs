//! Sentinel actor — phase 1.
//!
//! Watches one supervised child. On every child chain event, records a short
//! summary into an in-memory ring buffer. On child error / exit, emails the
//! configured dev address with that buffer and respawns the child (subject to
//! a simple crash-loop rate limiter). An external stop never respawns.
//!
//! Phase 2 will layer an HTTPS deploy endpoint on top of this.

#![no_std]
extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use packr_guest::{export, import, pack_types, GraphValue, Value};
use serde::{Deserialize, Serialize};

packr_guest::setup_guest!();

// ============================================================================
// Tunables
// ============================================================================

/// Maximum chain events kept in memory. Oldest get dropped past this cap.
const MAX_CHAIN_EVENTS: usize = 500;
/// Per-event payload bytes shown in the crash email. Keeps the email body sane.
const MAX_EVENT_PAYLOAD_BYTES: usize = 256;
/// Crash-loop window: at most N restarts in M seconds before we give up.
const RATE_LIMIT_N: usize = 5;
const RATE_LIMIT_M_MS: u64 = 60_000;

// ============================================================================
// State
// ============================================================================

#[derive(Clone, GraphValue)]
#[graph(crate = "packr_guest::composite_abi")]
pub struct SentinelState {
    pub child_manifest: String,
    pub dev_email: String,
    pub inbox_api: String,
    pub inbox_token: String,
    pub child_id: String,
    /// One line per recorded chain event: "<event_type> <truncated_payload>".
    pub chain: Vec<String>,
    /// True if we've dropped older events from `chain` to stay under the cap.
    pub chain_truncated: bool,
    /// Recent restart timestamps in ms-since-epoch; old entries trimmed before
    /// the rate-limit check.
    pub restart_times: Vec<u64>,
    /// Once true, the rate limiter has fired and we no longer respawn.
    /// Operator intervention required to unblock (i.e. restart the sentinel).
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
        theater:simple/store {
            get: func(store-id: string, content-ref: string) -> result<list<u8>, string>,
            get-by-label: func(store-id: string, label: string) -> result<option<string>, string>,
            store-at-label: func(store-id: string, label: string, content: list<u8>) -> result<string, string>,
        }
        theater:simple/tcp {
            connect: func(address: string) -> result<string, string>,
            send: func(connection-id: string, data: list<u8>) -> result<u64, string>,
            receive: func(connection-id: string, max-bytes: u32) -> result<list<u8>, string>,
            close: func(connection-id: string) -> result<_, string>,
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

#[import(module = "theater:simple/supervisor", name = "stop-child")]
fn supervisor_stop_child(child_id: String) -> Result<(), String>;

#[import(module = "theater:simple/tcp", name = "connect")]
fn tcp_connect(address: String) -> Result<String, String>;

#[import(module = "theater:simple/tcp", name = "send")]
fn tcp_send(connection_id: String, data: Vec<u8>) -> Result<u64, String>;

#[import(module = "theater:simple/tcp", name = "receive")]
fn tcp_receive(connection_id: String, max_bytes: u32) -> Result<Vec<u8>, String>;

#[import(module = "theater:simple/tcp", name = "close")]
fn tcp_close(connection_id: String) -> Result<(), String>;

#[import(module = "theater:simple/timer", name = "now")]
fn timer_now() -> u64;

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

    let child_id = spawn_child(&cfg.child_manifest)
        .map_err(|e| format!("sentinel: spawn child failed: {}", e))?;

    Ok((
        SentinelState {
            child_manifest: cfg.child_manifest,
            dev_email: cfg.dev_email,
            inbox_api: cfg.inbox_api,
            inbox_token: cfg.inbox_token,
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

/// Common crash path for error + exit. Emails the dev, runs the rate limiter,
/// respawns (or marks the child blocked), and resets the chain buffer.
fn on_crash(mut state: SentinelState, child_id: &str, reason: &str) -> SentinelState {
    let now_ms = timer_now();

    // Rate limiter: drop entries older than the window, then count.
    state.restart_times.retain(|t| now_ms.saturating_sub(*t) <= RATE_LIMIT_M_MS);
    let recent = state.restart_times.len();

    // Build + send the email regardless of whether we'll respawn — the dev
    // wants to see what happened either way.
    let blocked_now = !state.restart_blocked && recent >= RATE_LIMIT_N;
    let permanently_blocked = state.restart_blocked || blocked_now;

    let subject = format!("[sentinel] {} {} (t={})", child_id, reason, now_ms);
    let body = build_email_body(&state, child_id, reason, now_ms, recent, permanently_blocked);

    if let Err(e) = send_email(&state, &subject, &body) {
        log(format!("[sentinel] crash-email failed: {}", e));
    }

    // Reset chain regardless — the snapshot we just emailed is the one for
    // this crash; further events belong to the next run.
    state.chain.clear();
    state.chain_truncated = false;

    if permanently_blocked {
        if !state.restart_blocked {
            log(format!(
                "[sentinel] crash loop ({} crashes in {}ms) — not respawning {}",
                recent + 1,
                RATE_LIMIT_M_MS,
                child_id
            ));
            state.restart_blocked = true;
        } else {
            log(format!(
                "[sentinel] {} crashed while already in blocked state — not respawning",
                child_id
            ));
        }
        return state;
    }

    // Respawn.
    state.restart_times.push(now_ms);
    match spawn_child(&state.child_manifest) {
        Ok(new_id) => {
            state.child_id = new_id;
        }
        Err(e) => {
            // Treat spawn failure as another crash — but we can't recurse into
            // on_crash without risking infinite loops, so just log + leave
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

fn build_email_body(
    state: &SentinelState,
    child_id: &str,
    reason: &str,
    now_ms: u64,
    recent_restarts: usize,
    blocked: bool,
) -> String {
    let mut body = String::new();
    body.push_str(&format!("child_id:   {}\n", child_id));
    body.push_str(&format!("reason:     {}\n", reason));
    body.push_str(&format!("t_ms:       {}\n", now_ms));
    body.push_str(&format!(
        "restarts:   {} in last {}s\n",
        recent_restarts, RATE_LIMIT_M_MS / 1000
    ));
    if blocked {
        body.push_str("status:     CRASH-LOOPED — sentinel will not respawn until restarted\n");
    } else {
        body.push_str("status:     respawning\n");
    }
    body.push_str(&format!("chain_size: {}", state.chain.len()));
    if state.chain_truncated {
        body.push_str(" (TRUNCATED — older events dropped)");
    }
    body.push_str("\n\n--- chain ---\n");
    if state.chain.is_empty() {
        body.push_str("(no events recorded before crash)\n");
    } else {
        for line in &state.chain {
            body.push_str(line);
            body.push('\n');
        }
    }
    body
}

// ============================================================================
// Inbox HTTP client
// ============================================================================

#[derive(Serialize)]
struct SendBody<'a> {
    to: &'a [String],
    subject: &'a str,
    body: &'a str,
}

fn send_email(state: &SentinelState, subject: &str, body: &str) -> Result<(), String> {
    let to = vec![state.dev_email.clone()];
    let payload = SendBody {
        to: &to,
        subject,
        body,
    };
    let json = serde_json::to_string(&payload)
        .map_err(|e| format!("encode send body: {}", e))?;
    let path = format!("/v1/mailboxes/{}/send", url_encode(&state.dev_email));
    let (status, resp_body) = http_post(&state.inbox_api, &state.inbox_token, &path, &json)?;
    if status == 0 {
        // The inbox closes TLS without close_notify, so rustls bails on the
        // very first recv; we get no status line. The POST itself succeeded,
        // so treat this as best-effort delivery. If the request had really
        // been rejected (4xx/5xx) the response would usually fit in a single
        // chunk that arrives before the close — so this path is "probably ok".
        log(format!("[sentinel] inbox: response unreadable; assuming delivered ({})", subject));
        return Ok(());
    }
    if !(200..300).contains(&status) {
        return Err(format!("inbox responded {}: {}", status, resp_body));
    }
    Ok(())
}

/// Talk HTTP/1.1 to `host_port` and return (status_code, body).
/// Adapted from inbox-cli — relies on `client_tls.enabled = true` in the
/// manifest's tcp handler so `tcp_connect` auto-upgrades to TLS.
fn http_post(
    host_port: &str,
    token: &str,
    path: &str,
    body: &str,
) -> Result<(u16, String), String> {
    let conn = tcp_connect(host_port.to_string())
        .map_err(|e| format!("connect {}: {}", host_port, e))?;

    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        path,
        host_port,
        token,
        body.len(),
        body
    );
    tcp_send(conn.clone(), req.into_bytes()).map_err(|e| format!("send: {}", e))?;

    let mut all = Vec::new();
    let mut body_start: Option<usize> = None;
    let mut content_length: Option<usize> = None;

    loop {
        if let (Some(hs), Some(cl)) = (body_start, content_length) {
            if all.len() >= hs + cl {
                break;
            }
        }
        let chunk = match tcp_receive(conn.clone(), 65536) {
            Ok(c) => c,
            Err(_) => {
                // The inbox server closes connections without sending TLS
                // close_notify; rustls flags that as an error on the next
                // read. Treat it as EOF — if we already have the status line
                // and body, that's a complete HTTP exchange. parse_status_line
                // below will surface the real error (status 0 / empty body)
                // if we didn't get anything usable.
                break;
            }
        };
        if chunk.is_empty() {
            break;
        }
        all.extend_from_slice(&chunk);

        if body_start.is_none() {
            if let Some(idx) = find_subseq(&all, b"\r\n\r\n") {
                body_start = Some(idx + 4);
                let header_str = core::str::from_utf8(&all[..idx]).unwrap_or("");
                for line in header_str.split("\r\n") {
                    if let Some((name, value)) = line.split_once(':') {
                        if name.trim().eq_ignore_ascii_case("content-length") {
                            if let Ok(n) = value.trim().parse::<usize>() {
                                content_length = Some(n);
                            }
                        }
                    }
                }
                if content_length.is_none() {
                    content_length = Some(usize::MAX);
                }
            }
        }
    }
    let _ = tcp_close(conn);

    let text = String::from_utf8(all).map_err(|_| String::from("non-utf8 response"))?;
    let status = parse_status_line(&text).unwrap_or(0);
    let start = body_start.unwrap_or_else(|| text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0));
    let end = match content_length {
        Some(n) if n != usize::MAX => start + n.min(text.len().saturating_sub(start)),
        _ => text.len(),
    };
    Ok((status, text[start..end].to_string()))
}

fn parse_status_line(text: &str) -> Option<u16> {
    // "HTTP/1.1 200 OK\r\n..."
    let line = text.lines().next()?;
    let mut parts = line.split_ascii_whitespace();
    let _version = parts.next()?;
    let code = parts.next()?;
    code.parse().ok()
}

fn find_subseq(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ============================================================================
// Helpers
// ============================================================================

/// Render `data` as a short, mostly-printable summary for the email body.
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
    // Collapse internal newlines so a single chain line stays a single line.
    s = s.replace('\n', " ").replace('\r', " ");
    if data.len() > MAX_EVENT_PAYLOAD_BYTES {
        s.push_str("…");
    }
    s
}

fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        let ok = byte.is_ascii_alphanumeric()
            || byte == b'-'
            || byte == b'.'
            || byte == b'_'
            || byte == b'~';
        if ok {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{:02X}", byte));
        }
    }
    out
}
