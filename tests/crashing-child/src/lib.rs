//! Deliberately-crashing child for testing the sentinel's crash → email →
//! respawn loop. On init: logs, then calls `runtime.shutdown(Some(bytes))` so
//! the actor exits with an error payload. The supervising sentinel sees this
//! as `handle-child-exit` with non-None result and fires its crash flow.

#![no_std]
extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use packr_guest::{export, import, pack_types, Value};

packr_guest::setup_guest!();

pack_types! {
    imports {
        theater:simple/runtime {
            log: func(msg: string),
            shutdown: func(data: option<list<u8>>) -> result<_, string>,
        }
    }
    exports {
        theater:simple/actor.init: func(state: value) -> result<tuple<bool, _>, string>,
    }
}

#[import(module = "theater:simple/runtime", name = "log")]
fn log(msg: String);

#[import(module = "theater:simple/runtime", name = "shutdown")]
fn shutdown(data: Option<Vec<u8>>) -> Result<(), String>;

#[export(name = "theater:simple/actor.init")]
fn init(_state: Value) -> Result<(bool, ()), String> {
    log(String::from("[crashing-child] init — about to shutdown with error"));
    let _ = shutdown(Some(b"deliberate test crash".to_vec()));
    Ok((false, ()))
}
