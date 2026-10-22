// Copyright 2026 Yossi Eliaz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A sandbox guest that masks payment card numbers.
//!
//! It implements the host ABI: `alloc`, `transform`, and an optional `reset`. Both buffers are
//! reused across records, so the guest allocates nothing per record and never needs `reset`.
//!
//! Build:
//!   cargo build --release --target wasm32-unknown-unknown

use std::ptr::addr_of_mut;

static mut INPUT: Vec<u8> = Vec::new();
static mut OUTPUT: Vec<u8> = Vec::new();

/// Hands the host a buffer of `size` bytes to write the request into.
#[no_mangle]
pub extern "C" fn alloc(size: i32) -> i32 {
    unsafe {
        let buffer = &mut *addr_of_mut!(INPUT);
        buffer.clear();
        buffer.resize(size as usize, 0);
        buffer.as_mut_ptr() as i32
    }
}

/// Nothing to reset: the buffers above are reused rather than reallocated.
#[no_mangle]
pub extern "C" fn reset() {}

/// Transforms one record. Returns `(out_ptr << 32) | out_len`.
#[no_mangle]
pub extern "C" fn transform(ptr: i32, len: i32) -> i64 {
    let request = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let response = redact(request);
    unsafe {
        let buffer = &mut *addr_of_mut!(OUTPUT);
        buffer.clear();
        buffer.extend_from_slice(&response);
        ((buffer.as_ptr() as i64) << 32) | (buffer.len() as i64)
    }
}

fn redact(request: &[u8]) -> Vec<u8> {
    let envelope: serde_json::Value = match serde_json::from_slice(request) {
        Ok(value) => value,
        Err(error) => return error_response(&format!("unparseable envelope: {error}")),
    };

    let mut value = match envelope.get("value") {
        Some(value) => value.clone(),
        None => return error_response("envelope has no value"),
    };

    // With schemas.enable=true -- the default -- Connect's JsonConverter wraps the record as
    // {"schema":..., "payload":...}. The fields live under payload, so a guest that inspects
    // only the outer object silently forwards the data untouched. Resolve the real data node
    // first, and hand the envelope back intact so the schema survives the round trip.
    let data = if value.get("schema").is_some() && value.get("payload").is_some() {
        match value.get_mut("payload") {
            Some(payload) => payload,
            None => return error_response("schema envelope has no payload"),
        }
    } else {
        &mut value
    };

    // Drop anything explicitly marked internal rather than forwarding it downstream.
    if data.get("internal").and_then(serde_json::Value::as_bool) == Some(true) {
        return br#"{"drop":true}"#.to_vec();
    }

    if let Some(object) = data.as_object_mut() {
        for field in ["card", "card_number", "pan"] {
            if let Some(serde_json::Value::String(number)) = object.get(field) {
                let masked = mask(number);
                object.insert(field.to_string(), serde_json::Value::String(masked));
            }
        }
    }

    serde_json::to_vec(&serde_json::json!({ "value": value }))
        .unwrap_or_else(|error| error_response(&format!("unserialisable result: {error}")))
}

/// Keeps the last four digits, masks the rest.
fn mask(number: &str) -> String {
    let digits: Vec<char> = number.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() <= 4 {
        return "*".repeat(digits.len());
    }
    let visible: String = digits[digits.len() - 4..].iter().collect();
    format!("{}{}", "*".repeat(digits.len() - 4), visible)
}

fn error_response(message: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "error": message }))
        .unwrap_or_else(|_| br#"{"error":"internal"}"#.to_vec())
}
