//! Command Handlers — thin layer that parses args and dispatches to engine.
//!
//! LO.HELLO: EFA session establishment
//! LO.GET key [rkey remote_addr]: engine::execute_get
//! LO.SET key <data>                (TCP): engine::execute_set
//! LO.SET key len rkey remote_addr  (EFA): engine::execute_set

use std::sync::Arc;

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, LO_TYPE};
use crate::engine::{self, DataSource, Transport};
use crate::errors;
use crate::transport::{self, session, EfaAddress, Session};

/// The EFA session the client previously established with LO.HELLO.
fn efa_session(ctx: &Context) -> Result<Arc<Session>, ValkeyError> {
    session::lookup(ctx.get_client_id()).ok_or(ValkeyError::Str(errors::ERR_NO_DMA_SESSION))
}

// ─── LO.HELLO ────────────────────────────────────────────────────────────────
//
// Establishes an EFA session with the client.
// Client sends its EFA address (32 bytes hex). Server calls fi_av_insert on all
// N EFA devices and returns all N server EFA addresses.

pub fn lo_hello(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let efa_ctx = transport::efa_context();
    if !efa_ctx.is_available() {
        return Err(ValkeyError::Str(errors::ERR_EFA_UNAVAILABLE));
    }

    let peer_hex = args[1].to_string_lossy();
    let peer_bytes =
        hex_decode(&peer_hex).map_err(|_| ValkeyError::Str(errors::ERR_INVALID_PEER_ADDR_HEX))?;
    if peer_bytes.len() != 32 {
        return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_LEN));
    }
    let mut addr = [0u8; 32];
    addr.copy_from_slice(&peer_bytes);
    let peer_addr = EfaAddress(addr);

    let session = Session::new(efa_ctx, &peer_addr)
        .map_err(|e| ValkeyError::String(format!("{}: {}", errors::ERR_SESSION_CREATE, e)))?;
    let server_addrs = session.server_addrs();
    session::insert(ctx.get_client_id(), session);

    let reply: Vec<ValkeyValue> = server_addrs
        .iter()
        .map(|a| ValkeyValue::BulkString(hex_encode(&a.0)))
        .collect();
    Ok(ValkeyValue::Array(reply))
}

// ─── LO.GET ──────────────────────────────────────────────────────────────────
//
// Parse args → resolve key → determine transport → dispatch to engine.

pub fn lo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    // Lookup LoValue in keyspace.
    let key = ctx.open_key(&args[1]);
    let lo_value: &LoValue = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Ok(ValkeyValue::Null),
    };

    let object_id = lo_value.object_id;
    let obj_len = lo_value.len;

    // Pin the file to protect it from asynchronous deletion in tiered mode.
    let file = lo_value.file.clone();

    // Determine transport: EFA if rkey+remote_addr provided, else TCP.
    let transport = if args.len() >= 4 {
        let rkey: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let session = efa_session(ctx)?;
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        }
    } else {
        Transport::Tcp
    };

    // Dispatch to engine — it decides sync vs async internally.
    match engine::execute_get(ctx, object_id, obj_len, file, transport) {
        engine::EngineResult::Sync(result) => result,
        engine::EngineResult::Async => Ok(ValkeyValue::NoReply),
    }
}

// ─── LO.SET ──────────────────────────────────────────────────────────────────
//
// Parse args → determine data source → dispatch to engine.

pub fn lo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Determine data source and obj_len based on arg count.
    let (obj_len, data_source) = if args.len() >= 5 {
        // EFA path: LO.SET key len rkey remote_addr
        // len is required — server needs to know how many bytes to fi_read from client GPU.
        let obj_len: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;
        let rkey: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[4]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let session = efa_session(ctx)?;
        (
            obj_len,
            DataSource::Efa {
                session,
                rkey,
                remote_addr,
            },
        )
    } else if args.len() >= 3 {
        // TCP path: LO.SET key <data>
        // data.len() IS the authoritative length. No user-provided len needed.
        let data = args[2].as_slice().to_vec();
        let obj_len = data.len() as u64;
        (obj_len, DataSource::Tcp(data))
    } else {
        return Err(ValkeyError::WrongArity);
    };

    // Reject zero-length values / 0-byte cases.
    if obj_len == 0 {
        return Err(ValkeyError::Str("ERR object length must be > 0"));
    }

    // Dispatch to engine — it decides sync vs async internally.
    match engine::execute_set(ctx, &args[1], obj_len, data_source) {
        engine::EngineResult::Sync(result) => result,
        engine::EngineResult::Async => Ok(ValkeyValue::NoReply),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
    if !s.len().is_multiple_of(2) {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
