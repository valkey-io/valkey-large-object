//! Command Handlers — thin layer that parses args and dispatches to engine.
//!
//! LO.HELLO: EFA session establishment
//! LO.GET key [rkey remote_addr]: engine::execute_get
//! LO.SET key <data>                (TCP): engine::execute_set
//! LO.SET key len rkey remote_addr  (EFA): engine::execute_set
//! LO.INFO key [LEN|CRC|TIER]: metadata from LoValue, no engine call

use std::sync::Arc;

use dma_libfabric_protocol::{decode_hex, encode_hex};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, LO_TYPE};
use crate::engine::{self, DataSource, Transport};
use crate::errors;
use crate::transport::config::FabricProvider;
use crate::transport::{self, session, Session};

/// The EFA session the client previously established with LO.HELLO.
fn efa_session(ctx: &Context) -> Result<Arc<Session>, ValkeyError> {
    session::lookup(ctx.get_client_id()).ok_or(ValkeyError::Str(errors::ERR_NO_DMA_SESSION))
}

// ─── LO.HELLO ────────────────────────────────────────────────────────────────
//
// Establishes a fabric session with the client.
// Client sends its fabric address as hex, opaque to us and in the provider's own format. The
// server inserts it into each domain's address vector and returns an address per server,
// so both sides hold each other before the first transfer.

pub fn lo_hello(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let Some(fabric) = transport::fabric() else {
        return Err(ValkeyError::Str(errors::ERR_EFA_UNAVAILABLE));
    };

    let peer_address = decode_hex(args[1].as_slice())
        .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_PEER_ADDR_HEX))?;
    // An EFA address is exactly 32 bytes; a tcp one is a sockaddr, opaque beyond being non-empty.
    match crate::fabric_provider() {
        FabricProvider::EfaDirect if peer_address.len() != 32 => {
            return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_LEN));
        }
        FabricProvider::Emulated if peer_address.is_empty() => {
            return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_EMPTY));
        }
        FabricProvider::EfaDirect | FabricProvider::Emulated => {}
    }

    let client_id = ctx.get_client_id();
    // One endpoint per connection. A second HELLO would hold the old address-vector entry while
    // inserting the new one; when the client's old endpoint has died and the new one reuses its
    // QPN, efa-direct cannot represent both (vdma/.claude/open_issue.md). Reconnect instead.
    if session::lookup(client_id).is_some() {
        return Err(ValkeyError::Str(errors::ERR_DMA_SESSION_EXISTS));
    }
    fabric
        .add_peer(client_id, &peer_address)
        .map_err(|e| ValkeyError::String(format!("{}: {}", errors::ERR_SESSION_CREATE, e)))?;
    session::insert(client_id, Session::new(client_id, peer_address));

    let reply: Vec<ValkeyValue> = fabric
        .local_addresses()
        .map(|address| ValkeyValue::BulkString(encode_hex(address)))
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
    let crc32c = lo_value.crc32c;

    // Pin the file to protect it from asynchronous deletion in tiered mode.
    let file = lo_value.file.clone();

    // Determine transport: EFA if rkey+remote_addr provided, else TCP.
    // TODO: Add a client buffer length argument to LO.GET so the server can
    // validate the address space covers obj_len before fi_write. Also add
    // validation when multi-address support lands (sum of address lengths >= obj_len).
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
    match engine::execute_get(ctx, object_id, obj_len, crc32c, file, transport) {
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

// ─── LO.INFO ─────────────────────────────────────────────────────────────────
//
// LO.INFO key [LEN | CRC | TIER]
//
// Parse args → resolve key → return metadata field or all fields as array.

pub fn lo_info(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if !(2..=3).contains(&args.len()) {
        return Err(ValkeyError::WrongArity);
    }

    let key = ctx.open_key(&args[1]);
    let value = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Err(ValkeyError::Str(errors::ERR_NOT_FOUND)),
    };

    let len = ValkeyValue::Integer(value.len as i64);
    let crc = ValkeyValue::Integer(i64::from(value.crc32c));
    let tier = ValkeyValue::SimpleStringStatic(value.tier().as_str());

    if args.len() == 2 {
        return Ok(ValkeyValue::Array(vec![
            ValkeyValue::SimpleStringStatic("len"),
            len,
            ValkeyValue::SimpleStringStatic("crc"),
            crc,
            ValkeyValue::SimpleStringStatic("tier"),
            tier,
        ]));
    }

    match args[2].to_string_lossy().to_uppercase().as_str() {
        "LEN" => Ok(len),
        "CRC" => Ok(crc),
        "TIER" => Ok(tier),
        _ => Err(ValkeyError::Str(errors::ERR_INVALID_INFO_FIELD)),
    }
}
