//! Command Handlers — thin layer that parses args and dispatches to engine.
//!
//! BLOB.HELLO: EFA session establishment
//! BLOB.GET key                                            (TCP): engine::execute_get
//! BLOB.GET key rkey1 addr1 len1 [rkey2 addr2 len2 ...]   (EFA): engine::execute_get
//! BLOB.SET key <data>                                     (TCP): engine::execute_set
//! BLOB.SET key total_len rkey1 addr1 len1 [...]           (EFA): engine::execute_set
//! BLOB.INFO key [LEN|CRC|TIER]: metadata from LoValue, no engine call

use std::sync::Arc;

use dma_libfabric_protocol::{decode_hex, encode_hex};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, LO_TYPE};
use crate::engine::{self, DataSource, Transport};
use crate::errors;
use crate::storage::ClientEFAAddress;
use crate::transport::config::FabricProvider;
use crate::transport::{self, session, Session};

/// Upper bound on client memory addresses in one EFA command.
const MAX_EFA_ADDRESSES: usize = 256;

/// The EFA session the client previously established with BLOB.HELLO.
fn efa_session(ctx: &Context) -> Result<Arc<Session>, ValkeyError> {
    session::lookup(ctx.get_client_id()).ok_or(ValkeyError::Str(errors::ERR_NO_DMA_SESSION))
}

/// Parse the client's memory addresses from `args[start_idx..]`, which must be
/// `(rkey, addr, len)` triples. The address count is inferred from the arg count.
/// Addresses may cover more than `required_len`; only a shortfall is an error.
fn parse_efa_addresses(
    args: &[ValkeyString],
    start_idx: usize,
    required_len: u64,
) -> Result<Vec<ClientEFAAddress>, ValkeyError> {
    let tail = &args[start_idx..];
    if tail.is_empty() || !tail.len().is_multiple_of(3) {
        return Err(ValkeyError::Str(errors::ERR_MALFORMED_ADDR_ARGS));
    }
    let n_addrs = tail.len() / 3;
    if n_addrs > MAX_EFA_ADDRESSES {
        return Err(ValkeyError::Str(errors::ERR_TOO_MANY_ADDRESSES));
    }
    let mut addrs = Vec::with_capacity(n_addrs);
    let mut total_addr_len: u64 = 0;
    for i in 0..n_addrs {
        let base = i * 3;
        let rkey: u64 = tail[base]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let addr: u64 = tail[base + 1]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let len: u64 = tail[base + 2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_ADDR_LEN))?;
        // A zero-length address can never absorb bytes
        if 0 == len {
            return Err(ValkeyError::Str(errors::ERR_INVALID_ADDR_LEN));
        }
        addrs.push((addr, len as usize, rkey));
        total_addr_len = total_addr_len.saturating_add(len);
    }
    if total_addr_len < required_len {
        return Err(ValkeyError::Str(errors::ERR_INSUFFICIENT_ADDR_SPACE));
    }
    Ok(addrs)
}

// ─── BLOB.HELLO ────────────────────────────────────────────────────────────────
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

// ─── BLOB.GET ──────────────────────────────────────────────────────────────────
//
// Parse args → resolve key → determine transport → dispatch to engine.

pub fn lo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Strict arity, decided before the key lookup so a malformed EFA call cannot be
    // answered as if it were a TCP GET (or as a missing key).
    //   2 args: TCP — BLOB.GET key
    //  ≥5 args: EFA — BLOB.GET key rkey1 addr1 len1 [rkey2 addr2 len2 ...]
    let efa = match args.len() {
        2 => false,
        len if len >= 5 => true,
        _ => return Err(ValkeyError::WrongArity),
    };

    // Lookup LoValue in keyspace.
    let key = ctx.open_key(&args[1]);
    let lo_value: &LoValue = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) if !v.reclaim_in_progress() => v,
        _ => return Ok(ValkeyValue::Null),
    };

    let object_id = lo_value.object_id;
    let obj_len = lo_value.len;
    let crc32c = lo_value.crc32c;

    // Pin the file to protect it from asynchronous deletion in tiered mode.
    let file = lo_value.file.clone();

    // EFA: BLOB.GET key rkey1 addr1 len1 [rkey2 addr2 len2 ...]
    // Address count is inferred from the triplet args.
    let transport = if efa {
        let addrs = parse_efa_addresses(&args, 2, obj_len)?;
        let session = efa_session(ctx)?;
        Transport::Efa { session, addrs }
    } else {
        Transport::Tcp
    };

    // Dispatch to engine — it decides sync vs async internally.
    match engine::execute_get(ctx, object_id, obj_len, crc32c, file, transport) {
        engine::EngineResult::Sync(result) => result,
        engine::EngineResult::Async => Ok(ValkeyValue::NoReply),
    }
}

// ─── BLOB.SET ──────────────────────────────────────────────────────────────────
//
// Parse args → determine data source → dispatch to engine.

pub fn lo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    // Strict arity.
    //   3 args: TCP — BLOB.SET key <data>
    //  ≥6 args: EFA — BLOB.SET key total_len rkey1 addr1 len1 [rkey2 addr2 len2 ...]
    // 4 and 5 args are rejected: neither a valid TCP call (exactly 3) nor a valid EFA
    // call (needs total_len + at least one triplet).
    let efa = match args.len() {
        3 => false,
        len if len >= 6 => true,
        _ => return Err(ValkeyError::WrongArity),
    };

    // Determine data source and obj_len based on transport.
    let (obj_len, data_source) = if efa {
        // EFA: BLOB.SET key total_len rkey1 addr1 len1 [rkey2 addr2 len2 ...]
        // total_len is required — server needs to know how many bytes to fi_read
        // from the client.
        let obj_len: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;
        let addrs = parse_efa_addresses(&args, 3, obj_len)?;
        let session = efa_session(ctx)?;
        (obj_len, DataSource::Efa { session, addrs })
    } else {
        // TCP path: BLOB.SET key <data>
        // data.len() IS the authoritative length. No user-provided len needed.
        let data = args[2].as_slice().to_vec();
        let obj_len = data.len() as u64;
        (obj_len, DataSource::Tcp(data))
    };

    // Reject zero-length values / 0-byte cases.
    if obj_len == 0 {
        return Err(ValkeyError::Str(errors::ERR_ZERO_LENGTH_OBJECT));
    }

    // Reject objects exceeding the configured max object size. The config
    // constraint enforces max-object-size <= segment-size, so this also
    // covers objects that would not fit in a single segment.
    let max_obj_size = crate::max_object_size();
    if obj_len > max_obj_size {
        return Err(ValkeyError::Str(errors::ERR_MAX_OBJECT_SIZE_EXCEEDED));
    }

    // Dispatch to engine — it decides sync vs async internally.
    match engine::execute_set(ctx, &args[1], obj_len, data_source) {
        engine::EngineResult::Sync(result) => result,
        engine::EngineResult::Async => Ok(ValkeyValue::NoReply),
    }
}

// ─── BLOB.INFO ─────────────────────────────────────────────────────────────────
//
// BLOB.INFO key [LEN | CRC | TIER]
//
// Parse args → resolve key → return metadata field or all fields as array.

pub fn lo_info(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if !(2..=3).contains(&args.len()) {
        return Err(ValkeyError::WrongArity);
    }

    let key = ctx.open_key(&args[1]);
    let value = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) if !v.reclaim_in_progress() => v,
        _ => return Err(ValkeyError::Str(errors::ERR_NOT_FOUND)),
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

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "enable-system-alloc"))]
mod tests {
    use super::*;

    /// Build a `Vec<ValkeyString>` from string slices using test shims.
    fn vs(args: &[&str]) -> Vec<ValkeyString> {
        args.iter().map(|s| ValkeyString::test(*s)).collect()
    }

    /// Call `parse_efa_addresses` starting at index 0 and return the error string on failure.
    fn reject(args: &[&str], required_len: u64) -> String {
        let vs = vs(args);
        match parse_efa_addresses(&vs, 0, required_len) {
            Ok(addrs) => panic!("expected rejection, parsed {addrs:?}"),
            Err(ValkeyError::Str(message)) => message.to_string(),
            Err(ValkeyError::WrongArity) => "WrongArity".to_string(),
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn addresses_parse_in_client_order() {
        // ClientEFAAddress is (addr, len, rkey) — not the arg order, which is rkey first.
        let args = vs(&["999", "4096", "8192"]);
        assert_eq!(
            parse_efa_addresses(&args, 0, 8192).unwrap(),
            vec![(4096, 8192, 999)]
        );
        // Order is load-bearing: ChunkIterator consumes addresses front to back, so the
        // object's first bytes land in the first address.
        let args = vs(&["11", "1000", "1024", "22", "9000", "3072"]);
        assert_eq!(
            parse_efa_addresses(&args, 0, 4096).unwrap(),
            vec![(1000, 1024, 11), (9000, 3072, 22)]
        );
        // A zero addr is a valid base: without FI_MR_VIRT_ADDR it is an offset into the
        // registered memory, so it must not be read as unset.
        let args = vs(&["7", "0", "4096"]);
        assert_eq!(
            parse_efa_addresses(&args, 0, 4096).unwrap(),
            vec![(0, 4096, 7)]
        );
    }

    #[test]
    fn start_idx_skips_leading_args() {
        // Simulates BLOB.GET key rkey addr len — start_idx=2 skips "BLOB.GET" and "key".
        let args = vs(&["BLOB.GET", "key", "7", "64", "4096"]);
        assert_eq!(
            parse_efa_addresses(&args, 2, 4096).unwrap(),
            vec![(64, 4096, 7)]
        );
    }

    #[test]
    fn address_space_is_validated_against_the_object() {
        // Exact fit and surplus both pass.
        let args = vs(&["7", "64", "4096"]);
        assert!(parse_efa_addresses(&args, 0, 4096).is_ok());
        let args = vs(&["7", "64", "1048576"]);
        assert_eq!(
            parse_efa_addresses(&args, 0, 4096).unwrap(),
            vec![(64, 1048576, 7)]
        );
        // Shortfall is rejected.
        assert_eq!(
            reject(&["7", "64", "4095"], 4096),
            errors::ERR_INSUFFICIENT_ADDR_SPACE
        );
        // Summed across addresses, still one byte short.
        assert_eq!(
            reject(&["7", "64", "2048", "8", "9000", "2047"], 4096),
            errors::ERR_INSUFFICIENT_ADDR_SPACE
        );
    }

    #[test]
    fn incomplete_triples_are_rejected() {
        assert_eq!(
            reject(&[] as &[&str], 4096),
            errors::ERR_MALFORMED_ADDR_ARGS
        );
        assert_eq!(reject(&["7"], 4096), errors::ERR_MALFORMED_ADDR_ARGS);
        assert_eq!(reject(&["7", "64"], 4096), errors::ERR_MALFORMED_ADDR_ARGS);
        // One complete triple + one leftover.
        assert_eq!(
            reject(&["7", "64", "4096", "extra"], 4096),
            errors::ERR_MALFORMED_ADDR_ARGS
        );
    }

    #[test]
    fn too_many_addresses_are_rejected() {
        let strs: Vec<String> = (0..=MAX_EFA_ADDRESSES)
            .flat_map(|i| ["7".to_string(), (i * 4096).to_string(), "4096".to_string()])
            .collect();
        let refs: Vec<&str> = strs.iter().map(|s| s.as_str()).collect();
        assert_eq!(reject(&refs, 4096), errors::ERR_TOO_MANY_ADDRESSES);
    }

    #[test]
    fn malformed_triple_fields_are_rejected_by_field() {
        assert_eq!(
            reject(&["notakey", "64", "4096"], 4096),
            errors::ERR_INVALID_RKEY
        );
        assert_eq!(
            reject(&["7", "notanaddr", "4096"], 4096),
            errors::ERR_INVALID_REMOTE_ADDR
        );
        assert_eq!(
            reject(&["7", "64", "notalen"], 4096),
            errors::ERR_INVALID_ADDR_LEN
        );
        assert_eq!(
            reject(&["7", "64", "0"], 4096),
            errors::ERR_INVALID_ADDR_LEN
        );
    }
}
