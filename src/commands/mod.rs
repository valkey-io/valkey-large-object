//! Command Handlers — DMA.HELLO, DMA.GET, DMA.SET
//!
//! Option 2: Per-request rkey with HELLO.
//!   - DMA.HELLO <client_efa_addr>
//!     Registers client on all N server EFA devices. Returns all server addresses.
//!
//!   - DMA.GET <key> <rkey> <remote_addr> <len>
//!     Reads value from NVMe, fi_writes it into client's registered memory.
//!
//!   - DMA.SET <key> <rkey> <remote_addr> <len>
//!     fi_reads from client's memory into server buffer, writes to NVMe.
//!
//! TCP fallback paths remain for non-EFA clients:
//!   - DMA.GET <key>           → bulk string reply (TCP)
//!   - DMA.SET <key> <len> <data>  → inline data (TCP)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use valkey_module::server_events::{ClientChangeSubevent, CLIENT_CHANGED_SERVER_EVENTS_LIST};
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::storage::{self, Storage};
use crate::transport::{self, EfaAddress, RmaOp, Session, WorkItem};

// ─── Per-Client Session Store ────────────────────────────────────────────────

lazy_static::lazy_static! {
    static ref SESSIONS: Mutex<HashMap<u64, Arc<Session>>> = Mutex::new(HashMap::new());
}

// ─── Reply Data ──────────────────────────────────────────────────────────────

enum ReplyData {
    GetOk {
        bytes_written: u64,
    },
    GetOkTcp {
        data: Vec<u8>,
    },
    SetOk {
        key_name: Vec<u8>,
        oid: ObjectId,
        len: u64,
        crc: u32,
    },
    Err(String),
}

// ─── DMA.HELLO ───────────────────────────────────────────────────────────────

/// DMA.HELLO <client_efa_addr_hex>
///
/// Registers the client's EFA address on all N server EFA devices.
/// Returns: array of all server EFA addresses (one per device).
///
/// After HELLO, the client should fi_av_insert all returned addresses so its
/// NIC accepts incoming RDMA writes from any server device.
pub fn dma_hello(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let pool = transport::worker_pool().ok_or(ValkeyError::Str(errors::ERR_EFA_UNAVAILABLE))?;

    // Parse client EFA address (64 hex chars = 32 bytes)
    let peer_hex = args[1].to_string_lossy();
    let peer_bytes =
        hex_decode(&peer_hex).map_err(|_| ValkeyError::Str(errors::ERR_INVALID_PEER_ADDR_HEX))?;
    if peer_bytes.len() != 32 {
        return Err(ValkeyError::Str(errors::ERR_PEER_ADDR_LEN));
    }
    let mut addr = [0u8; 32];
    addr.copy_from_slice(&peer_bytes);
    let peer_addr = EfaAddress(addr);

    // Create session: fi_av_insert on ALL N devices
    let session = Session::new(pool, &peer_addr)
        .map_err(|e| ValkeyError::String(format!("{}: {}", errors::ERR_SESSION_CREATE, e)))?;

    // Store session keyed by client ID
    let client_id = ctx.get_client_id();
    SESSIONS
        .lock()
        .unwrap()
        .insert(client_id, Arc::new(session));

    // Return all server EFA addresses
    let server_addrs = pool.server_addrs();
    let reply: Vec<ValkeyValue> = server_addrs
        .iter()
        .map(|a| ValkeyValue::BulkString(hex_encode(&a.0)))
        .collect();
    Ok(ValkeyValue::Array(reply))
}

// ─── DMA.GET ─────────────────────────────────────────────────────────────────

/// DMA.GET <key> [<rkey> <remote_addr> <len>]
///
/// With EFA args: reads value from NVMe, fi_writes into client memory at remote_addr.
/// Without EFA args: TCP fallback — returns bulk string.
pub fn dma_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 2 {
        return Err(ValkeyError::WrongArity);
    }

    let key = ctx.open_key(&args[1]);
    let lo_value: &LoValue = match key.get_value::<LoValue>(&LO_TYPE)? {
        Some(v) => v,
        None => return Ok(ValkeyValue::Null),
    };

    let object_id = lo_value.object_id;
    let obj_len = lo_value.len;

    // Parse EFA args: rkey, remote_addr, len (optional — if absent, TCP path)
    let efa_args = if args.len() >= 5 {
        let rkey: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let len: u64 = args[4]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REGION_LEN))?;
        Some((rkey, remote_addr, len))
    } else {
        None
    };

    let client_id = ctx.get_client_id();

    // Clone Arc<Session> before entering async path
    let session_arc = if efa_args.is_some() {
        let sessions = SESSIONS.lock().unwrap();
        match sessions.get(&client_id) {
            Some(s) => Some(Arc::clone(s)),
            None => return Err(ValkeyError::Str(errors::ERR_NO_DMA_SESSION)),
        }
    } else {
        None
    };

    let storage = storage::get();
    let buf = storage
        .pool_get()
        .ok_or(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED))?;

    let blocked_client = ctx.block_client();

    // NVMe read — then EFA write or TCP reply
    storage.read_into(
        object_id,
        buf,
        obj_len,
        Box::new(move |buf, read_result| {
            match read_result {
                Err(e) => {
                    unblock_client(
                        blocked_client,
                        ReplyData::Err(format!("{}: {}", errors::ERR_NVME_READ, e)),
                    );
                }
                Ok(bytes_read) => {
                    if let Some((rkey, remote_addr, _len)) = efa_args {
                        // EFA path: submit fi_write to worker pool
                        if let Some(session) = session_arc {
                            let pool = match transport::worker_pool() {
                                Some(p) => p,
                                None => {
                                    unblock_client(
                                        blocked_client,
                                        ReplyData::Err(errors::ERR_EFA_UNAVAILABLE.to_string()),
                                    );
                                    return;
                                }
                            };

                            let work_item = WorkItem {
                                op: RmaOp::Write,
                                fi_addrs: session.fi_addrs.clone(),
                                buf,
                                len: bytes_read as usize,
                                remote_addr,
                                rkey,
                                on_complete: Box::new(move |_buf, result| {
                                    let reply = match result {
                                        Ok(()) => ReplyData::GetOk {
                                            bytes_written: bytes_read,
                                        },
                                        Err(e) => ReplyData::Err(format!(
                                            "{}: {}",
                                            errors::ERR_EFA_WRITE,
                                            e
                                        )),
                                    };
                                    unblock_client(blocked_client, reply);
                                }),
                            };

                            if pool.submit(work_item).is_err() {
                                // submit() already called on_complete with error
                                // and returned the buffer — nothing more to do
                            }
                        } else {
                            unblock_client(
                                blocked_client,
                                ReplyData::Err(errors::ERR_SESSION_GONE.to_string()),
                            );
                        }
                    } else {
                        // TCP fallback
                        if crate::bench_mode() {
                            unblock_client(
                                blocked_client,
                                ReplyData::GetOk {
                                    bytes_written: bytes_read,
                                },
                            );
                        } else {
                            let data = unsafe {
                                std::slice::from_raw_parts(buf.ptr(), bytes_read as usize).to_vec()
                            };
                            unblock_client(blocked_client, ReplyData::GetOkTcp { data });
                        }
                    }
                }
            }
        }),
    );

    Ok(ValkeyValue::NoReply)
}

// ─── DMA.SET ─────────────────────────────────────────────────────────────────

/// DMA.SET <key> <rkey> <remote_addr> <len>
///
/// With EFA args: fi_reads from client memory, writes to NVMe.
/// TCP fallback: DMA.SET <key> <len> <data>
pub fn dma_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    // Determine if this is an EFA path or TCP path.
    // EFA: DMA.SET <key> <rkey> <remote_addr> <len>  (4 args after command name)
    // TCP: DMA.SET <key> <len> [<data>]              (2-3 args after command name)
    //
    // Heuristic: if we have exactly 5 args and a session exists, it's EFA.
    // If no session exists or only 3-4 args, it's TCP.

    let client_id = ctx.get_client_id();
    let has_session = SESSIONS.lock().unwrap().contains_key(&client_id);

    let is_efa_path = args.len() >= 5 && has_session;

    if is_efa_path {
        // EFA path: DMA.SET <key> <rkey> <remote_addr> <len>
        let key_name = args[1].clone();
        let rkey: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        let obj_len: u64 = args[4]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;

        let storage = storage::get();
        if obj_len > storage.pool_buf_size() as u64 {
            return Err(ValkeyError::Str(errors::ERR_OBJECT_EXCEEDS_BUF));
        }

        let session_arc = {
            let sessions = SESSIONS.lock().unwrap();
            Arc::clone(sessions.get(&client_id).unwrap())
        };

        let buf = storage
            .pool_get()
            .ok_or(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED))?;
        let blocked_client = ctx.block_client();
        let key_for_reply = key_name.as_slice().to_vec();

        let pool = transport::worker_pool().ok_or(ValkeyError::Str(errors::ERR_EFA_UNAVAILABLE))?;

        let work_item = WorkItem {
            op: RmaOp::Read,
            fi_addrs: session_arc.fi_addrs.clone(),
            buf,
            len: obj_len as usize,
            remote_addr,
            rkey,
            on_complete: Box::new(move |buf, result| {
                match result {
                    Ok(()) => {
                        // fi_read complete — now write buf to NVMe
                        let storage = storage::get();
                        storage.write_new(
                            buf,
                            obj_len,
                            Box::new(move |_buf, write_result| {
                                let reply = match write_result {
                                    Ok((oid, crc)) => ReplyData::SetOk {
                                        key_name: key_for_reply,
                                        oid,
                                        len: obj_len,
                                        crc,
                                    },
                                    Err(e) => {
                                        ReplyData::Err(format!("{}: {}", errors::ERR_NVME_WRITE, e))
                                    }
                                };
                                unblock_client(blocked_client, reply);
                            }),
                        );
                    }
                    Err(e) => {
                        unblock_client(
                            blocked_client,
                            ReplyData::Err(format!("{}: {}", errors::ERR_EFA_READ, e)),
                        );
                    }
                }
            }),
        };

        if pool.submit(work_item).is_err() {
            // submit() already called on_complete with error
        }

        Ok(ValkeyValue::NoReply)
    } else {
        // TCP path: DMA.SET <key> <len> [<data>]
        let key_name = args[1].clone();
        let obj_len: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;

        let storage = storage::get();
        if obj_len > storage.pool_buf_size() as u64 {
            return Err(ValkeyError::Str(errors::ERR_OBJECT_EXCEEDS_BUF));
        }

        let buf = storage
            .pool_get()
            .ok_or(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED))?;
        let blocked_client = ctx.block_client();

        // Copy inline data to buffer
        if args.len() > 3 {
            let data = args[3].as_slice();
            let copy_len = data.len().min(obj_len as usize);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf.ptr(), copy_len) };
        }

        let key_for_reply = key_name.as_slice().to_vec();
        storage.write_new(
            buf,
            obj_len,
            Box::new(move |_buf, write_result| {
                let reply = match write_result {
                    Ok((oid, crc)) => ReplyData::SetOk {
                        key_name: key_for_reply,
                        oid,
                        len: obj_len,
                        crc,
                    },
                    Err(e) => ReplyData::Err(format!("{}: {}", errors::ERR_NVME_WRITE, e)),
                };
                unblock_client(blocked_client, reply);
            }),
        );

        Ok(ValkeyValue::NoReply)
    }
}

// ─── UnblockClient ───────────────────────────────────────────────────────────

fn unblock_client(bc: valkey_module::BlockedClient, reply: ReplyData) {
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(bc);
    match reply {
        ReplyData::GetOk { bytes_written } => {
            thread_ctx.reply(Ok(ValkeyValue::Integer(bytes_written as i64)));
        }
        ReplyData::GetOkTcp { data } => {
            thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
        }
        ReplyData::SetOk {
            key_name,
            oid,
            len,
            crc,
        } => {
            let ctx = thread_ctx.lock();
            let key_str = ctx.create_string(key_name.as_slice());
            let key = ctx.open_key_writable(&key_str);
            let lo_value = LoValue {
                object_id: oid,
                len,
                crc32c: crc,
            };
            key.set_value(&LO_TYPE, lo_value).unwrap();
            drop(key);
            drop(key_str);
            drop(ctx);
            thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
        }
        ReplyData::Err(msg) => {
            thread_ctx.reply(Err(ValkeyError::String(msg)));
        }
    }
}

// ─── Session Disconnect Cleanup ──────────────────────────────────────────────

/// Called by the Valkey event system when a client connects or disconnects.
/// On disconnect, removes the client's DMA session and cleans up AV entries
/// on all EFA workers so the AV slots can be reused.
#[linkme::distributed_slice(CLIENT_CHANGED_SERVER_EVENTS_LIST)]
fn on_client_change(ctx: &Context, subevent: ClientChangeSubevent) {
    if subevent != ClientChangeSubevent::Disconnected {
        return;
    }

    let client_id = ctx.get_client_id();

    // Remove session from the map
    let session = SESSIONS.lock().unwrap().remove(&client_id);

    // If this client had a DMA session, clean up its AV entries
    if let Some(session) = session {
        if let Some(pool) = transport::worker_pool() {
            pool.remove_peer(&session.fi_addrs);
        }
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
