//! Command Handlers — LO.HELLO, LO.GET, LO.SET
//!
//! LO.GET key [rkey remote_addr len]
//!   NVMe read is the same either way. Branch at completion:
//!   - EFA: session.write(buf → client GPU)
//!   - TCP: reply with bulk string from buf
//!
//! LO.SET key len [rkey remote_addr]
//!   NVMe write is the same either way. Source of bytes differs:
//!   - EFA: session.read(client GPU → buf) then NVMe write
//!   - TCP: bytes already inline in RESP, fill buf, then NVMe write

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::storage::{self, Storage};
use crate::transport::{self, EfaAddress, Session};

// ─── Per-Client Session Store ────────────────────────────────────────────────

lazy_static::lazy_static! {
    static ref SESSIONS: Mutex<HashMap<u64, Arc<Session>>> = Mutex::new(HashMap::new());
}

// ─── Reply Data ──────────────────────────────────────────────────────────────

enum ReplyData {
    GetOk {
        bytes_read: u64,
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

// ─── LO.HELLO ────────────────────────────────────────────────────────────────

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

    let client_id = ctx.get_client_id();
    SESSIONS
        .lock()
        .unwrap()
        .insert(client_id, Arc::new(session));

    let reply: Vec<ValkeyValue> = server_addrs
        .iter()
        .map(|a| ValkeyValue::BulkString(hex_encode(&a.0)))
        .collect();
    Ok(ValkeyValue::Array(reply))
}

// ─── LO.GET ──────────────────────────────────────────────────────────────────

pub fn lo_get(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
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

    let efa_args = if args.len() >= 4 {
        let rkey: u64 = args[2]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        Some((rkey, remote_addr))
    } else {
        None
    };

    let client_id = ctx.get_client_id();

    // Clone the Arc<Session> BEFORE entering the async callback chain.
    // This avoids locking SESSIONS inside the io_uring completion callback.
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

    // NVMe read — buf moved in, comes back in callback.
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
                    if let Some((rkey, remote_addr)) = efa_args {
                        // EFA: RDMA write buf → client GPU.
                        // session_arc was cloned before entering this callback — no lock needed.
                        if let Some(session) = session_arc {
                            session.write(
                                buf,
                                bytes_read as usize,
                                rkey,
                                remote_addr,
                                Box::new(move |_buf, write_result| {
                                    let reply = match write_result {
                                        Ok(()) => ReplyData::GetOk { bytes_read },
                                        Err(e) => ReplyData::Err(format!(
                                            "{}: {}",
                                            errors::ERR_EFA_WRITE,
                                            e
                                        )),
                                    };
                                    unblock_client(blocked_client, reply);
                                }),
                            );
                        } else {
                            unblock_client(
                                blocked_client,
                                ReplyData::Err(errors::ERR_SESSION_GONE.to_string()),
                            );
                        }
                    } else {
                        // TCP: copy bytes from buf, release buf, reply with data
                        // In bench-mode: reply with size only (skips TCP output buffer copy)
                        if crate::bench_mode() {
                            unblock_client(blocked_client, ReplyData::GetOk { bytes_read });
                        } else {
                            // SAFETY: buf.ptr() is valid pool memory, bytes_read <= buf.len.
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

// ─── LO.SET ──────────────────────────────────────────────────────────────────

pub fn lo_set(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    if args.len() < 3 {
        return Err(ValkeyError::WrongArity);
    }

    let key_name = args[1].clone();
    let obj_len: u64 = args[2]
        .to_string_lossy()
        .parse()
        .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_LEN))?;

    let storage = storage::get();
    if obj_len > storage.pool_buf_size() as u64 {
        return Err(ValkeyError::Str(errors::ERR_OBJECT_EXCEEDS_BUF));
    }

    let efa_args = if args.len() >= 5 {
        let rkey: u64 = args[3]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_RKEY))?;
        let remote_addr: u64 = args[4]
            .to_string_lossy()
            .parse()
            .map_err(|_| ValkeyError::Str(errors::ERR_INVALID_REMOTE_ADDR))?;
        Some((rkey, remote_addr))
    } else {
        None
    };

    let client_id = ctx.get_client_id();

    // Clone Arc<Session> before entering async path.
    let session_arc = if efa_args.is_some() {
        let sessions = SESSIONS.lock().unwrap();
        match sessions.get(&client_id) {
            Some(s) => Some(Arc::clone(s)),
            None => return Err(ValkeyError::Str(errors::ERR_NO_DMA_SESSION)),
        }
    } else {
        None
    };

    let buf = storage
        .pool_get()
        .ok_or(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED))?;

    let blocked_client = ctx.block_client();

    if let Some((rkey, remote_addr)) = efa_args {
        // EFA: read from client GPU into buf, then NVMe write.
        let key_for_reply = key_name.as_slice().to_vec();
        if let Some(session) = session_arc {
            session.read(
                buf,
                obj_len as usize,
                rkey,
                remote_addr,
                Box::new(move |buf, read_result| {
                    match read_result {
                        Ok(()) => {
                            // Now write buf to NVMe.
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
                                        Err(e) => ReplyData::Err(format!(
                                            "{}: {}",
                                            errors::ERR_NVME_WRITE,
                                            e
                                        )),
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
            );
        } else {
            unblock_client(
                blocked_client,
                ReplyData::Err(errors::ERR_SESSION_GONE.to_string()),
            );
        }
    } else {
        // TCP: bytes come inline as args[3].
        if args.len() > 3 {
            let data = args[3].as_slice();
            let copy_len = data.len().min(obj_len as usize);
            // SAFETY: buf.ptr() is a valid pool buffer with capacity >= buf_size >= obj_len.
            // data.as_ptr() is valid for data.len() bytes. copy_len <= both.
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf.ptr(), copy_len) };
        }

        // Write buf to NVMe.
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
    }

    Ok(ValkeyValue::NoReply)
}

// ─── UnblockClient ───────────────────────────────────────────────────────────

fn unblock_client(bc: valkey_module::BlockedClient, reply: ReplyData) {
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(bc);
    match reply {
        ReplyData::GetOk { bytes_read } => {
            // EFA path or bench-mode TCP: reply with size (no bulk data)
            thread_ctx.reply(Ok(ValkeyValue::Integer(bytes_read as i64)));
        }
        ReplyData::GetOkTcp { data } => {
            // TCP path: reply with the object bytes
            thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
        }
        ReplyData::SetOk {
            key_name,
            oid,
            len,
            crc,
        } => {
            // Scoped block ensures drop order: key, key_str, ctx.
            // key_str must be freed (VM_FreeString) while ctx is still alive,
            // because ctx's autoMemory tracks the string. Without this scope,
            // explicit drop(ctx) frees the context first, then key_str's Drop
            // calls VM_FreeString on freed memory (use-after-free).
            {
                let ctx = thread_ctx.lock();
                let key_str = ctx.create_string(key_name.as_slice());
                let key = ctx.open_key_writable(&key_str);
                let lo_value = LoValue {
                    object_id: oid,
                    len,
                    crc32c: crc,
                };
                key.set_value(&LO_TYPE, lo_value).unwrap();
            }
            thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
        }
        ReplyData::Err(msg) => {
            thread_ctx.reply(Err(ValkeyError::String(msg)));
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
