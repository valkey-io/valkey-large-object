//! Command Engine — routes GET/SET through the correct path based on
//! operating mode (DRAM-only vs Tiered) and transport (TCP vs EFA).
//!
//! Architecture (STORAGE_DESIGN.md §9.2):
//!   TCP GET, DRAMPool hit       → serve inline (no tokio)
//!   TCP GET, DRAMPool miss      → tokio task (Tiered: NVMe read; DRAM-only: impossible)
//!   TCP SET, DRAM-only          → inline (alloc + memcpy, no NVMe)
//!   TCP SET, Tiered             → tokio task (NVMe write)
//!   EFA anything                → tokio task
//!
//! Promotion (Tiered GET miss):
//!   If admission policy says yes → alloc ObjectContext in DRAMPool,
//!   ReadFixed directly into DRAMPool buffers, mark Filling→Ready.
//!   Concurrent GETs coalesce on Filling ObjectContext.

use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::Arc;

use dma_libfabric_protocol::{checksum, DmaError};
use valkey_module::{ValkeyError, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::storage::{self, uring, ObjectContext, ObjectFile};
use crate::transport::Session;
use crate::OperatingMode;

// ─── Transport context passed to engine ──────────────────────────────────────

pub enum Transport {
    Tcp,
    Efa {
        session: Arc<Session>,
        rkey: u64,
        remote_addr: u64,
    },
}

// ─── Engine Result ────────────────────────────────────────────────────────────

/// Result of an engine dispatch. Command handler matches on this.
pub enum EngineResult {
    /// Sync path completed — return this value directly to Valkey.
    Sync(Result<ValkeyValue, ValkeyError>),
    /// Async path — client is blocked, reply will come from tokio task.
    Async,
}

// ─── In-flight pin invariant ─────────────────────────────────────────────────
//
// A DEL / overwrite / expiry / eviction / flush runs `lo_free`, which drops the
// keyspace's refs — the DRAM map's `Arc<ObjectContext>` and `LoValue.file` — at
// any await point of an in-flight request. So any async request that reads or
// writes keyspace-reachable backing state across an `.await` MUST hold its own
// clone of that state for the whole operation.
//
// Must pin `Arc<ObjectContext>` (owns the DRAM buffer; its Drop frees it):
//   - Every DRAM serve that transfers a map-resident object — `serve_from_dram`'s
//     EFA path, reached from both the DRAM-hit caller and the post-promotion
//     caller. (TCP serves copy synchronously with no await, so no pin is needed.)
//   - The promotion read, whose target buffer lives in the Filling `ObjectContext`
//     already inserted in the map — the task moves that Arc in for the read.
//   NOT needed on SET: the buffer is private until `set_value` + `insert_object`
//   commit it, so no concurrent free can reach it.
//
// Must pin `Arc<ObjectFile>` (the object's on-disk existence; its Drop unlinks). The
// open fd is a separate `Arc<OwnedFd>` from `ensure_open`, held for the read's duration:
//   - Every Tiered request that READS the object: the NVMe promotion read, the
//     transient NVMe read, and — by the blanket rule — the DRAM serve that follows
//     a read. The `ObjectFile` pin is held for the whole request, read plus transfer.
//   NOT needed in Dram mode (there is no `ObjectFile`), and NOT on the SET write path:
//   the `ObjectFile` is created at commit, never read during the write. An overwritten
//   old `ObjectFile` is protected by refcount on the replaced `LoValue` (via
//   `lo_free`), not by the writer.

// ─── GET Engine ──────────────────────────────────────────────────────────────

/// Collect all DRAMPool buffers into a contiguous Vec for TCP reply.
fn collect_dram_bytes(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &ObjectContext,
    obj_len: u64,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(obj_len as usize);
    let mut remaining = obj_len as usize;
    for buf in &obj_ctx.buffers {
        let to_copy = remaining.min(buf.len as usize);
        let ptr = dram_pool.buffer_ptr(buf);
        let slice = unsafe { std::slice::from_raw_parts(ptr, to_copy) };
        data.extend_from_slice(slice);
        remaining -= to_copy;
    }
    data
}

/// Execute LO.GET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_get(
    ctx: &valkey_module::Context,
    object_id: ObjectId,
    obj_len: u64,
    file: Option<Arc<ObjectFile>>,
    transport: Transport,
) -> EngineResult {
    let mode = crate::operating_mode();

    match (mode, &transport) {
        (OperatingMode::Dram, Transport::Tcp) => {
            // Fully sync — serve from DRAMPool, return directly.
            EngineResult::Sync(serve_get_dram_tcp(object_id, obj_len))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            match mode {
                OperatingMode::Dram => {
                    execute_get_dram_efa(object_id, obj_len, transport, blocked_client);
                }
                OperatingMode::Tiered => {
                    let file =
                        file.expect("Tiered GET: LoValue.file must be Some (created at commit)");
                    execute_get_tiered(object_id, obj_len, file, transport, blocked_client);
                }
            }
            EngineResult::Async
        }
    }
}

/// Sync DRAM-only TCP GET: serve object data directly from DRAMPool.
fn serve_get_dram_tcp(object_id: ObjectId, obj_len: u64) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();
    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            if crate::bench_mode() {
                Ok(ValkeyValue::Integer(obj_len as i64))
            } else {
                Ok(ValkeyValue::StringBuffer(collect_dram_bytes(
                    dram_pool, &obj_ctx, obj_len,
                )))
            }
        }
        Some(_) => {
            panic!("DRAM-only GET: object in Filling state — SET is synchronous, this is a bug");
        }
        None => {
            panic!("DRAM-only GET: LoValue exists but ObjectContext missing — logic bug");
        }
    }
}

/// DRAM-only GET: object MUST be in DRAMPool. If not found → key doesn't exist
/// (shouldn't happen — LoValue exists implies ObjectContext exists in DRAM-only mode).
fn execute_get_dram_efa(
    object_id: ObjectId,
    obj_len: u64,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            // Serve from DRAMPool.
            serve_from_dram(dram_pool, &obj_ctx, obj_len, transport, thread_ctx, None);
        }
        Some(_obj_ctx) => {
            todo!("DRAM-only GET: object in Filling state. Needs Request Coalescing");
        }
        None => {
            panic!("DRAM-only GET: LoValue exists but ObjectContext missing — logic bug");
        }
    }
}

/// Tiered GET: check DRAMPool → try promote → fall back to NVMe.
/// `file` pins the object's `ObjectFile` (existence) for the whole GET operation; the
/// open fd is a separate `Arc<OwnedFd>` obtained via `ensure_open`.
fn execute_get_tiered(
    object_id: ObjectId,
    obj_len: u64,
    file: Arc<ObjectFile>,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();

    // ─── DRAMPool hit ────────────────────────────────────────────────────
    if let Some(obj_ctx) = dram_pool.get_object(&object_id) {
        if obj_ctx.is_ready() {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            serve_from_dram(
                dram_pool,
                &obj_ctx,
                obj_len,
                transport,
                thread_ctx,
                Some(file),
            );
            return;
        }
        // Filling state: promotion in progress.
        // TODO: coalesce — register as waiter on this ObjectContext.
        // For now: fall through to NVMe read.
    }

    // ─── Try DRAMPool promotion ──────────────────────────────────────────
    // If pool has space and object is eligible, read directly into DRAMPool.
    // `file` (captured by the promotion task below) pins the ObjectFile across the
    // NVMe read and transfer — see the in-flight pin invariant at the top of file.
    if let Some(obj_ctx) = dram_pool.try_promote_object(object_id, obj_len) {
        // TODO: Multi-buffer streaming/chunking (STORAGE_DESIGN.md §7.3).
        if obj_ctx.buffers.len() != 1 {
            todo!("streaming and chunking not yet implemented");
        }
        let seg_buf = &obj_ctx.buffers[0];
        let buf_ptr_usize = dram_pool.buffer_ptr(seg_buf) as usize;
        let read_op = uring::UringOp {
            iovec_index: dram_pool.segments()[seg_buf.segment_idx as usize].iovec_index,
            buf_ptr: buf_ptr_usize as *mut u8,
            file_offset: 0,
            len: obj_len,
        };

        let fd_pool = storage::get_fd_pool();
        let fd = match file.ensure_open(fd_pool, &crate::nvme_dir()) {
            Some(fd) => fd,
            None => {
                // remove_object drops the map's Arc; obj_ctx drops at end of scope
                // → ObjectContext::Drop frees the buffer automatically.
                dram_pool.remove_object(&object_id);
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
                return;
            }
        };

        // Read from NVMe directly into DRAMPool buffer, then serve.
        crate::runtime_handle().spawn(async move {
            let result = uring::submit_read(fd.as_raw_fd(), read_op).await;
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

            match result {
                Ok(Ok(_)) => {
                    // NVMe read complete — transition Filling→Ready.
                    // Release ordering ensures buffer data is visible to any
                    // thread that subsequently sees is_ready() == true.
                    obj_ctx.mark_ready();
                    let dram_pool = storage::get_dram_pool();
                    // Move ObjectFile in so the ObjectFile stays pinned for the read's duration.
                    serve_from_dram(
                        dram_pool,
                        &obj_ctx,
                        obj_len,
                        transport,
                        thread_ctx,
                        Some(file),
                    );
                }
                _ => {
                    // Read failed — remove entry. Buffers freed by ObjectContext Drop.
                    // ObjectFile drops here → teardown if this was the last ref.
                    storage::get_dram_pool().remove_object(&object_id);
                    thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
                }
            }
        });
        return;
    }

    // ─── NVMePool fallback (DRAMPool full) ───────────────────────────────
    // Transient read: alloc NVMePool buffer, serve, free.
    let nvme_pool = storage::get_nvme_pool();
    let seg_buf = match nvme_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_INSUFFICIENT_NVME_BUFFERS)));
            return;
        }
    };

    // Transient read StreamingContext owns only the NVMe buffer; the `ObjectFile`
    // pin is held by the spawned task (see `_keep_alive` below).
    let stream_ctx = storage::StreamingContext::new(vec![seg_buf], obj_len, 1);

    let fd_pool = storage::get_fd_pool();
    let fd = match file.ensure_open(fd_pool, &crate::nvme_dir()) {
        Some(fd) => fd,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
            return;
        }
    };

    let buf_ptr_usize = nvme_pool.buffer_ptr(&stream_ctx.buffers[0]) as usize;
    // Single-chunk today: one UringOp for the entire object.
    // Streaming (STORAGE_DESIGN.md §7.3) will iterate stream_ctx.buffers and submit per-chunk ops in a loop.
    let read_op = uring::UringOp {
        iovec_index: nvme_pool.segments()[stream_ctx.buffers[0].segment_idx as usize].iovec_index,
        buf_ptr: buf_ptr_usize as *mut u8,
        file_offset: 0,
        len: obj_len,
    };

    // Spawn tokio task for NVMe read + serve (no caching — transient).
    // stream_ctx is moved into the async block so its Drop (which returns the
    // NVMe buffer to the pool) doesn't fire until the task completes.
    crate::runtime_handle().spawn(async move {
        // Pin the buffer (stream_ctx) and the `ObjectFile` (`file`) for the transfer's duration.
        let _keep_alive = (stream_ctx, file);

        let read_result = uring::submit_read(fd.as_raw_fd(), read_op).await;
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        handle_nvme_read_result(read_result, transport, obj_len, buf_ptr_usize, thread_ctx).await;
    });
}

/// Handle NVMe read result — reply to the blocked client based on the io_uring
/// completion result and transport type. Shared across GET paths that read from NVMe.
async fn handle_nvme_read_result(
    read_result: Result<Result<u64, storage::StorageError>, tokio::sync::oneshot::error::RecvError>,
    transport: Transport,
    obj_len: u64,
    buf_ptr_usize: usize,
    thread_ctx: valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
) {
    match read_result {
        Ok(Ok(_bytes_read)) => match transport {
            Transport::Tcp => {
                if crate::bench_mode() {
                    thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
                } else {
                    let data = unsafe {
                        std::slice::from_raw_parts(buf_ptr_usize as *const u8, obj_len as usize)
                            .to_vec()
                    };
                    thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
                }
            }
            Transport::Efa {
                session,
                rkey,
                remote_addr,
            } => {
                match efa_write_to_client(
                    session,
                    buf_ptr_usize,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                )
                .await
                {
                    Ok(()) => {
                        thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
                    }
                    Err(e) => {
                        thread_ctx.reply(Err(e));
                    }
                }
            }
        },
        // io_uring read completed with an error (EIO, short read, etc.)
        Ok(Err(e)) => {
            thread_ctx.reply(Err(ValkeyError::String(format!(
                "{}: {}",
                errors::ERR_NVME_READ,
                e
            ))));
        }
        // RecvError: io_uring poller thread dropped the oneshot sender.
        // This means the poller panicked or shut down unexpectedly.
        // TODO: Add error metric counter for poller channel failures.
        Err(_) => {
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
        }
    }
}

// ─── SET Engine ──────────────────────────────────────────────────────────────

/// Execute LO.SET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_set(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data_source: DataSource,
) -> EngineResult {
    let mode = crate::operating_mode();
    // Assign object_id at command dispatch time (main thread) — establishes
    // ordering by arrival, not completion. Used for version checks on async paths.
    let object_id = ObjectId::next();

    match (mode, &data_source) {
        (OperatingMode::Dram, DataSource::Tcp(data)) => {
            // Fully sync — alloc + memcpy + create LoValue inline.
            EngineResult::Sync(serve_set_dram_tcp(ctx, key_name, obj_len, data, object_id))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            let key_name_bytes = key_name.as_slice().to_vec();
            match mode {
                OperatingMode::Dram => {
                    execute_set_dram_efa(
                        key_name_bytes,
                        obj_len,
                        data_source,
                        blocked_client,
                        object_id,
                    );
                }
                OperatingMode::Tiered => {
                    execute_set_tiered(
                        key_name_bytes,
                        obj_len,
                        data_source,
                        blocked_client,
                        object_id,
                    );
                }
            }
            EngineResult::Async
        }
    }
}

/// Sync DRAM-only TCP SET: alloc + memcpy + create LoValue on main thread.
fn serve_set_dram_tcp(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data: &[u8],
    object_id: ObjectId,
) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();

    let seg_buf = match dram_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => return Err(ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED)),
    };

    let buf_ptr = dram_pool.buffer_ptr(&seg_buf);
    let copy_len = data.len().min(obj_len as usize);
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len) };

    let crc = checksum(unsafe { std::slice::from_raw_parts(buf_ptr, obj_len as usize) });

    // set_value BEFORE insert_object — sync path, no version check needed
    // (single-threaded main thread, our object_id is always the latest).
    // If set_value fails, only the buffer needs freeing — no map entry to undo.
    let key = ctx.open_key_writable(key_name);
    let lo_value = LoValue {
        object_id,
        len: obj_len,
        crc32c: crc,
        file: None,
    };
    if key.set_value(&LO_TYPE, lo_value).is_err() {
        dram_pool.free(&seg_buf);
        return Err(ValkeyError::Str("ERR failed to set key"));
    }

    let obj_ctx = Arc::new(ObjectContext::new_ready(vec![seg_buf], obj_len));
    dram_pool.insert_object(object_id, obj_ctx);

    Ok(ValkeyValue::SimpleStringStatic("OK"))
}

pub enum DataSource {
    /// TCP: data is inline bytes from the RESP command args.
    Tcp(Vec<u8>),
    /// EFA: data pulled from client GPU via session.read.
    Efa {
        session: Arc<Session>,
        rkey: u64,
        remote_addr: u64,
    },
}

/// DRAM-only SET: alloc in DRAMPool, fill, create ObjectContext + LoValue.
/// No NVMe. Synchronous for TCP, tokio for EFA.
fn execute_set_dram_efa(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
    object_id: ObjectId,
) {
    let dram_pool = storage::get_dram_pool();

    // Overwriting a key is safe: the winning commit's set_value fires lo_free on the
    // replaced LoValue, dropping its Arc<ObjectContext> (the DRAMPool entry). Dram mode
    // has no file, so there is no fd or .dat to tear down here.

    // Alloc from DRAMPool (this IS the final storage).
    let seg_buf = match dram_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED)));
            return;
        }
    };

    let buf_ptr = dram_pool.buffer_ptr(&seg_buf);
    let buf_ptr_usize = buf_ptr as usize;

    match data_source {
        DataSource::Tcp(_) => {
            unreachable!("Dram+TCP SET routed to sync path via EngineResult");
        }
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: transport.read into DRAMPool buffer via tokio task.
            crate::runtime_handle().spawn(async move {
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

                match efa_read_from_client(
                    session,
                    buf_ptr_usize,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                )
                .await
                {
                    Ok(crc) => {
                        // Version check + set_value BEFORE insert_object.
                        {
                            let ctx = thread_ctx.lock();
                            let key_str = ctx.create_string(key_name);
                            let key = ctx.open_key_writable(&key_str);
                            if let Ok(Some(existing)) = key.get_value::<LoValue>(&LO_TYPE) {
                                if existing.object_id > object_id {
                                    // Stale write — a newer SET already completed. Discard silently.
                                    storage::get_dram_pool().free(&seg_buf);
                                    thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
                                    return;
                                }
                            }
                            let lo_value = LoValue {
                                object_id,
                                len: obj_len,
                                crc32c: crc,
                                file: None,
                            };
                            if key.set_value(&LO_TYPE, lo_value).is_err() {
                                storage::get_dram_pool().free(&seg_buf);
                                thread_ctx.reply(Err(ValkeyError::Str("ERR failed to set key")));
                                return;
                            }
                        }

                        let obj_ctx = Arc::new(ObjectContext::new_ready(vec![seg_buf], obj_len));
                        storage::get_dram_pool().insert_object(object_id, obj_ctx);
                        thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
                    }
                    Err(_) => {
                        storage::get_dram_pool().free(&seg_buf);
                        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_EFA_READ)));
                    }
                }
            });
        }
    }
}

/// Tiered SET: write to NVMe (invalidate DRAMPool entry if exists).
fn execute_set_tiered(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
    object_id: ObjectId,
) {
    let nvme_pool = storage::get_nvme_pool();

    // Overwriting a key is safe: the winning commit's set_value fires lo_free on the
    // replaced LoValue, dropping its Arc<ObjectFile> which allows for a thread-safe
    // fd close + old .dat unlink once inflight requests settle.

    // Alloc NVMePool buffer for the write.
    let seg_buf = match nvme_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_INSUFFICIENT_NVME_BUFFERS)));
            return;
        }
    };

    // StreamingContext owns the NVMePool buffer for this SET operation.
    // Single buffer today; multi-batch streaming adds more buffers here.
    let stream_ctx = storage::StreamingContext::new(vec![seg_buf], obj_len, 1);

    let buf_ptr = nvme_pool.buffer_ptr(&stream_ctx.buffers[0]);
    let buf_ptr_usize = buf_ptr as usize;

    match data_source {
        DataSource::Tcp(data) => {
            // TCP: memcpy into NVMePool buffer, then write to NVMe.
            let copy_len = data.len().min(obj_len as usize);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len) };
            crate::runtime_handle().spawn(async move {
                do_tiered_nvme_write(
                    buf_ptr_usize,
                    obj_len,
                    None,
                    stream_ctx,
                    blocked_client,
                    key_name,
                    object_id,
                )
                .await;
            });
        }
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: transport.read into NVMePool buffer, then write to NVMe.
            crate::runtime_handle().spawn(async move {
                match efa_read_from_client(
                    session,
                    buf_ptr_usize,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                )
                .await
                {
                    Ok(crc) => {
                        do_tiered_nvme_write(
                            buf_ptr_usize,
                            obj_len,
                            Some(crc),
                            stream_ctx,
                            blocked_client,
                            key_name,
                            object_id,
                        )
                        .await;
                    }
                    Err(e) => {
                        let thread_ctx =
                            valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                        thread_ctx.reply(Err(e));
                    }
                }
            });
        }
    }
}

/// Shared Tiered NVMe write: CRC → open tmp → WriteFixed → rename → create LoValue.
/// Must be called from within a tokio task (awaits io_uring write). `crc` can be
/// precomputed, or `None` hashes it here.
async fn do_tiered_nvme_write(
    buf_ptr_usize: usize,
    obj_len: u64,
    crc: Option<u32>,
    stream_ctx: storage::StreamingContext,
    blocked_client: valkey_module::BlockedClient,
    key_name: Vec<u8>,
    object_id: ObjectId,
) {
    // On-disk size for the reservation and verified by stat after the write completes.
    let disk_len = storage::object_disk_len(obj_len);
    // Atomically reserve the disk budget up-front. Reject if writing this object would
    // exceed nvme-maxmemory. stream_ctx drop frees the buffer on return.
    if !uring::try_reserve_nvme_disk_usage(disk_len) {
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_CAPACITY_EXCEEDED)));
        return;
    }
    let crc = match crc {
        Some(crc) => crc,
        None => checksum(unsafe {
            std::slice::from_raw_parts(buf_ptr_usize as *const u8, obj_len as usize)
        }),
    };

    let dir = crate::nvme_dir();
    let file_path = object_id.file_path(&dir);

    let c_path = std::ffi::CString::new(file_path.as_str()).expect("file_path null");
    let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    if crate::direct_io() {
        write_flags |= libc::O_DIRECT;
    }
    // FdPool intentionally not used on SET path — fd cached lazily on first GET via ensure_open.
    let raw_fd = unsafe { libc::open(c_path.as_ptr(), write_flags, 0o644) };
    if raw_fd < 0 {
        uring::decrease_nvme_disk_usage(disk_len);
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        return;
    }
    // OwnedFd closes on drop — no leak on early return.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

    let nvme_pool = storage::get_nvme_pool();
    // Single-chunk today: one UringOp for the entire object.
    // Streaming (STORAGE_DESIGN.md §7.3) will iterate stream_ctx.buffers and submit per-chunk ops in a loop.
    let write_op = uring::UringOp {
        iovec_index: nvme_pool.segments()[stream_ctx.buffers[0].segment_idx as usize].iovec_index,
        buf_ptr: buf_ptr_usize as *mut u8,
        file_offset: 0,
        len: obj_len,
    };

    let write_result = uring::submit_write(fd.as_raw_fd(), write_op).await;
    // fd (OwnedFd) drops when out of scope and close() is automatic.

    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

    match write_result {
        Ok(Ok(())) => {
            // Version check + set_value. No DRAMPool involvement on Tiered SET.
            {
                let ctx = thread_ctx.lock();
                let key_str = ctx.create_string(key_name);
                let key = ctx.open_key_writable(&key_str);
                if let Ok(Some(existing)) = key.get_value::<LoValue>(&LO_TYPE) {
                    if existing.object_id > object_id {
                        // Stale write — a newer SET already completed. Discard silently.
                        // No ObjectFile was created, so release the reservation here.
                        uring::decrease_nvme_disk_usage(disk_len);
                        if let Err(e) = std::fs::remove_file(&file_path) {
                            storage::warn_failed_unlink("SET write cleanup", &file_path, &e);
                        }
                        thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
                        return;
                    }
                }
                // Winning branch (our OID ≥ any committed OID). Confirm the write
                // produced exactly the size we accounted for.
                let on_disk = std::fs::metadata(&file_path)
                    .unwrap_or_else(|e| {
                        panic!(
                            "NVMe accounting: cannot stat object {object_id:?} at {file_path} \
                             to verify write size: {e}"
                        )
                    })
                    .len();
                assert_eq!(
                    on_disk, disk_len,
                    "NVMe accounting: object {object_id:?} on disk is {on_disk} B but we \
                     reserved {disk_len} B — write path and accounting have diverged"
                );
                // Create the new version's ObjectFile owning this file's existence,
                // lazy read fd, and NVMe bytes. An overwrite triggers lo_free on the
                // old LoValue, dropping its ObjectFile (teardown).
                let lo_value = LoValue {
                    object_id,
                    len: obj_len,
                    crc32c: crc,
                    file: Some(Arc::new(ObjectFile::new(object_id, disk_len))),
                };
                if key.set_value(&LO_TYPE, lo_value).is_err() {
                    thread_ctx.reply(Err(ValkeyError::Str("ERR failed to set key")));
                    return;
                }
            }
            thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
        }
        Ok(Err(e)) => {
            uring::decrease_nvme_disk_usage(disk_len);
            if let Err(e) = std::fs::remove_file(&file_path) {
                storage::warn_failed_unlink("SET write cleanup", &file_path, &e);
            }
            thread_ctx.reply(Err(ValkeyError::String(format!(
                "{}: {}",
                errors::ERR_NVME_WRITE,
                e
            ))));
        }
        // RecvError: io_uring poller thread dropped the oneshot sender.
        // This means the poller panicked or shut down unexpectedly.
        // TODO: Add error metric counter for poller channel failures.
        Err(_) => {
            uring::decrease_nvme_disk_usage(disk_len);
            if let Err(e) = std::fs::remove_file(&file_path) {
                storage::warn_failed_unlink("SET write cleanup", &file_path, &e);
            }
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        }
    }
}

// ─── Serve from DRAMPool ─────────────────────────────────────────────────────

/// Serve a Ready object from DRAMPool buffers.
///
/// On the EFA path two pins are moved into the async write task and held for the
/// whole RDMA transfer:
///   - `obj_ctx` — owns the DRAM buffer the RDMA engine reads across the await.
///   - `file` — the object's `ObjectFile` (Tiered only; `None` in Dram mode).
fn serve_from_dram(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &Arc<ObjectContext>,
    obj_len: u64,
    transport: Transport,
    thread_ctx: valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    file: Option<Arc<ObjectFile>>,
) {
    match transport {
        Transport::Tcp => {
            if crate::bench_mode() {
                thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
            } else {
                thread_ctx.reply(Ok(ValkeyValue::StringBuffer(collect_dram_bytes(
                    dram_pool, obj_ctx, obj_len,
                ))));
            }
        }
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: write from DRAMPool buffer to client GPU.
            // TODO: Multi-buffer streaming/chunking (STORAGE_DESIGN.md §7.3).
            if obj_ctx.buffers.len() != 1 {
                todo!("streaming and chunking not yet implemented");
            }
            let buf = &obj_ctx.buffers[0];
            let buf_ptr = dram_pool.buffer_ptr(buf) as usize;
            let obj_ctx = Arc::clone(obj_ctx);
            crate::runtime_handle().spawn(async move {
                let _keep_alive = (obj_ctx, file); // pin the object and file for the transfer's duration
                match efa_write_to_client(session, buf_ptr, obj_len as usize, rkey, remote_addr)
                    .await
                {
                    Ok(()) => thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64))),
                    Err(e) => thread_ctx.reply(Err(e)),
                }
            });
        }
    }
}

// ─── EFA Transport Helpers ───────────────────────────────────────────────────

/// Read from client GPU into local buffer via EFA. Must be awaited in a tokio task.
async fn efa_read_from_client(
    session: Arc<Session>,
    buf_ptr: usize,
    len: usize,
    rkey: u64,
    remote_addr: u64,
) -> Result<u32, ValkeyError> {
    let failed =
        |error: DmaError| ValkeyError::String(format!("{}: {error}", errors::ERR_EFA_READ));
    let transfer = session
        .read(buf_ptr as *mut u8, len, rkey, remote_addr)
        .map_err(failed)?;
    let (outcome, _operand) = transfer.await;
    let done = outcome.map_err(failed)?;
    done.checksum.ok_or_else(|| {
        failed(DmaError::Transfer(
            "no checksum on a checksummed read".into(),
        ))
    })
}

/// Write from local buffer to client GPU via EFA. Must be awaited in a tokio task.
async fn efa_write_to_client(
    session: Arc<Session>,
    buf_ptr: usize,
    len: usize,
    rkey: u64,
    remote_addr: u64,
) -> Result<(), ValkeyError> {
    let failed = ValkeyError::Str(errors::ERR_EFA_WRITE);
    let Ok(transfer) = session.write(buf_ptr as *mut u8, len, rkey, remote_addr) else {
        return Err(failed);
    };
    let (outcome, _operand) = transfer.await;
    match outcome {
        Ok(_done) => Ok(()),
        Err(_) => Err(failed),
    }
}
