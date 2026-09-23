//! Command Engine — routes GET/SET through the correct path based on
//! operating mode (DRAM-only vs Tiered) and transport (TCP vs EFA).
//!
//! Architecture:
//!   TCP GET, DRAMPool hit       → serve inline (no tokio)
//!   TCP GET, DRAMPool miss      → tokio task (Tiered: NVMe read; DRAM-only: impossible)
//!   TCP SET, DRAM-only          → inline (alloc + memcpy, no NVMe)
//!   TCP SET, Tiered             → tokio task (NVMe write)
//!   EFA anything                → tokio task
//!
//! Streaming:
//!   All paths use multi-buffer chunked I/O via ChunkIterator. NVMe paths
//!   write FileHeader at offset 0, data at offset 4096+. SET computes CRC
//!   incrementally per chunk; GET verifies CRC via FileHeader comparison only
//!   (no rolling hash on the read path).
//!
//! Promotion (Tiered GET miss):
//!   If admission policy says yes → alloc ObjectContext in DRAMPool,
//!   ReadFixed directly into DRAMPool buffers, mark Filling→Ready.
//!   Concurrent GETs coalesce on Filling ObjectContext.

use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use valkey_module::{ValkeyError, ValkeyValue, VALKEY_OK};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::info;
use crate::storage::{self, nvme, uring, ChunkIterator, ObjectContext, ObjectFile};
use crate::transport::Session;
use crate::OperatingMode;

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
//     EFA path, and `do_tiered_promote_and_serve_tcp/efa`. (TCP serves copy
//     synchronously with no await via `collect_dram_bytes`, so no pin is needed.)
//   - The promotion read, whose target buffer lives in the Filling `ObjectContext`
//     already inserted in the map — the task moves that Arc in for the read.
//   NOT needed on SET: the buffer is private until `set_value` + `insert_object`
//   commit it, so no concurrent free can reach it.
//
// Must pin `Arc<ObjectFile>` (the object's on-disk existence; its Drop unlinks). The
// open fd is a separate `Arc<OwnedFd>` from `ensure_open`, held for the read's duration:
//   - Every Tiered request that READS the object: the NVMe promotion read
//     (`do_tiered_promote_and_serve_tcp/efa`), the transient NVMe read
//     (`do_tiered_nvme_read_and_serve_tcp/efa`), and — by the blanket rule — the
//     DRAM serve that follows a promotion. The `ObjectFile` pin is held for the
//     whole request, read plus transfer, via `_keep_alive = (file, fd)`.
//   NOT needed in Dram mode (there is no `ObjectFile`), and NOT on the SET write
//   path: the `ObjectFile` is created at commit via `set_finalize`, never read
//   during the write. An overwritten old `ObjectFile` is protected by refcount
//   on the replaced `LoValue` (via `lo_free`), not by the writer.
// ─────────────────────────────────────────────────────────────────────────────

// ─── Transport context passed to engine ──────────────────────────────────────

pub enum Transport {
    Tcp,
    Efa {
        session: Arc<Session>,
        rkey: u64,
        remote_addr: u64,
    },
}

/// Direction for EFA multi-address transfers.
enum EfaDirection {
    Read,
    Write,
}

/// Resolved object identity for GET operations — the subset of LoValue fields
/// needed by async read tasks.
struct GetObjectInfo {
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
}

/// Object identity for SET operations — shared fields passed to async write tasks.
struct SetObjectInfo {
    object_id: ObjectId,
    obj_len: u64,
    key_name: Vec<u8>,
}

// ─── Engine Result ────────────────────────────────────────────────────────────

/// Result of an engine dispatch. Command handler matches on this.
pub enum EngineResult {
    /// Sync path completed — return this value directly to Valkey.
    Sync(Result<ValkeyValue, ValkeyError>),
    /// Async path — client is blocked, reply will come from tokio task.
    Async,
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Increment a metric counter and reply with an error.
/// Consolidates the most common error-reply pattern in the engine.
fn reply_err(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    metric: &std::sync::atomic::AtomicU64,
    err: ValkeyError,
) {
    metric.fetch_add(1, Ordering::Relaxed);
    thread_ctx.reply(Err(err));
}

/// Test hook: pause between NVMe write completion and set_finalize to allow
/// integration tests to inject a DEL and deterministically exercise the
/// delete-during-SET race. Controlled by `test-pause-before-finalize-set-ms`
/// config. 0 = disabled (production default).
async fn test_pause_before_finalize() {
    let pause_ms = crate::test_pause_before_finalize_set_ms();
    if pause_ms > 0 {
        tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(pause_ms));
        })
        .await
        .ok();
    }
}

/// Collect all DRAMPool buffers into a contiguous Vec for TCP reply.
fn collect_dram_bytes(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &ObjectContext,
    obj_len: u64,
    chunk_iter: &mut ChunkIterator,
) -> Vec<u8> {
    chunk_iter.reset_cursor();
    let mut data = Vec::with_capacity(obj_len as usize);
    while let Some(chunk) = chunk_iter.next_chunk() {
        let buf = &obj_ctx.buffers[chunk.buffer_idx];
        let ptr = dram_pool.buffer_ptr(buf);
        let slice = unsafe { std::slice::from_raw_parts(ptr, chunk.user_data_len) };
        data.extend_from_slice(slice);
    }
    data
}

/// Outcome of `set_finalize` — distinguishes a successful write from a stale discard.
enum SetFinalizeOutcome {
    /// Value was written and attached to the key.
    ValueSet,
    /// A newer version already existed; this write was silently discarded.
    StaleDiscarded,
}

/// Version check + set_value on the async SET path.
/// On success the `ObjectFile` is moved into the `LoValue` and lives with the key.
/// On stale or error the `ObjectFile` drops, which removes the file and releases
/// the NVMe disk budget automatically.
fn set_finalize(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    key_name: &[u8],
    object_file: Arc<ObjectFile>,
    obj_len: u64,
    crc: u32,
) -> Result<SetFinalizeOutcome, ValkeyError> {
    let object_id = object_file.object_id();
    let disk_len = object_file.disk_len();
    let file_path = object_id.file_path(&crate::nvme_dir());
    let ctx = thread_ctx.lock();
    let key_str = ctx.create_string(key_name.to_vec());
    let key = ctx.open_key_writable(&key_str);
    if let Ok(Some(existing)) = key.get_value::<LoValue>(&LO_TYPE) {
        if existing.object_id > object_id {
            // ObjectFile drops here — removes file + releases disk budget.
            return Ok(SetFinalizeOutcome::StaleDiscarded);
        }
    }
    let on_disk = std::fs::metadata(&file_path)
        .unwrap_or_else(|e| {
            panic!(
                "NVMe accounting: cannot stat object {:?} at {} \
                 to verify write size: {e}",
                object_id, file_path
            )
        })
        .len();
    assert_eq!(
        on_disk, disk_len,
        "NVMe accounting: object {:?} on disk is {on_disk} B but we \
         reserved {} B — write path and accounting have diverged",
        object_id, disk_len
    );
    let lo_value = LoValue {
        object_id,
        len: obj_len,
        crc32c: crc,
        file: Some(object_file),
    };
    if key.set_value(&LO_TYPE, lo_value).is_err() {
        // set_value failed — LoValue dropped, ObjectFile drops, cleanup automatic.
        return Err(ValkeyError::Str(errors::ERR_SET_VALUE));
    }
    Ok(SetFinalizeOutcome::ValueSet)
}

// ═══════════════════════════════════════════════════════════════════════════════
// GET Engine
// ═══════════════════════════════════════════════════════════════════════════════

/// Execute LO.GET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_get(
    ctx: &valkey_module::Context,
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
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
                    execute_get_dram_efa(object_id, obj_len, crc32c, transport, blocked_client);
                }
                OperatingMode::Tiered => {
                    let file =
                        file.expect("Tiered GET: LoValue.file must be Some (created at commit)");
                    execute_get_tiered(object_id, obj_len, crc32c, file, transport, blocked_client);
                }
            }
            EngineResult::Async
        }
    }
}

// ─── DRAM-only TCP GET ────────────────────────────────────────────

/// Sync DRAM-only TCP GET: serve object data directly from DRAMPool.
/// Multi-buffer: collect_dram_bytes iterates all buffers, copying up to obj_len total.
fn serve_get_dram_tcp(object_id: ObjectId, obj_len: u64) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();
    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            if crate::bench_mode() {
                Ok(ValkeyValue::Integer(obj_len as i64))
            } else {
                let mut chunk_iter =
                    ChunkIterator::new(obj_len, crate::chunk_size(), obj_ctx.buffers.len(), None);
                Ok(ValkeyValue::StringBuffer(collect_dram_bytes(
                    dram_pool,
                    &obj_ctx,
                    obj_len,
                    &mut chunk_iter,
                )))
            }
        }
        Some(_) => {
            panic!("DRAM-only GET: object in Filling state — SET is synchronous, this is a bug")
        }
        None => panic!("DRAM-only GET: LoValue exists but ObjectContext missing — logic bug"),
    }
}

// ─── DRAM-only EFA GET ────────────────────────────────────────────

/// DRAM-only EFA GET: object MUST be in DRAMPool. If not found → key doesn't exist
/// (shouldn't happen — LoValue exists implies ObjectContext exists in DRAM-only mode).
fn execute_get_dram_efa(
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            // Serve from DRAMPool.
            serve_from_dram(
                dram_pool, &obj_ctx, obj_len, crc32c, transport, thread_ctx, None,
            );
        }
        Some(_obj_ctx) => {
            // TODO: Replace with waiter registration on the watch channel (coalescing).
            todo!("DRAM-only GET: object in Filling state. Needs Request Coalescing");
        }
        None => panic!("DRAM-only GET: LoValue exists but ObjectContext missing — logic bug"),
    }
}

// ─── Tiered GET ──────────────────────────────────────────────

/// Tiered GET: check DRAMPool → try promote → fall back to NVMe.
/// `file` pins the object's `ObjectFile` (existence) for the whole GET operation; the
/// open fd is a separate `Arc<OwnedFd>` obtained via `ensure_open`.
fn execute_get_tiered(
    object_id: ObjectId,
    obj_len: u64,
    crc32c: u32,
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
                crc32c,
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
    if let Some(obj_ctx) = dram_pool.try_promote_object(object_id, obj_len) {
        let fd_pool = storage::get_fd_pool();
        let fd = match file.ensure_open(fd_pool, &crate::nvme_dir()) {
            Some(fd) => fd,
            None => {
                // remove_object drops the map's Arc; obj_ctx drops at end of scope
                // → ObjectContext::Drop frees the buffer automatically.
                dram_pool.remove_object(&object_id);
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                reply_err(
                    &thread_ctx,
                    &info::NVME_READ_ERRORS,
                    ValkeyError::Str(errors::ERR_NVME_READ),
                );
                return;
            }
        };
        let raw_fd = fd.as_raw_fd();
        // All N DRAMPool buffers are allocated upfront by try_promote_object.
        // max_sqes_per_batch throttles how many ReadFixed SQEs we submit per
        // io_uring_submit() call — it does NOT control buffer count.
        let max_sqes_per_batch = crate::max_buffers_per_op();
        match &transport {
            Transport::Tcp => {
                crate::runtime_handle().spawn(async move {
                    let _keep_alive = (file, fd);
                    do_tiered_promote_and_serve_tcp(
                        GetObjectInfo {
                            object_id,
                            obj_len,
                            crc32c,
                        },
                        obj_ctx,
                        raw_fd,
                        blocked_client,
                        max_sqes_per_batch,
                    )
                    .await;
                });
            }
            Transport::Efa {
                session,
                rkey,
                remote_addr,
            } => {
                let session = session.clone();
                let efa_addrs = single_efa_addrs(*rkey, *remote_addr, obj_len);
                crate::runtime_handle().spawn(async move {
                    let _keep_alive = (file, fd);
                    do_tiered_promote_and_serve_efa(
                        GetObjectInfo {
                            object_id,
                            obj_len,
                            crc32c,
                        },
                        obj_ctx,
                        raw_fd,
                        blocked_client,
                        max_sqes_per_batch,
                        session,
                        efa_addrs,
                    )
                    .await;
                });
            }
        }
        return;
    }
    // ─── NVMePool fallback (promotion skipped) ───────────────────────────
    // Reaches here when try_promote_object returns None: pool full, object
    // exceeds max-promote-size, or another GET is already promoting this OID.
    // Future: LRFU admission policy may also reject promotion here.
    let nvme_pool = storage::get_nvme_pool();
    let max_buffers = crate::max_buffers_per_op();
    let min_buffers = crate::min_buffers_per_op();
    let buffers = match nvme_pool.alloc_window(obj_len as usize, max_buffers, min_buffers) {
        Some(bufs) => bufs,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::NVME_BUFFER_EXHAUSTED,
                ValkeyError::Str(errors::ERR_INSUFFICIENT_NVME_BUFFERS),
            );
            return;
        }
    };
    let stream_ctx = storage::StreamingContext::new(buffers);
    let fd_pool = storage::get_fd_pool();
    let fd = match file.ensure_open(fd_pool, &crate::nvme_dir()) {
        Some(fd) => fd,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::NVME_READ_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_READ),
            );
            return;
        }
    };
    let raw_fd = fd.as_raw_fd();
    match &transport {
        Transport::Tcp => {
            crate::runtime_handle().spawn(async move {
                // StreamingContext owns the NVMe buffers (freed on drop). ObjectFile
                // pin and open fd are held alive for the read's duration.
                let _keep_alive = (file, fd);
                do_tiered_nvme_read_and_serve_tcp(
                    object_id,
                    obj_len,
                    crc32c,
                    stream_ctx,
                    raw_fd,
                    blocked_client,
                )
                .await;
            });
        }
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            let session = session.clone();
            let efa_addrs = single_efa_addrs(*rkey, *remote_addr, obj_len);
            crate::runtime_handle().spawn(async move {
                // StreamingContext owns the NVMe buffers (freed on drop). ObjectFile
                // pin and open fd are held alive for the read's duration.
                let _keep_alive = (file, fd);
                do_tiered_nvme_read_and_serve_efa(
                    GetObjectInfo {
                        object_id,
                        obj_len,
                        crc32c,
                    },
                    stream_ctx,
                    raw_fd,
                    blocked_client,
                    session,
                    efa_addrs,
                )
                .await;
            });
        }
    }
}

/// TCP promotion — batched ReadFixed into DRAMPool, then accumulate and reply.
async fn do_tiered_promote_and_serve_tcp(
    get_info: GetObjectInfo,
    obj_ctx: Arc<ObjectContext>,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
    max_sqes_per_batch: usize,
) {
    let GetObjectInfo {
        object_id,
        obj_len,
        crc32c: crc32c_expected,
    } = get_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), None);
    let total_chunks = chunk_iter.total_chunks();
    let bench = crate::bench_mode();
    // Pre-loop: read and verify FileHeader. Panics on corrupt data.
    // TODO: Parallelize header and data read submission. Currently serialized
    // because buffers[0] is shared between the header read and chunk 0's data
    // read — submitting both concurrently causes the data read to overwrite
    // header bytes before validation.
    let hdr_buf = &obj_ctx.buffers[0];
    storage::read_and_verify_file_header(
        fd,
        dram_pool.iovec_index_for_buf(hdr_buf),
        dram_pool.buffer_ptr(hdr_buf) as usize,
        object_id,
        obj_len,
        crc32c_expected,
    )
    .await;
    // Batch loop: ReadFixed into DRAMPool buffers.
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = max_sqes_per_batch.min((total_chunks - chunks_done) as usize);
        let mut ops = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let buf = &obj_ctx.buffers[chunk.buffer_idx];
            ops.push(uring::UringOp {
                iovec_index: dram_pool.iovec_index_for_buf(buf),
                buf_ptr: dram_pool.buffer_ptr(buf),
                file_offset: storage::FILE_HEADER_SIZE + chunk.index as u64 * chunk_size as u64,
                len: chunk.user_data_len as u64,
            });
        }
        let receivers = uring::submit_read_batch(fd, ops);
        if let Err(_e) = uring::await_batch(receivers, uring::UringDirection::Read).await {
            dram_pool.remove_object(&object_id);
            reply_err(
                &thread_ctx,
                &info::NVME_READ_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_READ),
            );
            return;
        }
        obj_ctx.advance_chunks_ready(batch_count as u32);
        // TODO: obj_ctx.notify_progress() for coalesced waiters.
        chunks_done += batch_count as u32;
    }
    // Post-loop: mark ready, serve from DRAMPool.
    obj_ctx.mark_ready();
    if bench {
        thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
    } else {
        let data = collect_dram_bytes(dram_pool, &obj_ctx, obj_len, &mut chunk_iter);
        thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
    }
}

/// EFA promotion — batched ReadFixed into DRAMPool + parallelized EFA write per batch.
async fn do_tiered_promote_and_serve_efa(
    get_info: GetObjectInfo,
    obj_ctx: Arc<ObjectContext>,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
    max_sqes_per_batch: usize,
    session: Arc<Session>,
    efa_addrs: Vec<storage::ClientEFAAddress>,
) {
    let GetObjectInfo {
        object_id,
        obj_len,
        crc32c: crc32c_expected,
    } = get_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    // Pre-loop: read and verify FileHeader. Panics on corrupt data.
    // TODO: Parallelize header and data read submission. Currently serialized
    // because buffers[0] is shared between the header read and chunk 0's data
    // read — submitting both concurrently causes the data read to overwrite
    // header bytes before validation.
    let hdr_buf = &obj_ctx.buffers[0];
    storage::read_and_verify_file_header(
        fd,
        dram_pool.iovec_index_for_buf(hdr_buf),
        dram_pool.buffer_ptr(hdr_buf) as usize,
        object_id,
        obj_len,
        crc32c_expected,
    )
    .await;
    let mut chunk_iter =
        ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), Some(efa_addrs));
    let total_chunks = chunk_iter.total_chunks();
    // Batch loop: ReadFixed into DRAMPool, interleaved EFA write.
    // On EFA failure: stop sending but continue ReadFixed for coalesced waiters.
    let mut transport_err: Option<ValkeyError> = None;
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = max_sqes_per_batch.min((total_chunks - chunks_done) as usize);
        let batch_start = chunks_done;
        let mut ops = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let buf = &obj_ctx.buffers[chunk.buffer_idx];
            ops.push(uring::UringOp {
                iovec_index: dram_pool.iovec_index_for_buf(buf),
                buf_ptr: dram_pool.buffer_ptr(buf),
                file_offset: storage::FILE_HEADER_SIZE + chunk.index as u64 * chunk_size as u64,
                len: chunk.user_data_len as u64,
            });
        }
        let receivers = uring::submit_read_batch(fd, ops);
        // Interleaved NVMe read -> EFA write: as each read completes,
        // immediately fire the EFA write for that chunk.
        let mut completions = uring::into_completions(receivers);
        let mut efa_in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut nvme_read_err = false;
        while let Some((batch_idx, result)) = completions.next().await {
            if let Err(_e) = result {
                nvme_read_err = true;
                break;
            }
            if transport_err.is_none() {
                let chunk = chunk_iter.peek_chunk(batch_start + batch_idx as u32);
                let buf = &obj_ctx.buffers[chunk.buffer_idx];
                let buf_ptr = dram_pool.buffer_ptr(buf) as usize;
                let addrs_owned = chunk.addrs.clone().expect("chunk missing EFA addrs");
                let session = session.clone();
                efa_in_flight.push(async move {
                    efa_transfer_addrs(&session, buf_ptr, &addrs_owned, EfaDirection::Write).await
                });
            }
        }
        if nvme_read_err {
            // Drain remaining completions so in-flight io_uring ops finish
            // before DRAMPool buffers are freed.
            while completions.next().await.is_some() {}
            dram_pool.remove_object(&object_id);
            reply_err(
                &thread_ctx,
                &info::NVME_READ_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_READ),
            );
            return;
        }
        // Drain in-flight EFA writes.
        while let Some(result) = efa_in_flight.next().await {
            if let Err(e) = result {
                transport_err = Some(e);
                // Don't return — continue filling DRAMPool for coalesced waiters.
                break;
            }
        }
        obj_ctx.advance_chunks_ready(batch_count as u32);
        // TODO: obj_ctx.notify_progress() for coalesced waiters.
        chunks_done += batch_count as u32;
    }
    obj_ctx.mark_ready();
    match transport_err {
        Some(e) => reply_err(&thread_ctx, &info::EFA_WRITE_ERRORS, e),
        None => {
            thread_ctx.reply(Ok(ValkeyValue::Integer(crc32c_expected as i64)));
        }
    }
}

/// TCP serve-and-discard — batched ReadFixed from NVMe, accumulate into Vec.
async fn do_tiered_nvme_read_and_serve_tcp(
    object_id: ObjectId,
    obj_len: u64,
    crc32c_expected: u32,
    stream_ctx: storage::StreamingContext,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
) {
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let bench = crate::bench_mode();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, None);
    let total_chunks = chunk_iter.total_chunks();
    // Pre-loop: read and verify FileHeader. Panics on corrupt data.
    // TODO: Parallelize header and data read submission. Currently serialized
    // because buffers[0] is shared between the header read and chunk 0's data
    // read — submitting both concurrently causes the data read to overwrite
    // header bytes before validation.
    let hdr_buf = &stream_ctx.buffers[0];
    storage::read_and_verify_file_header(
        fd,
        nvme_pool.iovec_index_for_buf(hdr_buf),
        nvme_pool.buffer_ptr(hdr_buf) as usize,
        object_id,
        obj_len,
        crc32c_expected,
    )
    .await;
    // Batch loop: read chunks from NVMe, accumulate into reply_buf.
    let mut reply_buf = if bench {
        Vec::new()
    } else {
        Vec::with_capacity(obj_len as usize)
    };
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = batch_size.min((total_chunks - chunks_done) as usize);
        let batch_start = chunks_done;
        let mut ops = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let buf = &stream_ctx.buffers[chunk.buffer_idx];
            ops.push(uring::UringOp {
                iovec_index: nvme_pool.iovec_index_for_buf(buf),
                buf_ptr: nvme_pool.buffer_ptr(buf),
                file_offset: storage::FILE_HEADER_SIZE + chunk.index as u64 * chunk_size as u64,
                len: chunk.user_data_len as u64,
            });
        }
        let receivers = uring::submit_read_batch(fd, ops);
        if let Err(_e) = uring::await_batch(receivers, uring::UringDirection::Read).await {
            reply_err(
                &thread_ctx,
                &info::NVME_READ_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_READ),
            );
            return;
        }
        // Copy batch results into reply buffer.
        if !bench {
            for i in 0..batch_count {
                let chunk = chunk_iter.peek_chunk(batch_start + i as u32);
                let buf = &stream_ctx.buffers[chunk.buffer_idx];
                let ptr = nvme_pool.buffer_ptr(buf);
                let slice = unsafe { std::slice::from_raw_parts(ptr, chunk.user_data_len) };
                reply_buf.extend_from_slice(slice);
            }
        }
        chunks_done += batch_count as u32;
    }
    // Post-loop: reply with accumulated data or bench integer.
    // StreamingContext dropped on return → NVMe buffers freed.
    if bench {
        thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
    } else {
        thread_ctx.reply(Ok(ValkeyValue::StringBuffer(reply_buf)));
    }
}

/// EFA serve-and-discard — batched ReadFixed from NVMe, parallel EFA write per batch.
async fn do_tiered_nvme_read_and_serve_efa(
    get_info: GetObjectInfo,
    stream_ctx: storage::StreamingContext,
    fd: RawFd,
    blocked_client: valkey_module::BlockedClient,
    session: Arc<Session>,
    efa_addrs: Vec<storage::ClientEFAAddress>,
) {
    let GetObjectInfo {
        object_id,
        obj_len,
        crc32c: crc32c_expected,
    } = get_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, Some(efa_addrs));
    let total_chunks = chunk_iter.total_chunks();
    // Pre-loop: read and verify FileHeader. Panics on corrupt data.
    // TODO: Parallelize header and data read submission. Currently serialized
    // because buffers[0] is shared between the header read and chunk 0's data
    // read — submitting both concurrently causes the data read to overwrite
    // header bytes before validation.
    let hdr_buf = &stream_ctx.buffers[0];
    storage::read_and_verify_file_header(
        fd,
        nvme_pool.iovec_index_for_buf(hdr_buf),
        nvme_pool.buffer_ptr(hdr_buf) as usize,
        object_id,
        obj_len,
        crc32c_expected,
    )
    .await;
    // Batch loop: read chunks from NVMe, interleaved EFA write.
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = batch_size.min((total_chunks - chunks_done) as usize);
        let batch_start = chunks_done;
        let mut ops = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let buf = &stream_ctx.buffers[chunk.buffer_idx];
            ops.push(uring::UringOp {
                iovec_index: nvme_pool.iovec_index_for_buf(buf),
                buf_ptr: nvme_pool.buffer_ptr(buf),
                file_offset: storage::FILE_HEADER_SIZE + chunk.index as u64 * chunk_size as u64,
                len: chunk.user_data_len as u64,
            });
        }
        let receivers = uring::submit_read_batch(fd, ops);
        // Interleaved NVMe read → EFA write: as each read completes, immediately
        // fire the EFA write for that chunk.
        let mut completions = uring::into_completions(receivers);
        let mut efa_in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        while let Some((batch_idx, result)) = completions.next().await {
            if let Err(_e) = result {
                // Drain remaining completions so io_uring ops finish before
                // StreamingContext drop frees NVMe buffers.
                while completions.next().await.is_some() {}
                reply_err(
                    &thread_ctx,
                    &info::NVME_READ_ERRORS,
                    ValkeyError::Str(errors::ERR_NVME_READ),
                );
                return;
            }
            let chunk = chunk_iter.peek_chunk(batch_start + batch_idx as u32);
            let buf = &stream_ctx.buffers[chunk.buffer_idx];
            let buf_ptr = nvme_pool.buffer_ptr(buf) as usize;
            let addrs_owned = chunk.addrs.clone().expect("chunk missing EFA addrs");
            let session = session.clone();
            efa_in_flight.push(async move {
                efa_transfer_addrs(&session, buf_ptr, &addrs_owned, EfaDirection::Write).await
            });
        }
        // Drain in-flight EFA writes before advancing to next batch (buffers reused).
        while let Some(result) = efa_in_flight.next().await {
            if let Err(e) = result {
                reply_err(&thread_ctx, &info::EFA_WRITE_ERRORS, e);
                return;
            }
        }
        chunks_done += batch_count as u32;
    }
    thread_ctx.reply(Ok(ValkeyValue::Integer(crc32c_expected as i64)));
}

// ═══════════════════════════════════════════════════════════════════════════════
// SET Engine
// ═══════════════════════════════════════════════════════════════════════════════

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

// ─── DRAM-only TCP SET ───────────────────────────────────────────────────────

/// Sync DRAM-only TCP SET: chunked alloc + chunked memcpy + create LoValue.
fn serve_set_dram_tcp(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data: &[u8],
    object_id: ObjectId,
) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, u32::MAX as usize, None);
    let buffers = match dram_pool.alloc_exact(obj_len as usize) {
        Some(bufs) => bufs,
        None => {
            // Reactive expansion: pool exhausted — try adding one segment, then retry.
            if dram_pool.try_expand(ctx).is_none() {
                info::DRAM_POOL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                return Err(ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED));
            }
            match dram_pool.alloc_exact(obj_len as usize) {
                Some(bufs) => bufs,
                None => {
                    info::DRAM_POOL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                    return Err(ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED));
                }
            }
        }
    };
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    while let Some(chunk) = chunk_iter.next_chunk() {
        let src_offset = chunk.index as usize * chunk_size;
        let src = &data[src_offset..src_offset + chunk.user_data_len];
        let buf = &buffers[chunk.buffer_idx];
        let dst = dram_pool.buffer_ptr(buf);
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, chunk.user_data_len) };
        digest.update(src);
    }
    let crc = digest.finalize() as u32;
    // set_value BEFORE insert_object — sync path, no version check needed
    // (single-threaded main thread, our object_id is always the latest).
    // If set_value fails, only the buffers need freeing — no map entry to undo.
    let key = ctx.open_key_writable(key_name);
    let lo_value = LoValue {
        object_id,
        len: obj_len,
        crc32c: crc,
        file: None,
    };
    if key.set_value(&LO_TYPE, lo_value).is_err() {
        dram_pool.free_n(&buffers);
        info::SET_VALUE_FAILURES.fetch_add(1, Ordering::Relaxed);
        return Err(ValkeyError::Str(errors::ERR_SET_VALUE));
    }
    let obj_ctx = Arc::new(ObjectContext::new_ready(buffers));
    dram_pool.insert_object(object_id, obj_ctx);
    VALKEY_OK
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

// ─── DRAM-only EFA SET ───────────────────────────────────────────────────────

/// DRAM-only EFA SET: chunked alloc in DRAMPool, parallel EFA read + post-hoc CRC, create LoValue.
fn execute_set_dram_efa(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
    object_id: ObjectId,
) {
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    // Overwriting a key is safe: the winning commit's set_value fires lo_free on the
    // replaced LoValue, dropping its Arc<ObjectContext> (the DRAMPool entry). Dram mode
    // has no file, so there is no fd or .dat to tear down here.
    // DRAMPool::alloc_exact: all-or-nothing.
    let buffers = match dram_pool.alloc_exact(obj_len as usize) {
        Some(bufs) => bufs,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::DRAM_POOL_EXHAUSTED,
                ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED),
            );
            return;
        }
    };
    match data_source {
        DataSource::Tcp(_) => unreachable!("Dram+TCP SET routed to sync path"),
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA SET: parallel reads into all buffers, then sequential CRC pass.
            crate::runtime_handle().spawn(async move {
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                let dram_pool = storage::get_dram_pool();
                let efa_addrs = single_efa_addrs(rkey, remote_addr, obj_len);
                let mut chunk_iter =
                    ChunkIterator::new(obj_len, chunk_size, buffers.len(), Some(efa_addrs));
                // Parallel EFA reads for all chunks.
                let mut efa_futures = FuturesUnordered::new();
                while let Some(chunk) = chunk_iter.next_chunk() {
                    let chunk_index = chunk.index;
                    let buf = &buffers[chunk.buffer_idx];
                    let buf_ptr = dram_pool.buffer_ptr(buf) as usize;
                    let addrs_owned = chunk.addrs.clone().expect("chunk missing EFA addrs");
                    let session = session.clone();
                    efa_futures.push(async move {
                        let crc =
                            efa_transfer_addrs(&session, buf_ptr, &addrs_owned, EfaDirection::Read)
                                .await?;
                        Ok::<_, ValkeyError>((chunk_index, crc))
                    });
                }
                while let Some(result) = efa_futures.next().await {
                    match result {
                        Ok((chunk_index, crc)) => {
                            chunk_iter.record_checksum(chunk_index, crc);
                        }
                        Err(_) => {
                            reply_err(
                                &thread_ctx,
                                &info::EFA_READ_ERRORS,
                                ValkeyError::Str(errors::ERR_EFA_READ),
                            );
                            dram_pool.free_n(&buffers);
                            return;
                        }
                    }
                }
                // Post-hoc CRC pass (sequential, in chunk order).
                let crc = chunk_iter.combine_checksums();
                // Insert ObjectContext BEFORE set_value so the key is never visible
                // without its ObjectContext. On discard, remove the entry —
                // ObjectContext::Drop returns buffers to DRAMPool automatically.
                let obj_ctx = Arc::new(ObjectContext::new_ready(buffers));
                dram_pool.insert_object(object_id, obj_ctx);
                {
                    let ctx = thread_ctx.lock();
                    let key_str = ctx.create_string(key_name.clone());
                    let key = ctx.open_key_writable(&key_str);
                    if let Ok(Some(existing)) = key.get_value::<LoValue>(&LO_TYPE) {
                        if existing.object_id > object_id {
                            // Stale write — a newer SET already completed.
                            info::SET_FINALIZE_STALE.fetch_add(1, Ordering::Relaxed);
                            dram_pool.remove_object(&object_id);
                            thread_ctx.reply(VALKEY_OK);
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
                        dram_pool.remove_object(&object_id);
                        reply_err(
                            &thread_ctx,
                            &info::SET_VALUE_FAILURES,
                            ValkeyError::Str(errors::ERR_SET_VALUE),
                        );
                        return;
                    }
                }
                thread_ctx.reply(VALKEY_OK);
            });
        }
    }
}

// ─── Tiered SET ──────────────────────────────────────────────────────────────

/// Tiered SET: streaming batch write to NVMe via NVMePool buffer window.
fn execute_set_tiered(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
    object_id: ObjectId,
) {
    let max_buffers = crate::max_buffers_per_op();
    let min_buffers = crate::min_buffers_per_op();
    let nvme_pool = storage::get_nvme_pool();
    let buffers = match nvme_pool.alloc_window(obj_len as usize, max_buffers, min_buffers) {
        Some(bufs) => bufs,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            reply_err(
                &thread_ctx,
                &info::NVME_BUFFER_EXHAUSTED,
                ValkeyError::Str(errors::ERR_INSUFFICIENT_NVME_BUFFERS),
            );
            return;
        }
    };
    let stream_ctx = storage::StreamingContext::new(buffers);
    match data_source {
        DataSource::Tcp(data) => {
            crate::runtime_handle().spawn(async move {
                do_tiered_nvme_write_tcp(
                    data,
                    SetObjectInfo {
                        object_id,
                        obj_len,
                        key_name,
                    },
                    stream_ctx,
                    blocked_client,
                )
                .await;
            });
        }
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            let efa_addrs = single_efa_addrs(rkey, remote_addr, obj_len);
            crate::runtime_handle().spawn(async move {
                do_tiered_nvme_write_efa(
                    SetObjectInfo {
                        object_id,
                        obj_len,
                        key_name,
                    },
                    stream_ctx,
                    blocked_client,
                    session,
                    efa_addrs,
                )
                .await;
            });
        }
    }
}

/// TCP Tiered SET — CRC computed incrementally, data written in batches, FileHeader last.
async fn do_tiered_nvme_write_tcp(
    data: Vec<u8>,
    set_info: SetObjectInfo,
    stream_ctx: storage::StreamingContext,
    blocked_client: valkey_module::BlockedClient,
) {
    let SetObjectInfo {
        object_id,
        obj_len,
        key_name,
    } = set_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, None);
    let total_chunks = chunk_iter.total_chunks();
    let disk_len = storage::object_disk_len(&mut chunk_iter);
    if !nvme::try_reserve_nvme_disk_usage(disk_len) {
        reply_err(
            &thread_ctx,
            &info::NVME_CAPACITY_EXCEEDED,
            ValkeyError::Str(errors::ERR_NVME_CAPACITY_EXCEEDED),
        );
        return;
    }
    // FdPool not used on SET: this write fd is short-lived and never cached.
    // FdPool caches read fds lazily on first GET via ensure_open.
    let dir = crate::nvme_dir();
    let file_path = object_id.file_path(&dir);
    let fd = match storage::open_nvme_file_for_write(&file_path) {
        Ok(fd) => fd,
        Err(_e) => {
            nvme::decrease_nvme_disk_usage(disk_len);
            reply_err(
                &thread_ctx,
                &info::NVME_WRITE_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_WRITE),
            );
            return;
        }
    };
    // ObjectFile owns cleanup from here: Drop removes the file and releases disk budget.
    let object_file = Arc::new(ObjectFile::new(object_id, disk_len));
    // Batch loop: chunk data into NVMePool buffers, write to NVMe.
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = batch_size.min((total_chunks - chunks_done) as usize);
        let mut ops = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let src_offset = chunk.index as usize * chunk_size;
            let src = &data[src_offset..src_offset + chunk.user_data_len];
            let buf = &stream_ctx.buffers[chunk.buffer_idx];
            let dst = nvme_pool.buffer_ptr(buf);
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, chunk.user_data_len) };
            digest.update(src);
            ops.push(uring::UringOp {
                iovec_index: nvme_pool.iovec_index_for_buf(buf),
                buf_ptr: dst,
                file_offset: storage::FILE_HEADER_SIZE + chunk.index as u64 * chunk_size as u64,
                len: chunk.user_data_len as u64,
            });
        }
        let receivers = uring::submit_write_batch(fd.as_raw_fd(), ops);
        if let Err(_e) = uring::await_batch(receivers, uring::UringDirection::Write).await {
            reply_err(
                &thread_ctx,
                &info::NVME_WRITE_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_WRITE),
            );
            return;
        }
        chunks_done += batch_count as u32;
    }
    let crc = digest.finalize() as u32;
    // Post-loop: write FileHeader using buffer[0] (reused after data loop).
    if let Err(_e) = storage::write_file_header(
        fd.as_raw_fd(),
        object_id,
        obj_len,
        crc,
        &stream_ctx.buffers[0],
        nvme_pool,
    )
    .await
    {
        reply_err(
            &thread_ctx,
            &info::NVME_WRITE_ERRORS,
            ValkeyError::Str(errors::ERR_NVME_WRITE),
        );
        return;
    }
    test_pause_before_finalize().await;
    match set_finalize(&thread_ctx, &key_name, object_file, obj_len, crc) {
        Ok(SetFinalizeOutcome::ValueSet) => {
            thread_ctx.reply(VALKEY_OK);
        }
        Ok(SetFinalizeOutcome::StaleDiscarded) => {
            info::SET_FINALIZE_STALE.fetch_add(1, Ordering::Relaxed);
            thread_ctx.reply(VALKEY_OK);
        }
        Err(e) => {
            reply_err(&thread_ctx, &info::SET_VALUE_FAILURES, e);
        }
    }
}

/// EFA Tiered SET — parallel EFA read per batch + post-hoc CRC + batched NVMe write.
async fn do_tiered_nvme_write_efa(
    set_info: SetObjectInfo,
    stream_ctx: storage::StreamingContext,
    blocked_client: valkey_module::BlockedClient,
    session: Arc<Session>,
    efa_addrs: Vec<storage::ClientEFAAddress>,
) {
    let SetObjectInfo {
        object_id,
        obj_len,
        key_name,
    } = set_info;
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    let chunk_size = crate::chunk_size();
    let batch_size = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_size, Some(efa_addrs));
    let total_chunks = chunk_iter.total_chunks();
    let disk_len = storage::object_disk_len(&mut chunk_iter);
    if !nvme::try_reserve_nvme_disk_usage(disk_len) {
        reply_err(
            &thread_ctx,
            &info::NVME_CAPACITY_EXCEEDED,
            ValkeyError::Str(errors::ERR_NVME_CAPACITY_EXCEEDED),
        );
        return;
    }
    // FdPool not used on SET: this write fd is short-lived and never cached.
    // FdPool caches read fds lazily on first GET via ensure_open.
    let dir = crate::nvme_dir();
    let file_path = object_id.file_path(&dir);
    let fd = match storage::open_nvme_file_for_write(&file_path) {
        Ok(fd) => fd,
        Err(_e) => {
            nvme::decrease_nvme_disk_usage(disk_len);
            reply_err(
                &thread_ctx,
                &info::NVME_WRITE_ERRORS,
                ValkeyError::Str(errors::ERR_NVME_WRITE),
            );
            return;
        }
    };
    // ObjectFile owns cleanup from here: Drop removes the file and releases disk budget.
    let object_file = Arc::new(ObjectFile::new(object_id, disk_len));
    let mut chunks_done: u32 = 0;
    while chunks_done < total_chunks {
        let batch_count = batch_size.min((total_chunks - chunks_done) as usize);
        // Interleaved EFA read → NVMe write: as each EFA read completes,
        // immediately submit the NVMe write for that chunk. This overlaps
        // network and disk I/O within the batch.
        let mut efa_futures = FuturesUnordered::new();
        for _ in 0..batch_count {
            let chunk = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            let chunk_index = chunk.index;
            let buf = &stream_ctx.buffers[chunk.buffer_idx];
            let buf_ptr = nvme_pool.buffer_ptr(buf) as usize;
            let addrs_owned = chunk.addrs.clone().expect("chunk missing EFA addrs");
            let session = session.clone();
            efa_futures.push(async move {
                let crc =
                    efa_transfer_addrs(&session, buf_ptr, &addrs_owned, EfaDirection::Read).await?;
                Ok::<_, ValkeyError>((chunk_index, crc))
            });
        }
        // As each EFA read completes, submit the NVMe write.
        let mut nvme_write_receivers = Vec::new();
        while let Some(result) = efa_futures.next().await {
            match result {
                Ok((chunk_index, crc)) => {
                    chunk_iter.record_checksum(chunk_index, crc);
                    // EFA read for this chunk complete — submit NVMe write immediately.
                    let chunk = chunk_iter.peek_chunk(chunk_index);
                    let buf = &stream_ctx.buffers[chunk.buffer_idx];
                    let write_op = uring::UringOp {
                        iovec_index: nvme_pool.iovec_index_for_buf(buf),
                        buf_ptr: nvme_pool.buffer_ptr(buf),
                        file_offset: storage::FILE_HEADER_SIZE
                            + chunk.index as u64 * chunk_size as u64,
                        len: chunk.user_data_len as u64,
                    };
                    nvme_write_receivers.push(uring::submit_write(fd.as_raw_fd(), write_op));
                }
                Err(_) => {
                    reply_err(
                        &thread_ctx,
                        &info::EFA_READ_ERRORS,
                        ValkeyError::Str(errors::ERR_EFA_READ),
                    );
                    return;
                }
            }
        }
        // Drain NVMe writes (may already be done — they started during EFA reads).
        for rx in nvme_write_receivers {
            match rx.await {
                Ok(Ok(())) => {}
                _ => {
                    reply_err(
                        &thread_ctx,
                        &info::NVME_WRITE_ERRORS,
                        ValkeyError::Str(errors::ERR_NVME_WRITE),
                    );
                    return;
                }
            }
        }
        chunks_done += batch_count as u32;
    }
    let crc = chunk_iter.combine_checksums();
    // Post-loop: write FileHeader.
    if let Err(_e) = storage::write_file_header(
        fd.as_raw_fd(),
        object_id,
        obj_len,
        crc,
        &stream_ctx.buffers[0],
        nvme_pool,
    )
    .await
    {
        reply_err(
            &thread_ctx,
            &info::NVME_WRITE_ERRORS,
            ValkeyError::Str(errors::ERR_NVME_WRITE),
        );
        return;
    }
    test_pause_before_finalize().await;
    match set_finalize(&thread_ctx, &key_name, object_file, obj_len, crc) {
        Ok(SetFinalizeOutcome::ValueSet) => {
            thread_ctx.reply(VALKEY_OK);
        }
        Ok(SetFinalizeOutcome::StaleDiscarded) => {
            info::SET_FINALIZE_STALE.fetch_add(1, Ordering::Relaxed);
            thread_ctx.reply(VALKEY_OK);
        }
        Err(e) => {
            reply_err(&thread_ctx, &info::SET_VALUE_FAILURES, e);
        }
    }
}

// ─── Serve from DRAMPool ─────────────────────────────────────────────────────

/// Serve a Ready ObjectContext from DRAMPool. TCP accumulates into Vec;
/// EFA writes per-chunk to client GPU (parallelized).
/// On the EFA path, `obj_ctx` and `file` are pinned for the async transfer's duration.
fn serve_from_dram(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &Arc<ObjectContext>,
    obj_len: u64,
    crc32c: u32,
    transport: Transport,
    thread_ctx: valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    file: Option<Arc<ObjectFile>>,
) {
    match transport {
        Transport::Tcp => {
            if crate::bench_mode() {
                thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
            } else {
                let mut chunk_iter =
                    ChunkIterator::new(obj_len, crate::chunk_size(), obj_ctx.buffers.len(), None);
                thread_ctx.reply(Ok(ValkeyValue::StringBuffer(collect_dram_bytes(
                    dram_pool,
                    obj_ctx,
                    obj_len,
                    &mut chunk_iter,
                ))));
            }
        }
        Transport::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: parallel write from DRAMPool buffers to client GPU.
            let obj_ctx = obj_ctx.clone();
            crate::runtime_handle().spawn(async move {
                let _keep_alive = (&obj_ctx, file);
                let dram_pool = storage::get_dram_pool();
                let chunk_size = crate::chunk_size();
                let efa_addrs = single_efa_addrs(rkey, remote_addr, obj_len);
                let mut chunk_iter =
                    ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), Some(efa_addrs));
                let mut efa_futures = FuturesUnordered::new();
                while let Some(chunk) = chunk_iter.next_chunk() {
                    let buf = &obj_ctx.buffers[chunk.buffer_idx];
                    let buf_ptr = dram_pool.buffer_ptr(buf) as usize;
                    let addrs_owned = chunk.addrs.clone().expect("chunk missing EFA addrs");
                    let session = session.clone();
                    efa_futures.push(async move {
                        efa_transfer_addrs(&session, buf_ptr, &addrs_owned, EfaDirection::Write)
                            .await
                    });
                }
                while let Some(result) = efa_futures.next().await {
                    if let Err(e) = result {
                        reply_err(&thread_ctx, &info::EFA_WRITE_ERRORS, e);
                        return;
                    }
                }
                thread_ctx.reply(Ok(ValkeyValue::Integer(crc32c as i64)));
            });
        }
    }
}

// ─── EFA Transport Helpers ───────────────────────────────────────────────────

/// Wrap a single contiguous EFA address as a ClientEFAAddress list.
/// Temporary: once multi-address support lands, callers will receive
/// Vec<ClientEFAAddress> directly from the transport layer. For now, we
/// perform the transformation to Vec in this function.
fn single_efa_addrs(rkey: u64, remote_addr: u64, obj_len: u64) -> Vec<storage::ClientEFAAddress> {
    vec![(remote_addr, obj_len as usize, rkey)]
}

/// EFA transfer for a chunk's addresses. Each address is (remote_addr, len, rkey).
/// Fires all sub-transfers in parallel via FuturesUnordered. Returns the combined
/// transport checksum (CRC32C for reads, 0 for writes).
async fn efa_transfer_addrs(
    session: &Arc<Session>,
    buf_ptr: usize,
    addrs: &[storage::ClientEFAAddress],
    direction: EfaDirection,
) -> Result<u32, ValkeyError> {
    // TODO: Track specific EFA error types (e.g. timeout, connection reset) before
    // collapsing to the generic ERR_EFA_READ/ERR_EFA_WRITE reply string.
    let err_str = match direction {
        EfaDirection::Write => errors::ERR_EFA_WRITE,
        EfaDirection::Read => errors::ERR_EFA_READ,
    };
    let mut indexed_futures = FuturesUnordered::new();
    let mut buf_offset = 0usize;
    let mut sub_lens: Vec<usize> = Vec::with_capacity(addrs.len());
    for (i, &(addr, len, rkey)) in addrs.iter().enumerate() {
        let transfer = match direction {
            EfaDirection::Write => {
                session.write((buf_ptr + buf_offset) as *mut u8, len, rkey, addr)
            }
            EfaDirection::Read => session.read((buf_ptr + buf_offset) as *mut u8, len, rkey, addr),
        }
        .map_err(|_| ValkeyError::Str(err_str))?;
        sub_lens.push(len);
        indexed_futures.push(async move { (i, transfer.await) });
        buf_offset += len;
    }
    let mut results: Vec<Option<u32>> = vec![None; addrs.len()];
    while let Some((idx, (outcome, _operand))) = indexed_futures.next().await {
        let done = outcome.map_err(|_| ValkeyError::Str(err_str))?;
        results[idx] = match direction {
            // SET path: transport must provide a checksum for CRC combination.
            EfaDirection::Read => Some(
                done.checksum
                    .expect("EFA Read completion missing checksum — transport must provide CRC"),
            ),
            // GET path: checksum not needed (already stored in FileHeader).
            EfaDirection::Write => Some(0),
        };
    }
    // GET (Write) path: callers ignore the returned CRC — skip combination.
    if matches!(direction, EfaDirection::Write) {
        return Ok(0);
    }
    let mut combined = results[0].expect("EFA transfer result missing") as u64;
    for i in 1..results.len() {
        combined = crc_fast::checksum_combine(
            crc_fast::CrcAlgorithm::Crc32Iscsi,
            combined,
            results[i].expect("EFA transfer result missing") as u64,
            sub_lens[i] as u64,
        );
    }
    Ok(combined as u32)
}
