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

use valkey_module::{NotifyEvent, ValkeyError, ValkeyValue, VALKEY_OK};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::errors;
use crate::info;
use crate::storage::{self, nvme, ChunkIterator, Crc, ObjectContext, ObjectFile};
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
//   - Every DRAM serve that transfers a map-resident object — `cmd_get_from_dram`'s
//     EFA path, and the promotion serve in `cmd_get_tiered` / `cmd_get_tiered_run`. (TCP
//     serves copy synchronously with no await via `collect_dram_bytes`, so no pin
//     is needed.)
//   - The promotion read, whose target buffer lives in the Filling `ObjectContext`
//     already inserted in the map — the task moves that Arc in for the read.
//   NOT needed on SET: the buffer is private until `set_value` + `insert_object`
//   commit it, so no concurrent free can reach it.
//
// Must pin `Arc<ObjectFile>` (the object's on-disk existence; its Drop unlinks). The
// open fd is a separate `Arc<OwnedFd>` from `ensure_open`, held for the read's duration:
//   - Every Tiered request that READS the object: the NVMe promotion read and the
//     transient NVMe read (both in `cmd_get_tiered` / `cmd_get_tiered_run`), and — by the
//     blanket rule — the DRAM serve that follows a promotion. The `ObjectFile` pin
//     is held for the whole request, read plus transfer, via `_keep_alive = (file, fd)`.
//   NOT needed in Dram mode (there is no `ObjectFile`), and NOT on the SET write
//   path: the `ObjectFile` is moved into the `LoValue` at commit (`commit_lo_value`),
//   never read during the write. An overwritten old `ObjectFile` is protected by refcount
//   on the replaced `LoValue` (via `lo_free`), not by the writer.
// ─────────────────────────────────────────────────────────────────────────────

// ─── Transport context passed to engine ──────────────────────────────────────

pub enum Transport {
    Tcp,
    Efa {
        session: Arc<Session>,
        /// The client's memory regions, in the order it listed them. ChunkIterator
        /// consumes them front to back and straddles their boundaries.
        addrs: Vec<storage::ClientEFAAddress>,
    },
}

/// Resolved object identity for GET operations — the subset of LoValue fields
/// needed by async read tasks.
struct GetObjectInfo {
    object_id: ObjectId,
    obj_len: u64,
    crc32c: Crc,
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
pub(crate) fn reply_err(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    metric: &std::sync::atomic::AtomicU64,
    err: ValkeyError,
) {
    metric.fetch_add(1, Ordering::Relaxed);
    thread_ctx.reply(Err(err));
}

/// Keep `_buffers` alive until all in-flight EFA drain tasks complete.
/// Called on EFA error paths so the client is unblocked immediately while
/// hardware DMA finishes safely. If no drains are pending this is a no-op
/// and the buffers drop synchronously.
fn spawn_buffer_guard(
    drain_handles: Arc<crate::stream::DrainHandles>,
    _buffers: impl Send + 'static,
) {
    let handles: Vec<_> = drain_handles.lock().unwrap().drain(..).collect();
    if handles.is_empty() {
        return;
    }
    crate::runtime_handle().spawn(async move {
        for h in handles {
            let _ = h.await;
        }
        drop(_buffers);
    });
}

/// Test hook: pause between NVMe write completion and the commit (`commit_lo_value`) to allow
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

/// The EFA GET success reply: a two-element array `[obj_len, crc32c]`. The regions may
/// cover more than the object, so `obj_len` is how the client bounds the valid bytes.
fn efa_get_reply(obj_len: u64, crc32c: Crc) -> ValkeyValue {
    ValkeyValue::Array(vec![
        ValkeyValue::Integer(obj_len as i64),
        ValkeyValue::Integer(crc32c as i64),
    ])
}

/// Outcome of `commit_lo_value` — distinguishes a successful write from a stale discard.
enum CommitOutcome {
    /// Value was written and attached to the key.
    ValueSet,
    /// A newer version already existed; this write was silently discarded.
    StaleDiscarded,
}

/// The one shared SET commit: version-guarded `set_value`. Knows nothing about
/// files, DRAM, accounting, metrics or replies — the caller builds the `LoValue`
/// (with `file: Some`/`None`), does any accounting BEFORE calling, and handles its
/// own cleanup/metric/reply on each outcome. On `StaleDiscarded`/`Err` the moved-in
/// `LoValue` drops here; for NVMe that drops its `ObjectFile` → unlink + budget release.
fn commit_lo_value(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    key_name: &[u8],
    object_id: ObjectId,
    lo_value: LoValue,
) -> Result<CommitOutcome, ValkeyError> {
    let ctx = thread_ctx.lock();
    let key_str = ctx.create_string(key_name.to_vec());
    let key = ctx.open_key_writable(&key_str);
    // One lookup drives both the version guard and the create/update event.
    let event = match key.get_value::<LoValue>(&LO_TYPE) {
        Ok(Some(existing)) if existing.object_id > object_id => {
            return Ok(CommitOutcome::StaleDiscarded);
        }
        Ok(Some(_)) => EVENT_UPDATE,
        _ => EVENT_CREATE,
    };
    if key.set_value(&LO_TYPE, lo_value).is_err() {
        return Err(ValkeyError::Str(errors::ERR_SET_VALUE));
    }
    ctx.notify_keyspace_event(NotifyEvent::MODULE, event, &key_str);
    Ok(CommitOutcome::ValueSet)
}

/// Keyspace event names published after a successful BLOB.SET.
pub(crate) const EVENT_CREATE: &str = "largeobj.create";
pub(crate) const EVENT_UPDATE: &str = "largeobj.update";

// ═══════════════════════════════════════════════════════════════════════════════
// GET Engine
// ═══════════════════════════════════════════════════════════════════════════════

/// Execute BLOB.GET with mode + transport routing.
/// Engine owns all routing decisions. Command handler just matches EngineResult.
pub fn execute_get(
    ctx: &valkey_module::Context,
    object_id: ObjectId,
    obj_len: u64,
    crc32c: Crc,
    file: Option<Arc<ObjectFile>>,
    transport: Transport,
) -> EngineResult {
    let mode = crate::operating_mode();
    match (mode, &transport) {
        (OperatingMode::Dram, Transport::Tcp) => {
            // Fully sync — serve from DRAMPool, return directly.
            EngineResult::Sync(cmd_get_dram_tcp(object_id, obj_len))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            match mode {
                OperatingMode::Dram => {
                    cmd_get_dram_efa(object_id, obj_len, crc32c, transport, blocked_client);
                }
                OperatingMode::Tiered => {
                    let file =
                        file.expect("Tiered GET: LoValue.file must be Some (created at commit)");
                    cmd_get_tiered(object_id, obj_len, crc32c, file, transport, blocked_client);
                }
            }
            EngineResult::Async
        }
    }
}

// ─── DRAM-only TCP GET ────────────────────────────────────────────

/// Sync DRAM-only TCP GET: serve object data directly from DRAMPool.
/// Multi-buffer: collect_dram_bytes iterates all buffers, copying up to obj_len total.
fn cmd_get_dram_tcp(object_id: ObjectId, obj_len: u64) -> Result<ValkeyValue, ValkeyError> {
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
fn cmd_get_dram_efa(
    object_id: ObjectId,
    obj_len: u64,
    crc32c: Crc,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();
    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
    match dram_pool.get_object(&object_id) {
        Some(obj_ctx) if obj_ctx.is_ready() => {
            // Serve from DRAMPool.
            cmd_get_from_dram(
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
fn cmd_get_tiered(
    object_id: ObjectId,
    obj_len: u64,
    crc32c: Crc,
    file: Arc<ObjectFile>,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();
    let cache = dram_pool.tiered_cache();
    // ─── DRAMPool hit ────────────────────────────────────────────────────
    let mut filling = false;
    if let Some(obj_ctx) = dram_pool.get_object(&object_id) {
        if obj_ctx.is_ready() {
            cache.stats.record_hit(&obj_ctx.stats);
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            cmd_get_from_dram(
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
        filling = true;
    }
    // Everything below reads from NVMe, whether or not it also promotes.
    cache.stats.record_miss();
    // ─── Try DRAMPool promotion ──────────────────────────────────────────
    // Admit via the admission filter, then allocate (reclaiming cold copies if full).
    // Skip both if another GET is already promoting this OID (Filling).
    let promoted = if !filling && cache.admission.admit(object_id, obj_len) {
        dram_pool.try_promote_object(object_id, obj_len)
    } else {
        None
    };
    if let Some(obj_ctx) = promoted {
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
        let batch_width = obj_ctx.buffers.len();
        let get_info = GetObjectInfo {
            object_id,
            obj_len,
            crc32c,
        };
        let (chunk_iter, target) = cmd_get_transport_parts(transport, obj_len, batch_width);
        crate::runtime_handle().spawn(async move {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            // Declared after thread_ctx so it drops first: the fd clone is gone
            // before the client unblocks, so its next GET sees the fd unpinned.
            let _keep_alive = (file, fd);
            // Promotion: read the NVMe file INTO the DRAM buffers (pool=Dram), and the
            // progress hook marks the cached entry Ready.
            let progress = crate::stream::PromotionProgress {
                obj_ctx: &obj_ctx,
                dram_pool,
                object_id,
            };
            let drain_handles = cmd_get_tiered_run(
                get_info,
                &obj_ctx.buffers,
                crate::stream::Pool::Dram(dram_pool),
                Some(&progress),
                raw_fd,
                max_sqes_per_batch,
                chunk_iter,
                &thread_ctx,
                target,
            )
            .await;
            // Keep DRAM buffers alive until any background EFA drains complete.
            if let Some(dh) = drain_handles {
                spawn_buffer_guard(dh, obj_ctx.clone());
            }
        });
        return;
    }
    // ─── NVMePool fallback (promotion skipped) ───────────────────────────
    // Reaches here when admission rejected the object, try_promote_object returned None
    // or another GET is already promoting this OID.
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
    let batch_width = stream_ctx.buffers.len();
    let get_info = GetObjectInfo {
        object_id,
        obj_len,
        crc32c,
    };
    let nvme_pool = storage::get_nvme_pool();
    let (chunk_iter, target) = cmd_get_transport_parts(transport, obj_len, batch_width);
    crate::runtime_handle().spawn(async move {
        // StreamingContext owns the NVMe buffers (freed on drop). ObjectFile pin and
        // open fd are held alive for the read's duration. No promotion → no cache,
        // source reads straight from the NVMe pool window.
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        // Declared after thread_ctx so it drops first (see the promotion path).
        let _keep_alive = (file, fd);
        let drain_handles = cmd_get_tiered_run(
            get_info,
            &stream_ctx.buffers,
            crate::stream::Pool::Nvme(nvme_pool),
            None,
            raw_fd,
            batch_width,
            chunk_iter,
            &thread_ctx,
            target,
        )
        .await;
        // Keep NVMe buffers alive until any background EFA drains complete.
        if let Some(dh) = drain_handles {
            spawn_buffer_guard(dh, stream_ctx);
        }
    });
}

/// The per-transport variant of a GET's target — the ONLY thing that differs
/// between the TCP and EFA read paths. Matched once inside the GET envelopes to
/// build the target and produce the success reply.
enum GetTarget {
    /// TCP: accumulate chunks into a reply buffer; reply the collected bytes.
    Tcp,
    /// EFA: fi_write each chunk to the client; reply [obj_len, crc32c].
    Efa(Arc<Session>),
}

/// Build the per-transport GET pieces once: the chunk iterator (EFA carries the
/// client addresses, TCP does not) and the matching `GetTarget`. Used by both the
/// promotion and the streaming Tiered GET paths so the transport branch lives once.
fn cmd_get_transport_parts(
    transport: Transport,
    obj_len: u64,
    batch_width: usize,
) -> (ChunkIterator, GetTarget) {
    let chunk_size = crate::chunk_size();
    match transport {
        Transport::Tcp => (
            ChunkIterator::new(obj_len, chunk_size, batch_width, None),
            GetTarget::Tcp,
        ),
        Transport::Efa { session, addrs } => (
            ChunkIterator::new(obj_len, chunk_size, batch_width, Some(addrs)),
            GetTarget::Efa(session),
        ),
    }
}

/// The ONE Tiered GET body, for both promotion (read NVMe → DRAM cache, `progress`
/// set) and serve-and-discard streaming (`progress` None). `source_pool` is the
/// pool backing the buffers the NvmeSource reads into (DRAM for promotion, NVMe for
/// streaming). Builds the job + source, runs the driver, and replies: TCP the
/// collected bytes, EFA [obj_len, crc32c]. A latched target error (promotion
/// continue-filling) or a run error maps through `reply_stream_err`.
#[allow(clippy::too_many_arguments)]
async fn cmd_get_tiered_run(
    get_info: GetObjectInfo,
    buffers: &[storage::SegmentBuffer],
    source_pool: crate::stream::Pool,
    progress: Option<&crate::stream::PromotionProgress<'_>>,
    fd: RawFd,
    batch_width: usize,
    chunk_iter: ChunkIterator,
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    target: GetTarget,
) -> Option<Arc<crate::stream::DrainHandles>> {
    let GetObjectInfo {
        object_id,
        obj_len,
        crc32c: crc32c_expected,
    } = get_info;
    // Dedicated header buffer — can't share buffers[0] because the header read
    // and chunk 0's data read would race on the same memory.
    let hdr_buf = match source_pool.alloc_for_file_header() {
        Some(buf) => buf,
        None => {
            let (metric, err) = match source_pool {
                crate::stream::Pool::Nvme(_) => (
                    &info::NVME_BUFFER_EXHAUSTED,
                    errors::ERR_INSUFFICIENT_NVME_BUFFERS,
                ),
                crate::stream::Pool::Dram(_) => {
                    (&info::DRAM_POOL_EXHAUSTED, errors::ERR_DRAM_POOL_EXHAUSTED)
                }
            };
            // Promotion path: remove the Filling entry from the DRAM pool.
            if let crate::stream::Pool::Dram(p) = source_pool {
                p.remove_object(&object_id);
            }
            reply_err(thread_ctx, metric, ValkeyError::Str(err));
            return None;
        }
    };
    let job = crate::stream::StreamJob::for_nvme_get(
        fd,
        obj_len,
        crate::chunk_size(),
        object_id,
        crc32c_expected,
        batch_width,
        hdr_buf,
        source_pool,
    );
    let source = crate::stream::Source::NvmeRead {
        buffers,
        pool: source_pool,
    };
    // Build the target, run the driver, reply — TCP: collected bytes; EFA: [obj_len, crc32c].
    let drain_handles = match &target {
        GetTarget::Efa(_) => Some(Arc::new(crate::stream::DrainHandles::new(Vec::new()))),
        GetTarget::Tcp => None,
    };
    let outcome = match &target {
        GetTarget::Tcp => {
            let tgt = crate::stream::Target::tcp_reply(obj_len, crate::bench_mode());
            crate::stream::run_get(&job, chunk_iter, &source, &tgt, progress)
                .await
                .map(|target_err| (target_err, tgt.into_reply(obj_len)))
        }
        GetTarget::Efa(session) => {
            let tgt = crate::stream::Target::EfaWrite {
                session: session.clone(),
                drain_handles: drain_handles.clone().unwrap(),
            };
            crate::stream::run_get(&job, chunk_iter, &source, &tgt, progress)
                .await
                .map(|target_err| (target_err, efa_get_reply(obj_len, crc32c_expected)))
        }
    };
    match outcome {
        Ok((Some(e), _)) => crate::stream::reply_stream_err(thread_ctx, e), // promotion continue-filling
        Ok((None, reply)) => {
            thread_ctx.reply(Ok(reply));
        }
        Err(e) => crate::stream::reply_stream_err(thread_ctx, e),
    }
    drain_handles
}

// ═══════════════════════════════════════════════════════════════════════════════
// SET Engine
// ═══════════════════════════════════════════════════════════════════════════════

/// Execute BLOB.SET with mode + transport routing.
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
            EngineResult::Sync(cmd_set_dram_tcp(ctx, key_name, obj_len, data, object_id))
        }
        _ => {
            // Async — block client, dispatch to tokio.
            let blocked_client = ctx.block_client();
            let key_name_bytes = key_name.as_slice().to_vec();
            match mode {
                OperatingMode::Dram => {
                    cmd_set_dram_efa(
                        ctx,
                        key_name_bytes,
                        obj_len,
                        data_source,
                        blocked_client,
                        object_id,
                    );
                }
                OperatingMode::Tiered => {
                    cmd_set_tiered(
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
fn cmd_set_dram_tcp(
    ctx: &valkey_module::Context,
    key_name: &valkey_module::ValkeyString,
    obj_len: u64,
    data: &[u8],
    object_id: ObjectId,
) -> Result<ValkeyValue, ValkeyError> {
    let dram_pool = storage::get_dram_pool();
    let chunk_size = crate::chunk_size();
    let buffers = match dram_pool.alloc_exact_or_expand(ctx, obj_len) {
        Some(bufs) => bufs,
        None => {
            info::DRAM_POOL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
            return Err(ValkeyError::Str(errors::ERR_DRAM_POOL_EXHAUSTED));
        }
    };
    // DRAM-only SET allocates one buffer per chunk (no sliding window), so the
    // real buffer count is the batch width — the same derivation as the NVMe/EFA
    // paths, no special-case sentinel needed.
    let mut chunk_iter = ChunkIterator::new(obj_len, chunk_size, buffers.len(), None);
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
    // Insert ObjectContext BEFORE set_value so the key is never visible without
    // its ObjectContext — the same order as the EFA path. On set_value failure,
    // remove the entry; ObjectContext::Drop returns the buffers to DRAMPool.
    // (Sync path: single-threaded main thread, our object_id is always the
    // latest, so no version check is needed — a plain set_value ordered last.)
    let obj_ctx = Arc::new(ObjectContext::new_ready(buffers));
    dram_pool.insert_object(object_id, obj_ctx);
    let key = ctx.open_key_writable(key_name);
    let lo_value = LoValue {
        object_id,
        len: obj_len,
        crc32c: crc,
        file: None,
    };
    let event = if key.is_empty() {
        EVENT_CREATE
    } else {
        EVENT_UPDATE
    };
    if key.set_value(&LO_TYPE, lo_value).is_err() {
        dram_pool.remove_object(&object_id);
        info::SET_VALUE_FAILURES.fetch_add(1, Ordering::Relaxed);
        return Err(ValkeyError::Str(errors::ERR_SET_VALUE));
    }
    ctx.notify_keyspace_event(NotifyEvent::MODULE, event, key_name);
    VALKEY_OK
}

pub enum DataSource {
    /// TCP: data is inline bytes from the RESP command args.
    Tcp(Vec<u8>),
    /// EFA: data pulled from client GPU via session.read.
    Efa {
        session: Arc<Session>,
        /// The client's memory regions, in the order it listed them.
        addrs: Vec<storage::ClientEFAAddress>,
    },
}

// ─── DRAM-only EFA SET ───────────────────────────────────────────────────────

/// DRAM-only EFA SET: chunked alloc in DRAMPool, parallel EFA read + post-hoc CRC, create LoValue.
fn cmd_set_dram_efa(
    ctx: &valkey_module::Context,
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
    // DRAMPool::alloc_exact_or_expand: all-or-nothing with reactive expansion.
    let buffers = match dram_pool.alloc_exact_or_expand(ctx, obj_len) {
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
        DataSource::Efa { session, addrs } => {
            // EFA SET: parallel reads into all buffers, then sequential CRC pass.
            crate::runtime_handle().spawn(async move {
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                let dram_pool = storage::get_dram_pool();
                let obj_ctx = Arc::new(ObjectContext::new_ready(buffers));
                let chunk_iter =
                    ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), Some(addrs));
                // Dram EFA SET: EFA-read every chunk into the DRAM buffers via the
                // ONE streaming driver (source=EFA client, target=DRAM resident).
                let job = crate::stream::StreamJob::for_dram(
                    obj_len,
                    chunk_size,
                    0,
                    obj_ctx.buffers.len(),
                );
                let drain_handles = Arc::new(crate::stream::DrainHandles::new(Vec::new()));
                let source = crate::stream::Source::EfaRead {
                    session,
                    buffers: &obj_ctx.buffers,
                    pool: crate::stream::Pool::Dram(dram_pool),
                    drain_handles: drain_handles.clone(),
                };
                let target = crate::stream::Target::DramResident;
                let crc = match crate::stream::run_set(&job, chunk_iter, &source, &target, |ci| {
                    ci.combine_checksums()
                })
                .await
                {
                    Ok(crc) => crc,
                    Err(e) => {
                        // SET path: no RDMA writes into client memory, no warning needed.
                        crate::stream::reply_stream_err(&thread_ctx, e);
                        spawn_buffer_guard(drain_handles, obj_ctx);
                        return;
                    }
                };
                // Insert ObjectContext BEFORE set_value so the key is never visible
                // without its ObjectContext. On discard, remove the entry —
                // ObjectContext::Drop returns buffers to DRAMPool automatically.
                dram_pool.insert_object(object_id, obj_ctx);
                let lo_value = LoValue {
                    object_id,
                    len: obj_len,
                    crc32c: crc,
                    file: None,
                };
                match commit_lo_value(&thread_ctx, &key_name, object_id, lo_value) {
                    Ok(CommitOutcome::ValueSet) => {
                        thread_ctx.reply(VALKEY_OK);
                    }
                    Ok(CommitOutcome::StaleDiscarded) => {
                        // Stale write — a newer SET already completed.
                        info::SET_FINALIZE_STALE.fetch_add(1, Ordering::Relaxed);
                        dram_pool.remove_object(&object_id);
                        thread_ctx.reply(VALKEY_OK);
                    }
                    Err(e) => {
                        dram_pool.remove_object(&object_id);
                        reply_err(&thread_ctx, &info::SET_VALUE_FAILURES, e);
                    }
                }
            });
        }
    }
}

// ─── Tiered SET ──────────────────────────────────────────────────────────────

/// Tiered SET: streaming batch write to NVMe via NVMePool buffer window.
fn cmd_set_tiered(
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
    let chunk_size = crate::chunk_size();
    let batch_width = stream_ctx.buffers.len();
    match data_source {
        DataSource::Tcp(data) => {
            let chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_width, None);
            crate::runtime_handle().spawn(async move {
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                let set_info = SetObjectInfo {
                    object_id,
                    obj_len,
                    key_name,
                };
                cmd_set_tiered_run(
                    set_info,
                    stream_ctx,
                    chunk_iter,
                    thread_ctx,
                    SetSource::Tcp(data),
                )
                .await;
            });
        }
        DataSource::Efa { session, addrs } => {
            let chunk_iter = ChunkIterator::new(obj_len, chunk_size, batch_width, Some(addrs));
            crate::runtime_handle().spawn(async move {
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                let set_info = SetObjectInfo {
                    object_id,
                    obj_len,
                    key_name,
                };
                cmd_set_tiered_run(
                    set_info,
                    stream_ctx,
                    chunk_iter,
                    thread_ctx,
                    SetSource::Efa(session),
                )
                .await;
            });
        }
    }
}

/// The per-transport variant of a Tiered NVMe SET — the ONLY thing that differs
/// between the TCP and EFA write paths. `cmd_set_tiered_run` matches this once to build the
/// right source + object-CRC rule; everything else in the envelope is shared.
enum SetSource {
    /// TCP: inline payload memcpy'd into the buffers; CRC is over the whole payload.
    Tcp(Vec<u8>),
    /// EFA: each chunk fi_read from the client; CRC combines per-chunk transport CRCs.
    Efa(Arc<Session>),
}

/// Envelope shared by both Tiered NVMe-write SET paths (TCP + EFA). Reserve disk →
/// open write fd → ObjectFile (owns cleanup) → run(source → NvmeTarget) → finalize.
/// `variant` is the ONLY per-transport difference (source construction + CRC rule);
/// it is matched once here. On any error the fd + ObjectFile drop on return,
/// unlinking the file and releasing the disk budget.
async fn cmd_set_tiered_run(
    set_info: SetObjectInfo,
    stream_ctx: storage::StreamingContext,
    chunk_iter: ChunkIterator,
    thread_ctx: valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    variant: SetSource,
) {
    let SetObjectInfo {
        object_id,
        obj_len,
        key_name,
    } = set_info;
    let chunk_size = crate::chunk_size();
    let batch_width = stream_ctx.buffers.len();
    let nvme_pool = storage::get_nvme_pool();
    let mut chunk_iter = chunk_iter;
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
    let file_path = object_id.file_path(&crate::nvme_dir());
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
    // Header write reuses a data buffer — safe because it runs after drive_window
    // completes, unlike the GET path which needs a dedicated header buffer.
    let job = crate::stream::StreamJob::for_nvme_set(
        fd.as_raw_fd(),
        obj_len,
        chunk_size,
        object_id,
        batch_width,
        &stream_ctx.buffers[0],
        nvme_pool,
    );
    let target = crate::stream::Target::NvmeWrite {
        buffers: &stream_ctx.buffers,
        pool: nvme_pool,
    };
    // The one per-transport branch: build the source + choose the object-CRC rule.
    let drain_handles = Arc::new(crate::stream::DrainHandles::new(Vec::new()));
    let result = match &variant {
        SetSource::Tcp(data) => {
            let source = crate::stream::Source::TcpInline {
                data,
                buffers: &stream_ctx.buffers,
                pool: crate::stream::Pool::Nvme(nvme_pool),
            };
            crate::stream::run_set(&job, chunk_iter, &source, &target, |_| {
                crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, data) as u32
            })
            .await
        }
        SetSource::Efa(session) => {
            let source = crate::stream::Source::EfaRead {
                session: session.clone(),
                buffers: &stream_ctx.buffers,
                pool: crate::stream::Pool::Nvme(nvme_pool),
                drain_handles: drain_handles.clone(),
            };
            crate::stream::run_set(&job, chunk_iter, &source, &target, |ci| {
                ci.combine_checksums()
            })
            .await
        }
    };
    let crc = match result {
        Ok(crc) => crc,
        Err(e) => {
            // SET path: no RDMA writes into client memory, no warning needed.
            crate::stream::reply_stream_err(&thread_ctx, e);
            // Keep NVMe buffers alive until any background EFA drains complete.
            spawn_buffer_guard(drain_handles, stream_ctx);
            return;
        }
    };
    test_pause_before_finalize().await;
    // NVMe disk accounting stays with the NVMe caller (no file on the DRAM path).
    let object_id = object_file.object_id();
    let disk_len = object_file.disk_len();
    let file_path = object_id.file_path(&crate::nvme_dir());
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
    match commit_lo_value(&thread_ctx, &key_name, object_id, lo_value) {
        Ok(CommitOutcome::ValueSet) => {
            thread_ctx.reply(VALKEY_OK);
        }
        Ok(CommitOutcome::StaleDiscarded) => {
            // lo_value dropped in commit_lo_value → ObjectFile drop unlinks + frees budget.
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
fn cmd_get_from_dram(
    dram_pool: &storage::DRAMPool,
    obj_ctx: &Arc<ObjectContext>,
    obj_len: u64,
    crc32c: Crc,
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
        Transport::Efa { session, addrs } => {
            // EFA: write every DRAM buffer to the client via the ONE streaming
            // driver (source=DRAM resident, target=EFA client).
            let obj_ctx = obj_ctx.clone();
            crate::runtime_handle().spawn(async move {
                let _keep_alive = (&obj_ctx, file);
                let dram_pool = storage::get_dram_pool();
                let chunk_size = crate::chunk_size();
                let chunk_iter =
                    ChunkIterator::new(obj_len, chunk_size, obj_ctx.buffers.len(), Some(addrs));
                let job = crate::stream::StreamJob::for_dram(
                    obj_len,
                    chunk_size,
                    crc32c,
                    obj_ctx.buffers.len(),
                );
                let source = crate::stream::Source::DramResident {
                    buffers: &obj_ctx.buffers,
                    pool: crate::stream::Pool::Dram(dram_pool),
                };
                let drain_handles = Arc::new(crate::stream::DrainHandles::new(Vec::new()));
                let target = crate::stream::Target::EfaWrite {
                    session,
                    drain_handles: drain_handles.clone(),
                };
                match crate::stream::run_get(&job, chunk_iter, &source, &target, None).await {
                    // [obj_len, crc32c] on clean success; a Dram GET has no progress
                    // hook, so a target error already surfaced as Err below.
                    Ok(_) => {
                        thread_ctx.reply(Ok(efa_get_reply(obj_len, crc32c)));
                    }
                    Err(e) => {
                        crate::stream::reply_stream_err(&thread_ctx, e);
                        // Keep DRAM buffers alive until any background EFA drains complete.
                        spawn_buffer_guard(drain_handles, obj_ctx.clone());
                    }
                }
            });
        }
    }
}
