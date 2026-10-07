//! Unified streaming I/O driver — ONE driver for every GET/SET path.
//!
//! Every streaming operation moves an object's chunks from a **source** to a
//! **target**, one bounded window of chunks at a time:
//!
//! ```text
//!            ┌────────┐   per-chunk ready stream    ┌────────┐
//!   bytes ──▶│ Source │ ─────────────────────────▶ │ Target │──▶ bytes
//!            └────────┘   (produce as each ready)   └────────┘   (consume as each arrives)
//! ```
//!
//! The eight paths are just source/target pairs. `run_get` and `run_set` are thin
//! wrappers over one shared window loop (`drive_window`); each carries only its own
//! verb's work (GET verifies the on-disk header and may run a promotion progress
//! hook; SET computes the object CRC — TCP over the whole payload, EFA by combining
//! per-chunk transport CRCs — and writes the header), so neither signature carries
//! fields inert to the other.
//!
//! | Op  | Mode   | Transport | Source                 | Target                |
//! |-----|--------|-----------|------------------------|-----------------------|
//! | GET | Tiered | TCP       | `Source::NvmeRead`     | `Target::TcpReply`    |
//! | GET | Tiered | EFA       | `Source::NvmeRead`     | `Target::EfaWrite`    |
//! | GET | Dram   | TCP       | `Source::DramResident` | `Target::TcpReply`    |
//! | GET | Dram   | EFA       | `Source::DramResident` | `Target::EfaWrite`    |
//! | SET | Tiered | TCP       | `Source::TcpInline`    | `Target::NvmeWrite`   |
//! | SET | Tiered | EFA       | `Source::EfaRead`      | `Target::NvmeWrite`   |
//! | SET | Dram   | TCP       | `Source::TcpInline`    | `Target::DramResident`|
//! | SET | Dram   | EFA       | `Source::EfaRead`      | `Target::DramResident`|
//!
//! ## The seam is a per-chunk ready stream, so interleave falls out for free
//!
//! `drive_window` does NOT "produce the whole batch, then consume the whole batch".
//! It fans out each chunk's `Source::produce` future into a `FuturesUnordered`
//! and, **as each one resolves**, immediately hands that chunk to
//! `Target::consume`. So the target acts on chunk *i* the instant its source
//! read lands — the Tiered-EFA SET network↔disk overlap that used to be a
//! hand-written special case is now the generic behaviour of every path.
//!
//! DRAM is the degenerate case: `Source::DramResident::produce` resolves
//! immediately (bytes already resident) and `Target::DramResident::consume` is a
//! no-op (bytes land in the pool buffer directly). No spawn, no wait — the same
//! loop, trivially fast.
//!
//! ## Window = backpressure = buffer-reuse safety
//!
//! The driver reuses a fixed window of `batch_width` pool buffers across batches.
//! A batch is fully drained (every produce AND its consume complete) before the
//! next batch reuses those buffers, so a buffer is never overwritten while an
//! in-flight WriteFixed / fi_write still references it. That drain-per-batch IS
//! the bound; there is no separate channel to size.
//!
//! ## Cleanup model
//!
//! The write targets take a raw `fd`; the CALLER owns the `Arc<ObjectFile>` (and
//! its `OwnedFd`), whose Drop unlinks the file and releases the NVMe disk budget,
//! and keeps that owner alive for the whole transfer. The driver does NO teardown
//! — on any error it returns it and the caller's early `return` drops the
//! ObjectFile (closing the fd, unlinking, releasing budget). On success the target
//! yields the object CRC to commit.

// The pool/transport seams are internal traits used only within this crate; the
// raw-pointer buffer accessors and async trait methods are idiomatic here.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(async_fn_in_trait)]

use std::os::unix::io::RawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use valkey_module::{ValkeyError, ValkeyValue};

use crate::data_type::ObjectId;
use crate::storage::{
    self, uring, ChunkIterator, ChunkRef, ClientEFAAddress, Crc, ObjectContext, SegmentBuffer,
};
use crate::transport::Session;

// `Crc` (a CRC32C checksum value) is defined in `storage` and imported below —
// `produce` returns `Option<Crc>` so the value reads as "maybe a checksum".

// ═══════════════════════════════════════════════════════════════════════════
//  Source: where each chunk's bytes come from (a closed set of 4 behaviours)
// ═══════════════════════════════════════════════════════════════════════════

/// The four ways a chunk's bytes become present in its window buffer. `produce`
/// resolves when the chunk is ready; the driver fires a batch of them and consumes
/// each as it lands, so a slow source (NVMe/EFA read) interleaves with the target
/// while a resident source (DRAM) resolves instantly. `pool` backs the buffers
/// (Nvme streaming vs Dram promotion for NvmeRead; Nvme vs Dram SET for the rest).
pub enum Source<'a> {
    /// io_uring ReadFixed from the NVMe file (GET). Same read whether the buffers
    /// are the NVMe streaming window or the DRAM promotion buffers.
    NvmeRead {
        buffers: &'a [SegmentBuffer],
        pool: Pool,
    },
    /// Bytes already resident in the DRAM object buffers (GET) — nothing to fetch.
    DramResident {
        buffers: &'a [SegmentBuffer],
        pool: Pool,
    },
    /// fi_read each chunk from the client into its buffer (SET).
    EfaRead {
        buffers: &'a [SegmentBuffer],
        pool: Pool,
        session: Arc<Session>,
        drain_handles: Arc<DrainHandles>,
    },
    /// memcpy inline command bytes into the buffer (SET); resolves immediately.
    TcpInline {
        buffers: &'a [SegmentBuffer],
        pool: Pool,
        data: &'a [u8],
    },
}

impl Source<'_> {
    /// The window buffers and their backing pool — shared by every variant.
    fn parts(&self) -> (&[SegmentBuffer], Pool) {
        match self {
            Source::NvmeRead { buffers, pool }
            | Source::DramResident { buffers, pool }
            | Source::EfaRead { buffers, pool, .. }
            | Source::TcpInline { buffers, pool, .. } => (buffers, *pool),
        }
    }

    fn buffer_ptr(&self, i: usize) -> *mut u8 {
        let (buffers, pool) = self.parts();
        pool.ptr(&buffers[i])
    }

    /// Make `chunk`'s bytes present in its buffer. `Ok(Some(crc))` carries the
    /// transport CRC for an EFA read (SET); the other sources return `Ok(None)`.
    ///
    /// "produce" is the stream's term, not the client data direction: GET produces
    /// from NVMe or DRAM (DRAM is a no-op — bytes already resident); SET produces
    /// from EFA (fi_read from the client) or TCP (memcpy the inline payload).
    async fn produce(
        &self,
        fd: Option<RawFd>,
        chunk: &ChunkRef,
        chunk_size: usize,
    ) -> Result<Option<Crc>, StreamError> {
        let (buffers, pool) = self.parts();
        let buf = &buffers[chunk.buffer_idx];
        match self {
            Source::NvmeRead { .. } => {
                let rx = uring::submit_read(
                    pool.pool_id(),
                    fd.expect("NvmeRead requires an fd"),
                    uring::UringOp {
                        iovec_index: pool.iovec(buf),
                        buf_ptr: pool.ptr(buf),
                        file_offset: storage::FILE_HEADER_SIZE
                            + chunk.index as u64 * chunk_size as u64,
                        len: chunk.user_data_len as u64,
                        use_fixed: pool.is_io_uring_registered(buf),
                    },
                );
                match rx.await {
                    Ok(Ok(_)) => Ok(None),
                    _ => Err(StreamError::NvmeRead),
                }
            }
            Source::DramResident { .. } => Ok(None), // resident
            Source::EfaRead {
                session,
                drain_handles,
                ..
            } => {
                let addrs = chunk.addrs.as_ref().expect("EFA chunk missing addrs");
                let crc = efa_transfer_addrs(
                    session,
                    pool.ptr(buf) as usize,
                    addrs,
                    EfaDirection::Read,
                    drain_handles,
                )
                .await
                .map_err(|e| match e {
                            ValkeyError::Str(s) if s == crate::errors::ERR_EFA_TIMEOUT => {
                                StreamError::EfaTimeout
                            }
                            _ => StreamError::EfaRead,
                        })?;
                Ok(Some(crc))
            }
            Source::TcpInline { data, .. } => {
                let src_offset = chunk.index as usize * chunk_size;
                let src = &data[src_offset..src_offset + chunk.user_data_len];
                // SAFETY: dst is this chunk's pool buffer (>= chunk.user_data_len); src in-bounds.
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), pool.ptr(buf), chunk.user_data_len)
                };
                Ok(None)
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Target: what happens to each chunk once its bytes are ready (closed set of 4)
// ═══════════════════════════════════════════════════════════════════════════

/// The four ways a ready chunk is consumed. `consume` returns a future so a slow
/// target (NVMe/EFA write) interleaves with the next chunk's source; a resident
/// target (DRAM) is a no-op.
///
/// Mirror of `produce`: GET consumes to the client — accumulate into the reply
/// (TCP) or fi_write to the client (EFA); SET consumes to storage — WriteFixed to
/// NVMe (Tiered) or a no-op (DRAM, bytes already landed in the pool buffer).
pub enum Target<'a> {
    /// Accumulate chunk bytes into the reply buffer (GET TCP); bench = size only.
    TcpReply {
        reply: std::sync::Mutex<Vec<u8>>,
        bench: bool,
    },
    /// fi_write each ready chunk to the client (GET EFA).
    EfaWrite {
        session: Arc<Session>,
        drain_handles: Arc<DrainHandles>,
    },
    /// io_uring WriteFixed each ready chunk to the NVMe file (SET tiered).
    NvmeWrite {
        buffers: &'a [SegmentBuffer],
        pool: &'static storage::NVMePool,
    },
    /// Bytes already landed in the DRAM buffer via the source (SET dram) — no-op.
    DramResident,
}

impl Target<'_> {
    /// GET TCP reply constructor.
    pub fn tcp_reply(obj_len: u64, bench: bool) -> Self {
        Target::TcpReply {
            reply: std::sync::Mutex::new(if bench {
                Vec::new()
            } else {
                Vec::with_capacity(obj_len as usize)
            }),
            bench,
        }
    }

    /// GET reply after the loop: collected bytes, or obj_len in bench mode.
    /// Only valid on `TcpReply`; the EFA GET reply is [obj_len, crc32c] built by the caller.
    pub fn into_reply(self, obj_len: u64) -> ValkeyValue {
        match self {
            Target::TcpReply { reply, bench } => {
                if bench {
                    ValkeyValue::Integer(obj_len as i64)
                } else {
                    ValkeyValue::StringBuffer(reply.into_inner().expect("reply lock poisoned"))
                }
            }
            _ => unreachable!("into_reply is only called on the TcpReply GET target"),
        }
    }

    async fn consume(
        &self,
        fd: Option<RawFd>,
        chunk: &ChunkRef,
        buf_ptr: usize,
        chunk_size: usize,
    ) -> Result<(), StreamError> {
        match self {
            Target::TcpReply { reply, bench } => {
                if !*bench {
                    // SAFETY: buffer holds `chunk.user_data_len` bytes the source just produced.
                    let slice = unsafe {
                        std::slice::from_raw_parts(buf_ptr as *mut u8, chunk.user_data_len)
                    };
                    reply
                        .lock()
                        .expect("reply lock poisoned")
                        .extend_from_slice(slice);
                }
                Ok(())
            }
            Target::EfaWrite {
                session,
                drain_handles,
            } => {
                let addrs = chunk.addrs.as_ref().expect("EFA chunk missing addrs");
                efa_transfer_addrs(session, buf_ptr, addrs, EfaDirection::Write, drain_handles)
                    .await
                    .map(|_| ())
                    .map_err(|e| match e {
                        ValkeyError::Str(s) if s == crate::errors::ERR_EFA_TIMEOUT => {
                            StreamError::EfaTimeout
                        }
                        _ => StreamError::EfaWrite,
                    })
            }
            Target::NvmeWrite { buffers, pool } => {
                let buf = &buffers[chunk.buffer_idx];
                let rx = uring::submit_write(
                    uring::PoolType::Nvme,
                    fd.expect("NvmeWrite requires an fd"),
                    uring::UringOp {
                        iovec_index: pool.iovec_index_for_buf(buf),
                        buf_ptr: pool.buffer_ptr(buf),
                        file_offset: storage::FILE_HEADER_SIZE
                            + chunk.index as u64 * chunk_size as u64,
                        len: chunk.user_data_len as u64,
                        use_fixed: pool.is_buf_io_uring_registered(buf),
                    },
                );
                match rx.await {
                    Ok(Ok(())) => Ok(()),
                    _ => Err(StreamError::NvmeWrite),
                }
            }
            Target::DramResident => Ok(()),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Pool selector: which pool backs a source's buffers
// ═══════════════════════════════════════════════════════════════════════════

/// `NvmeSource` and `EfaSource` each serve buffers backed by EITHER pool (NVMe
/// streaming vs DRAM promotion for NvmeSource; Tiered vs Dram SET for EfaSource).
/// Two known variants, resolved statically — no trait, no vtable, no generic on
/// the driver. The two methods forward to the accessors both pools already expose.
#[derive(Clone, Copy)]
pub enum Pool {
    Nvme(&'static storage::NVMePool),
    Dram(&'static storage::DRAMPool),
}
impl Pool {
    pub(crate) fn ptr(&self, b: &SegmentBuffer) -> *mut u8 {
        match self {
            Pool::Nvme(p) => p.buffer_ptr(b),
            Pool::Dram(p) => p.buffer_ptr(b),
        }
    }
    pub(crate) fn iovec(&self, b: &SegmentBuffer) -> u16 {
        match self {
            Pool::Nvme(p) => p.iovec_index_for_buf(b),
            Pool::Dram(p) => p.iovec_index_for_buf(b),
        }
    }
    /// Whether the buffer's segment is registered in the kernel io_uring buffer
    /// table — drives the per-op fixed vs non-fixed path. Startup segments are
    /// registered; a segment added by expand() is not until the pool's poller
    /// runs the whole-table unregister + re-register.
    pub(crate) fn is_io_uring_registered(&self, b: &SegmentBuffer) -> bool {
        match self {
            Pool::Nvme(p) => p.is_buf_io_uring_registered(b),
            Pool::Dram(p) => p.is_buf_io_uring_registered(b),
        }
    }
    /// Which pool's io_uring ring backs these buffers — routes each op to the
    /// engine that owns its buffer's registered table.
    pub(crate) fn pool_id(&self) -> uring::PoolType {
        match self {
            Pool::Nvme(_) => uring::PoolType::Nvme,
            Pool::Dram(_) => uring::PoolType::Dram,
        }
    }
    /// Return a single buffer to its owning pool.
    pub(crate) fn free_buf(&self, b: &SegmentBuffer) {
        match self {
            Pool::Nvme(p) => p.free(b),
            Pool::Dram(p) => p.free(b),
        }
    }
    /// Allocate a dedicated buffer for the on-disk FileHeader read (GET path).
    /// NVMe: alloc_window sized to FILE_HEADER_SIZE.
    /// DRAM (promotion): alloc_exact_or_expand with Context::dummy() to get used_memory.
    pub(crate) fn alloc_for_file_header(&self) -> Option<SegmentBuffer> {
        let size = storage::FILE_HEADER_SIZE as usize;
        let mut bufs = match self {
            Pool::Nvme(p) => p.alloc_window(size, 1, 1)?,
            Pool::Dram(p) => {
                let dummy = valkey_module::Context::dummy();
                p.alloc_exact_or_expand(&dummy, storage::FILE_HEADER_SIZE)?
            }
        };
        Some(bufs.remove(0))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
//  Errors + progress policy
// ═══════════════════════════════════════════════════════════════════════════

/// Which stage failed — the caller maps this to its reply string + metric.
#[derive(Clone, Copy)]
pub enum StreamError {
    NvmeRead,
    NvmeWrite,
    EfaRead,
    EfaWrite,
    EfaTimeout,
}

/// Map a `StreamError` to its metric + reply string and send the error.
/// One place, so every path that drives `run_get` / `run_set` reports failures
/// identically.
pub(crate) fn reply_stream_err(
    thread_ctx: &valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
    e: StreamError,
) {
    use StreamError::*;
    let (metric, err): (&std::sync::atomic::AtomicU64, &str) = match e {
        NvmeRead => (&crate::info::NVME_READ_ERRORS, crate::errors::ERR_NVME_READ),
        NvmeWrite => (
            &crate::info::NVME_WRITE_ERRORS,
            crate::errors::ERR_NVME_WRITE,
        ),
        EfaRead => (&crate::info::EFA_READ_ERRORS, crate::errors::ERR_EFA_READ),
        EfaWrite => (&crate::info::EFA_WRITE_ERRORS, crate::errors::ERR_EFA_WRITE),
        EfaTimeout => (
            &crate::info::EFA_TIMEOUT_ERRORS,
            crate::errors::ERR_EFA_TIMEOUT,
        ),
    };
    crate::engine::reply_err(thread_ctx, metric, ValkeyError::Str(err));
}

/// DRAM-promotion side effects run around the batch loop (GET only). When present,
/// the driver advances chunks_ready / marks Ready and, on a source (NVMe) error,
/// evicts the half-filled DRAM entry; on a target (EFA) error it keeps reading so
/// the DRAM copy still fills for coalesced waiters, replying the error at the end.
pub struct PromotionProgress<'a> {
    pub obj_ctx: &'a Arc<ObjectContext>,
    pub dram_pool: &'static storage::DRAMPool,
    pub object_id: ObjectId,
}

// ═══════════════════════════════════════════════════════════════════════════
//  The driver job + the ONE run() loop
// ═══════════════════════════════════════════════════════════════════════════

/// The FileHeader a SET-to-NVMe path writes after its batch loop. Absent for GET
/// (which reads+verifies the on-disk header via `verify_file_header`) and for Dram
/// SET (no file). Carries exactly what `write_file_header` needs.
pub struct FileHeaderWrite<'a> {
    pub hdr_buf: &'a SegmentBuffer,
    pub pool: &'static storage::NVMePool,
}

/// Where a GET reads the on-disk FileHeader into, before the data reads.
/// `Some` on a GET (verify the header); `None` on SET and DRAM (no header read).
pub struct FileHeaderRead {
    /// Which pool's ring owns the header buffer (DRAM promotion buffer vs NVMe
    /// streaming buffer) — routes the header read to the correct engine and
    /// resolves iovec/ptr/use_fixed at read time via the global pool accessor.
    pub pool_id: uring::PoolType,
    /// Dedicated header buffer (owned, separate from the data window), freed
    /// after validation via pool_id → global pool accessor. Allocated by
    /// `cmd_get_tiered_run` so the header read runs in parallel with data reads.
    pub dedicated_buffer: SegmentBuffer,
}

/// Everything the loop needs that isn't the source/target themselves.
pub struct StreamJob<'a> {
    /// NVMe file fd — `Some` on NVMe GET/SET, `None` on DRAM (no file). Only the
    /// `NvmeRead`/`NvmeWrite` arms read it; other arms never touch it.
    pub fd: Option<RawFd>,
    pub obj_len: u64,
    pub chunk_size: usize,
    /// NVMe object id — `Some` when a file header is read/written, `None` on DRAM.
    pub object_id: Option<ObjectId>,
    /// GET verifies this before the loop; SET-to-NVMe writes it in the header.
    pub crc32c_expected: Crc,
    /// GET header-read placement — `Some` means "read+verify the on-disk FileHeader
    /// before the data reads"; `None` on SET and DRAM (no header read).
    pub verify_file_header: Option<FileHeaderRead>,
    /// Window width — SQEs/transfers in flight per batch, and the buffer-reuse bound.
    pub batch_width: usize,
    /// `Some` for SET-to-NVMe: the FileHeader to persist after the batch loop.
    /// `None` for GET and for DRAM-only SET (no file to write a header to).
    pub persist_file_header: Option<FileHeaderWrite<'a>>,
}

impl<'a> StreamJob<'a> {
    /// A StreamJob for the DRAM path (GET serve from resident buffers, or DRAM
    /// SET): no file, so no fd and no file-header work — `verify_file_header` and
    /// `persist_file_header` are both `None`, and `object_id` (read only under
    /// those) is always `None` too. `crc32c_expected` is the GET's expected CRC,
    /// or 0 on SET.
    pub fn for_dram(
        obj_len: u64,
        chunk_size: usize,
        crc32c_expected: Crc,
        batch_width: usize,
    ) -> Self {
        StreamJob {
            fd: None,
            obj_len,
            chunk_size,
            object_id: None,
            crc32c_expected,
            verify_file_header: None,
            batch_width,
            persist_file_header: None,
        }
    }

    /// A StreamJob for the NVMe GET path: reads+verifies the on-disk FileHeader
    /// before the data reads. Uses a dedicated header buffer (owned, separate from
    /// the data window) so the header read can run in parallel with data reads.
    /// `persist_file_header` is `None` (GET writes no header).
    #[allow(clippy::too_many_arguments)]
    pub fn for_nvme_get(
        fd: RawFd,
        obj_len: u64,
        chunk_size: usize,
        object_id: ObjectId,
        crc32c_expected: Crc,
        batch_width: usize,
        hdr_buf: SegmentBuffer,
        pool: Pool,
    ) -> Self {
        StreamJob {
            fd: Some(fd),
            obj_len,
            chunk_size,
            object_id: Some(object_id),
            crc32c_expected,
            verify_file_header: Some(FileHeaderRead {
                pool_id: pool.pool_id(),
                dedicated_buffer: hdr_buf,
            }),
            batch_width,
            persist_file_header: None,
        }
    }

    /// A StreamJob for the NVMe SET path: persists the FileHeader after the batch
    /// loop. Builds the `FileHeaderWrite` from the header buffer and its NVMe pool.
    /// `verify_file_header` is `None` and `crc32c_expected` is 0 (SET reads no
    /// header; the object CRC is computed and written after the loop).
    /// Borrows `buffers[0]` for the header write — safe because `run_set` writes
    /// the header only after `drive_window` completes (no concurrent data I/O).
    pub fn for_nvme_set(
        fd: RawFd,
        obj_len: u64,
        chunk_size: usize,
        object_id: ObjectId,
        batch_width: usize,
        hdr_buf: &'a SegmentBuffer,
        pool: &'static storage::NVMePool,
    ) -> Self {
        StreamJob {
            fd: Some(fd),
            obj_len,
            chunk_size,
            object_id: Some(object_id),
            crc32c_expected: 0,
            verify_file_header: None,
            batch_width,
            persist_file_header: Some(FileHeaderWrite { hdr_buf, pool }),
        }
    }
}

/// The single streaming driver. Moves every chunk from `source` to `target`,
/// interleaving per chunk within a window, preserving all of main's semantics.
///
/// The shared window loop for BOTH verbs: snapshot each batch, fan out
/// `source.produce`, and as each chunk lands feed `target.consume` (the
/// interleave), latching the first source and first target error. This is the
/// only code GET and SET share, so it lives here once; `run_get` / `run_set` wrap
/// it with their own pre/post work.
///
/// `progress` present ⇒ DRAM promotion (a GET): a target (EFA-write) error is
/// latched but the loop keeps filling DRAM for coalesced waiters, and each batch
/// advances `chunks_ready` / marks Ready. Absent ⇒ a target error aborts at once.
/// A source error always aborts (evicting the half-filled DRAM entry under `progress`).
///
/// Returns the (advanced) iterator — so the caller can compute the object CRC from
/// its recorded per-chunk checksums — and the latched target error, if any.
async fn drive_window(
    job: &StreamJob<'_>,
    mut chunk_iter: ChunkIterator,
    source: &Source<'_>,
    target: &Target<'_>,
    progress: Option<&PromotionProgress<'_>>,
) -> Result<(ChunkIterator, Option<StreamError>), StreamError> {
    let total_chunks = chunk_iter.total_chunks();
    let window = job.batch_width;
    let mut chunks_done: u32 = 0;
    let mut target_err: Option<StreamError> = None;

    while chunks_done < total_chunks {
        let batch_count = window.min((total_chunks - chunks_done) as usize);

        // Snapshot this batch's chunks (advances the iterator by batch_count).
        // next_chunk() lazily computes the per-chunk EFA addrs; clone each out of
        // the borrowed iterator so it can move into a per-chunk future below.
        let mut batch: Vec<ChunkRef> = Vec::with_capacity(batch_count);
        for _ in 0..batch_count {
            let c = chunk_iter
                .next_chunk()
                .expect("ChunkIterator out of bounds");
            batch.push(c.clone());
        }

        // Per-chunk ready stream: fan out every source.produce, and as each
        // resolves, immediately run target.consume — that IS the interleave.
        // Each chunk flows produce → (record CRC) → consume, moving by value.
        let mut produce = FuturesUnordered::new();
        for chunk in batch.into_iter() {
            produce.push(async move {
                let r = source.produce(job.fd, &chunk, job.chunk_size).await;
                (chunk, r)
            });
        }
        let mut consume = FuturesUnordered::new();
        let mut source_err: Option<StreamError> = None;

        while let Some((chunk, produced)) = produce.next().await {
            match produced {
                Ok(produced_crc) => {
                    // EFA-read SET yields a per-chunk transport CRC; record it on the
                    // driver-owned iterator so run_set can combine_checksums. Other
                    // sources yield None and record nothing.
                    if let Some(crc) = produced_crc {
                        chunk_iter.record_checksum(chunk.index, crc);
                    }
                    // Once any error is latched, stop feeding the target but keep
                    // producing so a promotion's DRAM copy still fills for waiters.
                    if target_err.is_none() && source_err.is_none() {
                        let buf_ptr = source.buffer_ptr(chunk.buffer_idx) as usize;
                        consume.push(async move {
                            target
                                .consume(job.fd, &chunk, buf_ptr, job.chunk_size)
                                .await
                        });
                    }
                }
                Err(e) if source_err.is_none() => source_err = Some(e),
                Err(_) => {}
            }
        }
        // Drain the consume side; first target error wins (latched, not aborted-in-place).
        while let Some(res) = consume.next().await {
            if let Err(e) = res {
                if target_err.is_none() {
                    target_err = Some(e);
                }
            }
        }

        // A source error is fatal to the whole op (bad disk read / lost client read).
        if let Some(e) = source_err {
            if let Some(p) = progress {
                p.dram_pool.remove_object(&p.object_id); // evict half-filled DRAM entry
            }
            return Err(e);
        }

        // A target (client-transfer) error: without a progress hook (streaming GET
        // or any SET) abort now; a DRAM promotion latches it and keeps filling for
        // coalesced waiters, surfacing the error to the caller after the loop.
        if let Some(e) = target_err {
            if progress.is_none() {
                return Err(e);
            }
        }

        if let Some(p) = progress {
            p.obj_ctx.advance_chunks_ready(batch_count as u32);
            // TODO: obj_ctx.notify_progress() for coalesced waiters.
        }
        chunks_done += batch_count as u32;
    }

    if let Some(p) = progress {
        p.obj_ctx.mark_ready();
    }
    Ok((chunk_iter, target_err))
}

/// GET driver: verify the on-disk FileHeader, then move every chunk source →
/// target through the shared window. No header write, no committed CRC — the
/// caller builds the reply from the target. Returns the latched target error
/// (`Some` only for a DRAM promotion whose client transfer failed mid-fill).
pub async fn run_get(
    job: &StreamJob<'_>,
    chunk_iter: ChunkIterator,
    source: &Source<'_>,
    target: &Target<'_>,
    progress: Option<&PromotionProgress<'_>>,
) -> Result<Option<StreamError>, StreamError> {
    // GET pairs a GET source with a GET target; callers guarantee this.
    assert!(
        matches!(
            source,
            Source::NvmeRead { .. } | Source::DramResident { .. }
        ) && matches!(target, Target::TcpReply { .. } | Target::EfaWrite { .. }),
        "run_get called with a non-GET source/target pairing"
    );
    if let Some(fh) = job.verify_file_header.as_ref() {
        // Parallel path: dedicated header buffer (separate from the data window),
        // so the header read and data reads can land on the ring together.
        let header_fut = async {
            let pool = match fh.pool_id {
                uring::PoolType::Nvme => Pool::Nvme(storage::get_nvme_pool()),
                uring::PoolType::Dram => Pool::Dram(storage::get_dram_pool()),
            };
            storage::read_and_verify_file_header(
                job.fd.expect("GET header read requires an fd"),
                fh.pool_id,
                pool.iovec(&fh.dedicated_buffer),
                pool.ptr(&fh.dedicated_buffer) as usize,
                pool.is_io_uring_registered(&fh.dedicated_buffer),
                job.object_id
                    .expect("GET header read requires an object_id"),
                job.obj_len,
                job.crc32c_expected,
            )
            .await;
            // Free the dedicated header buffer now that validation is done.
            pool.free_buf(&fh.dedicated_buffer);
        };
        let ((), window_result) = futures::join!(
            header_fut,
            drive_window(job, chunk_iter, source, target, progress)
        );
        let (_chunk_iter, target_err) = window_result?;
        return Ok(target_err);
    }
    let (_chunk_iter, target_err) = drive_window(job, chunk_iter, source, target, progress).await?;
    Ok(target_err)
}

/// SET driver: move every chunk source → target through the shared window, then
/// compute the object CRC (`object_crc`) and, for an NVMe SET, persist the
/// FileHeader. No header verify, no progress, no target-error latching (a SET has
/// no progress hook, so any target error already aborted in `drive_window`).
/// Returns the committed object CRC.
///
/// `object_crc`: SET-TCP checksums the whole payload post-hoc (one pass over the
/// full `data`, ignoring the iterator); SET-EFA combines the per-chunk transport
/// CRCs. TCP's closure takes `|_|` because it has the whole payload in `data`; only
/// EFA needs the iterator's per-chunk CRCs.
pub async fn run_set(
    job: &StreamJob<'_>,
    chunk_iter: ChunkIterator,
    source: &Source<'_>,
    target: &Target<'_>,
    object_crc: impl FnOnce(&ChunkIterator) -> Crc,
) -> Result<u32, StreamError> {
    // SET pairs a SET source with a SET target; callers guarantee this.
    assert!(
        matches!(source, Source::EfaRead { .. } | Source::TcpInline { .. })
            && matches!(target, Target::NvmeWrite { .. } | Target::DramResident),
        "run_set called with a non-SET source/target pairing"
    );
    let (chunk_iter, _target_err) = drive_window(job, chunk_iter, source, target, None).await?;
    let crc = object_crc(&chunk_iter);
    if let Some(hw) = job.persist_file_header.as_ref() {
        // SET-to-NVMe: persist the FileHeader. A failure is an NVMe write error;
        // the caller's ObjectFile Drop unlinks the file + releases the budget.
        if storage::write_file_header(
            job.fd.expect("SET header write requires an fd"),
            job.object_id
                .expect("SET header write requires an object_id"),
            job.obj_len,
            crc,
            hw.hdr_buf,
            hw.pool,
        )
        .await
        .is_err()
        {
            return Err(StreamError::NvmeWrite);
        }
    }
    Ok(crc)
}

// ═══════════════════════════════════════════════════════════════════════════
//  EFA transport primitive (moved here from engine.rs — its only callers are the
//  EfaSource / EfaTarget above, so it lives with them)
// ═══════════════════════════════════════════════════════════════════════════

/// Direction for EFA multi-address transfers.
#[derive(Clone, Copy, PartialEq)]
pub enum EfaDirection {
    Read,
    Write,
}

/// Per-EFA-operation timeout for the client-facing result. If the NIC doesn't
/// complete within this, we return an error. The caller is responsible for
/// draining remaining in-flight DMA transfers (via `drain_handles`) and keeping
/// the underlying buffers alive until that drain completes.
/// Overridable at runtime via the hidden `test-efa-op-timeout-ms` config (0 = default).
fn efa_op_timeout() -> std::time::Duration {
    let test_ms = crate::test_efa_op_timeout_ms();
    if test_ms > 0 {
        std::time::Duration::from_millis(test_ms)
    } else {
        std::time::Duration::from_secs(10)
    }
}

/// Collector for background EFA drain tasks. On timeout or transport error,
/// `efa_transfer_addrs` spawns a task that awaits the remaining `Transfer`
/// futures (so hardware DMA finishes) and pushes its `JoinHandle` here. The
/// engine checks this after `run_set`/`run_get` returns with an error and
/// keeps the buffer handles alive until every drain completes.
pub type DrainHandles = std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>;

/// EFA transfer for ONE chunk. Called once per chunk — from `Source::produce`
/// (Read, the SET path) and `Target::consume` (Write, the GET path). `addrs` is
/// that single chunk's client-side scatter list: a chunk may map to several client
/// memory regions, one `(remote_addr, len, rkey)` sub-transfer each.
///
/// Fires this chunk's sub-transfers in parallel via `FuturesUnordered`, racing
/// them against `EFA_OP_TIMEOUT`. On timeout or transport error, remaining futures
/// are drained in a background task (pushed to `drain_handles`). Returns the
/// chunk's combined transport CRC32C (Read/SET) or 0 (Write/GET, checksum
/// unneeded). This is the INTRA-chunk combination (across one chunk's addresses);
/// the caller's `combine_checksums` later does the INTER-chunk combination
/// (across all chunks) into the whole-object CRC.
pub(crate) async fn efa_transfer_addrs(
    session: &Arc<Session>,
    buf_ptr: usize,
    addrs: &[ClientEFAAddress],
    direction: EfaDirection,
    drain_handles: &DrainHandles,
) -> Result<u32, ValkeyError> {
    let err_str = match direction {
        EfaDirection::Write => crate::errors::ERR_EFA_WRITE,
        EfaDirection::Read => crate::errors::ERR_EFA_READ,
    };
    let mut indexed_futures = FuturesUnordered::new();
    let mut buf_offset = 0usize;
    let mut sub_lens: Vec<usize> = Vec::with_capacity(addrs.len());
    let mut err: Option<ValkeyError> = None;
    for (i, &(addr, len, rkey)) in addrs.iter().enumerate() {
        let transfer = match direction {
            EfaDirection::Write => {
                session.write((buf_ptr + buf_offset) as *mut u8, len, rkey, addr)
            }
            EfaDirection::Read => session.read((buf_ptr + buf_offset) as *mut u8, len, rkey, addr),
        };
        match transfer {
            Ok(t) => {
                sub_lens.push(len);
                indexed_futures.push(async move { (i, t.await) });
                buf_offset += len;
            }
            // Break so already-submitted transfers get drained.
            Err(_) => {
                err = Some(ValkeyError::Str(err_str));
                break;
            }
        }
    }
    let deadline = tokio::time::Instant::now() + efa_op_timeout();
    let mut results: Vec<Option<u32>> = vec![None; addrs.len()];
    loop {
        let next = tokio::select! {
            biased;
            item = indexed_futures.next() => item,
            _ = tokio::time::sleep_until(deadline) => {
                err = Some(ValkeyError::Str(crate::errors::ERR_EFA_TIMEOUT));
                break;
            }
        };
        let Some((idx, (outcome, _operand))) = next else {
            break; // all futures resolved
        };
        match outcome {
            Ok(done) => {
                results[idx] = match direction {
                    EfaDirection::Read => Some(
                        done.checksum
                            .expect("EFA Read completion missing checksum — transport must provide CRC"),
                    ),
                    EfaDirection::Write => Some(0),
                };
            }
            Err(_) => {
                err = Some(ValkeyError::Str(err_str));
                break;
            }
        }
    }
    if let Some(e) = err {
        // Spawn a background task to drain remaining in-flight DMA transfers.
        // The engine keeps the underlying buffers alive until these handles resolve.
        if !indexed_futures.is_empty() {
            let handle = tokio::spawn(async move {
                while indexed_futures.next().await.is_some() {
                    crate::info::EFA_DRAIN_COUNT.fetch_add(1, Ordering::Relaxed);
                }
            });
            drain_handles.lock().unwrap().push(handle);
        }
        return Err(e);
    }
    // GET (Write) path: callers ignore the returned CRC — skip combination.
    if matches!(direction, EfaDirection::Write) {
        return Ok(0);
    }
    // Intra-chunk checksum accumulation: combine the CRCs of this chunk's
    // sub-transfers (one per client address) into a single per-chunk CRC.
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
