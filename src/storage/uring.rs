//! NVMe io_uring Engine — registered segments, ReadFixed/WriteFixed, CQ poller thread.
//!
//! Only used in Tiered mode. Dram-only mode has no io_uring engine.
//!
//! Architecture:
//!   Caller: submit(IoRequest) via channel → returns immediately
//!   Poller thread: owns io_uring ring, submits ReadFixed/WriteFixed, polls CQ,
//!                  sends completion result via oneshot channel.
//!
//! Segments are registered with IORING_REGISTER_BUFFERS at startup.
//! ReadFixed/WriteFixed use buf_index (segment index) + offset within segment.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::oneshot;

use super::StorageError;

// ─── NVMe Disk Usage Tracking ────────────────────────────────────────────────

/// Tracks total NVMe disk usage in bytes. Incremented on file creation, decremented on deletion.
static NVME_DISK_USAGE: AtomicU64 = AtomicU64::new(0);

/// Increment NVMe disk usage after a file is created.
pub fn increase_nvme_disk_usage(bytes: u64) {
    NVME_DISK_USAGE.fetch_add(bytes, Ordering::Relaxed);
}

/// Decrement NVMe disk usage after a file is deleted.
///
/// FATAL on underflow: freeing more than is tracked means corrupt accounting, which
/// must be accurate for capacity checks, so assert on the issue.
pub fn decrease_nvme_disk_usage(bytes: u64) {
    if let Err(tracked) =
        NVME_DISK_USAGE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            cur.checked_sub(bytes)
        })
    {
        panic!(
            "NVMe disk-usage underflow: tried to free {bytes} B but only {tracked} B tracked \
             — accounting is corrupt (double-free or size mismatch)"
        );
    }
}

/// Atomically reserve `bytes` of NVMe disk budget if it fits within nvme-maxmemory.
/// Returns true and increments the counter on success; returns false and leaves the
/// counter unchanged if the reservation would exceed the cap (or overflow).
/// Returns true if nvme-maxmemory is 0 (unlimited).
pub fn try_reserve_nvme_disk_usage(bytes: u64) -> bool {
    let max = crate::nvme_maxmemory();
    NVME_DISK_USAGE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            let next = cur.checked_add(bytes)?;
            if max == 0 || next <= max {
                Some(next)
            } else {
                None
            }
        })
        .is_ok()
}

/// Current tracked NVMe disk usage in bytes.
pub fn nvme_disk_usage() -> u64 {
    NVME_DISK_USAGE.load(Ordering::Relaxed)
}

// ─── Request Types ───────────────────────────────────────────────────────────

/// A single buffer operation descriptor for io_uring ReadFixed/WriteFixed.
/// Constructed from ObjectContext or StreamingContext + their owning pool.
#[derive(Debug)]
pub struct UringOp {
    /// Segment's position in the registered iovec array (IORING_REGISTER_BUFFERS).
    pub iovec_index: u16,
    /// Pointer to the buffer within the segment (absolute address).
    pub buf_ptr: *mut u8,
    /// Offset within the NVMe file.
    pub file_offset: u64,
    /// Number of bytes to read/write.
    pub len: u64,
}

// SAFETY: buf_ptr points to segment memory that is stable for module lifetime.
unsafe impl Send for UringOp {}

/// I/O request — internal transport to the poller thread.
/// Contains the oneshot sender directly — no callback boxing.
enum IoRequest {
    Read {
        fd: RawFd,
        op: UringOp,
        tx: oneshot::Sender<Result<u64, StorageError>>,
    },
    Write {
        fd: RawFd,
        op: UringOp,
        tx: oneshot::Sender<Result<(), StorageError>>,
    },
}

// SAFETY: IoRequest contains raw pointers (inside UringOp) referring to segment-allocated memory
// that is stable for module lifetime. oneshot::Sender is Send. Only sent across a
// bounded channel to the single poller thread.
unsafe impl Send for IoRequest {}

// ─── Pending Operation Tracking ──────────────────────────────────────────────

enum PendingOp {
    Read {
        tx: oneshot::Sender<Result<u64, StorageError>>,
        /// Expected byte count for this I/O op. Short reads are rejected.
        expected_bytes: u64,
    },
    Write {
        tx: oneshot::Sender<Result<(), StorageError>>,
        /// Expected byte count for this I/O op. Short writes are rejected.
        expected_bytes: u64,
    },
}

impl PendingOp {
    /// Send an error to the waiting caller. Used when submit fails fatally.
    fn send_error(self, code: i32) {
        match self {
            PendingOp::Read { tx, .. } => {
                let _ = tx.send(Err(StorageError::IoError { code }));
            }
            PendingOp::Write { tx, .. } => {
                let _ = tx.send(Err(StorageError::IoError { code }));
            }
        }
    }
}

// ─── Global NVMe Engine ──────────────────────────────────────────────────────

static NVME_ENGINE: OnceLock<UringNvmeEngine> = OnceLock::new();

pub fn set_nvme_engine(engine: UringNvmeEngine) {
    if NVME_ENGINE.set(engine).is_err() {
        panic!("NVMe engine already initialized");
    }
}

/// Submit an IoRequest to the poller thread.
/// Returns SendError with the request back on failure (channel disconnected)
/// so the caller can extract the oneshot sender and fire an explicit error.
fn submit(req: IoRequest) -> Result<(), crossbeam_channel::SendError<IoRequest>> {
    match NVME_ENGINE.get() {
        Some(engine) => engine.tx.send(req),
        None => Err(crossbeam_channel::SendError(req)),
    }
}

// ─── Async submit helpers ────────────────────────────────────────────────────
//
// Create a oneshot channel, send the tx inside the IoRequest to the poller.
// Poller fires tx.send() on CQE completion. Caller awaits rx.

/// Submit a ReadFixed and return a oneshot receiver.
/// If the poller is dead, sends an explicit error on the oneshot.
pub fn submit_read(fd: RawFd, op: UringOp) -> oneshot::Receiver<Result<u64, StorageError>> {
    let (tx, rx) = oneshot::channel();
    if let Err(crossbeam_channel::SendError(IoRequest::Read { tx, .. })) =
        submit(IoRequest::Read { fd, op, tx })
    {
        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
    }
    rx
}

/// Submit a WriteFixed and return a oneshot receiver.
/// If the poller is dead, sends an explicit error on the oneshot.
pub fn submit_write(fd: RawFd, op: UringOp) -> oneshot::Receiver<Result<(), StorageError>> {
    let (tx, rx) = oneshot::channel();
    if let Err(crossbeam_channel::SendError(IoRequest::Write { tx, .. })) =
        submit(IoRequest::Write { fd, op, tx })
    {
        let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
    }
    rx
}

// ─── Batch submit helpers ────────────────────────────────────────────────────

/// Submit multiple ReadFixed ops. Returns one receiver per op.
pub fn submit_read_batch(
    fd: RawFd,
    ops: Vec<UringOp>,
) -> Vec<oneshot::Receiver<Result<u64, StorageError>>> {
    ops.into_iter().map(|op| submit_read(fd, op)).collect()
}

/// Submit multiple WriteFixed ops. Returns one receiver per op.
pub fn submit_write_batch(
    fd: RawFd,
    ops: Vec<UringOp>,
) -> Vec<oneshot::Receiver<Result<(), StorageError>>> {
    ops.into_iter().map(|op| submit_write(fd, op)).collect()
}

/// Convert batch receivers into a stream of (batch_idx, result) pairs
/// that yields completions as they arrive (unordered). The caller drives
/// the stream — TCP drains it via `await_batch`, EFA acts on each completion.
pub fn into_completions<T: Send + 'static>(
    receivers: Vec<oneshot::Receiver<Result<T, StorageError>>>,
) -> FuturesUnordered<impl std::future::Future<Output = (usize, Result<T, StorageError>)>> {
    let stream = FuturesUnordered::new();
    for (i, rx) in receivers.into_iter().enumerate() {
        stream.push(async move {
            match rx.await {
                Ok(result) => (i, result),
                Err(_) => (i, Err(StorageError::IoError { code: libc::EIO })),
            }
        });
    }
    stream
}

/// `op` is "read" or "write" — used only in suppressed-error log messages.
/// Used by TCP path for awaiting all batch receivers.
pub async fn await_batch<T: Send + 'static>(
    receivers: Vec<oneshot::Receiver<Result<T, StorageError>>>,
    op: &str,
) -> Result<(), StorageError> {
    let mut completions = into_completions(receivers);
    let mut first_err: Option<StorageError> = None;
    while let Some((_idx, result)) = completions.next().await {
        match result {
            Ok(_) => {}
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                } else {
                    valkey_module::logging::log_warning(format!(
                        "largeobj: {op} batch error (suppressed): {e}"
                    ));
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

// ─── UringNvmeEngine ─────────────────────────────────────────────────────────

pub struct UringNvmeEngine {
    tx: Sender<IoRequest>,
    shutdown: Arc<AtomicBool>,
    _poller: Option<thread::JoinHandle<()>>,
}

impl Drop for UringNvmeEngine {
    fn drop(&mut self) {
        // Set shutdown flag BEFORE tx drops. This ensures the poller sees
        // shutdown=true when the channel disconnects, and exits cleanly
        // instead of panicking on unexpected disconnect.
        // Fires on: (1) init failure (local engine dropped), (2) process exit.
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl UringNvmeEngine {
    /// Create engine: init io_uring ring + register buffers on the calling thread,
    /// then spawn CQ poller with the working ring. Returns Err if the kernel
    /// doesn't support io_uring or buffer registration fails.
    pub fn new(iovecs: Vec<libc::iovec>) -> Result<Self, String> {
        // Create ring on main thread — fail gracefully instead of panicking.
        let ring =
            io_uring::IoUring::new(256).map_err(|e| format!("io_uring init failed: {}", e))?;

        // Register buffers on main thread.
        if !iovecs.is_empty() {
            unsafe { ring.submitter().register_buffers(&iovecs) }
                .map_err(|e| format!("IORING_REGISTER_BUFFERS failed: {}", e))?;
        }

        let (tx, rx) = bounded::<IoRequest>(4096);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        // Pass the fully initialized ring to the poller thread.
        let poller = thread::Builder::new()
            .name("lo-uring-poller".into())
            .spawn(move || {
                Self::poller_loop(rx, shutdown_clone, ring);
            })
            .expect("failed to spawn io_uring poller thread");

        Ok(Self {
            tx,
            shutdown,
            _poller: Some(poller),
        })
    }

    /// The CQ poller loop — owns the io_uring ring (received fully initialized).
    fn poller_loop(
        rx: Receiver<IoRequest>,
        shutdown: Arc<AtomicBool>,
        mut ring: io_uring::IoUring,
    ) {
        let mut pending: HashMap<u64, PendingOp> = HashMap::new();
        let mut next_token: u64 = 1;
        // Buffer registration is guaranteed by the caller (main thread).
        let use_fixed = true;
        let mut channel_alive = true;
        let mut submit_error: Option<i32> = None;

        loop {
            // Exit when shutdown requested and all in-flight ops are drained.
            if shutdown.load(Ordering::Relaxed) && pending.is_empty() {
                break;
            }
            // Channel disconnected without shutdown flag = bug. The sender lives
            // in an OnceLock for the entire process lifetime. If it's gone without
            // shutdown being set, something is seriously wrong.
            if !channel_alive && pending.is_empty() {
                if !shutdown.load(Ordering::Relaxed) {
                    panic!(
                        "largeobj: io_uring poller channel disconnected unexpectedly \
                         (shutdown flag not set)"
                    );
                }
                break;
            }

            // Phase 1: Drain channel → build SQEs.
            // Track which tokens belong to this batch so Phase 3b only errors
            // ops from this batch, not in-flight ops from previous iterations
            // whose buffers the kernel may still be accessing.
            let batch_start_token = next_token;
            let mut batch = 0;
            while batch < 64 {
                // If nothing is pending and this is the first iteration, block until
                // a request arrives. Zero CPU when idle, instant wakeup on work.
                // Once we have at least one pending op or one batched SQE, use try_recv
                // to drain without blocking.
                let req = if pending.is_empty() && batch == 0 {
                    match rx.recv() {
                        Ok(req) => req,
                        Err(_) => {
                            channel_alive = false;
                            break;
                        }
                    }
                } else {
                    match rx.try_recv() {
                        Ok(req) => req,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            channel_alive = false;
                            break;
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                    }
                };
                let token = next_token;
                next_token += 1;

                let (sqe, op) = match req {
                    IoRequest::Read { fd, op, tx } => {
                        let read_len = super::object_disk_len(op.len) as u32;
                        let sqe = if use_fixed {
                            io_uring::opcode::ReadFixed::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr,
                                read_len,
                                op.iovec_index,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        } else {
                            io_uring::opcode::Read::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr,
                                read_len,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        };
                        (
                            sqe,
                            PendingOp::Read {
                                tx,
                                expected_bytes: op.len,
                            },
                        )
                    }
                    IoRequest::Write { fd, op, tx } => {
                        let write_len = super::object_disk_len(op.len) as u32;
                        let sqe = if use_fixed {
                            io_uring::opcode::WriteFixed::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr as *const u8,
                                write_len,
                                op.iovec_index,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        } else {
                            io_uring::opcode::Write::new(
                                io_uring::types::Fd(fd),
                                op.buf_ptr as *const u8,
                                write_len,
                            )
                            .offset(op.file_offset)
                            .build()
                            .user_data(token)
                        };
                        (
                            sqe,
                            PendingOp::Write {
                                tx,
                                expected_bytes: op.len,
                            },
                        )
                    }
                };

                unsafe {
                    if ring.submission().is_full() {
                        let _ = ring.submit();
                    }
                    if ring.submission().push(&sqe).is_err() {
                        // SQ full even after flush — error the caller directly.
                        op.send_error(libc::EAGAIN);
                    } else {
                        pending.insert(token, op);
                    }
                }
                batch += 1;
            }

            // Phase 2: Submit + wait. Retry on EINTR (max 3 attempts).
            if !pending.is_empty() {
                for _ in 0..3 {
                    match ring.submit_and_wait(1) {
                        Ok(_) => break,
                        // Signal interrupted — normal, retry immediately.
                        Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                        // Kernel backpressure — reap CQEs in Phase 3, retry next loop.
                        // Self-resolving: kernel is actively processing, just needs time.
                        Err(ref e)
                            if e.raw_os_error() == Some(libc::EBUSY)
                                || e.raw_os_error() == Some(libc::EAGAIN) =>
                        {
                            break;
                        }
                        // Kernel can't allocate internal resources. Not self-resolving —
                        // reaping CQEs won't free the right memory. Flag it; Phase 3
                        // will error all remaining pending ops after reaping what it can.
                        Err(ref e) if e.raw_os_error() == Some(libc::ENOMEM) => {
                            submit_error = Some(libc::ENOMEM);
                            break;
                        }
                        // Unrecoverable: EFAULT (bad pointer), EINVAL (bad SQE),
                        // EBADF (ring dead), EPERM (environment broken).
                        Err(ref e) => {
                            panic!("largeobj: io_uring submit_and_wait unrecoverable: {}", e);
                        }
                    }
                }
            } else if shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Phase 3: Reap CQEs → send results on oneshot channels.
            let mut completed = Vec::new();
            for cqe in ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }

            for (token, result) in completed {
                if let Some(op) = pending.remove(&token) {
                    match op {
                        PendingOp::Read { tx, expected_bytes } => {
                            if result >= 0 && result as u64 >= expected_bytes {
                                let _ = tx.send(Ok(result as u64));
                            } else if result < 0 {
                                let _ = tx.send(Err(StorageError::IoError { code: -result }));
                            } else {
                                // Short read.
                                let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
                            }
                        }
                        PendingOp::Write { tx, expected_bytes } => {
                            if result >= 0 && result as u64 >= expected_bytes {
                                let _ = tx.send(Ok(()));
                            } else if result < 0 {
                                let _ = tx.send(Err(StorageError::IoError { code: -result }));
                            } else {
                                // Short write.
                                let _ = tx.send(Err(StorageError::IoError { code: libc::EIO }));
                            }
                        }
                    }
                }
            }

            // Phase 3b: If submit failed fatally (ENOMEM), error ops from this
            // batch only. In-flight ops from previous iterations stay in pending —
            // their buffers are still being accessed by the kernel via DMA, and
            // erroring them would let callers free buffers mid-I/O (corruption).
            if let Some(code) = submit_error.take() {
                let batch_tokens: Vec<u64> = pending
                    .keys()
                    .filter(|&&t| t >= batch_start_token)
                    .copied()
                    .collect();
                for token in batch_tokens {
                    if let Some(op) = pending.remove(&token) {
                        op.send_error(code);
                    }
                }
            }

            // Phase 4: CQ overflow detection — if the kernel dropped completions,
            // pending ops will never complete and tasks will hang forever.
            if ring.completion().overflow() > 0 {
                panic!(
                    "largeobj: io_uring CQ overflow detected ({} dropped). \
                     Pending ops will never complete. Aborting.",
                    ring.completion().overflow()
                );
            }
        }
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // NVME_DISK_USAGE is a process-global static shared by every test in this
    // binary, and cargo runs tests in parallel. Serialize the accounting tests so
    // their reads/writes don't interleave. Recover from a poisoned lock (the
    // underflow test panics by design) so one panicking test can't wedge the rest.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    // increase/decrease are exact inverses: an equal amount added and removed must
    // leave the counter where it started. Asserted as a delta against a fresh
    // baseline so a corrupt absolute value from another test can't affect it.
    #[test]
    fn test_increase_decrease_symmetry() {
        let _g = lock();
        let base = nvme_disk_usage();

        increase_nvme_disk_usage(4096);
        assert_eq!(nvme_disk_usage(), base + 4096);
        increase_nvme_disk_usage(8192);
        assert_eq!(nvme_disk_usage(), base + 12288);

        decrease_nvme_disk_usage(8192);
        assert_eq!(nvme_disk_usage(), base + 4096);
        decrease_nvme_disk_usage(4096);
        assert_eq!(nvme_disk_usage(), base, "counter must return to baseline");
    }

    // Freeing exactly what was reserved must return to baseline — the same
    // reserve-then-free balance the SET path relies on for aligned disk_len.
    #[test]
    fn test_reserve_then_free_returns_to_zero() {
        let _g = lock();
        let base = nvme_disk_usage();
        for len in [1u64, 4095, 4096, 4097, 1_048_576] {
            let disk_len = super::super::object_disk_len(len);
            increase_nvme_disk_usage(disk_len);
            decrease_nvme_disk_usage(disk_len);
        }
        assert_eq!(nvme_disk_usage(), base);
    }

    // Decrementing more than is tracked is a corrupt-accounting bug and MUST abort,
    // not silently wrap the counter (which would poison every capacity check).
    #[test]
    #[should_panic(expected = "underflow")]
    fn test_decrease_underflow_is_fatal() {
        let _g = lock();
        // Subtracting u64::MAX underflows from any real baseline, triggering the
        // fatal assert regardless of what the counter currently holds.
        decrease_nvme_disk_usage(u64::MAX);
    }
}
