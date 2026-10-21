//! NVMePool — short-lived transient I/O buffers.
//!
//! Thin wrapper around SegmentPool. No object map, no draining.
//! Sized at startup to ceil(nvme-staging-size / segment-size) uniform segments,
//! fixed thereafter — never expanded or shrunk (unlike DRAMPool).
//! StreamingContexts allocate from here and free on request completion.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::context::SegmentBuffer;
use super::segment_pool::SegmentPool;

/// Outcome of a cancellable staging-buffer wait. The caller replies to the client
/// only on `CantFit` — `Got` already has its buffers, and `Cancelled` means the
/// client disconnected and there is nobody to reply to.
pub enum AllocOutcome {
    /// Got the window.
    Got(Vec<SegmentBuffer>),
    /// The window can never fit in the pool, whatever frees — reject at once.
    CantFit,
    /// The client disconnected while queued; left the queue without allocating.
    Cancelled,
}

pub struct NVMePool {
    pool: SegmentPool,
    /// Wakes the op at the head of `queue` whenever buffers are freed.
    freed: tokio::sync::Notify,
    /// FIFO line for `alloc_window_cancellable`: tokio's Mutex grants the lock in
    /// request order, so ops get staging in arrival order and a large window cannot
    /// be starved by smaller ones arriving behind it.
    queue: tokio::sync::Mutex<()>,
    /// One cancel signal per client with request(s) currently queued. A disconnect
    /// fires the client's signal with `notify_waiters`, waking EVERY queued request
    /// for that client (pipelined clients can own several). Keyed by Valkey client id.
    cancels: Mutex<HashMap<u64, Arc<tokio::sync::Notify>>>,
}

impl NVMePool {
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size, super::uring::PoolType::Nvme),
            freed: tokio::sync::Notify::new(),
            queue: tokio::sync::Mutex::new(()),
            cancels: Mutex::new(HashMap::new()),
        }
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.free_n(std::slice::from_ref(buf))
    }

    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        self.pool.free_n(buffers);
        self.freed.notify_waiters();
    }

    /// Cancel every queued staging request owned by `client_id`. Called from the
    /// client-disconnect server event on the main thread. A no-op if the client has
    /// nothing queued. The waiter removes its own registry entry on exit, so firing a
    /// stale id is harmless.
    pub fn cancel_client(&self, client_id: u64) {
        let signal = self
            .cancels
            .lock()
            .expect("cancels lock unavailable")
            .get(&client_id)
            .cloned();
        if let Some(signal) = signal {
            signal.notify_waiters();
        }
    }

    /// Register this client's cancel signal (shared across its concurrent requests)
    /// and return it. Removed by `CancelGuard` when the last holder drops.
    fn register_cancel(&self, client_id: u64) -> Arc<tokio::sync::Notify> {
        self.cancels
            .lock()
            .expect("cancels lock unavailable")
            .entry(client_id)
            .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
            .clone()
    }

    /// `alloc_window`, but ops queue FIFO for staging and wait for frees instead of
    /// failing when the pool is momentarily full. The wait ends only three ways:
    /// the window fits (`Got`), the client disconnects (`Cancelled`), or the window
    /// is larger than any single segment could ever hold (`CantFit`, rejected before
    /// queueing — it would otherwise wait forever). There is no timeout: the client's
    /// own timeout bounds the wait, and a disconnect cancels the queued request.
    pub async fn alloc_window_cancellable(
        &self,
        client_id: u64,
        size: usize,
        max_buffers: usize,
        min_buffers: usize,
    ) -> AllocOutcome {
        // Fail fast: a window whose minimum viable form exceeds one segment can never
        // fit, no matter what frees. Reject before taking a queue slot.
        if !self.window_can_fit(size, min_buffers) {
            return AllocOutcome::CantFit;
        }
        let cancel = self.register_cancel(client_id);
        let _guard = CancelGuard {
            pool: self,
            client_id,
        };
        // Register as a cancel waiter once, up front: notify_waiters only wakes
        // already-registered waiters, so a disconnect landing mid-wait is not lost.
        let mut cancelled = std::pin::pin!(cancel.notified());
        cancelled.as_mut().enable();
        // Held until this op has its window, so nobody behind it can take buffers first.
        let _turn = tokio::select! {
            biased;
            () = cancelled.as_mut() => return AllocOutcome::Cancelled,
            turn = self.queue.lock() => turn,
        };
        loop {
            // Register as a `freed` waiter BEFORE trying: a free landing between the
            // attempt and the await is not lost.
            let mut freed = std::pin::pin!(self.freed.notified());
            freed.as_mut().enable();
            if let Some(buffers) = self.alloc_window(size, max_buffers, min_buffers) {
                return AllocOutcome::Got(buffers);
            }
            tokio::select! {
                biased;
                () = cancelled.as_mut() => return AllocOutcome::Cancelled,
                () = freed => {}
            }
        }
    }

    /// Whether a window for `size` could EVER fit, given the pool is empty. A window
    /// needs its buffers co-located in one segment (same invariant as `alloc_window`),
    /// so the minimum viable form must not exceed one segment. Independent of current
    /// usage — this rejects only the genuinely-impossible, so a wait never hangs forever.
    fn window_can_fit(&self, size: usize, min_buffers: usize) -> bool {
        let chunk_size = crate::chunk_size();
        let total_chunks = size.div_ceil(chunk_size);
        // Mirror alloc_window's minimum allocation exactly:
        //   - a single-chunk object is ONE buffer sized to the object itself;
        //   - otherwise the minimum is min(min_buffers, total_chunks) full chunks.
        let min_bytes = if total_chunks == 1 {
            size
        } else {
            min_buffers.min(total_chunks) * chunk_size
        };
        super::align_up(min_bytes) <= self.pool.segment_size
    }

    pub fn alloc_window(
        &self,
        size: usize,
        max_buffers: usize,
        min_buffers: usize,
    ) -> Option<Vec<SegmentBuffer>> {
        self.pool.alloc_window(size, max_buffers, min_buffers)
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.pool.iovec_index_for_buf(buf)
    }

    /// Whether the segment owning `buf` is registered in the io_uring kernel
    /// buffer table (picks fixed vs non-fixed I/O). See SegmentPool.
    pub fn is_buf_io_uring_registered(&self, buf: &SegmentBuffer) -> bool {
        self.pool.is_buf_io_uring_registered(buf)
    }

    /// Mark all current segments io_uring-registered (startup, post-register).
    pub fn mark_all_registered(&self) {
        self.pool.mark_all_registered();
    }

    /// Startup iovec snapshot for this pool's ring. See `SegmentPool::startup_iovecs`.
    pub fn startup_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.startup_iovecs()
    }

    /// See `SegmentPool::rebuild_dense_iovecs`. The NVMe pool never expands or
    /// shrinks, so its ring never actually re-registers; provided only so the poller's
    /// pool-generic rebuild path is total.
    pub fn rebuild_dense_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.rebuild_dense_iovecs()
    }

    /// See `SegmentPool::io_uring_registered_count`.
    pub fn io_uring_registered_count(&self) -> usize {
        self.pool.io_uring_registered_count()
    }

    /// Counts of (live, draining, unused) segments. Used by INFO largeobj.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        self.pool.segment_counts()
    }

    /// Total free-gap count across the staging pool's segments (talc fragmentation signal).
    pub fn fragment_count(&self) -> usize {
        self.pool.fragment_count()
    }

    /// Call `f` with each segment's base pointer and size. Used for EFA registration.
    pub fn with_live_segment_slices<F>(&self, f: F)
    where
        F: FnMut(*const u8, usize),
    {
        self.pool.with_live_segment_slices(f);
    }
}

/// Removes a client's cancel-registry entry when its last queued request leaves the
/// wait (got buffers, cancelled, or errored). Scoped to a single `alloc_window_cancellable`
/// call; the shared `Arc<Notify>` keeps a concurrent sibling request alive, and the entry
/// is only dropped once the last one releases its `Arc` via the strong-count check.
struct CancelGuard<'a> {
    pool: &'a NVMePool,
    client_id: u64,
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        let mut cancels = self.pool.cancels.lock().expect("cancels lock unavailable");
        // Remove only when this was the last holder: strong_count 2 = the map's Arc
        // plus this call's clone. A concurrent request for the same client keeps it.
        if let Some(signal) = cancels.get(&self.client_id) {
            if Arc::strong_count(signal) <= 2 {
                cancels.remove(&self.client_id);
            }
        }
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Below the 4 KiB chunk-size minimum, so every window is one buffer whatever
    /// chunk-size another test has set on the shared config.
    const OBJ: usize = 1000;
    const CLIENT: u64 = 7;

    impl AllocOutcome {
        fn is_got(&self) -> bool {
            matches!(self, AllocOutcome::Got(_))
        }
        fn is_cancelled(&self) -> bool {
            matches!(self, AllocOutcome::Cancelled)
        }
    }

    /// A one-segment pool with every buffer allocated, plus the windows holding it full.
    fn exhausted_pool() -> (Arc<NVMePool>, Vec<Vec<SegmentBuffer>>) {
        let pool = Arc::new(NVMePool::new(1, 65536));
        let mut held = Vec::new();
        while let Some(window) = pool.alloc_window(OBJ, 1, 1) {
            held.push(window);
        }
        assert!(!held.is_empty(), "pool should fit at least one window");
        (pool, held)
    }

    /// Multi-thread so spawned waiters run on their own worker promptly — matching
    /// production, where the wait runs on a tokio worker and frees/cancels arrive
    /// from another thread (the main event loop).
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .expect("test runtime")
    }

    /// Spin until the pool has registered at least `n` cancel entries' worth of
    /// in-flight requests for `client_id` (so a subsequent cancel cannot race ahead
    /// of registration). Each in-flight request adds one `Arc` strong ref beyond the
    /// map's own; `n` requests ⇒ strong_count > n.
    fn wait_registered(pool: &NVMePool, client_id: u64, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let registered = pool
                .cancels
                .lock()
                .unwrap()
                .get(&client_id)
                .map(|s| Arc::strong_count(s) > n)
                .unwrap_or(false);
            if registered {
                return;
            }
            assert!(Instant::now() < deadline, "waiter(s) never registered");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn wait_empty_registry(pool: &NVMePool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pool.cancels.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "registry never drained");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// A free unblocks a queued waiter, which then allocates the freed window.
    #[test]
    fn test_waiter_succeeds_when_another_op_frees() {
        let (pool, mut held) = exhausted_pool();
        runtime().block_on(async {
            let waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1);
            pool.free_n(&held.pop().expect("held window"));
            assert!(waiter.await.expect("waiter task").is_got());
        });
    }

    /// A disconnect cancels a queued waiter: it leaves the queue allocating nothing,
    /// and its registry entry is cleaned up.
    #[test]
    fn test_disconnect_cancels_waiter() {
        let (pool, _held) = exhausted_pool();
        runtime().block_on(async {
            let waiter = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1);
            pool.cancel_client(CLIENT);
            assert!(waiter.await.expect("waiter task").is_cancelled());
            wait_empty_registry(&pool);
        });
    }

    /// Pipelined client: two queued requests share one cancel signal, and a single
    /// disconnect cancels BOTH.
    #[test]
    fn test_disconnect_cancels_all_pipelined_requests() {
        let (pool, _held) = exhausted_pool();
        runtime().block_on(async {
            let one = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            let two = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            // Both must have registered (one holds the queue lock, the other waits on
            // it) before we fire the single disconnect.
            wait_registered(&pool, CLIENT, 2);
            pool.cancel_client(CLIENT);
            assert!(one.await.expect("task one").is_cancelled());
            assert!(two.await.expect("task two").is_cancelled());
            wait_empty_registry(&pool);
        });
    }

    /// FIFO fairness: a newcomer cannot take a buffer freed for the waiting head.
    #[test]
    fn test_newcomer_cannot_jump_the_queue() {
        let (pool, mut held) = exhausted_pool();
        runtime().block_on(async {
            let head = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(CLIENT, OBJ, 1, 1).await }
            });
            wait_registered(&pool, CLIENT, 1); // head holds the queue lock, parked on `freed`

            // Free one buffer, then a newcomer (different client) queues behind the
            // head, which still holds the queue lock. The newcomer cannot acquire it.
            pool.free_n(&held.pop().expect("held window"));
            let newcomer = tokio::spawn({
                let pool = Arc::clone(&pool);
                async move { pool.alloc_window_cancellable(8, OBJ, 1, 1).await }
            });
            wait_registered(&pool, 8, 1);
            // Cancel the newcomer so the test does not hang; it must NOT have allocated.
            pool.cancel_client(8);
            assert!(newcomer.await.expect("newcomer task").is_cancelled());
            assert!(head.await.expect("head task").is_got());
        });
    }

    /// A window that cannot fit in one segment is rejected at once, never queued.
    #[test]
    fn test_window_larger_than_segment_cant_fit() {
        // chunk-size may be set by another test on the shared config; read it so the
        // arithmetic matches `window_can_fit`.
        let chunk = crate::chunk_size();
        // One tiny segment that cannot hold even a single chunk.
        let pool = NVMePool::new(1, chunk / 2);
        let outcome = runtime().block_on(pool.alloc_window_cancellable(CLIENT, chunk, 1, 1));
        assert!(matches!(outcome, AllocOutcome::CantFit));
        // Nothing was queued, so no registry entry was left behind.
        assert!(pool.cancels.lock().unwrap().is_empty());
    }
}
