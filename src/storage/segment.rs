//! Segment — a contiguous registered memory region with its own talc allocator.
//!
//! Allocated via std::alloc::alloc_zeroed (ValkeyAlloc/zmalloc) so Valkey's
//! used_memory correctly reflects the allocation. Registered with io_uring
//! (one iovec entry) and EFA (one fi_mr_reg call).
//!
//! Each Segment carries its OWN `Talc<>` instance covering exactly its own
//! memory range. This eliminates the reverse-lookup pointer→segment path and
//! the shared-allocator lock: allocations are routed to a segment at the
//! picker level (SegmentPool::alloc_exact / alloc_window), and the segment's local talc handles
//! only its own address range.

use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Mutex;

use talc::source::Manual;
use talc::TalcCell;

/// This segment's talc allocator: manual-source, default-binning `TalcCell`.
/// `Manual` has no backing source — the segment claims its own `[base, base+size)`
/// range once and never grows. `TalcCell` is `!Sync`; the `Mutex<SegmentTalc>` on
/// `Segment` provides cross-thread exclusion.
pub type SegmentTalc = TalcCell<Manual>;

/// A contiguous registered memory region with an owned talc allocator.
pub struct Segment {
    /// Base pointer (4KB-aligned, allocated via ValkeyAlloc).
    pub base: *mut u8,
    /// Total size in bytes.
    pub size: usize,
    /// Index into this pool's io_uring iovec table (ReadFixed/WriteFixed) and
    /// into the SegmentPool's `slots: Vec<Option<Segment>>` vector. Assigned at
    /// creation and recomputed on each dense table rebuild.
    pub iovec_index: u16,
    /// This segment's own talc allocator. Claims exactly `[base, base+size)`.
    /// Each alloc/free on this segment locks THIS mutex — never contends with
    /// other segments' allocators.
    pub talc: Mutex<SegmentTalc>,
    /// Number of live allocations from this segment.
    /// +1 on talc alloc, -1 on talc free. When 0 + draining → safe to release.
    pub refcount: AtomicU32,
    /// Bytes currently allocated from this segment. Mirrors talc's authoritative
    /// `counters().allocated_bytes`, refreshed under the talc lock on every
    /// alloc/free — drift-free and overhead-aware (not a hand-summed estimate).
    /// Used for picker (max-loaded packing) and INFO utilization.
    /// Relaxed ordering — advisory, not correctness; the scaling cron reads it
    /// lock-free.
    pub allocated_bytes: AtomicUsize,
    /// Free-hole count in this segment's heap. Mirrors talc's
    /// `counters().fragment_count`, refreshed under the talc lock on every
    /// alloc/free. 1 = free space is one contiguous block (healthy); high = many
    /// scattered holes (fragmented). This is a hole COUNT, not a byte measure of
    /// fragmentation. Relaxed — advisory, read lock-free by cron/INFO.
    pub fragment_count: AtomicUsize,
    /// When true, no new allocations land on this segment. Set during shrink.
    pub draining: AtomicBool,
    /// Whether this segment's buffer is in the io_uring kernel table
    /// (IORING_REGISTER_BUFFERS). A segment starts `false` (its I/O uses plain
    /// Read/Write) and is flipped `true` once its memory is in the table — at
    /// startup, or after the post-expand rebuild for a segment added later.
    /// Write-once false→true, so Relaxed suffices: a stale `false` read just takes
    /// the always-correct non-fixed path.
    pub io_uring_registered: AtomicBool,
}

impl Segment {
    /// Allocate a new segment via ValkeyAlloc, create its talc, claim its range.
    /// `iovec_index` is set to 0 initially; the owning pool assigns the real
    /// pool-local index after creation (`SegmentPool::new` / `expand`).
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).expect("invalid segment layout");
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!base.is_null(), "segment allocation failed (out of memory)");

        let talc = TalcCell::new(Manual);
        // Safety: memory was just allocated exclusively for this Segment; nothing
        // else references [base, base+size), so claim's non-overlap invariant holds.
        // `claim(base, size)` establishes this segment's only heap; the returned
        // pointer is not retained — a segment is reclaimed by dropping it whole
        // (its talc metadata lives inside its own memory).
        unsafe {
            talc.claim(base, size)
                .expect("talc.claim failed for new segment");
        }

        Self {
            base,
            size,
            iovec_index: 0, // set by owning pool after creation
            talc: Mutex::new(talc),
            refcount: AtomicU32::new(0),
            allocated_bytes: AtomicUsize::new(0),
            fragment_count: AtomicUsize::new(0),
            draining: AtomicBool::new(false),
            // Not yet in the io_uring buffer table; marked registered after the
            // initial IORING_REGISTER_BUFFERS (startup) — see the field's doc.
            io_uring_registered: AtomicBool::new(false),
        }
    }

    /// Whether this segment's buffer is registered in the io_uring kernel table — see the field's doc.
    pub fn is_io_uring_registered(&self) -> bool {
        self.io_uring_registered.load(Ordering::Relaxed)
    }

    /// Mark this segment as registered — see the field's doc. Never un-set while the segment is live.
    pub fn mark_io_uring_registered(&self) {
        self.io_uring_registered.store(true, Ordering::Relaxed);
    }

    /// Check if safe to release (draining + no live allocations).
    pub fn is_releasable(&self) -> bool {
        // Acquire pairs with Release in dec_ref: when we see refcount == 0,
        // all buffer writes from prior users are guaranteed visible, making
        // it safe to deallocate the segment.
        self.draining.load(Ordering::Acquire) && self.refcount.load(Ordering::Acquire) == 0
    }

    pub fn inc_ref(&self) {
        // Relaxed is fine since we are claiming the segment before doing any work,
        // so there are no prior writes that need to be visible to others.
        self.refcount.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ref(&self) {
        // Release ensures any data written to this segment's buffers is visible
        // before another thread sees refcount == 0 and deallocates the segment.
        self.refcount.fetch_sub(1, Ordering::Release);
    }

    /// Get iovec for io_uring registration.
    pub fn iovec(&self) -> libc::iovec {
        libc::iovec {
            iov_base: self.base as *mut libc::c_void,
            iov_len: self.size,
        }
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        // Tear down EFA registration BEFORE freeing the memory: the DMA library requires
        // deregistration to precede the unmap (fi_close over freed pages is illegal). Drops
        // the segment's MemoryRegion handles (invalidate the cache entry, then fi_close once
        // no in-flight transfer still leases them). No-op if no fabric or never registered.
        crate::efa_release_segment(self.base as usize);
        // No io_uring deregistration: on 5.10 the only removal is the whole-table
        // rebuild. Safe at refcount == 0 — no in-flight fixed op references it.
        // talc's metadata lives inside this memory, so nothing to drop before dealloc.
        let layout = Layout::from_size_align(self.size, 4096).expect("Segment layout");
        unsafe { std::alloc::dealloc(self.base, layout) };
    }
}

// SAFETY: Segment memory is stable for its lifetime.
// Talc mutex protects concurrent alloc/free on this segment.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}
