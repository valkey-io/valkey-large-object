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

use talc::{ErrOnOom, Span, Talc};

/// A contiguous registered memory region with an owned talc allocator.
pub struct Segment {
    /// Base pointer (4KB-aligned, allocated via ValkeyAlloc).
    pub base: *mut u8,
    /// Total size in bytes.
    pub size: usize,
    /// Index into the sparse iovec table (io_uring ReadFixed/WriteFixed) and
    /// into the SegmentPool's `slots: Vec<Option<Segment>>` vector.
    /// Write-once at creation; immutable for the segment's lifetime.
    pub iovec_index: u16,
    /// This segment's own talc allocator. Claims exactly `[base, base+size)`.
    /// Each alloc/free on this segment locks THIS mutex — never contends with
    /// other segments' allocators.
    pub talc: Mutex<Talc<ErrOnOom>>,
    /// Number of live allocations from this segment.
    /// +1 on talc alloc, -1 on talc free. When 0 + draining → safe to release.
    pub refcount: AtomicU32,
    /// Bytes currently allocated from this segment (sum of align_up(alloc sizes)).
    /// Used for picker (max-loaded packing) and INFO utilization.
    /// Relaxed ordering — advisory, not correctness.
    pub allocated_bytes: AtomicUsize,
    /// When true, no new allocations land on this segment. Set during shrink.
    pub draining: AtomicBool,
}

impl Segment {
    /// Allocate a new segment via ValkeyAlloc, create its talc, claim its range.
    /// `iovec_index` is set to 0 initially; caller assigns after `append_iovec`.
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).expect("invalid segment layout");
        let base = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!base.is_null(), "segment allocation failed (out of memory)");

        let mut talc = Talc::new(ErrOnOom);
        let span = Span::from_base_size(base, size);
        // Safety: memory was just allocated exclusively for this Segment; nothing
        // else references [base, base+size), so claim's non-overlap invariant holds.
        // The claimed span is not retained — a segment is reclaimed by dropping it
        // whole (its talc metadata lives inside its own memory), so there is no
        // truncate/get_allocated_span path that needs the returned Span.
        unsafe { talc.claim(span).expect("talc.claim failed for new segment") };

        Self {
            base,
            size,
            iovec_index: 0, // set by caller after append_iovec
            talc: Mutex::new(talc),
            refcount: AtomicU32::new(0),
            allocated_bytes: AtomicUsize::new(0),
            draining: AtomicBool::new(false),
        }
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
        // Dropping the Talc first is not required — its metadata lives inside
        // the segment's own memory, so dropping the Mutex<Talc> is a no-op wrt
        // memory (talc has no external state). Then dealloc the backing memory.
        let layout = Layout::from_size_align(self.size, 4096).expect("Segment layout");
        unsafe { std::alloc::dealloc(self.base, layout) };
    }
}

// SAFETY: Segment memory is stable for its lifetime.
// Talc mutex protects concurrent alloc/free on this segment.
unsafe impl Send for Segment {}
unsafe impl Sync for Segment {}
