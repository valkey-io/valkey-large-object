//! NVMePool — short-lived transient I/O buffers.
//!
//! Thin wrapper around SegmentPool. No object map, no draining.
//! Sized at startup to ceil(nvme-staging-size / segment-size) uniform segments,
//! fixed thereafter — never expanded or shrunk (unlike DRAMPool).
//! StreamingContexts allocate from here and free on request completion.

use super::context::SegmentBuffer;
use super::segment_pool::SegmentPool;

pub struct NVMePool {
    pool: SegmentPool,
}

impl NVMePool {
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size),
        }
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.pool.free(buf)
    }

    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        self.pool.free_n(buffers)
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

    /// Counts of (live, draining, unused) segments. Used by INFO largeobj.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        self.pool.segment_counts()
    }

    /// Call `f` with each segment's base pointer and size. Used for EFA registration.
    pub fn with_live_segment_slices<F>(&self, f: F)
    where
        F: FnMut(*const u8, usize),
    {
        self.pool.with_live_segment_slices(f);
    }
}
