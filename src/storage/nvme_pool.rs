//! NVMePool — short-lived transient I/O buffers.
//!
//! Thin wrapper around SegmentPool. No object map, no draining.
//! Segments are fixed at startup, never resized.
//! StreamingContexts allocate from here and free on request completion.

use super::context::SegmentBuffer;
use super::segment::Segment;
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

    pub fn alloc(&self, size: usize) -> Option<SegmentBuffer> {
        self.pool.alloc(size)
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.pool.free(buf)
    }

    pub fn alloc_n(
        &self,
        chunk_size: usize,
        count: usize,
        min_required: usize,
    ) -> Option<Vec<SegmentBuffer>> {
        self.pool.alloc_n(chunk_size, count, min_required)
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    /// Access segments (needed by engine for buf_index lookup).
    pub fn segments(&self) -> &[Segment] {
        &self.pool.segments
    }
}
