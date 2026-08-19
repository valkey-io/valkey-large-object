//! Buffer ownership and pool management.
//!
//! Buffer: owned handle wrapping &'static PinnedBuffer. Moves through the pipeline.
//! BufferPool: holds available Buffers. get() pops, Drop pushes back.

use super::engine::PinnedBuffer;
use std::sync::Mutex;

/// Owned buffer handle. Wraps a &'static PinnedBuffer + registered index.
/// Holding this = exclusive access to the underlying pinned memory.
/// Move between layers freely. Drop returns it to the pool.
pub struct Buffer {
    pinned: &'static PinnedBuffer,
    idx: u16,
}

impl Buffer {
    pub(crate) fn from_pinned(pinned: &'static PinnedBuffer, idx: u16) -> Self {
        Self { pinned, idx }
    }

    pub fn ptr(&self) -> *mut u8 {
        self.pinned.as_mut_ptr()
    }
    pub fn len(&self) -> usize {
        self.pinned.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pinned.len() == 0
    }
    pub fn idx(&self) -> u16 {
        self.idx
    }
    pub fn pinned(&self) -> &'static PinnedBuffer {
        self.pinned
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        crate::storage::return_buffer(self.pinned, self.idx);
    }
}

/// The buffer pool. Holds available Buffers in a Vec behind a Mutex.
/// get() pops one out. Drop pushes it back.
pub struct BufferPool {
    pool: Mutex<Vec<Buffer>>,
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferPool {
    pub fn new() -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
        }
    }

    /// Fill the pool with Buffers pointing to the given PinnedBuffers.
    pub fn fill(&self, pinned_buffers: &'static [PinnedBuffer]) {
        let mut pool = self.pool.lock().unwrap();
        for (i, pb) in pinned_buffers.iter().enumerate() {
            pool.push(Buffer::from_pinned(pb, i as u16));
        }
    }

    /// Take a buffer from the pool. Returns None if exhausted.
    pub fn get(&self) -> Option<Buffer> {
        self.pool.lock().unwrap().pop()
    }

    /// Return a buffer to the pool (called by Buffer::Drop).
    pub fn put_back(&self, pinned: &'static PinnedBuffer, idx: u16) {
        self.pool
            .lock()
            .unwrap()
            .push(Buffer::from_pinned(pinned, idx));
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::PinnedBuffer;

    // Leak PinnedBuffers to get &'static references for testing.
    fn leak_pinned(count: usize, size: usize) -> &'static [PinnedBuffer] {
        let buffers: Vec<PinnedBuffer> = (0..count).map(|_| PinnedBuffer::new(size)).collect();
        Box::leak(buffers.into_boxed_slice())
    }

    #[test]
    fn test_pool_get_returns_none_when_empty() {
        let pool = BufferPool::new();
        assert!(pool.get().is_none());
    }

    #[test]
    fn test_pool_fill_and_get() {
        let pinned = leak_pinned(2, 4096);
        let pool = BufferPool::new();
        // Manually push buffers (bypass fill() which needs &'static [PinnedBuffer]).
        {
            let mut inner = pool.pool.lock().unwrap();
            for (i, pb) in pinned.iter().enumerate() {
                inner.push(Buffer::from_pinned(pb, i as u16));
            }
        }
        // Two gets succeed.
        let b1 = pool.get();
        assert!(b1.is_some());
        let b2 = pool.get();
        assert!(b2.is_some());
        // Third fails — pool exhausted.
        assert!(pool.get().is_none());

        // Drop buffers — they call return_buffer() which goes to the global STORAGE.
        // In test context STORAGE is not initialized, so put_back is a no-op (if-let fails).
        // We intentionally leak them here to avoid the no-op Drop path.
        std::mem::forget(b1);
        std::mem::forget(b2);
    }
}
