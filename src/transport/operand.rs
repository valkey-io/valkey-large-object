//! The local end of a DMA transfer, a slice of a pool segment. Each segment is registered
//! at load, so the worker resolves the slice out of a cached registration.

use dma_libfabric::Operands;

/// A talc allocation inside a pool segment, handed to the fabric worker for one transfer. The
/// engine keeps the owning `SegmentBuffer` (through its `ObjectContext` or `StreamingContext`)
/// alive across the await, so the slice outlives the transfer.
pub struct PoolOperand {
    pointer: *mut u8,
    length: usize,
}

impl PoolOperand {
    /// `pointer..pointer + length` must be one talc allocation that no other thread touches until
    /// the transfer completes. talc is the authority on disjointness, which is why this can hand
    /// out `&mut [u8]`.
    pub fn new(pointer: *mut u8, length: usize) -> Self {
        Self { pointer, length }
    }
}

impl std::fmt::Debug for PoolOperand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PoolOperand")
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

// SAFETY: segment memory is stable for the module's lifetime, and the range is a talc allocation
// owned by this transfer's initiator until the completion hands the operand back.
unsafe impl Send for PoolOperand {}

impl Operands for PoolOperand {
    fn source(&self) -> Option<&[u8]> {
        // SAFETY: see `new`; the initiator holds the allocation for the transfer's duration.
        Some(unsafe { std::slice::from_raw_parts(self.pointer, self.length) })
    }

    fn allocate(&mut self, length: usize) -> Option<&mut [u8]> {
        if self.length < length {
            return None;
        }
        // SAFETY: as `source`, and nothing else writes the allocation while the transfer runs.
        Some(unsafe { std::slice::from_raw_parts_mut(self.pointer, length) })
    }
}
