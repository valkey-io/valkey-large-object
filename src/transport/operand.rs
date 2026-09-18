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

// SAFETY: segment memory is stable for the module's lifetime, and the range is a talc allocation
// owned by this transfer's initiator until the completion hands the operand back.
unsafe impl Send for PoolOperand {}

// Operands is the dma-libfabric trait for providing the local end of a DMA operation from the
// user allocated memory type. This binds PoolOperand to how the memory is used inside of
// dma-libfabric.
impl Operands for PoolOperand {
    // Used when the operand is a byte source for an RDMA. This is used in case of `LO.GET`.
    fn source(&self) -> Option<&[u8]> {
        // SAFETY: see `new`; the initiator holds the allocation for the transfer's duration.
        Some(unsafe { std::slice::from_raw_parts(self.pointer, self.length) })
    }

    // Used when the operand is a byte destination for an RDMA. This is used in case of `LO.SET`.
    fn allocate(&mut self, length: usize) -> Option<&mut [u8]> {
        if self.length < length {
            // The RDMA transfer length exceeds the allocated buffer - it can't be used for the
            // operation.
            return None;
        }
        // SAFETY: as `source`, and nothing else writes the allocation while the transfer runs.
        Some(unsafe { std::slice::from_raw_parts_mut(self.pointer, length) })
    }
}
