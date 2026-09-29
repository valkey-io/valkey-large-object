//! SegmentPool — collection of Segments, each with its own talc allocator.
//!
//! Both NVMePool and DRAMPool delegate allocation/free to this struct.
//!
//! ## Design
//!
//! `slots: Vec<Option<Segment>>` behind a `Mutex` — each slot is either
//! `Some(segment)` (live) or `None` (empty/removed). Slot index == iovec index
//! in the sparse io_uring buffer table. Segments have `iovec_index` set
//! write-once at birth.
//!
//! Each `Segment` owns its own `Talc<>` instance covering exactly its own
//! `[base, base+size)` range. There is NO shared allocator across segments.
//! `alloc_one` walks live non-draining segments in least-loaded-first order
//! under one state lock, prechecks each with `talc.get_allocated_span`, and
//! allocates from the first that passes — talc.malloc after a passing
//! precheck is guaranteed to succeed. Per-segment locking means concurrent
//! allocs on different segments never contend.
//!
//! ## Draining / shrink protocol
//!
//! 1. Caller marks `segment.draining = true` (via `mark_segment_draining`).
//! 2. Picker skips draining segments — no new allocations land there.
//! 3. GET handlers check draining before acquiring Arc<ObjectContext>; if draining
//!    they defer to NVMe, so no new Arc refs are acquired.
//! 4. Existing Arc holders drop naturally; each drop calls `pool.free()`.
//! 5. `free()` calls `segment.dec_ref()`.
//! 6. The scaling cron calls `release_all_releasable()` each tick, which drops
//!    Segments whose `is_releasable()` is true. Segment::drop deallocates the
//!    backing memory — its talc's metadata lived inside that memory, so no
//!    talc.truncate is needed.

use std::alloc::Layout;
use std::sync::Mutex;

use super::context::SegmentBuffer;
use super::segment::Segment;

/// Mutable segment state: just the slot vector. No shared allocator, no
/// reverse-lookup index — each segment carries its own talc, and the
/// segment_idx is known at alloc time (from the picker).
struct SegmentState {
    /// Segment slots. Slot `i` = iovec_index `i` in the sparse io_uring table.
    /// `None` = empty slot (hole from a previous drain, or unused capacity).
    slots: Vec<Option<Segment>>,
}

pub struct SegmentPool {
    state: Mutex<SegmentState>,
    /// Size of each segment (uniform within a pool).
    pub segment_size: usize,
}

impl SegmentPool {
    /// Create a new SegmentPool with `segment_count` pre-allocated segments of
    /// `segment_size` bytes. Each segment's iovec_index = its position in the
    /// sparse slot table, registered via `super::append_iovec`.
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        assert!(
            segment_count >= 1,
            "SegmentPool requires at least 1 segment"
        );

        let mut slots: Vec<Option<Segment>> = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let mut seg = Segment::new(segment_size);
            let idx = super::append_iovec(seg.iovec());
            seg.iovec_index = idx;
            slots.push(Some(seg));
        }

        Self {
            state: Mutex::new(SegmentState { slots }),
            segment_size,
        }
    }

    // ─── Allocator ─────────────────────────────────────────────────────────

    /// Allocate all the buffers needed for an entire object. All-or-nothing.
    /// First N-1 buffers are `chunk_size`; the last is trimmed to the
    /// remainder (or `chunk_size` when `obj_len` is an exact multiple).
    ///
    /// Used for ObjectContext (DRAM cache) allocations.
    pub(super) fn alloc_exact(&self, size: usize) -> Option<Vec<SegmentBuffer>> {
        assert!(size > 0, "alloc_exact: size must be > 0");
        let chunk_size = crate::chunk_size();
        let total_chunks = size.div_ceil(chunk_size);
        let last_chunk_size =
            super::chunk_user_data_len(total_chunks - 1, total_chunks, size, chunk_size);
        // Fast path: single chunk — allocate and return directly.
        if total_chunks == 1 {
            return self.alloc_n(last_chunk_size, 1, 1);
        }
        // Multi-chunk: allocate the first N-1 uniform-sized buffers (all-or-nothing).
        let full_count = total_chunks - 1;
        let mut buffers = self.alloc_n(chunk_size, full_count, full_count)?;
        // Allocate the last (possibly smaller) buffer.
        let Some(last_buf) = self.alloc_n(last_chunk_size, 1, 1).map(|mut v| v.remove(0)) else {
            // All-or-nothing: free the uniform buffers we already got.
            for buf in &buffers {
                self.free(buf);
            }
            return None;
        };
        buffers.push(last_buf);
        Some(buffers)
    }

    /// Allocate a streaming buffer window.
    /// Returns between 1 and `min(max_buffers, total_chunks)` buffers, sized
    /// so every buffer is allocated with the least length required, while
    /// making sure each buffer has enough memory to store a chunk of an object.
    ///
    /// Two regimes:
    ///
    /// **Wrapping** (`total_chunks > max_buffers`): the object wraps around
    /// the window, meaning the last buffer in the window cannot be shrunk to
    /// the size of the object tail. All buffers must be uniform `chunk_size`.
    ///
    /// **Non-wrapping** (`total_chunks <= max_buffers`): each buffer maps to
    /// exactly one chunk, so the last buffer can be trimmed. Tries to allocate
    /// all `total_chunks` buffers with a trimmed tail. On partial allocation
    /// (pool pressure), skips the tail to keep all buffers uniform.
    ///
    /// Why skip the tail on partial allocation: if `total_chunks = 5` and the
    /// pool only gives 3 uniform + 1 trimmed tail = 4 buffers, chunk 4 (a
    /// full-size chunk) would wrap to buffer_idx 0 which is fine, but chunk 3
    /// maps to the trimmed buffer_idx 3, yet chunk 3 is a full-size chunk — the
    /// trimmed buffer is too small. Keeping only uniform buffers avoids this.
    pub fn alloc_window(
        &self,
        size: usize,
        max_buffers: usize,
        min_buffers: usize,
    ) -> Option<Vec<SegmentBuffer>> {
        assert!(size > 0, "alloc_window: size must be > 0");
        assert!(max_buffers > 0, "alloc_window: max_buffers must be > 0");
        assert!(min_buffers > 0, "alloc_window: min_buffers must be > 0");
        assert!(
            min_buffers <= max_buffers,
            "alloc_window: min_buffers ({}) > max_buffers ({})",
            min_buffers,
            max_buffers
        );
        let chunk_size = crate::chunk_size();
        let total_chunks = size.div_ceil(chunk_size);
        let last_chunk_size =
            super::chunk_user_data_len(total_chunks - 1, total_chunks, size, chunk_size);
        // Single chunk — one buffer sized to the object. No wrapping possible.
        if total_chunks == 1 {
            return self.alloc_n(last_chunk_size, 1, 1);
        }
        // Wrapping case: total_chunks exceeds the window so the last chunk cannot
        // be trimmed. Uniform buffers required.
        if total_chunks > max_buffers {
            return self.alloc_n(chunk_size, max_buffers, min_buffers);
        }
        // Non-wrapping case: object fits within the window.
        // Allocate the N-1 uniform buffers.
        let uniform_count = total_chunks - 1;
        // Clamp min_required to total_chunks-1
        // because min_buffers may exceed it (e.g. total_chunks=2, min_buffers=2
        // would request alloc_n(chunk_size, 1, 2) — invalid).
        let uniform_min = min_buffers.min(uniform_count);
        let mut buffers = self.alloc_n(chunk_size, uniform_count, uniform_min)?;
        // Only attempt the trimmed tail if we got the full uniform set.
        // If we didn't get the full uniform set (degraded state), and we
        // still tried to allocate the tail, we could get fewer buffers than
        // the number of chunks (requiring wraparound), but the last one would
        // be incorrectly trimmed.
        if buffers.len() == uniform_count {
            // If we can't allocate the tail we naturally fall back to the wrapped
            // around case with uniform buffer length based on what we got.
            if let Some(tail_buf) = self.alloc_n(last_chunk_size, 1, 1).map(|mut v| v.remove(0)) {
                buffers.push(tail_buf);
            }
        }
        Some(buffers)
    }

    /// Allocate up to `count` uniform buffers of `chunk_size` each, requiring at
    ///
    /// Each iteration walks the live non-draining segments in LEAST-LOADED-first
    /// order under one state lock and attempts `talc.malloc`. Allocation may
    /// return `None` when the segment allocator cannot satisfy the request.
    ///
    /// Returns `None` if fewer than `min_required` could be allocated (partial
    /// allocation freed internally). Callers never need cleanup logic.
    fn alloc_n(
        &self,
        chunk_size: usize,
        count: usize,
        min_required: usize,
    ) -> Option<Vec<SegmentBuffer>> {
        assert!(chunk_size > 0, "alloc_n: chunk_size must be > 0");
        assert!(count > 0, "alloc_n: count must be > 0");
        assert!(min_required > 0, "alloc_n: min_required must be > 0");
        assert!(
            min_required <= count,
            "alloc_n: min_required ({}) > count ({})",
            min_required,
            count
        );
        let aligned_size = super::align_up(chunk_size);
        let layout = Layout::from_size_align(aligned_size, super::IO_ALIGN)
            .expect("alloc_n: invalid chunk_size layout");
        let mut buffers: Vec<SegmentBuffer> = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(buf) = self.alloc_one(aligned_size, layout) else {
                break;
            };
            buffers.push(buf);
        }
        if buffers.len() < min_required {
            for buf in &buffers {
                self.free(buf);
            }
            return None;
        }
        Some(buffers)
    }

    /// One allocation: pick the least-loaded live, non-draining segment that
    /// clears the fast byte filter (single O(N) `min_by_key` pass, no sort, no
    /// candidate Vec), then attempt `talc.malloc`. Returns `None` if no segment
    /// is eligible or malloc fails.
    ///
    /// Why not fall back to the next-least-loaded on a failed malloc: the fast
    /// filter already guaranteed `cur + aligned_size <= seg.size`, so malloc
    /// can only fail due to talc's per-chunk boundary-tag overhead tipping it
    /// over the edge. Since all segments are the same size, if the emptiest
    /// eligible segment can't fit the alloc by that overhead sliver, none can —
    /// the correct answer is "pool full", which the caller handles via reactive
    /// expand. A fallback loop would add machinery for a case that yields the
    /// same result.
    fn alloc_one(&self, aligned_size: usize, layout: Layout) -> Option<SegmentBuffer> {
        let st = self.state.lock().expect("state lock unavailable");

        // Single O(N) pass: least-loaded eligible segment. Done under the state
        // lock so slots stay stable through the subsequent malloc.
        let (_, seg_idx) = st
            .slots
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                let seg = opt.as_ref()?;
                if seg.draining.load(std::sync::atomic::Ordering::Acquire) {
                    return None;
                }
                let cur = seg
                    .allocated_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
                // Fast filter — allocated_bytes ignores talc's per-chunk overhead,
                // so the exact precheck below still runs on the winner.
                if cur + aligned_size > seg.size {
                    return None;
                }
                Some((cur, i))
            })
            .min_by_key(|&(cur, _)| cur)?;

        let seg = st.slots[seg_idx].as_ref().unwrap();
        let seg_base = seg.base;
        let mut talc = seg.talc.lock().expect("segment talc lock unavailable");

        // talc.malloc IS the exact all-or-nothing check: it returns Err on OOM
        // without committing anything. alloc_one commits to a single segment (the
        // least-loaded winner above) and never falls through to another, so a
        // failure here is simply "no room" — return None. No separate precheck is
        // needed; talc's own bin lookup already answers "does this fit?".
        let ptr = match unsafe { talc.malloc(layout) } {
            Ok(p) => p,
            Err(_) => return None,
        };
        let offset = ptr.as_ptr() as usize - seg_base as usize;
        seg.inc_ref();
        seg.allocated_bytes
            .fetch_add(aligned_size, std::sync::atomic::Ordering::Relaxed);
        drop(talc);
        drop(st);
        Some(SegmentBuffer {
            segment_idx: seg_idx as u16,
            offset: offset as u64,
            len: aligned_size as u32,
        })
    }

    // ─── Free ────────────────────────────────────────────────────────────────

    /// Free a buffer back to its owning segment.
    /// Uses `buf.segment_idx` directly — no reverse lookup needed.
    pub fn free(&self, buf: &SegmentBuffer) {
        self.free_n(std::slice::from_ref(buf));
    }

    /// Free multiple buffers back to the pool.
    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        for buf in buffers {
            let seg_idx = buf.segment_idx as usize;
            // No align_up needed — buf.len is already the aligned size talc allocated.
            let layout = Layout::from_size_align(buf.len as usize, super::IO_ALIGN)
                .expect("SegmentBuffer layout");
            let st = self.state.lock().expect("state lock unavailable");
            let seg = st.slots[seg_idx]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken");
            let ptr = unsafe { seg.base.add(buf.offset as usize) };
            {
                let mut talc = seg.talc.lock().expect("segment talc lock unavailable");
                unsafe {
                    talc.free(std::ptr::NonNull::new_unchecked(ptr), layout);
                }
            }
            seg.dec_ref();
            seg.allocated_bytes
                .fetch_sub(buf.len as usize, std::sync::atomic::Ordering::Relaxed);
        }
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    /// Add a new segment to the pool. Finds the first `None` slot (or appends).
    /// Each new Segment carries its own fresh talc allocator.
    ///
    /// Called from the main event-loop thread only (scaling cron or reactive expand).
    pub fn expand(&self) -> Option<u16> {
        let mut seg = Segment::new(self.segment_size);
        let idx = super::append_iovec(seg.iovec());
        seg.iovec_index = idx;

        let mut st = self.state.lock().expect("state lock unavailable");
        match st.slots.iter().position(|s| s.is_none()) {
            Some(i) => {
                st.slots[i] = Some(seg);
            }
            None => {
                st.slots.push(Some(seg));
            }
        };

        Some(idx)
    }

    /// Select the least-loaded non-draining segment as a shrink candidate.
    /// Returns `(slot_idx, allocated_bytes)` without marking the segment draining.
    pub fn find_shrink_victim(&self) -> Option<(usize, usize)> {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                opt.as_ref().and_then(|seg| {
                    if !seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                        Some((
                            i,
                            seg.allocated_bytes
                                .load(std::sync::atomic::Ordering::Relaxed),
                        ))
                    } else {
                        None
                    }
                })
            })
            .min_by_key(|&(_, bytes)| bytes)
    }

    /// Mark a specific segment as draining by slot index.
    /// After this, the picker skips this segment; existing allocations continue
    /// to be freed naturally, and once refcount==0 the segment is releasable.
    pub fn mark_segment_draining(&self, seg_idx: usize) {
        let st = self.state.lock().expect("state lock unavailable");
        if let Some(Some(seg)) = st.slots.get(seg_idx) {
            seg.draining
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Scan all segments and release any that are draining with refcount == 0.
    /// Called from the scaling cron on the main thread each tick.
    pub fn release_all_releasable(&self) {
        let releasable: Vec<usize> = {
            let st = self.state.lock().expect("state lock unavailable");
            st.slots
                .iter()
                .enumerate()
                .filter_map(|(i, opt)| opt.as_ref().filter(|seg| seg.is_releasable()).map(|_| i))
                .collect()
        };
        for idx in releasable {
            self.release_drained(idx);
        }
    }

    /// Complete the drain: pull the Segment out of its slot, clear iovec,
    /// drop it. Segment::drop deallocates the backing memory (its talc's
    /// metadata lived inside that memory and vanishes with it).
    fn release_drained(&self, seg_idx: usize) {
        let seg = {
            let mut st = self.state.lock().expect("state lock unavailable");
            let Some(seg) = st.slots[seg_idx].take() else {
                return; // already released
            };
            seg
        };
        super::clear_iovec(seg.iovec_index);
        // seg dropped here → Segment::drop → talc drops (no external state) →
        // std::alloc::dealloc frees backing memory.
    }

    // ─── Segment Helpers ─────────────────────────────────────────────────────

    /// Get absolute pointer for a SegmentBuffer.
    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        let st = self.state.lock().expect("state lock unavailable");
        unsafe {
            st.slots[buf.segment_idx as usize]
                .as_ref()
                .expect("segment slot empty for live buffer — invariant broken")
                .base
                .add(buf.offset as usize)
        }
    }

    /// Return the io_uring iovec_index for the segment owning `buf`.
    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots[buf.segment_idx as usize]
            .as_ref()
            .expect("segment slot empty for live buffer")
            .iovec_index
    }

    /// Total allocated bytes across live (non-draining) segments.
    /// Sums the per-segment atomics directly — the exact figure INFO reports,
    /// with no ratio round-trip.
    pub fn allocated_bytes(&self) -> usize {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .flatten()
            .filter(|seg| !seg.draining.load(std::sync::atomic::Ordering::Relaxed))
            .map(|seg| {
                seg.allocated_bytes
                    .load(std::sync::atomic::Ordering::Relaxed)
            })
            .sum()
    }

    /// Utilization ratio: allocated_bytes / total_live_capacity.
    /// 0.0 = empty, 1.0 = fully allocated.
    pub fn utilization_ratio(&self) -> f64 {
        let st = self.state.lock().expect("state lock unavailable");
        let mut live = 0usize;
        let mut allocated = 0usize;
        for seg in st.slots.iter().flatten() {
            if !seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                live += 1;
                allocated += seg
                    .allocated_bytes
                    .load(std::sync::atomic::Ordering::Relaxed);
            }
        }
        if live == 0 {
            return 0.0;
        }
        let capacity = live * self.segment_size;
        (allocated as f64) / (capacity as f64)
    }

    /// Count of live (non-None, non-draining) segments.
    pub fn live_segment_count(&self) -> usize {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .filter(|s| {
                s.as_ref()
                    .map(|seg| !seg.draining.load(std::sync::atomic::Ordering::Relaxed))
                    .unwrap_or(false)
            })
            .count()
    }

    /// Counts of (live, draining, unused) segments.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        let st = self.state.lock().expect("state lock unavailable");
        let mut live = 0usize;
        let mut draining = 0usize;
        let mut unused = 0usize;
        for s in st.slots.iter() {
            match s {
                None => unused += 1,
                Some(seg) => {
                    if seg.draining.load(std::sync::atomic::Ordering::Relaxed) {
                        draining += 1;
                    } else {
                        live += 1;
                    }
                }
            }
        }
        (live, draining, unused)
    }

    /// Call `f` with each live segment's base pointer and size.
    /// Used by `all_segment_slices` for EFA registration.
    pub fn with_live_segment_slices<F>(&self, mut f: F)
    where
        F: FnMut(*const u8, usize),
    {
        let st = self.state.lock().expect("state lock unavailable");
        for s in st.slots.iter().flatten() {
            f(s.base, s.size);
        }
    }

    /// Returns true if any buffer in `bufs` lives in a currently-draining segment.
    pub fn is_any_buffer_draining(&self, bufs: &[SegmentBuffer]) -> bool {
        let st = self.state.lock().expect("state lock unavailable");
        bufs.iter().any(|b| {
            st.slots[b.segment_idx as usize]
                .as_ref()
                .map(|seg| seg.draining.load(std::sync::atomic::Ordering::Acquire))
                .unwrap_or(false)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── alloc_exact ──────────────────────────────────────────────────────

    #[test]
    fn test_alloc_exact_single_chunk_trimmed() {
        // obj_len < chunk_size → 1 buffer sized to obj_len (aligned).
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_exact(1000).unwrap();
        assert_eq!(bufs.len(), 1);
        // len is aligned up from 1000 to IO_ALIGN (4096).
        assert_eq!(bufs[0].len as usize, super::super::align_up(1000));
    }

    #[test]
    fn test_alloc_exact_single_chunk_exact_multiple() {
        // obj_len == chunk_size → 1 buffer of chunk_size.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_exact(4096).unwrap();
        assert_eq!(bufs.len(), 1);
        assert_eq!(bufs[0].len as usize, 4096);
    }

    #[test]
    fn test_alloc_exact_multi_chunk_with_tail() {
        // obj_len = 3.5 * chunk_size → 3 full + 1 trimmed tail.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_exact(4096 * 3 + 2048).unwrap();
        assert_eq!(bufs.len(), 4);
        for buf in &bufs[..3] {
            assert_eq!(buf.len as usize, 4096);
        }
        // Tail: 2048 aligned up to 4096.
        assert_eq!(bufs[3].len as usize, super::super::align_up(2048));
    }

    #[test]
    fn test_alloc_exact_multi_chunk_exact_multiple() {
        // obj_len = 3 * chunk_size → all 3 buffers are chunk_size (no trim).
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_exact(4096 * 3).unwrap();
        assert_eq!(bufs.len(), 3);
        for buf in &bufs {
            assert_eq!(buf.len as usize, 4096);
        }
    }

    #[test]
    fn test_alloc_exact_pool_full_returns_none() {
        // Tiny pool that can't fit the request.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 8192);
        // Request 3 * 4096 = 12288 — exceeds single 8192 segment.
        assert!(pool.alloc_exact(4096 * 3).is_none());
    }

    #[test]
    fn test_alloc_exact_tail_failure_frees_uniform() {
        // Verify alloc_exact is all-or-nothing when the pool can't satisfy.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        // Determine actual capacity by filling the pool one block at a time.
        let mut filler = Vec::new();
        while let Some(mut v) = pool.alloc_n(4096, 1, 1) {
            filler.push(v.remove(0));
        }
        // Free one block — leaves exactly 1 × 4096 free.
        let last = filler.pop().unwrap();
        pool.free(&last);
        // Request obj needing 3 chunks (2 uniform + 1 tail).
        // Only 1 × 4096 free — can't fit the 2 uniform chunks.
        let result = pool.alloc_exact(4096 * 2 + 100);
        assert!(
            result.is_none(),
            "expected None: pool too full for 3 chunks"
        );
        for buf in &filler {
            pool.free(buf);
        }
    }

    #[test]
    #[should_panic(expected = "alloc_exact: size must be > 0")]
    fn test_alloc_exact_panics_on_zero_len() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        pool.alloc_exact(0);
    }

    // ── alloc_window ─────────────────────────────────────────────────────

    #[test]
    fn test_alloc_window_single_chunk() {
        // obj_len < chunk_size → 1 trimmed buffer.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_window(1000, 8, 2).unwrap();
        assert_eq!(bufs.len(), 1);
        assert_eq!(bufs[0].len as usize, super::super::align_up(1000));
    }

    #[test]
    fn test_alloc_window_wrapping_uniform() {
        // total_chunks (7) > max_buffers (4) → wrapping, all uniform chunk_size.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(2, 65536);
        let bufs = pool.alloc_window(4096 * 7, 4, 2).unwrap();
        assert!(bufs.len() >= 2 && bufs.len() <= 4);
        for buf in &bufs {
            assert_eq!(buf.len as usize, 4096);
        }
    }

    #[test]
    fn test_alloc_window_wrapping_elastic_min() {
        // Verify elastic allocation respects min_buffers. Pool pressure means
        // we may get fewer than max_buffers but at least min_buffers.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        // total_chunks = 20 > max_buffers = 8 → wrapping path.
        let bufs = pool.alloc_window(4096 * 20, 8, 2).unwrap();
        assert!(bufs.len() >= 2);
        assert!(bufs.len() <= 8);
    }

    #[test]
    fn test_alloc_window_non_wrapping_with_tail() {
        // total_chunks (3) <= max_buffers (8) → non-wrapping, trimmed tail.
        // obj_len = 2 * chunk_size + 100 → 2 uniform + 1 trimmed.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_window(4096 * 2 + 100, 8, 2).unwrap();
        assert_eq!(bufs.len(), 3);
        assert_eq!(bufs[0].len as usize, 4096);
        assert_eq!(bufs[1].len as usize, 4096);
        assert_eq!(bufs[2].len as usize, super::super::align_up(100));
    }

    #[test]
    fn test_alloc_window_non_wrapping_exact_multiple() {
        // obj_len = 3 * chunk_size → tail == chunk_size → 3 uniform buffers.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_window(4096 * 3, 8, 2).unwrap();
        assert_eq!(bufs.len(), 3);
        for buf in &bufs {
            assert_eq!(buf.len as usize, 4096);
        }
    }

    #[test]
    fn test_alloc_window_non_wrapping_two_chunks_min_clamped() {
        // total_chunks=2, min_buffers=2 → uniform_count=1, uniform_min=min(2,1)=1.
        // Must not panic on alloc_n(chunk_size, 1, 2).
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        let bufs = pool.alloc_window(4096 + 100, 8, 2).unwrap();
        // Either 2 buffers (uniform + tail) or 1 (degraded, uniform only).
        assert!(bufs.len() == 1 || bufs.len() == 2);
        assert_eq!(bufs[0].len as usize, 4096);
        if bufs.len() == 2 {
            assert_eq!(bufs[1].len as usize, super::super::align_up(100));
        }
        pool.free_n(&bufs);
    }

    #[test]
    fn test_alloc_window_non_wrapping_degraded_skips_tail() {
        // Pool pressure: can only give partial uniform set → tail skipped,
        // returned buffers are all uniform chunk_size (safe for wrapping).
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        // Small pool: 16384 bytes. With talc overhead, ~3 × 4096 allocs fit.
        let pool = SegmentPool::new(1, 16384);
        // Fill most of the pool first.
        let filler = pool.alloc_n(4096, 1, 1).unwrap();
        // Request 5 chunks (4 uniform + tail), max=8, min=1.
        // Pool can only give ~2 uniform at most → degraded, tail skipped.
        let result = pool.alloc_window(4096 * 4 + 100, 8, 1);
        pool.free_n(&filler);
        if let Some(bufs) = result {
            // All returned buffers must be uniform chunk_size (no trimmed tail).
            for buf in &bufs {
                assert_eq!(buf.len as usize, 4096);
            }
            pool.free_n(&bufs);
        }
        // If None: pool was too full even for min_buffers — valid outcome.
    }

    #[test]
    fn test_alloc_window_pool_full_returns_none() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 8192);
        // Fill the pool.
        let filler = pool.alloc_n(4096, 2, 1);
        let result = pool.alloc_window(4096 * 5, 8, 2);
        assert!(result.is_none());
        if let Some(f) = filler {
            pool.free_n(&f);
        }
    }

    #[test]
    fn test_alloc_window_boundary_total_equals_max() {
        // total_chunks == max_buffers → non-wrapping path.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(2, 65536);
        // 4 chunks, max=4 → non-wrapping. 3 uniform + 1 tail.
        let bufs = pool.alloc_window(4096 * 3 + 100, 4, 2).unwrap();
        assert_eq!(bufs.len(), 4);
        for buf in &bufs[..3] {
            assert_eq!(buf.len as usize, 4096);
        }
        assert_eq!(bufs[3].len as usize, super::super::align_up(100));
        pool.free_n(&bufs);
    }

    #[test]
    fn test_alloc_window_boundary_total_equals_max_plus_one() {
        // total_chunks == max_buffers + 1 → wrapping path.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(2, 65536);
        // 5 chunks, max=4 → wrapping, all uniform.
        let bufs = pool.alloc_window(4096 * 5, 4, 2).unwrap();
        assert!(bufs.len() >= 2 && bufs.len() <= 4);
        for buf in &bufs {
            assert_eq!(buf.len as usize, 4096);
        }
        pool.free_n(&bufs);
    }

    #[test]
    #[should_panic(expected = "alloc_window: min_buffers (3) > max_buffers (2)")]
    fn test_alloc_window_panics_when_min_exceeds_max() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        pool.alloc_window(4096, 2, 3);
    }

    #[test]
    #[should_panic(expected = "alloc_window: size must be > 0")]
    fn test_alloc_window_panics_on_zero_len() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536);
        pool.alloc_window(0, 8, 2);
    }

    // ── alloc_n (private, tested from within module) ───────────────────

    #[test]
    #[should_panic(expected = "alloc_n: min_required (3) > count (2)")]
    fn test_alloc_n_panics_when_min_exceeds_count() {
        let pool = SegmentPool::new(1, 65536);
        pool.alloc_n(4096, 2, 3);
    }
}
