//! SegmentPool — collection of Segments, each with its own talc allocator.
//!
//! Both NVMePool and DRAMPool delegate allocation/free to this struct.
//!
//! ## Design
//!
//! `slots: Vec<Option<Segment>>` behind a `Mutex` — each slot is either
//! `Some(segment)` (live) or `None` (empty/removed). Slot index == this pool's
//! LOCAL iovec index in its own io_uring buffer table (each pool has its own
//! ring). A segment's `iovec_index` is assigned at creation and recomputed on
//! each dense table rebuild.
//!
//! Each `Segment` owns its own `Talc<>` instance covering exactly its own
//! `[base, base+size)` range. There is NO shared allocator across segments.
//! `alloc_one` walks live non-draining segments in least-loaded-first order
//! under one state lock, picks the least-loaded that clears a fast byte
//! filter, and allocates from its own talc — `talc.allocate` itself is the exact
//! all-or-nothing check (returns Err on OOM without committing). Per-segment
//! locking means concurrent allocs on different segments never contend.
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
use std::ptr::NonNull;
use std::sync::Mutex;

use allocator_api2::alloc::Allocator;

use super::context::SegmentBuffer;
use super::segment::Segment;

/// Mutable segment state: the slot vector plus this pool's own io_uring iovec
/// table.
struct SegmentState {
    /// Segment slots. `None` = empty slot (a drained hole or unused capacity).
    slots: Vec<Option<Segment>>,
    /// This pool's io_uring registered-buffer table as `(base, len)` pairs,
    /// index-aligned with `slots`. The index is the pool-local `iovec_index` a
    /// ReadFixed/WriteFixed op passes to name its buffer. `None` = a free index
    /// reused by the next expand.
    iovecs: Vec<Option<(usize, usize)>>,
}

pub struct SegmentPool {
    state: Mutex<SegmentState>,
    /// Size of each segment (uniform within a pool).
    pub segment_size: usize,
    /// Which pool (ring) this is — routes reregister to the correct io_uring
    /// engine, since each pool has its own ring + registered-buffer table.
    pool_id: super::uring::PoolType,
}

impl SegmentPool {
    /// Create a new SegmentPool with `segment_count` pre-allocated segments of
    /// `segment_size` bytes. Each segment's iovec_index = its position in this
    /// pool's own local iovec table (assigned in-order here), which lines
    /// up with its position in the slot table.
    pub fn new(segment_count: usize, segment_size: usize, pool_id: super::uring::PoolType) -> Self {
        assert!(
            segment_count >= 1,
            "SegmentPool requires at least 1 segment"
        );

        let mut slots: Vec<Option<Segment>> = Vec::with_capacity(segment_count);
        let mut iovecs: Vec<Option<(usize, usize)>> = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            let mut seg = Segment::new(segment_size);
            let iov = seg.iovec();
            // Pool-local index: position in this pool's own iovec table.
            let idx = iovecs.len();
            assert!(
                idx < super::MAX_SEGMENTS,
                "startup segment count exceeds MAX_SEGMENTS — init validation bypassed"
            );
            iovecs.push(Some((iov.iov_base as usize, iov.iov_len)));
            seg.iovec_index = idx as u16;
            slots.push(Some(seg));
        }

        Self {
            state: Mutex::new(SegmentState { slots, iovecs }),
            segment_size,
            pool_id,
        }
    }

    // ─── io_uring iovec table (pool-local) ─────────────────────────────────────

    /// Dense snapshot of this pool's iovec table (holes skipped) for the ring's
    /// startup `IORING_REGISTER_BUFFERS`. Dense because 5.10 has no sparse table.
    pub fn startup_iovecs(&self) -> Vec<libc::iovec> {
        let st = self.state.lock().expect("state lock unavailable");
        st.iovecs
            .iter()
            .filter_map(|opt| {
                opt.map(|(ptr, len)| libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: len,
                })
            })
            .collect()
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    /// Allocate all buffers for an entire object, **all co-located in a single
    /// segment**, all-or-nothing. First N-1 buffers are `chunk_size`, the last is
    /// trimmed to the remainder.
    ///
    /// Single-segment is the invariant that enables shrink: an object touches one
    /// segment's refcount, so relocating it is one contiguous move. Corollary: no
    /// object can exceed `segment_size` — oversized ones are rejected at SET
    /// admission (`lo_set`) and never reach here. Used for DRAM ObjectContext.
    pub(super) fn alloc_exact(&self, size: usize) -> Option<Vec<SegmentBuffer>> {
        self.alloc_object_in_one_segment(&Self::chunk_sizes(size), None)
    }

    /// `alloc_exact` in segment `seg_idx` only. None if that segment is gone,
    /// draining, or cannot fit the object.
    pub(super) fn alloc_exact_in(&self, seg_idx: usize, size: usize) -> Option<Vec<SegmentBuffer>> {
        self.alloc_object_in_one_segment(&Self::chunk_sizes(size), Some(seg_idx))
    }

    /// `(seg_idx, bytes_short)`: the segment `alloc_exact(size)` would pick
    /// (least-loaded live one) and how many bytes it lacks for `size`. 0 means
    /// the bytes fit and only talc overhead could block the alloc.
    pub(super) fn segment_shortfall(&self, size: usize) -> Option<(usize, usize)> {
        let total: usize = Self::chunk_sizes(size).iter().sum();
        let (i, cur) = self.find_shrink_victim()?;
        Some((i, (cur + total).saturating_sub(self.segment_size)))
    }

    /// Per-chunk aligned sizes: first N-1 are chunk_size, last is trimmed.
    fn chunk_sizes(size: usize) -> Vec<usize> {
        assert!(size > 0, "alloc_exact: size must be > 0");
        let chunk_size = crate::chunk_size();
        let total_chunks = size.div_ceil(chunk_size);
        (0..total_chunks)
            .map(|i| {
                super::align_up(super::chunk_user_data_len(
                    i,
                    total_chunks,
                    size,
                    chunk_size,
                ))
            })
            .collect()
    }

    /// Allocate every chunk of the object from ONE least-loaded eligible segment.
    /// Single `min_by_key` pass, no fallback loop (same as `alloc_one`): segments
    /// are uniform, so if the emptiest eligible one can't fit — only possible by
    /// talc's per-chunk boundary-tag overhead — none can, and `None` = pool full,
    /// which the caller handles via reactive expand. Keeps an object co-located —
    /// see `alloc_exact`.
    /// `target` pins the segment (reclaim retry); `None` picks the least-loaded.
    fn alloc_object_in_one_segment(
        &self,
        sizes: &[usize],
        target: Option<usize>,
    ) -> Option<Vec<SegmentBuffer>> {
        assert!(
            !sizes.is_empty(),
            "alloc_object_in_one_segment: empty sizes"
        );
        let total: usize = sizes.iter().sum();
        let st = self.state.lock().expect("state lock unavailable");
        // Single O(N) pass: least-loaded eligible segment, checked against the
        // object's TOTAL size. Segments are uniform, so if it can't fit, none
        // can. `target` pins the segment instead.
        let seg_idx = match target {
            Some(i) => i,
            None => Self::least_loaded(&st)?.0,
        };
        let seg = st.slots.get(seg_idx)?.as_ref()?;
        if seg.draining.load(std::sync::atomic::Ordering::Acquire)
            || seg
                .allocated_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
                + total
                > seg.size
        {
            return None;
        }
        let seg_base = seg.base;
        let talc = seg.talc.lock().expect("segment talc lock unavailable");
        let mut buffers: Vec<SegmentBuffer> = Vec::with_capacity(sizes.len());
        for &aligned_size in sizes {
            let layout = Layout::from_size_align(aligned_size, super::IO_ALIGN)
                .expect("alloc_object_in_one_segment: invalid chunk layout");
            // TalcCell::allocate (Allocator trait) is safe and the all-or-nothing fit check.
            let Ok(ptr) = talc.allocate(layout) else {
                // Partial failure by talc overhead: roll back what we took from
                // this segment and report pool-full. No next-segment fallback —
                // uniform segments mean no other segment would fit either.
                for buf in &buffers {
                    let l = Layout::from_size_align(buf.len as usize, super::IO_ALIGN)
                        .expect("alloc_object_in_one_segment: rollback layout");
                    // SAFETY: this pointer was returned by this TalcCell's allocate for this
                    // exact layout, and is freed exactly once here in rollback.
                    let p = unsafe { NonNull::new_unchecked(seg_base.add(buf.offset as usize)) };
                    unsafe { talc.deallocate(p, l) };
                    seg.dec_ref();
                }
                self.mirror_counters(seg, &talc);
                return None;
            };
            let offset = ptr.cast::<u8>().as_ptr() as usize - seg_base as usize;
            seg.inc_ref();
            buffers.push(SegmentBuffer {
                segment_idx: seg_idx as u16,
                offset: offset as u64,
                len: aligned_size as u32,
            });
        }
        // Mirror talc's authoritative figures once, while holding the lock
        // (drift-free, overhead-aware) — same rationale as alloc_one.
        self.mirror_counters(seg, &talc);
        Some(buffers)
    }

    /// Mirror talc's authoritative live figures (`allocated_bytes`,
    /// `fragment_count`) into the segment's atomics. Call while holding the
    /// segment's talc lock: talc updates these inside allocate/deallocate, so
    /// the mirror is drift-free and overhead-aware, and the scaling cron / INFO
    /// read the atomics lock-free. Single source of this two-store pattern —
    /// used by `alloc_one`, `free_n`, and `alloc_object_in_one_segment`.
    fn mirror_counters(&self, seg: &Segment, talc: &super::segment::SegmentTalc) {
        seg.allocated_bytes.store(
            talc.counters().allocated_bytes,
            std::sync::atomic::Ordering::Relaxed,
        );
        seg.fragment_count.store(
            talc.counters().fragment_count,
            std::sync::atomic::Ordering::Relaxed,
        );
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
    /// Allocate up to `count` uniform buffers of `chunk_size` each, requiring at
    /// least `min_required`. All-or-nothing when `min_required == count`.
    ///
    /// Each iteration walks the live non-draining segments in LEAST-LOADED-first
    /// order under one state lock and allocates from the least-loaded segment's
    /// own talc allocator (`talc.allocate` is the exact fit check). Allocation
    /// may return `None` when the segment allocator cannot satisfy the request.
    ///
    /// Returns `None` if fewer than `min_required` could be allocated (partial
    /// allocation freed internally). Callers never need cleanup logic.
    ///
    /// Private helper for `alloc_window` (StreamingContext). NOTE: buffers may
    /// land in DIFFERENT segments — this is fine for the streaming window, which
    /// is a rotating staging buffer, not a resident object. `alloc_exact`
    /// deliberately does NOT use this (it requires all chunks co-located in one
    /// segment; see the single-segment invariant on `alloc_exact`).
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
    /// candidate Vec), then allocate from its talc. Returns None if there is no
    /// eligible segment, or the picked segment's `talc.allocate` returns None.
    ///
    /// Why not fall back to the next-least-loaded on a failed malloc: the fast
    /// filter already guaranteed `cur + aligned_size <= seg.size`, so malloc can
    /// only fail by talc's per-chunk boundary-tag overhead tipping it over the
    /// edge. Since all segments are the same size, if the emptiest eligible
    /// segment can't fit the alloc by that overhead sliver, none can — the
    /// correct answer is "pool full", which the caller handles via reactive
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
                // so talc.allocate below is still the authoritative fit check.
                if cur + aligned_size > seg.size {
                    return None;
                }
                Some((cur, i))
            })
            .min_by_key(|&(cur, _)| cur)?;

        let seg = st.slots[seg_idx].as_ref().unwrap();
        let seg_base = seg.base;
        let talc = seg.talc.lock().expect("segment talc lock unavailable");

        // TalcCell::allocate (the Allocator trait) is safe and is the all-or-nothing
        // fit check: Err(AllocError) means no room (nothing committed), which we map
        // to None and propagate — no separate precheck needed.
        let ptr = talc.allocate(layout).ok()?.cast::<u8>();
        let offset = ptr.as_ptr() as usize - seg_base as usize;
        seg.inc_ref();
        // Mirror talc's authoritative live figures under the lock we hold
        // (drift-free, overhead-aware) — read lock-free by the cron/INFO.
        self.mirror_counters(seg, &talc);
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
                let talc = seg.talc.lock().expect("segment talc lock unavailable");
                // SAFETY: ptr was returned by this segment's TalcCell::allocate for this
                // exact layout, and is freed exactly once -- the buffer's sole owner
                // (ObjectContext/StreamingContext) calls free once in its Drop.
                unsafe {
                    talc.deallocate(NonNull::new_unchecked(ptr), layout);
                }
                // Mirror talc's authoritative post-free figures while holding the
                // lock (drift-free, overhead-aware) — see mirror_counters.
                self.mirror_counters(seg, &talc);
            }
            seg.dec_ref();
        }
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    /// Add a new segment to the pool at the pool-local `iovec_index` that the
    /// first-free-hole search assigns, so `slots[i]` and this pool's `iovecs[i]`
    /// stay index-aligned by construction. Each new Segment carries its own fresh
    /// talc allocator.
    ///
    /// Called from the main event-loop thread only (scaling cron or reactive expand).
    ///
    /// Returns `(iovec_index, segment_slice)` on success. The slice is `'static` (segment memory
    /// is stable for the module's lifetime) so the caller can register it with the transport
    /// layer (EFA `fi_mr_reg` / io_uring) without a reverse lookup. Returns `None` if this pool's
    /// iovec table is already at `MAX_SEGMENTS`.
    pub fn expand(&self) -> Option<(u16, &'static [u8])> {
        let seg = Segment::new(self.segment_size);
        // Segment memory is stable for the module's lifetime (never freed until
        // the segment is released, and a live segment is not released while a
        // registration references it). SAFETY: base/size describe the just-
        // allocated backing buffer.
        let slice: &'static [u8] = unsafe { std::slice::from_raw_parts(seg.base, seg.size) };
        let iov = seg.iovec();

        let mut st = self.state.lock().expect("state lock unavailable");
        // Pool-local iovec index: reuse the first hole, else append (rejected at
        // MAX_SEGMENTS). On None, `seg` drops here and nothing enters `slots`.
        let entry = Some((iov.iov_base as usize, iov.iov_len));
        let idx = match st.iovecs.iter().position(|s| s.is_none()) {
            Some(i) => {
                st.iovecs[i] = entry;
                i
            }
            None => {
                if st.iovecs.len() >= super::MAX_SEGMENTS {
                    return None;
                }
                st.iovecs.push(entry);
                st.iovecs.len() - 1
            }
        };
        let mut seg = seg;
        seg.iovec_index = idx as u16;
        // Place at slots[idx] so slots and iovecs stay index-aligned. Pad in case
        // idx is past the current len.
        if idx >= st.slots.len() {
            st.slots.resize_with(idx + 1, || None);
        }
        st.slots[idx] = Some(seg);

        Some((idx as u16, slice))
    }

    /// Select the least-loaded non-draining segment as a shrink candidate.
    /// Returns `(slot_idx, allocated_bytes)` without marking the segment draining.
    pub fn find_shrink_victim(&self) -> Option<(usize, usize)> {
        let st = self.state.lock().expect("state lock unavailable");
        Self::least_loaded(&st)
    }

    /// Least-loaded non-draining segment as `(slot_idx, allocated_bytes)`.
    /// Single O(N) pass; the caller holds the state lock so slots stay stable.
    /// The allocator's pick, the shrink victim, and `segment_shortfall` use it.
    fn least_loaded(st: &SegmentState) -> Option<(usize, usize)> {
        st.slots
            .iter()
            .enumerate()
            .filter_map(|(i, opt)| {
                let seg = opt.as_ref()?;
                (!seg.draining.load(std::sync::atomic::Ordering::Acquire)).then(|| {
                    (
                        i,
                        seg.allocated_bytes
                            .load(std::sync::atomic::Ordering::Relaxed),
                    )
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
        let mut released_any = false;
        for idx in releasable {
            self.release_drained(idx);
            released_any = true;
        }
        if released_any {
            // A segment left this pool — rebuild its ring's table without it
            // (no-op in Dram mode). Touches only this ring.
            super::uring::submit_reregister(self.pool_id);
        }
    }

    /// Complete the drain: pull the Segment out of its slot, clear this pool's
    /// iovec entry, drop it. Segment::drop deallocates the backing memory (its
    /// talc's metadata lived inside that memory and vanishes with it).
    fn release_drained(&self, seg_idx: usize) {
        let mut st = self.state.lock().expect("state lock unavailable");
        let Some(seg) = st.slots[seg_idx].take() else {
            return; // already released
        };
        let iovec_index = seg.iovec_index as usize;
        // Null this pool's iovec slot so the ring's dense rebuild omits it.
        if iovec_index < st.iovecs.len() {
            st.iovecs[iovec_index] = None;
        }
        drop(st);
        // seg dropped here → Segment::drop → talc drops (no external state) →
        // std::alloc::dealloc frees backing memory.
        drop(seg);
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

    /// Whether the segment owning `buf` is registered in the io_uring kernel
    /// buffer table (NOT EFA — that is tracked separately in the fabric layer).
    /// Callers use this to pick ReadFixed/WriteFixed (true) vs
    /// plain Read/Write (false). An expanded segment not yet kernel-registered
    /// returns false so its I/O never issues a fixed op against an unregistered
    /// iovec_index (which would EFAULT).
    pub fn is_buf_io_uring_registered(&self, buf: &SegmentBuffer) -> bool {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots[buf.segment_idx as usize]
            .as_ref()
            .expect("segment slot empty for live buffer")
            .is_io_uring_registered()
    }

    /// Mark every currently-live segment as io_uring-registered. Called once at
    /// startup after the initial IORING_REGISTER_BUFFERS succeeds, since those
    /// segments' iovecs are in the kernel table.
    pub fn mark_all_registered(&self) {
        let st = self.state.lock().expect("state lock unavailable");
        for slot in st.slots.iter() {
            if let Some(seg) = slot.as_ref() {
                seg.mark_io_uring_registered();
            }
        }
    }

    /// Rebuild this pool's ring table: reindex live segments densely from 0, mark
    /// them registered, and return the iovec array for `register_buffers`. Dense
    /// because 5.10 cannot register a sparse table.
    pub fn rebuild_dense_iovecs(&self) -> Vec<libc::iovec> {
        let mut st = self.state.lock().expect("state lock unavailable");
        let mut out = Vec::new();
        let mut next: u16 = 0;
        let mut new_iovecs: Vec<Option<(usize, usize)>> = Vec::new();
        for slot in st.slots.iter_mut() {
            if let Some(seg) = slot.as_mut() {
                seg.iovec_index = next;
                next += 1;
                let iov = seg.iovec();
                out.push(iov);
                new_iovecs.push(Some((iov.iov_base as usize, iov.iov_len)));
                seg.mark_io_uring_registered();
            }
        }
        st.iovecs = new_iovecs; // compacted to match the dense reindex above
        out
    }

    /// Count of live segments marked io_uring-registered. Below this pool's live
    /// count means an expanded segment's re-register hasn't completed yet. INFO metric.
    pub fn io_uring_registered_count(&self) -> usize {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .filter_map(|s| s.as_ref())
            .filter(|seg| seg.is_io_uring_registered())
            .count()
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

    /// Total free-gap count across live (non-draining) segments — the pool's
    /// fragmentation signal. Sums the per-segment `fragment_count` atomics.
    /// A count, not a byte figure: rising against flat `allocated_bytes` means
    /// free space is scattering into small holes.
    pub fn fragment_count(&self) -> usize {
        let st = self.state.lock().expect("state lock unavailable");
        st.slots
            .iter()
            .flatten()
            .filter(|seg| !seg.draining.load(std::sync::atomic::Ordering::Relaxed))
            .map(|seg| {
                seg.fragment_count
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
    fn test_segment_shortfall_picks_least_loaded() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(2, 65536, super::super::uring::PoolType::Dram);
        let a = pool.alloc_exact(4096 * 4).unwrap(); // seg 0: 16 KiB
        let b = pool.alloc_exact(4096 * 2).unwrap(); // seg 1: 8 KiB (least loaded)
        assert_eq!((a[0].segment_idx, b[0].segment_idx), (0, 1));
        // 60 KiB into seg 1 (8 KiB used of 64 KiB) is 4 KiB short.
        assert_eq!(pool.segment_shortfall(4096 * 15), Some((1, 4096)));
        // Fits by bytes: nothing short.
        assert_eq!(pool.segment_shortfall(4096), Some((1, 0)));
        pool.free_n(&a);
        pool.free_n(&b);
    }

    #[test]
    fn test_alloc_exact_in_uses_only_the_target_segment() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(2, 65536, super::super::uring::PoolType::Dram);
        let a = pool.alloc_exact(4096 * 4).unwrap(); // seg 0 now the fuller one
                                                     // Pinned to seg 0 even though seg 1 is emptier.
        let b = pool.alloc_exact_in(0, 4096 * 2).unwrap();
        assert!(b.iter().all(|buf| buf.segment_idx == 0));
        // Does not fit in seg 0, and never falls back to seg 1.
        assert!(pool.alloc_exact_in(0, 65536).is_none());
        // No such segment.
        assert!(pool.alloc_exact_in(5, 4096).is_none());
        pool.free_n(&a);
        pool.free_n(&b);
    }

    #[test]
    fn test_alloc_exact_single_chunk_trimmed() {
        // obj_len < chunk_size → 1 buffer sized to obj_len (aligned).
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
        let bufs = pool.alloc_exact(1000).unwrap();
        assert_eq!(bufs.len(), 1);
        // len is aligned up from 1000 to IO_ALIGN (4096).
        assert_eq!(bufs[0].len as usize, super::super::align_up(1000));
    }

    #[test]
    fn test_alloc_exact_single_chunk_exact_multiple() {
        // obj_len == chunk_size → 1 buffer of chunk_size.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
        let bufs = pool.alloc_exact(4096).unwrap();
        assert_eq!(bufs.len(), 1);
        assert_eq!(bufs[0].len as usize, 4096);
    }

    #[test]
    fn test_alloc_exact_multi_chunk_with_tail() {
        // obj_len = 3.5 * chunk_size → 3 full + 1 trimmed tail.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 8192, super::super::uring::PoolType::Dram);
        // Request 3 * 4096 = 12288 — exceeds single 8192 segment.
        assert!(pool.alloc_exact(4096 * 3).is_none());
    }

    #[test]
    fn test_alloc_exact_tail_failure_frees_uniform() {
        // Verify alloc_exact is all-or-nothing when the pool can't satisfy.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
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
    fn test_alloc_exact_co_location_and_no_split_under_pressure() {
        // Single-segment invariant, white-box (integration can't see segment_idx):
        // (a) a multi-chunk object's chunks all share ONE segment, and
        // (b) under pressure it either fits wholly in one segment or fails —
        //     it never splits across segments to use scattered free chunks.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        // Generous segments so capacity isn't boundary-tight against talc
        // per-chunk overhead; 2 segments.
        let pool = SegmentPool::new(2, 65536, super::super::uring::PoolType::Dram);
        // Drain the pool completely with single-chunk objects, tracking each.
        let mut singles = Vec::new();
        while let Some(b) = pool.alloc_exact(4096) {
            singles.push(b);
        }
        assert!(singles.len() >= 4, "expected a multi-chunk pool capacity");
        // Free exactly ONE chunk in each segment, so every segment has exactly
        // one free slot — total 2 free chunks, but none contiguous-per-segment
        // enough for a 2-chunk object.
        let mut freed_per_seg: std::collections::HashMap<u16, usize> = Default::default();
        let mut kept = Vec::new();
        for b in singles {
            let seg = b[0].segment_idx;
            let n = freed_per_seg.entry(seg).or_insert(0);
            if *n < 1 {
                *n += 1;
                pool.free_n(&b);
            } else {
                kept.push(b);
            }
        }
        // (b) A 2-chunk object cannot fit in any single segment (each has 1 free
        // chunk) → must return None, NOT split 1+1 across the two segments.
        assert!(
            pool.alloc_exact(4096 * 2).is_none(),
            "2-chunk object must not split across segments with 1 free chunk each"
        );
        // Free a second chunk in one segment → that segment now has 2 free →
        // the 2-chunk object fits wholly in it.
        let target = kept[0][0].segment_idx;
        pool.free_n(&kept[0]);
        let bufs = pool.alloc_exact(4096 * 2).unwrap();
        assert_eq!(bufs.len(), 2);
        // (a) co-location: both chunks share one segment — the one that regained
        // room, proving the allocator committed the whole object to a single seg.
        let seg = bufs[0].segment_idx;
        assert!(
            bufs.iter().all(|b| b.segment_idx == seg),
            "2-chunk object must be co-located, got {:?}",
            bufs.iter().map(|b| b.segment_idx).collect::<Vec<_>>()
        );
        assert_eq!(
            seg, target,
            "object lands in the segment that regained 2 slots"
        );
        for b in &kept[1..] {
            pool.free_n(b);
        }
        pool.free_n(&bufs);
    }

    #[test]
    #[should_panic(expected = "alloc_exact: size must be > 0")]
    fn test_alloc_exact_panics_on_zero_len() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
        pool.alloc_exact(0);
    }

    // ── alloc_window ─────────────────────────────────────────────────────

    #[test]
    fn test_alloc_window_single_chunk() {
        // obj_len < chunk_size → 1 trimmed buffer.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
        let bufs = pool.alloc_window(1000, 8, 2).unwrap();
        assert_eq!(bufs.len(), 1);
        assert_eq!(bufs[0].len as usize, super::super::align_up(1000));
    }

    #[test]
    fn test_alloc_window_wrapping_uniform() {
        // total_chunks (7) > max_buffers (4) → wrapping, all uniform chunk_size.
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(2, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 16384, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 8192, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(2, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(2, 65536, super::super::uring::PoolType::Dram);
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
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
        pool.alloc_window(4096, 2, 3);
    }

    #[test]
    #[should_panic(expected = "alloc_window: size must be > 0")]
    fn test_alloc_window_panics_on_zero_len() {
        crate::CFG_CHUNK_SIZE.store(4096, std::sync::atomic::Ordering::Relaxed);
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
        pool.alloc_window(0, 8, 2);
    }

    // ── alloc_n (private, tested from within module) ───────────────────

    #[test]
    #[should_panic(expected = "alloc_n: min_required (3) > count (2)")]
    fn test_alloc_n_panics_when_min_exceeds_count() {
        let pool = SegmentPool::new(1, 65536, super::super::uring::PoolType::Dram);
        pool.alloc_n(4096, 2, 3);
    }
}
