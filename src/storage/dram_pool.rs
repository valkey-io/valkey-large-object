//! DRAMPool — long-lived cached objects.
//!
//! SegmentPool + RwLock<HashMap<ObjectId, Arc<ObjectContext>>>.
//!
//! Expand: triggered reactively when alloc fails (pool full), or proactively
//! by the scaling cron when utilization exceeds the expand watermark.
//! Adds one `segment-size` segment via `try_expand()`.
//!
//! Shrink (Tiered-mode only): triggered proactively by the scaling cron when
//! `used_memory` approaches `maxmemory`. Picks the least-used segment, marks
//! it draining, and removes its cached objects from the HashMap so GET handlers
//! fall back to NVMe. The segment releases on the next cron tick when refcount hits 0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::context::{ObjectContext, SegmentBuffer};
use super::segment_pool::SegmentPool;
use crate::data_type::ObjectId;

pub struct DRAMPool {
    pool: SegmentPool,
    /// Cached objects: ObjectId → Arc<ObjectContext>.
    /// RwLock: main thread reads (GET hit), tokio writes (promotion insert).
    objects: RwLock<HashMap<ObjectId, Arc<ObjectContext>>>,
    /// Cumulative count of successful expand operations since module load.
    pub expand_count: AtomicU64,
    /// Cumulative count of successful shrink operations since module load.
    pub shrink_count: AtomicU64,
}

impl DRAMPool {
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size),
            objects: RwLock::new(HashMap::new()),
            expand_count: AtomicU64::new(0),
            shrink_count: AtomicU64::new(0),
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    /// Allocate all buffers for an object, expanding the pool as needed.
    /// Try alloc_exact first; on failure, expand one segment and retry.
    /// Terminates when alloc succeeds, try_expand returns None (dram-maxmemory
    /// cap or server maxmemory watermark), or the iteration cap is reached.
    ///
    /// The cap — ceil(obj_len / segment_size) + 1 — is a roomy upper bound
    /// to prevent OOM when both maxmemory and dram-maxmemory are unbounded
    /// (0). The +1 accounts for per-allocation talc overhead that can push
    /// the object's real footprint past one segment boundary.
    ///
    /// Callers on the main thread pass their command `&Context`; callers on
    /// tokio workers or data-type callbacks pass `&Context::dummy()` (null ctx
    /// is accepted by RM_GetServerInfo for the memory watermark check).
    pub fn alloc_exact_or_expand(
        &self,
        ctx: &valkey_module::Context,
        obj_len: u64,
    ) -> Option<Vec<super::context::SegmentBuffer>> {
        let max_expands = (obj_len as usize).div_ceil(self.pool.segment_size) + 1;
        let mut expands = 0;
        loop {
            if let Some(bufs) = self.pool.alloc_exact(obj_len as usize) {
                return Some(bufs);
            }
            if expands >= max_expands {
                return None;
            }
            self.try_expand(ctx)?;
            expands += 1;
        }
    }

    pub fn free(&self, buf: &SegmentBuffer) {
        self.pool.free(buf)
    }

    pub fn free_n(&self, buffers: &[SegmentBuffer]) {
        self.pool.free_n(buffers)
    }

    pub fn buffer_ptr(&self, buf: &SegmentBuffer) -> *mut u8 {
        self.pool.buffer_ptr(buf)
    }

    pub fn iovec_index_for_buf(&self, buf: &SegmentBuffer) -> u16 {
        self.pool.iovec_index_for_buf(buf)
    }

    // ─── Object Map ──────────────────────────────────────────────────────────

    /// Lookup a cached object.
    ///
    /// Returns None if the object is not cached, or if its segment is draining
    /// (caller should fall back to NVMe to prevent new Arc refs on a draining segment).
    pub fn get_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        let arc = self
            .objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .get(oid)
            .cloned()?;

        // If any buffer of this object lives in a draining segment, refuse the
        // Arc — forces the caller to NVMe and lets the refcount drain to zero.
        let is_draining = self.pool.is_any_buffer_draining(&arc.buffers);
        if is_draining {
            return None;
        }
        Some(arc)
    }

    /// Insert an ObjectContext (promotion path).
    pub fn insert_object(&self, oid: ObjectId, ctx: Arc<ObjectContext>) {
        self.objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .insert(oid, ctx);
    }

    /// Remove an ObjectContext (free callback / eviction).
    pub fn remove_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        self.objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .remove(oid)
    }

    /// Check if object exists (coalesce check — is promotion in progress?).
    pub fn contains_object(&self, oid: &ObjectId) -> bool {
        self.objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .contains_key(oid)
    }

    /// Number of cached objects.
    pub fn object_count(&self) -> usize {
        self.objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .len()
    }

    /// Try to allocate space and create an ObjectContext for this object.
    /// Returns None if pool is full or object exceeds max-promote-size.
    /// On success returns Arc<ObjectContext> in Filling state — caller reads
    /// NVMe data into the buffers, then calls mark_ready().
    /// Multi-buffer: allocates ceil(obj_len / chunk_size) buffers via
    /// alloc_exact_or_expand with all-or-nothing semantics.
    ///
    /// Pool full after expansion attempts → returns None. Caller falls back
    /// to NVMe read (Tiered mode).
    pub fn try_promote_object(
        &self,
        oid: ObjectId,
        obj_len: u64,
    ) -> Option<std::sync::Arc<super::context::ObjectContext>> {
        // Don't promote objects above the configured threshold.
        if obj_len > crate::max_promote_size() {
            return None;
        }
        // All-or-nothing with reactive expansion via dummy context (promotion
        // runs on tokio workers — null ctx is accepted by RM_GetServerInfo).
        // Alloc BEFORE write lock — talc scan under memory pressure
        // won't block GET readers waiting on get_object().
        let dummy = valkey_module::Context::dummy();
        let buffers = self.alloc_exact_or_expand(&dummy, obj_len)?;
        // Atomic check-and-insert under write lock to prevent TOCTOU race
        // (concurrent GETs promoting the same OID simultaneously).
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        if objects.contains_key(&oid) {
            self.free_n(&buffers);
            return None;
        }
        // buf.len stays chunk_size for all buffers — must match alloc size for free().
        let obj_ctx = std::sync::Arc::new(super::context::ObjectContext::new_filling(buffers));
        objects.insert(oid, obj_ctx.clone());
        Some(obj_ctx)
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    pub fn utilization_ratio(&self) -> f64 {
        self.pool.utilization_ratio()
    }

    /// Total allocated bytes across live segments. Used by INFO largeobj.
    pub fn allocated_bytes(&self) -> usize {
        self.pool.allocated_bytes()
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

    /// Free segments that finished draining since the last cron tick.
    ///
    /// A segment marked draining is freed asynchronously: its memory is not
    /// reclaimed until all in-flight Arc holders drop and refcount reaches 0.
    /// This function scans for segments where `draining && refcount == 0` and
    /// physically frees them: takes the Segment out of its slot, clears the
    /// io_uring iovec slot (`clear_iovec`), and drops it. Each segment owns its
    /// own talc whose metadata lives inside the segment's memory, so dropping
    /// the Segment deallocates that memory and the talc vanishes with it — no
    /// `talc.truncate` or free-list surgery is needed (unlike the old shared
    /// allocator).
    ///
    /// Must be called from the Valkey main event-loop thread only.
    pub fn release_drained_segments(&self) {
        self.pool.release_all_releasable();
    }

    /// Add one segment to the pool, gated by BOTH the server-wide `maxmemory`
    /// (the real OOM boundary, via `would_cross_memory_watermark`) and the
    /// module-local `dram-maxmemory` sub-budget if set.
    ///
    /// Called reactively when alloc fails, or proactively when utilization > watermark.
    /// Returns the new iovec_index on success, `None` if either ceiling would be
    /// crossed. Must be called on the main event-loop thread (reads server memory).
    pub fn try_expand(&self, ctx: &valkey_module::Context) -> Option<u16> {
        // Server-wide OOM guard: never grow into memory the shrink path would
        // immediately reclaim. No-op when the server has no maxmemory configured.
        if crate::would_cross_memory_watermark(ctx, self.pool.segment_size as u64) {
            return None;
        }

        // Module-local sub-budget (optional): honor dram-maxmemory if set > 0.
        let dram_max = crate::dram_maxmemory();
        if dram_max > 0 {
            let current_bytes = self.pool.live_segment_count() * self.pool.segment_size;
            if current_bytes as u64 >= dram_max {
                return None;
            }
        }
        let result = self.pool.expand();
        if result.is_some() {
            self.expand_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Mark the segment with the least cached bytes draining and remove its objects.
    ///
    /// Victim selection: the non-draining segment with the fewest allocated bytes.
    /// Uses the per-segment `allocated_bytes` counter — O(live segments), no HashMap scan.
    /// This minimises NVMe fallback work after eviction — clients re-read the least data.
    ///
    /// **Tiered mode:** always safe — data persists on NVMe; GETs fall back.
    /// **Dram mode:** only allowed when the victim segment has zero allocated bytes.
    ///   If it has live data, shrink is skipped — there is no NVMe fallback.
    ///
    /// Returns true if a victim was selected, false if nothing to shrink.
    pub fn try_shrink(&self) -> bool {
        let (victim_idx, victim_bytes) = match self.pool.find_shrink_victim() {
            Some(v) => v,
            None => return false,
        };

        if crate::operating_mode() == crate::OperatingMode::Dram && victim_bytes > 0 {
            // Can't evict — data would be lost with no NVMe fallback.
            // No unmark needed: segment was never marked draining.
            return false;
        }

        // Commit: mark draining only now that we know eviction is safe.
        self.pool.mark_segment_draining(victim_idx);

        // Remove cached objects on the victim segment from the HashMap.
        // Tiered: data persists on NVMe. Dram: verified empty above.
        self.objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .retain(|_oid, ctx| {
                ctx.buffers
                    .iter()
                    .all(|b| b.segment_idx as usize != victim_idx)
            });

        self.shrink_count.fetch_add(1, Ordering::Relaxed);
        true
    }
}
