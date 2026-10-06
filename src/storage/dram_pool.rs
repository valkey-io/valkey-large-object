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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::cache_policy::{
    now_minutes, sample_victim, OIDIndexedMap, OIDIndexedSet, TieredCache, RECLAIM_MAX_VICTIMS,
};
use super::context::{ObjectContext, SegmentBuffer};
use super::segment_pool::SegmentPool;
use crate::data_type::ObjectId;

/// Cached objects plus a per-segment index of the same OIDs, so reclaim can
/// sample one segment. Both change together, only through `insert`/`remove`.
#[derive(Default)]
struct ObjectMaps {
    all: OIDIndexedMap<Arc<ObjectContext>>,
    /// `by_segment[s]`: OIDs of the objects in segment `s`. An object never
    /// spans segments, so its first buffer names its segment.
    by_segment: Vec<OIDIndexedSet>,
}

impl ObjectMaps {
    fn insert(&mut self, oid: ObjectId, ctx: Arc<ObjectContext>) {
        let s = ctx.buffers[0].segment_idx as usize;
        if s >= self.by_segment.len() {
            self.by_segment.resize_with(s + 1, OIDIndexedSet::new);
        }
        self.by_segment[s].insert(oid);
        let old = self.all.insert(oid, ctx);
        debug_assert!(old.is_none(), "OIDs are never reused");
    }

    fn remove(&mut self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        let ctx = self.all.swap_remove(oid)?;
        self.by_segment[ctx.buffers[0].segment_idx as usize].swap_remove(oid);
        Some(ctx)
    }

    /// Remove every object in segment `seg` (shrink).
    fn remove_by_segment_id(&mut self, seg: usize) {
        let Some(ids) = self.by_segment.get_mut(seg) else {
            return;
        };
        for oid in std::mem::take(ids) {
            self.all.swap_remove(&oid);
        }
    }

    /// Remove and return the lowest-scoring unpinned object among up to
    /// `samples` drawn from segment `seg`.
    fn take_victim(
        &mut self,
        seg: usize,
        samples: usize,
        now_min: u16,
        decay_time: u64,
    ) -> Option<(ObjectId, Arc<ObjectContext>)> {
        let ids = self.by_segment.get_mut(seg)?;
        let victim = sample_victim(ids.len(), samples, |index| {
            let ctx = self.all.get(ids.get_index(index)?)?;
            (Arc::strong_count(ctx) == 1).then(|| ctx.stats.decayed_counter(now_min, decay_time))
        })?;
        let oid = ids.swap_remove_index(victim)?;
        Some((oid, self.all.swap_remove(&oid)?))
    }
}

pub struct DRAMPool {
    pool: SegmentPool,
    /// Cached objects: ObjectId → Arc<ObjectContext>, plus a per-segment index
    /// so reclaim can sample one segment at random.
    /// RwLock: main thread reads (GET hit), tokio writes (promotion insert).
    objects: RwLock<ObjectMaps>,
    /// Cumulative count of successful expand operations since module load.
    pub expand_count: AtomicU64,
    /// Cumulative count of successful shrink operations since module load.
    pub shrink_count: AtomicU64,
    /// Cached objects removed by `make_room_for` to free space.
    pub reclaims: AtomicU64,
    /// Admission filter and cache stats. `Some` in Tiered mode, `None` in Dram
    /// mode where the DRAMPool is the data, not a cache.
    pub cache: Option<TieredCache>,
}

impl DRAMPool {
    pub fn new(segment_count: usize, segment_size: usize, cache: Option<TieredCache>) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size, super::uring::PoolType::Dram),
            objects: RwLock::new(ObjectMaps::default()),
            expand_count: AtomicU64::new(0),
            shrink_count: AtomicU64::new(0),
            reclaims: AtomicU64::new(0),
            cache,
        }
    }

    /// The Tiered cache state. Tiered mode only; panics in Dram mode.
    pub fn tiered_cache(&self) -> &TieredCache {
        self.cache
            .as_ref()
            .expect("DRAMPool tiered cache is Tiered-mode only")
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    /// Allocate all buffers for an object, expanding once if needed.
    /// Try `alloc_exact`; on failure expand a single segment and retry.
    ///
    /// One expand suffices: an object is guaranteed <= `segment_size` (oversized
    /// ones are rejected at SET admission), so a fresh empty segment can hold it.
    /// If even a fresh segment can't (talc overhead on an object right at the
    /// boundary), no same-size segment can — so we return None rather than loop.
    ///
    /// Callers on the main thread pass their command `&Context`; callers on
    /// tokio workers or data-type callbacks pass `&Context::dummy()` (null ctx
    /// is accepted by RM_GetServerInfo for the memory watermark check).
    pub fn alloc_exact_or_expand(
        &self,
        ctx: &valkey_module::Context,
        obj_len: u64,
    ) -> Option<Vec<super::context::SegmentBuffer>> {
        if let Some(bufs) = self.pool.alloc_exact(obj_len as usize) {
            return Some(bufs);
        }
        // Existing segments are full for this object. Expand once (None if the
        // server maxmemory watermark would be crossed) and try the fresh segment.
        self.try_expand(ctx)?;
        self.pool.alloc_exact(obj_len as usize)
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

    /// Whether the segment owning `buf` is registered in the io_uring kernel
    /// buffer table (picks fixed vs non-fixed I/O). See SegmentPool.
    pub fn is_buf_io_uring_registered(&self, buf: &SegmentBuffer) -> bool {
        self.pool.is_buf_io_uring_registered(buf)
    }

    /// Mark all current segments io_uring-registered (startup, post-register).
    pub fn mark_all_registered(&self) {
        self.pool.mark_all_registered();
    }

    /// Startup iovec snapshot for this pool's ring. See `SegmentPool::startup_iovecs`.
    pub fn startup_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.startup_iovecs()
    }

    /// See `SegmentPool::rebuild_dense_iovecs`. Called by this pool's io_uring
    /// engine on a re-registration.
    pub fn rebuild_dense_iovecs(&self) -> Vec<libc::iovec> {
        self.pool.rebuild_dense_iovecs()
    }

    /// See `SegmentPool::io_uring_registered_count`.
    pub fn io_uring_registered_count(&self) -> usize {
        self.pool.io_uring_registered_count()
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
            .all
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
            .all
            .contains_key(oid)
    }

    /// Number of cached objects.
    pub fn object_count(&self) -> usize {
        self.objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .all
            .len()
    }

    /// Tiered mode only: promotes an NVMe object into the DRAM cache.
    /// Try to allocate space and create an ObjectContext for this object.
    /// Returns None if no space can be found. On success returns
    /// Arc<ObjectContext> in Filling state -- caller reads NVMe data into the
    /// buffers, then calls mark_ready().
    /// Multi-buffer: allocates ceil(obj_len / chunk_size) buffers via
    /// alloc_exact_or_expand with all-or-nothing semantics.
    /// If that fails (expansion refused at the maxmemory watermark), reclaims
    /// cold cached copies in one segment and allocates there. Size limits are
    /// checked in admit().
    pub fn try_promote_object(
        &self,
        oid: ObjectId,
        obj_len: u64,
    ) -> Option<std::sync::Arc<super::context::ObjectContext>> {
        // All-or-nothing with reactive expansion via dummy context (promotion
        // runs on tokio workers -- null ctx is accepted by RM_GetServerInfo).
        // Alloc BEFORE write lock -- talc scan under memory pressure
        // won't block GET readers waiting on get_object().
        let dummy = valkey_module::Context::dummy();
        // No space: reclaim in one segment and allocate there. That can still
        // fail (talc fragmentation, or a racing alloc takes the room); then we
        // skip promotion and the GET is served from NVMe.
        let buffers = self
            .alloc_exact_or_expand(&dummy, obj_len)
            .or_else(|| self.make_room_for(obj_len as usize))?;
        // Atomic check-and-insert under write lock to prevent TOCTOU race
        // (concurrent GETs promoting the same OID simultaneously).
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        if objects.all.contains_key(&oid) {
            self.free_n(&buffers);
            return None;
        }
        // buf.len stays chunk_size for all buffers — must match alloc size for free().
        let obj_ctx = std::sync::Arc::new(super::context::ObjectContext::new_filling(buffers));
        objects.insert(oid, obj_ctx.clone());
        self.tiered_cache()
            .stats
            .promotions
            .fetch_add(1, Ordering::Relaxed);
        Some(obj_ctx)
    }

    // ─── Reclaim ─────────────────────────────────────────────────────────────

    /// Reclaim cold cached copies (the data stays on NVMe) from the segment
    /// `alloc_exact` would pick, then allocate `obj_len` there. That segment
    /// is the least loaded, so it needs the fewest victims. None if it cannot
    /// be made to fit.
    ///
    /// TODO: hold the SegmentPool state lock from `segment_shortfall` through
    /// `alloc_exact_in` so a concurrent alloc cannot take the freed space. Needs
    /// lock-taking variants of the segment APIs and of the free path that
    /// `ObjectContext::drop` uses, and a fix for the lock order (shrink frees
    /// buffers while holding `objects`, so today the order is objects -> state).
    fn make_room_for(&self, obj_len: usize) -> Option<Vec<SegmentBuffer>> {
        let (seg, short) = self.pool.segment_shortfall(obj_len)?;
        // short == 0: the bytes fit but talc overhead blocked the alloc, so
        // free at least one victim.
        let victims = self.reclaim_in(seg, short.max(1), RECLAIM_MAX_VICTIMS)?;
        // Each victim's Arc is its last: dropping returns its buffers to `seg`.
        drop(victims);
        self.pool.alloc_exact_in(seg, obj_len)
    }

    /// Remove the lowest-scoring unpinned objects in segment `seg` until their
    /// bytes cover `need`, at most `max_victims`. All or nothing: if they fall
    /// short, every victim goes back and this returns None. The victims are
    /// returned so they drop after the write lock is released.
    fn reclaim_in(
        &self,
        seg: usize,
        need: usize,
        max_victims: usize,
    ) -> Option<Vec<(ObjectId, Arc<ObjectContext>)>> {
        let samples = crate::reclaim_sample_size();
        let now_min = now_minutes();
        let decay_time = crate::tiered_decay_time();
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        let mut victims = Vec::new();
        let mut freed = 0usize;
        while freed < need && victims.len() < max_victims {
            let Some((oid, ctx)) = objects.take_victim(seg, samples, now_min, decay_time) else {
                break;
            };
            freed += ctx.buffers.iter().map(|b| b.len as usize).sum::<usize>();
            victims.push((oid, ctx));
        }
        if freed < need {
            for (oid, ctx) in victims {
                objects.insert(oid, ctx);
            }
            return None;
        }
        self.reclaims
            .fetch_add(victims.len() as u64, Ordering::Relaxed);
        Some(victims)
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    pub fn utilization_ratio(&self) -> f64 {
        self.pool.utilization_ratio()
    }

    /// Total allocated bytes across live segments. Used by INFO largeobj.
    pub fn allocated_bytes(&self) -> usize {
        self.pool.allocated_bytes()
    }

    /// Total free-gap count across live segments — the fragmentation signal.
    /// Used by INFO largeobj.
    pub fn fragment_count(&self) -> usize {
        self.pool.fragment_count()
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

    /// Free segments that finished draining since the last scaling cron tick.
    ///
    /// A segment marked draining is freed asynchronously: its memory is not
    /// reclaimed until all in-flight Arc holders drop and refcount reaches 0.
    /// This function scans for segments where `draining && refcount == 0` and
    /// physically frees them: takes the Segment out of its slot, nulls this
    /// pool's io_uring iovec slot, and drops it. Each segment owns its
    /// own talc whose metadata lives inside the segment's memory, so dropping
    /// the Segment deallocates that memory and the talc vanishes with it — no
    /// `talc.truncate` is needed.
    ///
    /// Must be called from the Valkey main event-loop thread only.
    pub fn release_drained_segments(&self) {
        self.pool.release_all_releasable();
    }

    /// Add one segment to the pool, gated by the server-wide `maxmemory` (via
    /// `would_cross_memory_watermark`). When the server has no `maxmemory`
    /// configured (0), there is no ceiling and the pool grows on demand — the
    /// same unbounded behavior as core Valkey with `maxmemory 0`.
    ///
    /// Called reactively when alloc fails, or proactively when utilization > watermark.
    /// Returns the new iovec_index on success, `None` if the watermark would be
    /// crossed. Must be called on the main event-loop thread (reads server memory).
    pub fn try_expand(&self, ctx: &valkey_module::Context) -> Option<u16> {
        // Server-wide OOM guard: never grow into memory the shrink path would
        // immediately reclaim. No-op when the server has no maxmemory configured.
        if crate::would_cross_memory_watermark(ctx, self.pool.segment_size as u64) {
            return None;
        }

        let (idx, slice) = self.pool.expand()?;
        self.expand_count.fetch_add(1, Ordering::Relaxed);

        // Register the new segment with EFA (fatal on failure — see efa_register_segment).
        crate::efa_register_segment(slice);
        // Rebuild + re-register the DRAM ring's fixed-buffer table so the new
        // segment joins the fixed path (no-op in Dram mode). The iovecs table rebuild runs
        // on the DRAM poller only; the NVMe ring is untouched and keeps serving.
        super::uring::submit_reregister(super::uring::PoolType::Dram);
        Some(idx)
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
            .remove_by_segment_id(victim_idx);

        self.shrink_count.fetch_add(1, Ordering::Relaxed);
        true
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cache_policy::LFU_INIT_VAL;

    const BUF: u32 = 4096;

    fn pool() -> DRAMPool {
        DRAMPool::new(2, 1 << 20, Some(TieredCache::default()))
    }

    /// A Ready context with one fake buffer in segment `seg`. DRAM_POOL is
    /// unset in unit tests, so Drop does not try to free it.
    fn ctx(seg: u16, slot: u16) -> Arc<ObjectContext> {
        Arc::new(ObjectContext::new_ready(vec![SegmentBuffer {
            segment_idx: seg,
            offset: slot as u64 * BUF as u64,
            len: BUF,
        }]))
    }

    fn oids(victims: &[(ObjectId, Arc<ObjectContext>)]) -> Vec<u64> {
        let mut v: Vec<u64> = victims.iter().map(|(oid, _)| oid.0).collect();
        v.sort();
        v
    }

    /// Every cached object is indexed under its first buffer's segment, and
    /// the index holds nothing else.
    fn assert_index_consistent(p: &DRAMPool) {
        let m = p.objects.read().unwrap();
        for (oid, ctx) in &m.all {
            let s = ctx.buffers[0].segment_idx as usize;
            assert!(m.by_segment[s].contains(oid), "{oid:?} not indexed");
        }
        let indexed: usize = m.by_segment.iter().map(|ids| ids.len()).sum();
        assert_eq!(indexed, m.all.len());
    }

    #[test]
    fn reclaims_lowest_unpinned_in_target_segment_only() {
        let p = pool();
        // Empty segment: nothing to take.
        assert!(p.reclaim_in(0, 1, 16).is_none());
        let hot = ctx(0, 0);
        hot.stats.set(LFU_INIT_VAL + 10, now_minutes());
        let pinned = ctx(0, 2); // counter 5 but held by us
        p.insert_object(ObjectId(1), hot);
        p.insert_object(ObjectId(2), ctx(0, 1)); // counter 5
        p.insert_object(ObjectId(3), pinned.clone());
        // Colder than anything in segment 0, but in another segment.
        let other = ctx(1, 0);
        other.stats.set(0, now_minutes());
        p.insert_object(ObjectId(10), other);

        let victims = p.reclaim_in(0, BUF as usize, 16).unwrap();
        assert_eq!(oids(&victims), vec![2], "cold object is the victim");
        assert!(p.contains_object(&ObjectId(1)), "hot object must survive");
        assert!(
            p.contains_object(&ObjectId(3)),
            "pinned object must survive"
        );
        assert!(
            p.contains_object(&ObjectId(10)),
            "other segment is untouched"
        );
        assert_eq!(p.reclaims.load(Ordering::Relaxed), 1);
        drop(victims);
        assert_index_consistent(&p);

        // Pin the hot entry too: segment 0 is all pinned, so nothing goes.
        let _hot = p.get_object(&ObjectId(1)).unwrap();
        assert!(p.reclaim_in(0, BUF as usize, 16).is_none());
        assert_eq!(p.object_count(), 3);
        assert_index_consistent(&p);
    }

    #[test]
    fn stops_once_need_is_covered() {
        let p = pool();
        for i in 0..4u16 {
            p.insert_object(ObjectId(i as u64), ctx(0, i));
        }
        let victims = p.reclaim_in(0, 2 * BUF as usize, 16).unwrap();
        assert_eq!(victims.len(), 2, "no over-reclaim past the first fit");
        assert_eq!(p.object_count(), 2);
        assert_eq!(p.reclaims.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn rolls_back_when_segment_cannot_cover_need() {
        let p = pool();
        for i in 0..4u16 {
            p.insert_object(ObjectId(i as u64), ctx(0, i));
            p.insert_object(ObjectId(100 + i as u64), ctx(1, i));
        }
        // Segment 0 holds 4 BUF; other segments' bytes do not count.
        assert!(p.reclaim_in(0, 5 * BUF as usize, 16).is_none());
        // The victim cap stops short of 3 BUF.
        assert!(p.reclaim_in(0, 3 * BUF as usize, 2).is_none());
        assert_eq!(p.object_count(), 8);
        assert_eq!(p.reclaims.load(Ordering::Relaxed), 0);
        assert_index_consistent(&p);

        // Exactly what segment 0 holds goes through, and only segment 0 pays.
        let victims = p.reclaim_in(0, 4 * BUF as usize, 16).unwrap();
        assert_eq!(oids(&victims), vec![0, 1, 2, 3]);
        assert!((100..104).all(|i| p.contains_object(&ObjectId(i))));
        drop(victims);
        assert_index_consistent(&p);
    }

    #[test]
    fn index_tracks_insert_remove_and_shrink() {
        let p = pool();
        p.insert_object(ObjectId(1), ctx(0, 0));
        p.insert_object(ObjectId(2), ctx(1, 0));
        p.insert_object(ObjectId(3), ctx(1, 1));
        assert_index_consistent(&p);

        p.remove_object(&ObjectId(2));
        // Remove then insert in another segment: the index entry moves with it.
        p.remove_object(&ObjectId(1));
        p.insert_object(ObjectId(1), ctx(1, 2));
        assert_index_consistent(&p);
        assert!(p.objects.read().unwrap().by_segment[0].is_empty());

        // Shrink drains one segment's index and leaves the others alone.
        p.insert_object(ObjectId(4), ctx(0, 3));
        p.insert_object(ObjectId(5), ctx(0, 4));
        p.objects.write().unwrap().remove_by_segment_id(0);
        assert!(!p.contains_object(&ObjectId(4)));
        assert!(!p.contains_object(&ObjectId(5)));
        assert_eq!(p.object_count(), 2, "segment 1 objects survive");
        assert_index_consistent(&p);
        // A segment that never held anything is a no-op.
        p.objects.write().unwrap().remove_by_segment_id(7);
        assert_eq!(p.object_count(), 2);
    }
}
