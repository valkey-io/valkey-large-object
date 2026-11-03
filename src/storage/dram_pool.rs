//! DRAMPool — long-lived cached objects.
//!
//! SegmentPool + RwLock<HashMap<ObjectId, Arc<ObjectContext>>>.
//!
//! Expand: adds one `segment-size` segment. The slow part (allocation and EFA
//! registration) runs with no pool lock held, and the segment joins the pool only
//! once registered. The scaling cron calls `grow_in_background()` when utilization exceeds
//! the expand watermark, which does this on a blocking thread. A Dram EFA SET whose alloc
//! misses starts the same background grow (or joins the one running) and waits for it in its
//! task (`alloc_exact_await_expand`), so the main thread never stalls. A Tiered promotion
//! that finds the pool full starts the background grow and is skipped. The Dram TCP SET
//! cannot wait: it calls `grow_inline()`, which grows the pool on the main thread and stalls
//! for it; if a grow is already running it goes on to eviction instead of waiting.
//!
//! Shrink (Tiered only): triggered proactively by the scaling cron when
//! `used_memory` approaches `maxmemory`. Picks the least-used segment, marks it
//! draining, and removes its cached copies so the segment releases once in-flight
//! readers drop. GETs fall back to NVMe; no key is touched. Dram never shrinks: that
//! would delete keys whatever the policy.
//!
//! Evict (Dram): a SET or COPY into a full arena evicts objects the
//! `maxmemory-policy` allows (`alloc_exact_expand_evict`), where the server allows
//! eviction. The evicted oids go on the reclaim list; their keys read as missing
//! until the scaling cron deletes them. The freed bytes stay in their segments as holes.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::task::Poll;

use super::cache_policy::{
    now_minutes, sample_victim, OIDIndexedMap, OIDIndexedSet, TieredCache, RECLAIM_MAX_SEGMENTS,
    RECLAIM_MAX_VICTIMS,
};
use super::context::{ObjectContext, SegmentBuffer};
use super::eviction_candidates;
use super::segment::Segment;
use super::segment_pool::SegmentPool;
use crate::data_type::ObjectId;

/// An eviction takes `EVICT_BATCH` victims, then tries the allocation, for at most
/// `EVICT_ROUNDS` rounds: a write evicts at most 16 victims, whatever their size.
const EVICT_BATCH: usize = 8;
const EVICT_ROUNDS: usize = 2;

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

    /// Remove every object in segment `seg` (Tiered shrink) and return their OIDs.
    fn remove_by_segment_id(&mut self, seg: usize) -> OIDIndexedSet {
        let Some(ids) = self.by_segment.get_mut(seg) else {
            return OIDIndexedSet::new();
        };
        let ids = std::mem::take(ids);
        for oid in &ids {
            self.all.swap_remove(oid);
        }
        ids
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
        let victim = sample_victim(&*ids, samples, |oid| {
            let ctx = self.all.get(oid)?;
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
    /// Cumulative count of requests that waited for an expand: a SET task in
    /// `alloc_exact_await_expand` (once, however many expands it waits for), or a caller that
    /// expanded the pool inline in `grow_inline`.
    pub alloc_wait_count: AtomicU64,
    /// Cumulative count of successful shrink operations since module load.
    pub shrink_count: AtomicU64,
    /// True while a grow runs. Only one runs at a time: the RDMA registration limit counts
    /// published segments only, so two concurrent grows could each pass it and together exceed it.
    expanding: AtomicBool,
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
            alloc_wait_count: AtomicU64::new(0),
            shrink_count: AtomicU64::new(0),
            expanding: AtomicBool::new(false),
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

    /// Dram mode, main thread only: allocate all buffers for an object. Allocate in the
    /// segments that exist; else grow the pool inline (`grow_inline`) and allocate in the
    /// new segment; else evict objects the `maxmemory-policy` allows (`alloc_exact_evict`).
    ///
    /// A miss that grows the pool stalls the caller for the whole registration. If a grow is
    /// already running (the cron's background one, or another miss's) it is not waited for:
    /// the alloc goes on to eviction, then fails.
    ///
    /// One grow suffices: an object is guaranteed <= `segment_size` (oversized ones are
    /// rejected at SET admission), so a fresh empty segment can hold it.
    ///
    /// `ctx` may be a dummy (null ctx is accepted by RM_GetServerInfo for the memory
    /// watermark check).
    pub fn alloc_exact_expand_evict(
        &self,
        ctx: &valkey_module::Context,
        obj_len: u64,
    ) -> Option<Vec<super::context::SegmentBuffer>> {
        assert!(
            crate::operating_mode() == crate::OperatingMode::Dram,
            "alloc_exact_expand_evict: Dram mode only"
        );
        assert!(
            crate::is_main_thread(),
            "alloc_exact_expand_evict: off the main thread"
        );
        if let Some(bufs) = self.alloc_exact(obj_len) {
            return Some(bufs);
        }
        if self.grow_inline(ctx) {
            if let Some(bufs) = self.alloc_exact(obj_len) {
                return Some(bufs);
            }
        }
        self.alloc_exact_evict(ctx, obj_len)
    }

    /// Allocate in the segments that exist. No grow, no eviction.
    pub fn alloc_exact(&self, obj_len: u64) -> Option<Vec<super::context::SegmentBuffer>> {
        self.pool.alloc_exact(obj_len as usize)
    }

    /// Evict objects the `maxmemory-policy` allows until `obj_len` fits. Dram mode. Needs the
    /// server lock: the main thread, or `ThreadSafeContext::lock()`.
    pub fn alloc_exact_evict(
        &self,
        ctx: &valkey_module::Context,
        obj_len: u64,
    ) -> Option<Vec<super::context::SegmentBuffer>> {
        if !crate::eviction_allowed(ctx) {
            return None;
        }
        self.evict_for(obj_len as usize)
    }

    /// Wait for room for `obj_len` in a SET task: join the grow that is running, or start one.
    /// Returns None once the pool cannot grow (watermark, RDMA limit, `MAX_SEGMENTS`).
    ///
    /// Waiting SETs queue in arrival order (`SegmentPool::wait_for`). When the one at the
    /// front has its room, the next usually finds room in the same new segment and
    /// needs no grow of its own. A SET that still finds the room taken (by a SET that fit
    /// without waiting) waits for the next grow. Each round ends with room, a grow that added a
    /// segment, or a refusal, so the wait ends. An admitted object fits an empty segment
    /// (`max-object-size` is validated against `segment-size`), so a fresh segment always has
    /// room for it.
    ///
    /// The wait lasts at most one expand, so it is not cancelled when the client disconnects.
    /// Dropping the future leaves the queue; the expand itself runs on regardless.
    pub async fn alloc_exact_await_expand(
        &self,
        obj_len: u64,
    ) -> Option<Vec<super::context::SegmentBuffer>> {
        let mut counted = false;
        self.pool
            .wait_for(|| {
                let attempt = self.alloc_or_start_expand(obj_len);
                // Count the request once, however many expands it waits for.
                if attempt.is_pending() && !std::mem::replace(&mut counted, true) {
                    self.alloc_wait_count.fetch_add(1, Ordering::Relaxed);
                }
                attempt
            })
            .await
    }

    /// One attempt for `alloc_exact_await_expand`, answering `wait_for`:
    /// - `Ready(Some(bufs))`: there is room.
    /// - `Pending`: no room, and an expand is running (or just started here). Wait for it.
    /// - `Ready(None)`: no room, and the pool may not expand.
    fn alloc_or_start_expand(&self, obj_len: u64) -> Poll<Option<Vec<SegmentBuffer>>> {
        if let Some(bufs) = self.pool.alloc_exact(obj_len as usize) {
            return Poll::Ready(Some(bufs));
        }
        // Null ctx is accepted by the memory watermark check.
        if self.grow_in_background(&valkey_module::Context::dummy()) {
            return Poll::Pending;
        }
        // The pool may not expand, but room can have appeared since the miss above: a free
        // from another thread, or the cron's expand (the queue does not serialize that one).
        Poll::Ready(self.pool.alloc_exact(obj_len as usize))
    }

    /// Evict `EVICT_BATCH` victims from the eviction candidates, try the allocation, and
    /// repeat for `EVICT_ROUNDS` rounds. A victim's freed bytes may sit in a segment that
    /// cannot take the write: they stay as a hole for later writes, and victims stay
    /// evicted when the write still does not fit.
    fn evict_for(&self, obj_len: usize) -> Option<Vec<SegmentBuffer>> {
        for _ in 0..EVICT_ROUNDS {
            let mut victims = Vec::new();
            // Lock order: reclaim list before `objects`.
            super::reclaim::RECLAIM_LIST.add_with(|| {
                let mut objects = self
                    .objects
                    .write()
                    .expect("DRAMPool.objects lock unavailable");
                // A victim has no reader: the Arc in the map is the only one.
                let oids = eviction_candidates::pick(
                    EVICT_BATCH,
                    |oid| matches!(objects.all.get(&oid), Some(ctx) if Arc::strong_count(ctx) == 1),
                );
                for oid in &oids {
                    eviction_candidates::remove(*oid);
                    victims.extend(objects.remove(oid));
                }
                oids
            });
            // No allowed victim is left.
            if victims.is_empty() {
                return None;
            }
            crate::info::EVICTIONS.fetch_add(victims.len() as u64, Ordering::Relaxed);
            // The last Arcs: the buffers return to their segments now.
            drop(victims);
            if let Some(bufs) = self.pool.alloc_exact(obj_len) {
                return Some(bufs);
            }
        }
        None
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

    /// See `SegmentPool::rdma_registered_count`.
    pub fn rdma_registered_count(&self) -> usize {
        self.pool.rdma_registered_count()
    }

    // ─── Object Map ──────────────────────────────────────────────────────────

    /// Lookup a cached object.
    ///
    /// Returns None if the object is not cached, or if its segment is draining
    /// (no new Arc refs on a draining segment, so the drain can finish).
    pub fn get_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        let arc = self
            .objects
            .read()
            .expect("DRAMPool.objects lock unavailable")
            .all
            .get(oid)
            .cloned()?;

        // An object never spans segments: buffers[0] names the segment of all of them.
        // A Filling object (promotion in progress) is returned too; callers check
        // `is_ready()`.
        if self.pool.is_segment_draining(arc.buffers[0].segment_idx) {
            return None;
        }
        Some(arc)
    }

    pub fn is_segment_draining(&self, seg_idx: u16) -> bool {
        self.pool.is_segment_draining(seg_idx)
    }

    /// Insert an ObjectContext (promotion path). Dram: the object is an eviction candidate.
    pub fn insert_object(&self, oid: ObjectId, ctx: Arc<ObjectContext>) {
        self.objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .insert(oid, ctx);
        if self.cache.is_none() {
            eviction_candidates::insert(oid);
        }
    }

    /// Remove an ObjectContext (free callback / eviction). Dram: also clears `oid`
    /// from the eviction candidates and the reclaim list: its key is gone. Map first, then
    /// list: an eviction that runs in between no longer sees the object, so cannot list it.
    /// Tiered: the file's teardown clears them instead.
    pub fn remove_object(&self, oid: &ObjectId) -> Option<Arc<ObjectContext>> {
        let removed = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .remove(oid);
        if self.cache.is_none() {
            eviction_candidates::remove(*oid);
            super::reclaim::RECLAIM_LIST.remove(oid);
        }
        removed
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
    /// `alloc_exact` with all-or-nothing semantics. If the pool is full, a promotion never
    /// waits or grows the pool itself: it starts the background grow (`grow_in_background`)
    /// and is skipped, so the GET is served from NVMe and a later GET promotes into the new
    /// segment. If the pool cannot grow (watermark, RDMA limit), reclaims cold cached copies
    /// in one segment and allocates there. Size limits are checked in admit().
    pub fn try_promote_object(
        &self,
        oid: ObjectId,
        obj_len: u64,
    ) -> Option<std::sync::Arc<super::context::ObjectContext>> {
        // Alloc BEFORE write lock -- talc scan under memory pressure
        // won't block GET readers waiting on get_object().
        // Promotion runs on tokio workers -- null ctx is accepted by RM_GetServerInfo.
        let dummy = valkey_module::Context::dummy();
        let buffers = match self.alloc_exact(obj_len) {
            Some(bufs) => bufs,
            // A grow is running or just started: skip this promotion.
            None if self.grow_in_background(&dummy) => return None,
            // The pool cannot grow: reclaim in one segment and allocate there. That can
            // still fail (talc fragmentation, or a racing alloc takes the room); then we
            // skip promotion and the GET is served from NVMe.
            None => self.make_room_for(obj_len as usize)?,
        };
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

    /// Tiered: allocate `obj_len` in one of the `RECLAIM_MAX_SEGMENTS` least-loaded
    /// segments, demoting cold cached copies from each in turn if needed. None if it
    /// cannot be made to fit.
    ///
    /// TODO: hold the SegmentPool state lock from `segment_shortfalls` through
    /// `alloc_exact_in` so a concurrent alloc cannot take the freed space. Needs
    /// lock-taking variants of the segment APIs and of the free path that
    /// `ObjectContext::drop` uses, and a fix for the lock order (shrink frees
    /// buffers while holding `objects`, so today the order is objects -> state).
    fn make_room_for(&self, obj_len: usize) -> Option<Vec<SegmentBuffer>> {
        for (seg, short) in self.pool.segment_shortfalls(obj_len, RECLAIM_MAX_SEGMENTS) {
            // short == 0: the bytes fit but talc overhead blocked the alloc, so
            // free at least one victim.
            let Some(victims) = self.reclaim_in(seg, short.max(1)) else {
                continue;
            };
            // take_victim only takes objects no reader holds, and removes them from the map,
            // so each Arc here is the last: dropping it frees its buffers to `seg` now.
            drop(victims);
            if let Some(bufs) = self.pool.alloc_exact_in(seg, obj_len) {
                return Some(bufs);
            }
        }
        None
    }

    /// Demote the lowest-scoring unpinned objects in segment `seg` until their bytes
    /// cover `need`, at most `RECLAIM_MAX_VICTIMS`. All or nothing: if they fall short,
    /// every victim goes back and this returns None. The victims are returned so they drop
    /// after the locks are released. Tiered mode only.
    fn reclaim_in(&self, seg: usize, need: usize) -> Option<Vec<(ObjectId, Arc<ObjectContext>)>> {
        let victims = self.take_demotion_victims(seg, need)?;
        self.tiered_cache()
            .stats
            .demotions
            .fetch_add(victims.len() as u64, Ordering::Relaxed);
        Some(victims)
    }

    /// The victim search of `reclaim_in`, under the `objects` write lock: take
    /// sampled lowest-scoring objects from `seg` until they cover `need`, at most
    /// `RECLAIM_MAX_VICTIMS`. If they cannot, restore them all and return None.
    fn take_demotion_victims(
        &self,
        seg: usize,
        need: usize,
    ) -> Option<Vec<(ObjectId, Arc<ObjectContext>)>> {
        let samples = crate::victim_sample_size();
        let now_min = now_minutes();
        let decay_time = crate::blob_decay_time();
        let mut objects = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable");
        let mut victims = Vec::new();
        let mut freed = 0usize;
        while freed < need && victims.len() < RECLAIM_MAX_VICTIMS {
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
        Some(victims)
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    pub fn utilization_ratio(&self) -> f64 {
        self.pool.utilization_ratio()
    }

    /// Total allocated bytes across non-draining segments. Used by INFO largeobj.
    pub fn allocated_bytes(&self) -> usize {
        self.pool.allocated_bytes()
    }

    /// Total free-gap count across non-draining segments — the fragmentation signal.
    /// Used by INFO largeobj.
    pub fn fragment_count(&self) -> usize {
        self.pool.fragment_count()
    }

    /// Counts of (total, draining, unused) segments; total includes draining. Used by INFO largeobj.
    pub fn segment_counts(&self) -> (usize, usize, usize) {
        self.pool.segment_counts()
    }

    /// Call `f` with each segment's base pointer and size. Used for EFA registration.
    pub fn with_segment_slices<F>(&self, f: F)
    where
        F: FnMut(*const u8, usize),
    {
        self.pool.with_segment_slices(f);
    }

    /// Free segments that finished draining since the last scaling cron tick.
    ///
    /// A segment marked draining is freed asynchronously: its memory is not
    /// reclaimed until all in-flight Arc holders drop and refcount reaches 0.
    /// This function scans for segments where `draining && refcount == 0` and
    /// physically frees them: takes the Segment out of its slot and drops it.
    /// Each segment owns its own talc whose metadata lives inside the segment's
    /// memory, so dropping the Segment deallocates that memory and the talc
    /// vanishes with it — no `talc.truncate` is needed.
    ///
    /// Must be called from the Valkey main event-loop thread only.
    pub fn release_drained_segments(&self) {
        self.pool.release_all_releasable();
    }

    /// Whether the pool may take another segment. Needs `ctx` (reads server memory), so it
    /// runs on the caller's thread.
    fn may_expand(&self, ctx: &valkey_module::Context) -> bool {
        // Server-wide OOM guard: never grow into memory the shrink path would
        // immediately reclaim. No-op when the server has no maxmemory configured.
        // Past the EFA registration limit fi_mr_reg fails, which is fatal.
        !(crate::would_cross_memory_watermark(ctx, self.pool.segment_size as u64)
            || super::would_exceed_rdma_limit(self.pool.segment_size as u64)
            || self.pool.segment_counts().0 >= super::MAX_SEGMENTS)
    }

    /// Build, register and publish one segment, then clear the `expanding` flag the caller set.
    /// Allocation and `fi_mr_reg` touch no pool state and no Valkey API, so this runs
    /// on any thread while the pool keeps serving; only `publish` takes the pool lock. The
    /// allocator cannot see the segment before it is registered. Returns whether it was published.
    fn add_segment(&self) -> bool {
        assert!(
            self.expanding.load(Ordering::Acquire),
            "add_segment: the caller must set `expanding` first"
        );
        let segment = Segment::new(self.pool.segment_size);
        // Register the new segment with EFA (fatal on failure — see efa_register_segment).
        crate::efa_register_segment(segment.slice());
        let published = self.pool.publish(segment).is_some();
        if published {
            self.expand_count.fetch_add(1, Ordering::Relaxed);
            // Rebuild + re-register the DRAM ring's fixed-buffer table so the new
            // segment joins the fixed path (no-op in Dram mode). The iovecs table rebuild runs
            // on the DRAM poller only; the NVMe ring is untouched and keeps serving.
            super::uring::submit_reregister(super::uring::PoolType::Dram);
        }
        self.expanding.store(false, Ordering::Release);
        self.pool.notify_space_available();
        published
    }

    /// Grow the pool by one segment in the background and return at once, gated by the
    /// server-wide `maxmemory` (via `would_cross_memory_watermark`). When the server has no
    /// `maxmemory` configured (0), there is no ceiling and the pool grows on demand — the
    /// same unbounded behavior as core Valkey with `maxmemory 0`.
    ///
    /// Returns whether an expand is now running: true if this call started one or one was already
    /// running, false if the pool may not expand (watermark, RDMA limit, `MAX_SEGMENTS`).
    pub fn grow_in_background(&self, ctx: &valkey_module::Context) -> bool {
        // Join the expand that is running. Only an expand that really runs ever sets the
        // flag, so a caller that sees it set can wait for the wake that ends it.
        if !self.expanding.load(Ordering::Acquire) {
            if !self.may_expand(ctx) {
                return false;
            }
            // Lost the race to another caller: that one runs the expand, this one joins it.
            if !self.expanding.swap(true, Ordering::AcqRel) {
                crate::runtime_handle().spawn_blocking(|| {
                    super::get_dram_pool().add_segment();
                });
            }
        }
        true
    }

    /// Grow the pool by one segment on the caller's thread: the last resort for a caller that
    /// cannot wait (Dram TCP SET, COPY). The caller stalls for the whole registration. Returns
    /// false, without growing, if the pool may not expand or an expand is already running (not
    /// waited for). Counted in `alloc_wait_count`.
    fn grow_inline(&self, ctx: &valkey_module::Context) -> bool {
        if !self.may_expand(ctx) || self.expanding.swap(true, Ordering::AcqRel) {
            return false;
        }
        // This caller stalls for the whole expand.
        self.alloc_wait_count.fetch_add(1, Ordering::Relaxed);
        self.add_segment()
    }

    pub fn try_shrink(&self) -> bool {
        assert!(
            crate::operating_mode() == crate::OperatingMode::Tiered,
            "try_shrink: Tiered only"
        );
        let Some((victim_idx, _)) = self.pool.find_shrink_victim() else {
            return false;
        };
        self.pool.mark_segment_draining(victim_idx);
        let removed = self
            .objects
            .write()
            .expect("DRAMPool.objects lock unavailable")
            .remove_by_segment_id(victim_idx);
        self.tiered_cache()
            .stats
            .demotions
            .fetch_add(removed.len() as u64, Ordering::Relaxed);

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
        assert!(p.reclaim_in(0, 1).is_none());
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

        let victims = p.reclaim_in(0, BUF as usize).unwrap();
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
        assert_eq!(p.tiered_cache().stats.demotions.load(Ordering::Relaxed), 1);
        drop(victims);
        assert_index_consistent(&p);

        // Pin the hot entry too: segment 0 is all pinned, so nothing goes.
        let _hot = p.get_object(&ObjectId(1)).unwrap();
        assert!(p.reclaim_in(0, BUF as usize).is_none());
        assert_eq!(p.object_count(), 3);
        assert_index_consistent(&p);
    }

    #[test]
    fn stops_once_need_is_covered() {
        let p = pool();
        for i in 0..4u16 {
            p.insert_object(ObjectId(i as u64), ctx(0, i));
        }
        let victims = p.reclaim_in(0, 2 * BUF as usize).unwrap();
        assert_eq!(victims.len(), 2, "no over-reclaim past the first fit");
        assert_eq!(p.object_count(), 2);
        assert_eq!(p.tiered_cache().stats.demotions.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn the_victim_cap_stops_short_and_rolls_back() {
        let p = pool();
        let n = RECLAIM_MAX_VICTIMS as u16 + 1;
        for i in 0..n {
            p.insert_object(ObjectId(i as u64), ctx(0, i));
        }
        assert!(p
            .take_demotion_victims(0, n as usize * BUF as usize)
            .is_none());
        assert_eq!(p.object_count(), n as usize);
    }

    #[test]
    fn rolls_back_when_segment_cannot_cover_need() {
        let p = pool();
        for i in 0..4u16 {
            p.insert_object(ObjectId(i as u64), ctx(0, i));
            p.insert_object(ObjectId(100 + i as u64), ctx(1, i));
        }
        // Segment 0 holds 4 BUF; other segments' bytes do not count.
        assert!(p.reclaim_in(0, 5 * BUF as usize).is_none());
        assert_eq!(p.object_count(), 8);
        assert_eq!(p.tiered_cache().stats.demotions.load(Ordering::Relaxed), 0);
        assert_index_consistent(&p);

        // Exactly what segment 0 holds goes through, and only segment 0 pays.
        let victims = p.reclaim_in(0, 4 * BUF as usize).unwrap();
        assert_eq!(oids(&victims), vec![0, 1, 2, 3]);
        assert!((100..104).all(|i| p.contains_object(&ObjectId(i))));
        drop(victims);
        assert_index_consistent(&p);
    }

    fn dram_pool() -> DRAMPool {
        DRAMPool::new(2, 1 << 20, None)
    }

    #[test]
    fn eviction_takes_a_batch_of_candidates_in_any_segment_and_spares_pinned_objects() {
        let p = dram_pool();
        for i in 0..3u16 {
            p.insert_object(ObjectId(9_400 + i as u64), ctx(i % 2, i));
        }
        let pinned = p.get_object(&ObjectId(9_401)).unwrap();
        let bufs = p.evict_for(BUF as usize).expect("room after the batch");
        p.free_n(&bufs);
        assert_eq!(p.object_count(), 1);
        assert!(p.contains_object(&ObjectId(9_401)));
        assert_index_consistent(&p);
        // The victims left the candidates too; only the pinned object is still there.
        let left = eviction_candidates::pick(usize::MAX, |oid| (9_400..9_403).contains(&oid.0));
        assert_eq!(left, [ObjectId(9_401)]);
        assert!(p.evict_for(BUF as usize).is_none(), "nothing left to evict");
        drop(pinned);
        for oid in [9_400, 9_402] {
            super::super::reclaim::RECLAIM_LIST.remove(&ObjectId(oid));
        }
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
