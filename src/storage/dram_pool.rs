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
use std::sync::{Arc, Mutex, RwLock};

use super::context::{ObjectContext, SegmentBuffer};
use super::policy::{now_minutes, GhostTable, IndexedMap, EVICT_MAX_VICTIMS, GHOST_CAPACITY};
use super::segment_pool::SegmentPool;
use crate::data_type::ObjectId;

pub struct DRAMPool {
    pool: SegmentPool,
    /// Cached objects: ObjectId → Arc<ObjectContext>, plus a slot index so
    /// eviction can sample at random.
    /// RwLock: main thread reads (GET hit), tokio writes (promotion insert).
    objects: RwLock<IndexedMap<Arc<ObjectContext>>>,
    /// Cumulative count of successful expand operations since module load.
    pub expand_count: AtomicU64,
    /// Cumulative count of successful shrink operations since module load.
    pub shrink_count: AtomicU64,
    /// Tiered GETs served from a Ready cached object.
    pub cache_hits: AtomicU64,
    /// Tiered GETs that found no Ready cached object (absent or still Filling).
    pub cache_misses: AtomicU64,
    /// Successful `try_promote_object` calls.
    pub promotions: AtomicU64,
    /// Tiered GET misses served transiently because the object had not yet
    /// accumulated `promote-min-hits` misses in the ghost table.
    pub admission_rejects: AtomicU64,
    /// Cached copies removed by `evict_cached_for`; keys untouched.
    pub evictions: AtomicU64,
    /// Second-touch admission filter. Touched only on the miss path.
    ghost: Mutex<GhostTable>,
}

impl DRAMPool {
    pub fn new(segment_count: usize, segment_size: usize) -> Self {
        Self {
            pool: SegmentPool::new(segment_count, segment_size),
            objects: RwLock::new(IndexedMap::new()),
            expand_count: AtomicU64::new(0),
            shrink_count: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            promotions: AtomicU64::new(0),
            admission_rejects: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            ghost: Mutex::new(GhostTable::new(GHOST_CAPACITY)),
        }
    }

    // ─── Allocator ───────────────────────────────────────────────────────────

    pub fn alloc_exact(&self, size: usize) -> Option<Vec<SegmentBuffer>> {
        self.pool.alloc_exact(size)
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

    /// Record a tiered GET served from `ctx`: bump the hit counter and touch
    /// the object's LFU score. Atomic only; no lock needed.
    pub fn record_hit(&self, ctx: &ObjectContext) {
        ctx.stats.touch(now_minutes(), crate::lfu_decay_time());
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tiered GET that could not be served from the cache.
    pub fn record_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Second-touch admission: true once the object has missed `promote-min-hits`
    /// times. Oversize objects are refused without touching the ghost table.
    /// Entries stay after promotion and age out FIFO (design doc §4.3).
    pub fn admit(&self, oid: ObjectId, obj_len: u64) -> bool {
        if obj_len > crate::max_promote_size() {
            return false;
        }
        let min_hits = crate::promote_min_hits();
        if min_hits <= 1 {
            return true;
        }
        let misses = self
            .ghost
            .lock()
            .expect("DRAMPool.ghost lock unavailable")
            .record_miss(oid);
        if misses >= min_hits {
            true
        } else {
            self.admission_rejects.fetch_add(1, Ordering::Relaxed);
            false
        }
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
    /// Returns None if no space can be found. On success returns
    /// Arc<ObjectContext> in Filling state — caller reads NVMe data into the
    /// buffers, then calls mark_ready().
    /// Multi-buffer: allocates ceil(obj_len / chunk_size) buffers via alloc_exact
    /// with all-or-nothing semantics.
    /// If the pool is full, evicts cold cached copies and retries once.
    pub fn try_promote_object(
        &self,
        oid: ObjectId,
        obj_len: u64,
    ) -> Option<std::sync::Arc<super::context::ObjectContext>> {
        // All-or-nothing: alloc_exact rolls back internally if pool can't satisfy.
        // Alloc BEFORE write lock — talc scan under memory pressure
        // won't block GET readers waiting on get_object().
        let mut buffers = self.alloc_exact(obj_len as usize);
        if buffers.is_none() && self.evict_cached_for(obj_len as usize) {
            buffers = self.alloc_exact(obj_len as usize);
        }
        let buffers = buffers?;
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
        self.promotions.fetch_add(1, Ordering::Relaxed);
        Some(obj_ctx)
    }

    // ─── Eviction ────────────────────────────────────────────────────────────

    /// Evict up to `EVICT_MAX_VICTIMS` low-score cached copies (Tiered only; the
    /// data stays on NVMe) to free `need` bytes. Evicts nothing if that plus free
    /// space can't cover `need`. True if anything was evicted.
    pub fn evict_cached_for(&self, need: usize) -> bool {
        let (count, _bytes) = self.evict_victims_with(need, self.free_bytes(), EVICT_MAX_VICTIMS);
        count > 0
    }

    /// Eviction loop with free space and the round limit explicit for tests.
    /// Only entries whose sole `Arc` is in the map are evicted.
    pub(crate) fn evict_victims_with(
        &self,
        need: usize,
        free_now: usize,
        max_victims: usize,
    ) -> (usize, usize) {
        let samples = crate::evict_sample_size();
        let now_min = now_minutes();
        let decay_time = crate::lfu_decay_time();
        let mut victims: Vec<(ObjectId, Arc<ObjectContext>)> = Vec::new();
        let mut freed = 0usize;
        {
            let mut objects = self
                .objects
                .write()
                .expect("DRAMPool.objects lock unavailable");
            for _ in 0..max_victims {
                if freed >= need {
                    break;
                }
                let Some((oid, ctx)) = objects.evict_one(samples, |ctx| {
                    (Arc::strong_count(ctx) == 1)
                        .then(|| ctx.stats.decayed_counter(now_min, decay_time))
                }) else {
                    break;
                };
                freed += ctx.buffers.iter().map(|b| b.len as usize).sum::<usize>();
                victims.push((oid, ctx));
            }
            if freed.saturating_add(free_now) < need {
                for (oid, ctx) in victims {
                    objects.insert(oid, ctx);
                }
                return (0, 0);
            }
        }
        let count = victims.len();
        self.evictions.fetch_add(count as u64, Ordering::Relaxed);
        drop(victims);
        (count, freed)
    }

    // ─── Expand / Shrink ─────────────────────────────────────────────────────

    /// Unallocated bytes across live segments. Ignores talc overhead, so it is
    /// an upper bound; used only to rule out hopeless evictions.
    fn free_bytes(&self) -> usize {
        (self.pool.live_segment_count() * self.pool.segment_size)
            .saturating_sub(self.pool.allocated_bytes())
    }

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

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::policy::LFU_INIT_VAL;

    const BUF: u32 = 4096;

    fn pool() -> DRAMPool {
        DRAMPool::new(1, 1 << 20)
    }

    /// A Ready context with one fake buffer. DRAM_POOL is unset in unit tests,
    /// so Drop does not try to free it.
    fn ctx(slot: u16) -> Arc<ObjectContext> {
        Arc::new(ObjectContext::new_ready(vec![SegmentBuffer {
            segment_idx: 0,
            offset: slot as u64 * BUF as u64,
            len: BUF,
        }]))
    }

    #[test]
    fn evicts_lowest_score_and_skips_pinned() {
        let p = pool();
        // Empty map: nothing to sample.
        assert_eq!(p.evict_victims_with(1, 0, 16), (0, 0));
        let hot = ctx(0);
        hot.stats.set(LFU_INIT_VAL + 10, now_minutes());
        let pinned = ctx(2); // counter 5 but held by us
        p.insert_object(ObjectId(1), hot);
        p.insert_object(ObjectId(2), ctx(1)); // counter 5
        p.insert_object(ObjectId(3), pinned.clone());

        let (n, bytes) = p.evict_victims_with(1, 0, 1);
        assert_eq!((n, bytes), (1, BUF as usize));
        assert!(p.contains_object(&ObjectId(1)), "hot object must survive");
        assert!(
            !p.contains_object(&ObjectId(2)),
            "cold object is the victim"
        );
        assert!(
            p.contains_object(&ObjectId(3)),
            "pinned object must survive"
        );
        assert_eq!(p.evictions.load(Ordering::Relaxed), 1);

        // Pin the hot entry too: every entry is now pinned, so nothing goes.
        let _hot = p.get_object(&ObjectId(1)).unwrap();
        assert_eq!(p.evict_victims_with(1, 0, 16), (0, 0));
        assert_eq!(p.object_count(), 2);
    }

    #[test]
    fn stops_once_need_is_covered() {
        let p = pool();
        for i in 0..4u16 {
            p.insert_object(ObjectId(i as u64), ctx(i));
        }
        let (n, bytes) = p.evict_victims_with(2 * BUF as usize, 0, 16);
        assert_eq!((n, bytes), (2, 2 * BUF as usize));
        assert_eq!(p.object_count(), 2);
    }

    #[test]
    fn evicts_nothing_when_victims_cannot_cover_need() {
        let p = pool();
        for i in 0..4u16 {
            p.insert_object(ObjectId(i as u64), ctx(i));
        }
        // The 2-round cap frees 2 BUF, plus 1 BUF free: short of 4 BUF. Roll back.
        assert_eq!(
            p.evict_victims_with(4 * BUF as usize, BUF as usize, 2),
            (0, 0)
        );
        assert_eq!(p.object_count(), 4);
        assert_eq!(p.evictions.load(Ordering::Relaxed), 0);
        // The same need with enough free space goes through.
        let (n, _) = p.evict_victims_with(4 * BUF as usize, 2 * BUF as usize, 2);
        assert_eq!(n, 2);
        assert_eq!(p.object_count(), 2);
    }
}
