# DRAM and FD Pool Caching Policy

Companion to `STORAGE_DESIGN.md` sections 5.2, 6.4 and 6.5.

## 1. Problem

Tiered mode uses the DRAMPool as a read cache in front of NVMe. A cache needs a replacement policy: without one, a full pool can never take in a newly hot object, and whatever was promoted first stays resident. `DRAMPool::try_shrink` does not fill that role. It drops every object on the least populated segment when server memory crosses `scaling-shrink-watermark`, to return memory to core Valkey, and it does not run when the pool is simply full.

`FdPool` has the same need. On a 120 TB tier the file count can reach the millions, so an uncapped fd cache runs out of file descriptors on a large enough keyspace.

This document defines one policy component and applies it to both pools.

## 2. Workload

The target workload is LLM KV-cache reuse (LMCache via Glide). Its access pattern has three properties that drive the policy choice:

1. Heavily skewed popularity. Shared system prompts and common prefixes are read continuously. Per-conversation suffix chunks are read a handful of times and then never again.
2. Scan-like churn. A long conversation streams many chunks through the cache, most of which are touched once. Under LRU this flushes the hot prefixes.
3. Popularity moves. A prefix that was hot for an hour goes cold when its conversation ends. Under pure LFU it would keep its count forever and never be displaced.

The cost model is also unusual for a cache:

1. A miss costs one NVMe read either way. A promoted miss reads straight into DRAMPool buffers via `ReadFixed`; a non-promoted miss reads into a transient NVMePool buffer. Same I/O, zero extra copies. Admission is therefore free in I/O terms and only spends DRAM capacity.
2. Reclaim is a map remove. The buffers return to the owning segment's talc when the last `Arc<ObjectContext>` drops.
3. The benefit of a DRAM hit scales with object size. Large objects are bandwidth bound on the drive, while DRAM feeds the EFA link at line rate. Benefit per byte is roughly constant, so a size-normalized score (GDSF style, frequency divided by size) adds complexity without changing the ranking. Size matters only operationally: promoting a large object may require several reclaims.

## 3. Decision

Use Valkey's approximated LFU (a logarithmic frequency counter with time-based decay) as the score, a second-touch admission filter in front of promotion, and bounded inline reclaim on the promotion path.

Why this and not the alternatives:

| Option | Verdict | Reason |
|---|---|---|
| LRU | Rejected | Scan churn from one-touch chunks flushes hot prefixes. |
| Pure LFU (no aging) | Rejected | Stale hot objects are never displaced. |
| LFU with time decay (Valkey style) | Chosen | Frequency wins the steady state, decay lets popularity move. Operators already understand `lfu-decay-time`. This is an LFRU in practice. |
| W-TinyLFU / ARC | Rejected for now | Solves one-touch pollution through a sketch or ghost lists. Our object counts are small (DRAM divided by MB-scale objects) and the same protection comes from a small second-touch admission filter. Revisit only if measurements show the simple filter is insufficient. |
| Size-normalized score (GDSF) | Rejected | Benefit per byte is constant, see section 2. |
| Cron-only reclaim | Rejected | Leaves the cache frozen between ticks; a full pool would reject promotions for up to `scaling-poll-ms`. Inline reclaim is bounded and runs only on misses. |

The policy applies to Tiered mode only. In Dram mode the DRAMPool is the data, so dropping an entry is data loss, not a cache decision.

### 3.1 Configs and constants

Four runtime-mutable configs:

| Config | Default | Range | Meaning |
|---|---|---|---|
| `tiered-decay-time` | 1 | 0..=65535 | Minutes per one-point counter decay. 0 disables decay (pure LFU). Same semantics as core `lfu-decay-time`. |
| `promote-min-hits` | 2 | 1..=255 | Misses an object must accumulate before a GET promotes it. 1 promotes on the first GET. |
| `reclaim-sample-size` | 5 | 1..=64 | Entries sampled per reclaim round. Same idea as core's `maxmemory-samples`. |
| `max-cached-fds` | 1024 | 0..=1048576 | Cap on cached read fds. 0 means unlimited. The default stays well under Valkey's fd limit (about `maxclients + 32`), which the module's fds share with client sockets. |

Three tuning values are constants in `src/storage/cache_policy.rs`, because no operator has a reason to change them:

| Constant | Value | Reason |
|---|---|---|
| `LFU_LOG_FACTOR` | 10 | Core's default for `lfu-log-factor`; almost never tuned. |
| `ADMISSION_FILTER_CAPACITY` | 65536 | `promote-min-hits 1` is the single off switch for admission. |
| `RECLAIM_MAX_VICTIMS` | 16 | A loop bound, not a policy knob. |

## 4. Design

### 4.1 Score: packed LFU counter

Each cached entry carries one `AtomicU32`, laid out like Valkey's 24-bit `lru` field:

```
bits 0..8    counter        logarithmic frequency, 0..=255, starts at LFU_INIT_VAL (5)
bits 8..24   last_decr_min  minute (from now_minutes) of the last decay, wraps at 65536
```

Operations, all on `AccessStats` in `src/storage/cache_policy.rs`:

1. `touch(now_min, decay_time)`: decay the counter for the elapsed minutes, then increment with probability `p = 1 / ((counter - INIT) * LFU_LOG_FACTOR + 1)`, then store with `now_min` stamped in. At `INIT` the probability is 1, so one touch always outranks an untouched entry. Two racing touches can lose one increment, as in Valkey.
2. `decayed_counter(now_min, decay_time)`: the counter minus `elapsed / decay_time` periods, saturating at 0. Reclaim ranks by this. It never writes back. `decay_time = 0` disables decay.
3. `new(now_min)`: counter at `LFU_INIT_VAL`, stamped now. A fresh entry ranks below any entry that has been hit and above one that has fully decayed, as in Valkey.

`now_minutes()` counts minutes since its first call, wrapping at 16 bits; elapsed time uses `wrapping_sub`, so the wrap is harmless unless an entry goes untouched for more than 45 days, when it looks recent again. Callers read it once per operation and pass it down.

### 4.2 Where the score lives

`ObjectContext` has `pub stats: AccessStats`. The tiered GET hit path calls `CacheStats::record_hit`, which touches the score and counts the hit, with no write lock. The touch is not inside `get_object`, because `BLOB.INFO key TIER` and `COPY` also use `get_object` and are not cache accesses.

Stats are keyed by `ObjectId`, not by key. An `BLOB.SET` mints a new OID and the free callback drops the old DRAM entry, so a rewritten object starts with fresh stats. A RENAME is invisible to the policy.

### 4.3 Admission: second-touch filter

A miss (object not in the DRAM map) consults the `AdmissionFilter` before promoting:

```rust
pub struct AdmissionFilter {
    misses: Mutex<MissLog>,        // counts: HashMap<ObjectId, u8> + FIFO order
    capacity: usize,               // oldest dropped at capacity
    pub rejects: AtomicU64,
}

pub struct TieredCache {           // DRAMPool.cache, None in Dram mode
    pub admission: AdmissionFilter,
    pub stats: CacheStats,         // hits, misses, promotions
}
```

1. On a miss, `AdmissionFilter::admit(oid, obj_len)` calls `track(oid)`, which returns the object's miss count, inserting it at 1 if absent and dropping the FIFO head when full. A repeat miss does not move an entry in the FIFO, so the window is the last `ADMISSION_FILTER_CAPACITY` distinct missed objects.
2. If the count is at least `promote-min-hits`, promote. Otherwise serve through the transient NVMePool path and count an admission reject. The filter entry is not removed on promotion; it ages out FIFO. So an admitted object whose promotion fails is admitted again on its next miss, and an object reclaimed from DRAM while its filter entry is still present is promoted again on its first miss (an ARC-style ghost hit).
3. Objects above `max-promote-size` are refused without touching the filter, since they can never be promoted.
4. With `promote-min-hits 1`, `admit` returns true without touching the filter, so admission is off and costs nothing. Misses in that time are not counted, so after the setting is raised those objects start from zero.

The filter's miss log is a `Mutex` taken only in `track`, never together with the objects lock. ObjectIds are never reused, so an entry for a deleted object just ages out. Nothing is preallocated, so a pool that never misses pays nothing for the table.

A GET that finds a `Filling` entry (another GET is promoting the same object) skips both `admit` and `try_promote_object`, so concurrent GETs on one object do not inflate its miss count.

### 4.4 Reclaim: one segment, bounded, sampled

`DRAMPool::try_promote_object` allocates (all-or-nothing) before taking the write lock. When that fails:

```
buffers = alloc_exact_or_expand(obj_len)
    or make_room_for(obj_len)               // None: caller falls back to NVMe
```

Every object lives in one segment (`alloc_exact`), so freeing bytes spread over several segments cannot help. `make_room_for` works on one segment:

1. `SegmentPool::segment_shortfall(obj_len)` returns the segment `alloc_exact` would pick (the least loaded live one) and how many bytes it is short, using the same per-chunk aligned sizes as the alloc. Short is 0 when the bytes fit but talc's per-chunk tags blocked the alloc; reclaim then frees at least one victim.
2. `reclaim_in(seg, short, RECLAIM_MAX_VICTIMS)` takes the write lock on the object map. Until the victims' bytes cover `short`, it samples `reclaim-sample-size` OIDs from that segment's index (every one if there are that few), skips any entry whose `Arc::strong_count > 1`, and removes the lowest `decayed_counter`. It stops at the first fit, so it takes the fewest victims, never more.
3. If the segment runs out of unpinned entries or the cap is hit first, every victim goes back under the same lock and nothing is reclaimed. Readers wait on the lock, so none of them sees the entries missing.
4. The lock is released, then the victims drop. Each had a strong count of 1 under the lock, and cloning needs the lock, so this is the last reference and `ObjectContext::Drop` returns the buffers to talc.
5. `SegmentPool::alloc_exact_in(seg, obj_len)` allocates in that segment only.

Segment `allocated_bytes` is talc's count of requested layout sizes, and each victim's buffers are all in `seg`, so once the victims cover `short` the alloc's byte check passes. The alloc can still fail if talc cannot place a chunk in the freed holes (its tags, or tail holes smaller than a chunk), or if another promotion takes the room first. The caller then serves from NVMe.

Rules that fall out of the Arc model:

1. An entry with an in-flight reader is never reclaimed. The reader finishes on the buffers it holds.
2. An entry in `Filling` state is never reclaimed, because the promotion task holds an `Arc`.
3. Reclaim removes only the map's reference, as DEL does. The key and its NVMe file are untouched, and the next GET is a miss that goes through admission again.

There is no mode guard inside `make_room_for`. Its only caller is `try_promote_object`, which only the tiered GET handler reaches. The name states what it removes.

Cost: picking the segment is one pass over the segment slots, as the allocator already does. Under the write lock, at most `reclaim-sample-size` times `RECLAIM_MAX_VICTIMS` index probes (each one hash lookup into the object map) and `RECLAIM_MAX_VICTIMS` removes (80 and 16 by default), independent of how many objects are cached. Usually far fewer, since the loop stops at the first fit.

Known limitation: `RECLAIM_MAX_VICTIMS` is also a size ceiling. An object that needs more than 16 victims from its target segment (a 64 MiB object into a segment of 64 KiB objects) is never promoted and stays on NVMe. Step 3 makes that a no-op instead of reclaiming on every miss. If object sizes turn out to vary widely, bound the loop by bytes examined instead of a victim count.

### 4.5 Data structure

`HashMap` cannot be sampled at random, so both pools use `OIDIndexedMap<V>`, an alias for `indexmap::IndexMap<ObjectId, V>`. It keeps entries in a dense `Vec` with a hash table of positions, so sampling a slot is a plain index with no hashing. Insert pushes to the end and `swap_remove` is O(1). `cache_policy::reclaim_one(map, samples, score)` probes up to `samples` slots, removes the lowest-scoring entry the `score` closure accepts, and returns it; the fd pool reclaims through it.

DRAMPool keeps two maps under one lock, changed together only through `ObjectMaps::insert` and `remove`: `all` (OID to `Arc<ObjectContext>`) and `by_segment[s]` (an `OIDIndexedMap<()>` of the OIDs of the objects in segment `s`). The index holds `()`, not a second `Arc`, so `strong_count` still means "only the map holds it". Reclaim samples `by_segment[seg]` with the same `sample_victim` and looks each probe up in `all`. Both updates are O(1). An object never spans segments, so its first buffer names its segment, and `try_shrink` drains `by_segment[seg]` directly in O(objects in that segment).

### 4.6 Interaction with existing mechanisms

1. Segment shrink (`try_shrink`) is unchanged and remains the server-memory-pressure path; inline reclaim is the pool-full path. Both change the map under the same write lock, so they never interleave.
2. Request coalescing on `Filling` entries is still not implemented. Reads during a promotion count as misses and do not touch the score, so a newly promoted object starts at `LFU_INIT_VAL`.
3. The free callback (`lo_free`) still calls `remove_object`, so anything that deletes a key also drops its cached copy.

### 4.7 Observability

Fields in the `largeobj_dram` INFO section (`add_section("dram")`; the module name is added as a prefix):

```
cache_hits_total            tiered GET served from a Ready cached entry
cache_misses_total          tiered GET that read from NVMe (absent, Filling, rejected, or no space)
promotions_total            try_promote_object succeeded
admission_rejects_total     miss served transiently because the filter's miss count was below promote-min-hits
reclaims_total              cached copies removed by make_room_for (DRAMPool, both modes)
```

`cache_hits_total + cache_misses_total` is the number of tiered GETs. `cache_misses_total - promotions_total - admission_rejects_total` counts misses that were neither promoted nor rejected: objects above `max-promote-size`, a pool with no room even after reclaim, or a concurrent promotion of the same object. `reclaims_total` counts cached copies only, never keys.

### 4.8 FD pool

`FdPool` reuses `cache_policy.rs`:

```rust
struct FdEntry { fd: Arc<OwnedFd>, stats: AccessStats }
fds: RwLock<OIDIndexedMap<FdEntry>>
```

1. The `get_or_open` fast path touches `stats` under the read lock and hands out a clone. The re-check under the write lock (two concurrent first GETs on a cold object) does the same.
2. Slow path, under the write lock: if `max-cached-fds` is nonzero and `len >= cap`, remove the lowest-scoring sampled fd until there is room for the new one under the cap (normally one; more after the cap is lowered at runtime, since it is enforced on the next open). Unlike the DRAM pool, an fd held by an in-flight read is not skipped: reclaim only drops the map's `Arc`, the reader's clone keeps the fd open until the read completes, and it closes then. Open fds can briefly exceed the cap by the number of such readers. Victims are chosen before `open()`, so if the open fails they are still reclaimed and counted, costing one reopen each.
3. The DRAM pool keeps the held-entry skip because reclaim there must free bytes for an allocation that happens right after; a held entry frees nothing until its reader finishes. The fd cap has no such follow-up, so the skip buys nothing.
4. The next GET on a reclaimed fd reopens it, which costs one `open()` syscall, so a wrong fd reclaim is far cheaper than a wrong DRAM reclaim.
5. No admission filter for fds: a miss costs a syscall, not DRAM, so first-touch admission is fine.

`get_or_open_with` takes the cap as an argument so unit tests can set it without racing on the global config; the other settings are read live. `RECLAIM_MAX_VICTIMS` does not apply, since the fd loop reclaims exactly the overflow.

INFO section `largeobj_fd` (Tiered only): `open_fds`, `fd_reclaims_total`.


## 5. Open questions

1. Should `try_shrink` pick its victim segment by aggregate LFU score instead of fewest allocated bytes? Fewest bytes minimizes re-read work; lowest score minimizes hit loss. Leave as is until there is a measurement.
