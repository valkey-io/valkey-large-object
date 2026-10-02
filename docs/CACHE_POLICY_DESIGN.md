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
2. Demotion is a map remove. The buffers return to the owning segment's talc when the last `Arc<ObjectContext>` drops.
3. The benefit of a DRAM hit scales with object size. Large objects are bandwidth bound on the drive, while DRAM feeds the EFA link at line rate. Benefit per byte is roughly constant, so a size-normalized score (GDSF style, frequency divided by size) adds complexity without changing the ranking. Size matters only operationally: promoting a large object may require several demotions.

## 3. Decision

Use Valkey's approximated LFU (a logarithmic frequency counter with time-based decay) as the score, a second-touch admission filter in front of promotion, and bounded inline demotion on the promotion path.

Why this and not the alternatives:

| Option | Verdict | Reason |
|---|---|---|
| LRU | Rejected | Scan churn from one-touch chunks flushes hot prefixes. |
| Pure LFU (no aging) | Rejected | Stale hot objects are never displaced. |
| LFU with time decay (Valkey style) | Chosen | Frequency wins the steady state, decay lets popularity move. Operators already understand `lfu-decay-time`. This is an LFRU in practice. |
| W-TinyLFU / ARC | Rejected for now | Solves one-touch pollution through a sketch or ghost lists. Our object counts are small (DRAM divided by MB-scale objects) and the same protection comes from a small second-touch ghost table. Revisit only if measurements show the simple filter is insufficient. |
| Size-normalized score (GDSF) | Rejected | Benefit per byte is constant, see section 2. |
| Cron-only demotion | Rejected | Leaves the cache frozen between ticks; a full pool would reject promotions for up to `scaling-poll-ms`. Inline demotion is bounded and runs only on misses. |

The policy applies to Tiered mode only. In Dram mode the DRAMPool is the data, so dropping an entry is data loss, not a cache decision.

### 3.1 Configs and constants

Four runtime-mutable configs:

| Config | Default | Range | Meaning |
|---|---|---|---|
| `tiered-decay-time` | 1 | 0..=65535 | Minutes per one-point counter decay. 0 disables decay (pure LFU). Same semantics as core `lfu-decay-time`. |
| `promote-min-hits` | 2 | 1..=255 | Misses an object must accumulate before a GET promotes it. 1 promotes on the first GET. |
| `demote-sample-size` | 5 | 1..=64 | Entries sampled per demotion round. Same idea as core's `maxmemory-samples`. |
| `max-open-fds` | 1024 | 0..=1048576 | Cap on cached read fds. 0 means unlimited. The default stays well under Valkey's fd limit (about `maxclients + 32`), which the module's fds share with client sockets. |

Three tuning values are constants in `src/storage/cache_policy.rs`, because no operator has a reason to change them:

| Constant | Value | Reason |
|---|---|---|
| `LFU_LOG_FACTOR` | 10 | Core's default for `lfu-log-factor`; almost never tuned. |
| `GHOST_CAPACITY` | 65536 | `promote-min-hits 1` is the single off switch for admission. |
| `DEMOTE_MAX_VICTIMS` | 16 | A loop bound, not a policy knob. |

## 4. Design

### 4.1 Score: packed LFU counter

Each cached entry carries one `AtomicU32`, laid out like Valkey's 24-bit `lru` field:

```
bits 0..8    counter        logarithmic frequency, 0..=255, starts at LFU_INIT_VAL (5)
bits 8..24   last_decr_min  minute (from now_minutes) of the last decay, wraps at 65536
```

Operations, all on `AccessStats` in `src/storage/cache_policy.rs`:

1. `touch(now_min, decay_time)`: decay the counter for the elapsed minutes, then increment with probability `p = 1 / ((counter - INIT) * LFU_LOG_FACTOR + 1)`, then store with `now_min` stamped in. At `INIT` the probability is 1, so one touch always outranks an untouched entry. Two racing touches can lose one increment, as in Valkey.
2. `decayed_counter(now_min, decay_time)`: the counter minus `elapsed / decay_time` periods, saturating at 0. Demotion ranks by this. It never writes back. `decay_time = 0` disables decay.
3. `new(now_min)`: counter at `LFU_INIT_VAL`, stamped now. A fresh entry ranks below any entry that has been hit and above one that has fully decayed, as in Valkey.

`now_minutes()` counts minutes since its first call, wrapping at 16 bits; elapsed time uses `wrapping_sub`, so the wrap is harmless unless an entry goes untouched for more than 45 days, when it looks recent again. Callers read it once per operation and pass it down.

### 4.2 Where the score lives

`ObjectContext` has `pub stats: AccessStats`. The tiered GET hit path calls `CachePolicy::record_hit`, which touches the score and counts the hit, with no write lock. The touch is not inside `get_object`, because `LO.INFO key TIER` and `COPY` also use `get_object` and are not cache accesses.

Stats are keyed by `ObjectId`, not by key. An `LO.SET` mints a new OID and the free callback drops the old DRAM entry, so a rewritten object starts with fresh stats. A RENAME is invisible to the policy.

### 4.3 Admission: second-touch ghost table

A miss (object not in the DRAM map) consults a `GhostTable` before promoting:

```rust
pub struct GhostTable {
    hits: HashMap<ObjectId, u8>,   // misses seen for this OID
    order: VecDeque<ObjectId>,     // FIFO, oldest dropped at capacity
    capacity: usize,
}
```

1. On a miss, `CachePolicy::admit(oid, obj_len)` calls `record_miss(oid)`, which returns the object's miss count, inserting it at 1 if absent and dropping the FIFO head when full. A repeat miss does not move an entry in the FIFO, so the window is the last `GHOST_CAPACITY` distinct missed objects.
2. If the count is at least `promote-min-hits`, promote. Otherwise serve through the transient NVMePool path and count an admission reject. The ghost entry is not removed on promotion; it ages out FIFO. So an admitted object whose promotion fails is admitted again on its next miss, and an object demoted from DRAM while its ghost entry is still present is promoted again on its first miss (an ARC-style ghost hit).
3. Objects above `max-promote-size` are refused without touching the ghost, since they can never be promoted.
4. With `promote-min-hits 1`, `admit` returns true without touching the ghost, so admission is off and costs nothing. Misses in that time are not counted, so after the setting is raised those objects start from zero.

The ghost is a `Mutex` taken only in `admit`, never together with the objects lock. ObjectIds are never reused, so an entry for a deleted object just ages out. Nothing is preallocated, so a pool that never misses pays nothing for the table.

A GET that finds a `Filling` entry (another GET is promoting the same object) skips both `admit` and `try_promote_object`, so concurrent GETs on one object do not inflate its miss count.

### 4.4 Demotion: bounded, sampled, inline

`DRAMPool::try_promote_object` does `alloc_exact(obj_len)` (all-or-nothing) before taking the write lock. When that fails:

```
buffers = alloc_exact(obj_len)
if buffers is None and demote_for(obj_len):
    buffers = alloc_exact(obj_len)          // may still fail on fragmentation; caller falls back
```

Expansion is deliberately not attempted here. Segments are registered with io_uring (`IORING_REGISTER_BUFFERS`) and EFA once at startup, and a segment added at runtime updates only the module's mirror table, so the first `ReadFixed` into it fails with EFAULT and the header-verify path panics. This bug predates this work and is already reachable through the scaling cron's proactive expand in Tiered mode with `dram-maxmemory 0`. Once runtime registration exists (`IORING_REGISTER_BUFFERS_UPDATE` plus `fi_mr_reg` on expand), add `try_expand` and a retry before `demote_for`, so the cache is never shrunk while it can still grow.

`demote_for(need)` calls `demote_with(need, free_now, max_victims)`. Free space and the round limit are arguments so tests can set them; the sample size, decay time and clock are read live.

1. Take the write lock on the object map.
2. Up to `DEMOTE_MAX_VICTIMS` times, until the freed bytes alone cover `need`: call `demote_one`, which probes `demote-sample-size` slots (every slot if the map is that small), skips any entry whose `Arc::strong_count > 1`, and removes the lowest `decayed_counter`. Stop early if no probed entry is demotable. Current free space is not subtracted from `need`: the first alloc already failed, so that space is fragmented, and whole victim-sized holes are what make the retry work.
3. If the freed bytes plus the pool's free bytes are still below `need`, put every victim back under the same lock and demote nothing. Readers wait on the lock, so none of them sees the entries missing.
4. Release the lock, then drop the victims. Each had a strong count of 1 under the lock, and cloning needs the lock, so this is the last reference and `ObjectContext::Drop` returns the buffers to talc before the retry alloc.

Rules that fall out of the Arc model:

1. An entry with an in-flight reader is never demoted. The reader finishes on the buffers it holds.
2. An entry in `Filling` state is never demoted, because the promotion task holds an `Arc`.
3. Demotion removes only the map's reference, as DEL does. The key and its NVMe file are untouched, and the next GET is a miss that goes through admission again.

There is no mode guard inside `demote_for`. Its only caller is `try_promote_object`, which only the tiered GET handler reaches. The name states what it removes.

Cost bound: at most `demote-sample-size` times `DEMOTE_MAX_VICTIMS` score reads and `DEMOTE_MAX_VICTIMS` removes under the write lock, on the miss path only (80 and 16 by default). It runs on the main thread, which is why it is bounded.

The retry alloc can still fail after demotions; the caller then falls back to the transient read, and the freed space helps the next promotion. `alloc_exact` trims each object's last buffer to its tail, so a victim frees full-chunk holes plus one smaller hole. When the new object is larger than its victims, those tail holes cannot hold a full chunk unless they merge with neighboring free space, so mixed object sizes make a failed retry more likely than the byte count suggests. The free-bytes figure in step 3 also ignores allocator overhead, so it cannot rule out every failed retry.

Known limitation: `DEMOTE_MAX_VICTIMS` is also a size ceiling. An object that needs more than 16 victims' worth of space beyond the current free bytes (a 64 MiB object into a pool of 64 KiB objects) is never promoted and stays on NVMe. Step 3 makes that a no-op instead of demoting 16 entries on every miss. If object sizes turn out to vary widely, bound the loop by bytes examined instead of a victim count.

### 4.5 Data structure

`HashMap` cannot be sampled at random, so both pools use `IndexedMap<V>`, an alias for `indexmap::IndexMap<ObjectId, V>`. It keeps entries in a dense `Vec` with a hash table of positions, so sampling a slot is a plain index with no hashing. Insert pushes to the end and `swap_remove` is O(1). `cache_policy::demote_one(map, samples, score)` probes up to `samples` slots, removes the lowest-scoring entry the `score` closure accepts, and returns it; both pools demote only through it. `retain` (used by `try_shrink`) is O(n).

### 4.6 Interaction with existing mechanisms

1. Segment shrink (`try_shrink`) is unchanged and remains the server-memory-pressure path; inline demotion is the pool-full path. Both change the map under the same write lock, so they never interleave.
2. Request coalescing on `Filling` entries is still not implemented. Reads during a promotion count as misses and do not touch the score, so a newly promoted object starts at `LFU_INIT_VAL`.
3. The free callback (`lo_free`) still calls `remove_object`, so anything that deletes a key also drops its cached copy.

### 4.7 Observability

Fields in the `largeobj_dram` INFO section (`add_section("dram")`; the module name is added as a prefix):

```
cache_hits_total            tiered GET served from a Ready cached entry
cache_misses_total          tiered GET that read from NVMe (absent, Filling, rejected, or no space)
promotions_total            try_promote_object succeeded
admission_rejects_total     miss served transiently because the ghost count was below promote-min-hits
demotions_total             cached copies removed by demote_for
```

`cache_hits_total + cache_misses_total` is the number of tiered GETs. `cache_misses_total - promotions_total - admission_rejects_total` counts misses that were neither promoted nor rejected: objects above `max-promote-size`, a pool with no room even after demotion, or a concurrent promotion of the same object. `demotions_total` counts cached copies only, never keys.

### 4.8 FD pool

`FdPool` reuses `cache_policy.rs`:

```rust
struct FdEntry { fd: Arc<OwnedFd>, stats: AccessStats }
fds: RwLock<IndexedMap<FdEntry>>
```

1. The `get_or_open` fast path touches `stats` under the read lock and hands out a clone. The re-check under the write lock (two concurrent first GETs on a cold object) does the same.
2. Slow path, under the write lock: if `max-open-fds` is nonzero and `len >= cap`, remove the lowest-scoring sampled fd until there is room for the new one under the cap (normally one; more after the cap is lowered at runtime, since it is enforced on the next open). Unlike the DRAM pool, an fd held by an in-flight read is not skipped: demotion only drops the map's `Arc`, the reader's clone keeps the fd open until the read completes, and it closes then. Open fds can briefly exceed the cap by the number of such readers. Victims are chosen before `open()`, so if the open fails they are still demoted and counted, costing one reopen each.
3. The DRAM pool keeps the held-entry skip because demotion there must free bytes for an allocation that happens right after; a held entry frees nothing until its reader finishes. The fd cap has no such follow-up, so the skip buys nothing.
4. The next GET on a demoted fd reopens it, which costs one `open()` syscall, so a wrong fd demotion is far cheaper than a wrong DRAM demotion.
5. No ghost table for fds: a miss costs a syscall, not DRAM, so first-touch admission is fine.

`get_or_open_with` takes the cap as an argument so unit tests can set it without racing on the global config; the other settings are read live. `DEMOTE_MAX_VICTIMS` does not apply, since the fd loop demotes exactly the overflow.

INFO section `largeobj_fd` (Tiered only): `open_fds`, `fd_demotions_total`.


## 5. Open questions

1. Should `try_shrink` pick its victim segment by aggregate LFU score instead of fewest allocated bytes? Fewest bytes minimizes re-read work; lowest score minimizes hit loss. Leave as is until there is a measurement.
