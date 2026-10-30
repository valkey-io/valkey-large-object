//! DRAMPool scaling cron — proactive expand and shrink.
//!
//! Called on a configurable timer from the Valkey main event-loop thread.
//! Handles: draining completion, deleting the keys of evicted objects, proactive
//! expand, proactive shrink (Tiered only).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use valkey_module::Context;

use super::get_dram_pool;

/// No expand for this long after a shrink (see `would_cross_memory_watermark`).
const EXPAND_COOLDOWN: Duration = Duration::from_secs(30);
static LAST_SHRINK: Mutex<Option<Instant>> = Mutex::new(None);

/// True within `EXPAND_COOLDOWN` of the last shrink.
pub fn in_shrink_cooldown() -> bool {
    LAST_SHRINK
        .lock()
        .expect("LAST_SHRINK lock unavailable")
        .is_some_and(|t| t.elapsed() < EXPAND_COOLDOWN)
}

/// Scaling cron. Fires on the main event-loop thread via a Valkey module timer.
///
/// Performs these actions in order:
/// 1. Complete any segments whose drain finished (refcount reached 0).
/// 2. Delete the keys of evicted Dram objects (the reclaim list). While a Tiered
///    shrink is unfinished (a segment draining), stop here: one shrink at a time,
///    no expand or shrink on top.
///    While reclaim-list keys remain, the cron re-arms at `reaper-poll-ms`
///    instead of `scaling-poll-ms`, so cleanup runs in short, frequent slices.
/// 3. Proactive expand: if pool utilization exceeds the expand watermark,
///    add a segment before the hot path stalls on segment creation.
/// 4. Proactive shrink: if server memory pressure exceeds the shrink watermark,
///    drop the least-used segment's cached copies (Tiered only; data stays on NVMe).
pub fn scaling_cron(ctx: &Context) {
    let expand_watermark = crate::scaling_expand_watermark();
    let shrink_watermark = crate::scaling_shrink_watermark();
    let poll_ms = crate::scaling_poll_ms();
    let pool = get_dram_pool();

    // 1. Complete draining of any segments whose refcount hit 0.
    pool.release_drained_segments();

    // 2. Delete keys left pointing at reclaimed objects.
    super::reclaim::delete_reclaimed_keys(ctx);

    // A Tiered shrink is still draining: finish it before any more scaling. Dram
    // never drains a segment.
    let (_, draining, _) = pool.segment_counts();
    if draining > 0 {
        rearm_scaling_cron(ctx, next_poll_ms());
        return;
    }

    // 3. Proactive expand: grow before the pool fills so promotions don't
    //    stall on segment creation + EFA registration on the hot path.
    let util = pool.utilization_ratio();
    let expanded = util > expand_watermark && pool.try_expand(ctx).is_some();
    if expanded {
        ctx.log_notice(&format!(
            "largeobj: scaling — pool utilization {:.1}% > {:.0}%, added one DRAM segment",
            util * 100.0,
            expand_watermark * 100.0
        ));
    }

    // 4. Proactive shrink: yield memory back to core when server is under pressure.
    //
    // Only a segment release lowers used_memory: core evicting an LO key just
    // returns its buffer to the segment. Dram has no shrink (see `may_shrink`), so a
    // Dram pool keeps its segments and evicted objects leave holes.
    //
    // Shrink is SERVER-scoped (crate::server_memory), not module-scoped: we give
    // DRAM back only under Valkey-wide pressure, so the module's own pool pressure
    // (which drives expand) must not trigger shrink.
    //
    // Expand takes priority within a tick: expand and shrink read different
    // denominators (module utilization vs server memory), so both can cross on
    // the same tick, and firing both would add a segment then immediately drain
    // one — pure churn. Gated on expand-SUCCEEDED, not merely wanted, so a pool
    // that can't grow still shrinks under pressure.
    if expanded {
        rearm_scaling_cron(ctx, poll_ms);
        return;
    }
    let (used, maxmemory) = crate::server_memory(ctx);
    if maxmemory == 0 {
        // No server-wide maxmemory configured — no shrink pressure signal exists.
        rearm_scaling_cron(ctx, poll_ms);
        return;
    }
    let ratio = used as f64 / maxmemory as f64;
    // Only Tiered shrinks: it drops cached copies and deletes no key. Dram has no
    // shrink: releasing a segment would delete its keys whatever the policy says,
    // so evicted objects leave holes for later writes.
    let may_shrink = crate::operating_mode() == crate::OperatingMode::Tiered;
    if ratio > shrink_watermark && may_shrink && pool.try_shrink() {
        *LAST_SHRINK.lock().expect("LAST_SHRINK lock unavailable") = Some(Instant::now());
        ctx.log_notice(&format!(
            "largeobj: scaling — memory pressure {:.1}% > {:.0}%, dropped one cached DRAM segment",
            ratio * 100.0,
            shrink_watermark * 100.0
        ));
        // Release now if no reader holds the victim.
        pool.release_drained_segments();
    }

    // Evictions (Dram objects, Tiered files) may have filled the reclaim list: start
    // cleanup on the fast tick.
    rearm_scaling_cron(ctx, next_poll_ms());
}

/// `reaper-poll-ms` while reclaim-list keys remain, else `scaling-poll-ms`.
fn next_poll_ms() -> u64 {
    if super::reclaim::RECLAIM_LIST.is_empty() {
        crate::scaling_poll_ms()
    } else {
        crate::reaper_poll_ms()
    }
}

/// Re-arm the scaling cron for the next tick.
pub fn rearm_scaling_cron(ctx: &Context, poll_ms: u64) {
    ctx.create_timer(
        std::time::Duration::from_millis(poll_ms),
        |ctx, ()| {
            scaling_cron(ctx);
        },
        (),
    );
}
