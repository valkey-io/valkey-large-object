//! DRAMPool scaling cron — proactive expand and shrink.
//!
//! Called on a configurable timer from the Valkey main event-loop thread.
//! Handles: draining completion, proactive expand, proactive shrink.

use valkey_module::Context;

use super::get_dram_pool;

/// Scaling cron. Fires on the main event-loop thread via a Valkey module timer.
///
/// Performs three actions in order:
/// 1. Complete any segments whose drain finished (refcount reached 0).
/// 2. Proactive expand: if pool utilization exceeds the expand watermark,
///    add a segment before the hot path stalls on segment creation.
/// 3. Proactive shrink: if server memory pressure exceeds the shrink watermark,
///    evict the least-used segment (Tiered mode: safe, data on NVMe).
pub fn scaling_cron(ctx: &Context) {
    let expand_watermark = crate::scaling_expand_watermark();
    let shrink_watermark = crate::scaling_shrink_watermark();
    let poll_ms = crate::scaling_poll_ms();

    let pool = get_dram_pool();

    // 1. Complete draining of any segments whose refcount hit 0.
    pool.release_drained_segments();

    // 2. Proactive expand: grow before the pool fills so promotions don't
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

    // 3. Proactive shrink: yield memory back to core when server is under pressure.
    // try_shrink() is safe in both modes: in Dram mode it only drains segments
    // with zero allocated bytes, so no live data is ever lost.
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
    if ratio > shrink_watermark {
        // Skip shrink if a segment is already draining — its memory hasn't
        // been freed yet. Draining completes asynchronously as Arc holders
        // drop; firing another shrink now would drain a second segment before
        // the first is even released. Check on the next tick after
        // release_drained_segments() has had a chance to finish it.
        let (_, draining, _) = pool.segment_counts();
        if draining == 0 && pool.try_shrink() {
            ctx.log_notice(&format!(
                "largeobj: scaling — memory pressure {:.1}% > {:.0}%, evicted one DRAM segment",
                ratio * 100.0,
                shrink_watermark * 100.0
            ));
        }
    }

    rearm_scaling_cron(ctx, poll_ms);
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
