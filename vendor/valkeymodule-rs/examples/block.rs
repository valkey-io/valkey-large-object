use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use std::time::Duration;
use valkey_module::alloc::ValkeyAlloc;
use valkey_module::{
    valkey_module, Context, ThreadSafeContext, ValkeyResult, ValkeyString, ValkeyValue,
};

fn block(ctx: &Context, _args: Vec<ValkeyString>) -> ValkeyResult {
    let blocked_client = ctx.block_client();

    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        thread::sleep(Duration::from_millis(1000));
        thread_ctx.reply(Ok("42".into()));
    });

    // We will reply later, from the thread
    Ok(ValkeyValue::NoReply)
}

fn block_measured(ctx: &Context, _args: Vec<ValkeyString>) -> ValkeyResult {
    let mut blocked_client = ctx.block_client();
    // Count the thread's work toward the command's stats.
    blocked_client.measure_time_start();

    thread::spawn(move || {
        let thread_ctx = ThreadSafeContext::with_blocked_client(blocked_client);
        thread::sleep(Duration::from_millis(200));
        thread_ctx.reply(Ok("42".into()));
    });

    Ok(ValkeyValue::NoReply)
}

/// How many times `free_reply_data` got data, i.e. `reply_with_data` did not run.
static FREED: AtomicI64 = AtomicI64::new(0);

/// Runs on the main thread, with the data the thread set.
fn reply_with_data(_ctx: &Context, data: Option<i64>) -> ValkeyResult {
    Ok(ValkeyValue::Integer(
        data.expect("the thread always sets data"),
    ))
}

/// Runs on the main thread when `reply_with_data` did not, e.g. the client disconnected.
fn free_reply_data(_ctx: &Context, _data: i64) {
    FREED.fetch_add(1, Ordering::Relaxed);
}

/// `block.reply <ms>`: a thread sleeps `ms`, then hands 42 to `reply_with_data`.
fn block_reply(ctx: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let ms = args
        .get(1)
        .map_or(Ok(0), |arg| arg.parse_integer())?
        .unsigned_abs();
    let mut blocked_client =
        ctx.block_client_with_reply_callback(reply_with_data, Some(free_reply_data));

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(ms));
        blocked_client
            .set_blocked_private_data(42)
            .expect("free callback is set");
        // Dropping the BlockedClient unblocks the client.
    });

    Ok(ValkeyValue::NoReply)
}

fn block_freed(_ctx: &Context, _args: Vec<ValkeyString>) -> ValkeyResult {
    Ok(ValkeyValue::Integer(FREED.load(Ordering::Relaxed)))
}

//////////////////////////////////////////////////////

valkey_module! {
    name: "block",
    version: 1,
    allocator: (ValkeyAlloc, ValkeyAlloc),
    data_types: [],
    commands: [
        ["block", block, "", 0, 0, 0],
        ["block.measured", block_measured, "", 0, 0, 0],
        ["block.reply", block_reply, "", 0, 0, 0],
        ["block.freed", block_freed, "", 0, 0, 0],
    ],
}
