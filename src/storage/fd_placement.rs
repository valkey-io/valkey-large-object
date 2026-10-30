//! Object-file fd placement: keep the module's fds out of the fd numbers Valkey core
//! serves clients on. Design and rules: `docs/OBJECT_FD_PLACEMENT_DESIGN.md`.
//!
//! Valkey's event loop is an array indexed by fd number, sized `maxclients + 128`
//! (`CONFIG_FDSET_INCR`). Those numbers are the core range; a client socket numbered
//! past it is never served. Linux hands out the lowest free number, so object fds
//! left in the core range push later client sockets past its end. So cache misses and
//! SET write files go through `open_object_file`, which moves the fd to the module
//! range: from the module start (`maxclients + 128`) up to the fd limit. COPY opens
//! two files directly: it runs on the main thread and closes them before returning to
//! the event loop, so no client connects while they are open.
//! `raise_module_start` sets the module start at load and on every `CONFIG SET
//! maxclients`. When the fd limit ends inside the core range (Valkey's stock limit is
//! `maxclients + 32`), no client can be stranded and placement is off.
//!
//! The fd cache never takes the numbers commands need: it holds `max-cached-fds`,
//! limited to what the module range has after a command reserve of `maxclients`
//! numbers (`fd_cache_max_entries`). `fd-reserve Lenient` clamps or turns off what does
//! not fit and logs. `fd-reserve Strict` needs the reserve and the whole cache to fit:
//! it refuses to load, rejects a `CONFIG SET max-cached-fds`, or aborts on a
//! `CONFIG SET maxclients`.

use std::ffi::{c_int, c_void};
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use valkey_module::{raw, Context};

use crate::{fd_reserve, FdReserve};

/// Valkey's event loop holds `maxclients + CONFIG_FDSET_INCR` fds (server.h: 32 + 96).
const VALKEY_FDSET_INCR: u64 = 128;

/// The module start for the highest `maxclients` seen: the first fd number of the
/// module range, and the lowest number object files are moved to.
static MODULE_START: AtomicI32 = AtomicI32::new(0);

/// Whether placement is on: object fds are moved to the module range. Off when not
/// Tiered, while loading, and when the fd limit ends inside the core range.
static PLACEMENT_ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether `check_module_start_past_event_loop` has passed. The event loop is always
/// `maxclients + CONFIG_FDSET_INCR` wide, so one check at load covers every later module start.
static PROBED: AtomicBool = AtomicBool::new(false);

/// Object files currently open: the fd cache plus the fds of running GETs and SETs.
static OPEN_OBJECT_FDS: AtomicU64 = AtomicU64::new(0);

/// Object-file opens that found no free fd in the module range.
static PLACEMENT_FAILURES: AtomicU64 = AtomicU64::new(0);

/// How many object-file opens found no free fd in the module range.
pub fn placement_failures() -> u64 {
    PLACEMENT_FAILURES.load(Ordering::Relaxed)
}

/// Whether placement is on.
pub fn placement_enabled() -> bool {
    PLACEMENT_ENABLED.load(Ordering::Relaxed)
}

/// How many object files are open now: cached fds, plus the fds of running GETs
/// and SETs, including a GET's fd after the cache evicted it.
pub fn open_object_fds() -> u64 {
    OPEN_OBJECT_FDS.load(Ordering::Relaxed)
}

/// An open object file. It closes on drop and counts itself in `open_object_fds`
/// while open.
pub struct ObjectFd(OwnedFd);

impl AsRawFd for ObjectFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl Drop for ObjectFd {
    fn drop(&mut self) {
        OPEN_OBJECT_FDS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Raise the module start to the end of the core range for the current `maxclients`,
/// and turn placement on or off. Tiered only, main thread only.
///
/// The fd limit covers the command reserve when `limit >= module start + maxclients`;
/// `fd-reserve Strict` also needs `max-cached-fds` more. If it does not:
/// `fd-reserve Lenient` turns placement off (and with it the cache), logs, and returns
/// Ok; `fd-reserve Strict` returns Err naming the fix. Other Errs: `maxclients` too large
/// for an fd number, or the module start is not actually past the event loop (object
/// fds would then take fd numbers in the core range, and clients past them are never
/// served).
pub fn raise_module_start(ctx: &Context) -> Result<(), String> {
    let maxclients: u64 = ctx
        .config_get("maxclients".to_string())
        .expect("CONFIG GET maxclients")
        .to_string()
        .parse()
        .expect("maxclients is numeric");
    let module_start = i32::try_from(maxclients + VALKEY_FDSET_INCR)
        .map_err(|_| format!("maxclients {maxclients} is too large for an fd number"))?;
    let limit = nofile_limit().rlim_cur;
    // Only ever up: Valkey grows its event loop when maxclients grows, but never
    // shrinks it.
    let module_start = MODULE_START
        .fetch_max(module_start, Ordering::Relaxed)
        .max(module_start);
    let max_cached_fds = crate::max_cached_fds() as u64;
    let strict = fd_reserve() == FdReserve::Strict;
    // Strict needs the whole fd cache to fit too; Lenient needs only the command reserve.
    let cache_needed = if strict { max_cached_fds } else { 0 };
    let needed = fd_limit_needed(module_start as u64, cache_needed);
    if limit < needed {
        PLACEMENT_ENABLED.store(false, Ordering::Relaxed);
        let fix = limit_fix(module_start as u64, cache_needed);
        if strict {
            return Err(format!(
                "fd-reserve Strict: open-file limit {limit} is too low; {fix}, or use \
                 fd-reserve Lenient"
            ));
        }
        ctx.log_warning(&format!(
            "largeobj: fd-reserve Lenient: open-file limit {limit} is too low, so fd placement \
             and the fd cache are off; {fix} to turn them on"
        ));
        return Ok(());
    }
    // Placement turns on only once the module start is known to be past the event loop.
    if !PROBED.load(Ordering::Relaxed) {
        check_module_start_past_event_loop(module_start)?;
        PROBED.store(true, Ordering::Relaxed);
    }
    PLACEMENT_ENABLED.store(true, Ordering::Relaxed);
    ctx.log_notice(&format!(
        "largeobj: object-file fd placement is on: module range starts at fd {module_start}"
    ));
    if strict {
        ctx.log_notice(&format!(
            "largeobj: fd-reserve Strict: a CONFIG SET maxclients above {} aborts the server \
             (open-file limit {limit})",
            limit.saturating_sub(VALKEY_FDSET_INCR + max_cached_fds) / 2
        ));
    }
    warn_if_cache_limited(max_cached_fds);
    Ok(())
}

/// `CONFIG SET maxclients` grows Valkey's event loop, possibly up into the fds the
/// module placed at load. Raise the module start, drop the cached fds now below it so
/// their next GET reopens them in the module range, and trim the cache to the new
/// limit. Valkey has already applied the change and a module cannot veto it.
/// If the fd limit no longer covers the command reserve, `fd-reserve Lenient` turns
/// placement off and empties the cache (logged), and `fd-reserve Strict` aborts the
/// server. Strict also aborts if the reserve fits but `max-cached-fds` no longer does;
/// Lenient clamps the cache. Routed from `configs::on_server_config_changed`.
pub fn on_maxclients_changed(ctx: &Context) {
    let Some(pool) = super::FD_POOL.get() else {
        return;
    };
    if let Err(msg) = raise_module_start(ctx) {
        // The panic hook logs the message and aborts the server.
        if fd_reserve() == FdReserve::Strict {
            panic!("CONFIG SET maxclients: {msg}");
        }
        ctx.log_warning(&format!("largeobj: CONFIG SET maxclients: {msg}"));
    }
    pool.drop_fds_below_module_start();
    pool.shrink_to(fd_cache_max_entries());
}

/// `fd-reserve Strict` only: whether a new `max-cached-fds` still fits in the fd limit
/// beside the module start and the command reserve. Lenient always passes: it clamps.
/// Used to reject `CONFIG SET max-cached-fds`; the load and `maxclients` checks are in
/// `raise_module_start`. Err names the fd limit that would fit.
pub fn check_cache_fits_if_strict(max_cached_fds: u64) -> Result<(), String> {
    if fd_reserve() != FdReserve::Strict {
        return Ok(());
    }
    let limit = nofile_limit().rlim_cur;
    let module_start = MODULE_START.load(Ordering::Relaxed) as u64;
    if limit < fd_limit_needed(module_start, max_cached_fds) {
        return Err(format!(
            "open-file limit {limit} is too low for max-cached-fds {max_cached_fds}; {}, or use \
             fd-reserve Lenient",
            limit_fix(module_start, max_cached_fds)
        ));
    }
    Ok(())
}

/// The command reserve: one fd number per client, kept free for command fds. It is the
/// highest `maxclients` seen, which is what the module start was computed from. Valkey
/// keeps clients above a lowered `maxclients` connected, so this is the client bound.
fn command_reserve(module_start: u64) -> u64 {
    module_start.saturating_sub(VALKEY_FDSET_INCR)
}

/// The fd limit that holds the module start, the command reserve and `cache_entries`
/// cached fds.
fn fd_limit_needed(module_start: u64, cache_entries: u64) -> u64 {
    module_start + command_reserve(module_start) + cache_entries
}

/// What to do about a too-low fd limit. Every message about a low limit ends with this,
/// so the operator always gets the number.
fn limit_fix(module_start: u64, cache_entries: u64) -> String {
    format!(
        "set LimitNOFILE to at least {}",
        fd_limit_needed(module_start, cache_entries)
    )
}

/// How many cached read fds the fd limit lets the module range hold once the command
/// reserve (one number per client) is set aside for command fds (a SET's write fd, or
/// a GET's fd evicted mid-read; both block their client, so a client holds at most
/// one). Zero when placement is off: a cached fd would then hold a client's fd number
/// for good.
fn cache_headroom() -> u64 {
    if !PLACEMENT_ENABLED.load(Ordering::Relaxed) {
        return 0;
    }
    cache_headroom_for(
        nofile_limit().rlim_cur,
        MODULE_START.load(Ordering::Relaxed),
    )
}

fn cache_headroom_for(limit: u64, module_start: i32) -> u64 {
    limit.saturating_sub(fd_limit_needed(module_start as u64, 0))
}

/// The fd cache max entries: the most fds the fd cache may hold. `max-cached-fds`,
/// limited to what the fd limit allows.
pub fn fd_cache_max_entries() -> usize {
    (crate::max_cached_fds() as u64).min(cache_headroom()) as usize
}

/// Log, when `max_cached_fds` fds cannot all be cached, how many can and what limit
/// would fit.
pub fn warn_if_cache_limited(max_cached_fds: u64) {
    let headroom = cache_headroom();
    if max_cached_fds == 0 || max_cached_fds <= headroom {
        return;
    }
    let limit = nofile_limit().rlim_cur;
    let fix = limit_fix(MODULE_START.load(Ordering::Relaxed) as u64, max_cached_fds);
    let msg = if PLACEMENT_ENABLED.load(Ordering::Relaxed) {
        format!(
            "largeobj: open-file limit {limit} is too low for max-cached-fds {max_cached_fds}: \
             the fd cache holds {headroom}; {fix} to cache all"
        )
    } else {
        format!(
            "largeobj: the fd cache is off, so max-cached-fds {max_cached_fds} has no effect: \
             open-file limit {limit} is too low; {fix}"
        )
    };
    valkey_module::logging::log_warning(msg);
}

/// The process's `RLIMIT_NOFILE`.
fn nofile_limit() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit is a valid rlimit out-parameter.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    // getrlimit fails only on a bad pointer or resource (EFAULT, EINVAL).
    assert_eq!(
        rc,
        0,
        "getrlimit(RLIMIT_NOFILE): {}",
        std::io::Error::last_os_error()
    );
    limit
}

/// One-shot check that `module_start` is past the end of Valkey's event loop, so the
/// copied `maxclients + CONFIG_FDSET_INCR` really marks the end of the Valkey core
/// range. `EventLoopAdd` answers ERANGE exactly when `fd >= setsize`, and that check
/// looks only at the number, so the probe asks about `module_start` itself with no fd
/// of ours at that number. Anything but ERANGE means the number is inside the loop.
/// Main thread only.
fn check_module_start_past_event_loop(module_start: i32) -> Result<(), String> {
    unsafe extern "C" fn noop(_fd: c_int, _data: *mut c_void, _mask: c_int) {}
    // EventLoopAdd would register whatever is open at this number.
    // SAFETY: F_GETFD only reads the fd table.
    if unsafe { libc::fcntl(module_start, libc::F_GETFD) } != -1 {
        return Err(format!(
            "cannot check the event loop: fd {module_start} is already open"
        ));
    }
    let readable = raw::REDISMODULE_EVENTLOOP_READABLE as c_int;
    // SAFETY: the module API is initialized (OnLoad or a server event on the main
    // thread); no fd is open at module_start.
    let rc = unsafe {
        raw::RedisModule_EventLoopAdd.expect("EventLoopAdd API available")(
            module_start,
            readable,
            Some(noop),
            std::ptr::null_mut(),
        )
    };
    // Reads errno: right after the call.
    let past_loop = std::io::Error::last_os_error().raw_os_error() == Some(libc::ERANGE);
    if rc == raw::REDISMODULE_OK as c_int {
        // Only if another thread opened module_start after the check above.
        // SAFETY: module_start was just registered by us.
        unsafe {
            raw::RedisModule_EventLoopDel.expect("EventLoopDel API available")(
                module_start,
                readable,
            )
        };
    } else if past_loop {
        return Ok(());
    }
    Err(format!(
        "Valkey's event loop is larger than maxclients + {VALKEY_FDSET_INCR}: \
         fd {module_start} is inside it"
    ))
}

/// Whether `fd` is below the module start, inside the core range. Always false when
/// placement is off.
pub fn below_module_start(fd: RawFd) -> bool {
    PLACEMENT_ENABLED.load(Ordering::Relaxed) && fd < MODULE_START.load(Ordering::Relaxed)
}

/// `open(2)` an object file, then move the fd in the module range set by
/// `raise_module_start`. When no fd is free in the module range it closes the low fd
/// and fails: keeping it would take an fd number in the core range. The
/// caller turns the failure into an error reply.
pub fn open_object_file(path: &str, opts: &std::fs::OpenOptions) -> std::io::Result<ObjectFd> {
    let mut fd: OwnedFd = opts.open(path)?.into();
    if PLACEMENT_ENABLED.load(Ordering::Relaxed) {
        fd = move_above(fd, MODULE_START.load(Ordering::Relaxed))?;
    }
    OPEN_OBJECT_FDS.fetch_add(1, Ordering::Relaxed);
    Ok(ObjectFd(fd))
}

/// Move `fd` to the lowest free number at or above `module_start`; the original closes
/// on drop either way. An fd already at or above `module_start` is returned as it is:
/// moving it would need a second free number, which a full range does not have.
fn move_above(fd: OwnedFd, module_start: i32) -> std::io::Result<OwnedFd> {
    let raw = fd.as_raw_fd();
    if raw >= module_start {
        return Ok(fd);
    }
    // SAFETY: fd keeps raw open; F_DUPFD_CLOEXEC returns a new fd >= module_start or -1.
    let high = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, module_start) };
    if high < 0 {
        // Read errno before fd drops: close can overwrite it.
        let err = std::io::Error::last_os_error();
        PLACEMENT_FAILURES.fetch_add(1, Ordering::Relaxed);
        return Err(err);
    }
    // SAFETY: high is a fresh fd owned only by us.
    Ok(unsafe { OwnedFd::from_raw_fd(high) })
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn open_dev_null() -> OwnedFd {
        std::fs::File::open("/dev/null")
            .expect("open /dev/null")
            .into()
    }

    #[test]
    fn move_above_fails_without_panicking_when_no_fd_is_free() {
        // F_DUPFD rejects the module start at or above the limit, which is what a full range does.
        let failures = placement_failures();
        let err = move_above(open_dev_null(), i32::MAX).expect_err("no fd at i32::MAX");
        assert!(
            matches!(err.raw_os_error(), Some(libc::EINVAL | libc::EMFILE)),
            "{err}"
        );
        assert_eq!(placement_failures(), failures + 1);
    }

    #[test]
    fn open_object_file_counts_the_fd_and_sets_cloexec() {
        let fd = open_object_file("/dev/null", std::fs::OpenOptions::new().read(true))
            .expect("open /dev/null");
        // The counter is process-wide and other tests open object files in parallel, so
        // only a lower bound holds. The integration tests check it returns to zero.
        assert!(open_object_fds() >= 1);
        // Close-on-exec whether or not placement moved the fd.
        // SAFETY: the fd is open; F_GETFD only reads its flags.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0 && flags & libc::FD_CLOEXEC != 0);
    }

    #[test]
    fn move_above_keeps_an_fd_already_in_the_module_range() {
        let fd = open_dev_null();
        let raw = fd.as_raw_fd();
        // Module start 0: fd is already above it, so the same fd comes back.
        assert_eq!(move_above(fd, 0).expect("already above").as_raw_fd(), raw);
    }

    #[test]
    fn limit_fix_names_the_limit_to_set() {
        // maxclients 100: module start 228, command reserve 100.
        assert_eq!(limit_fix(228, 0), "set LimitNOFILE to at least 328");
        assert_eq!(limit_fix(228, 100), "set LimitNOFILE to at least 428");
    }

    #[test]
    fn cache_headroom_reserves_maxclients_for_commands() {
        let (module_start, maxclients) = (10_128, 10_000);
        // No numbers in the module range, or fewer than one per client: no room.
        assert_eq!(cache_headroom_for(10_032, module_start), 0);
        assert_eq!(cache_headroom_for(10_240, module_start), 0);
        assert_eq!(
            cache_headroom_for(module_start as u64 + maxclients, module_start),
            0
        );
        // Each number past that is one cached fd.
        assert_eq!(
            cache_headroom_for(module_start as u64 + maxclients + 1, module_start),
            1
        );
        assert_eq!(cache_headroom_for(65_536, module_start), 45_408);
    }
}
