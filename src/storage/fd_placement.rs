//! Object-file fd placement — keep the module's long-lived fds out of the fd
//! numbers Valkey serves clients on.
//!
//! Valkey's event loop is an array indexed by fd number, sized `maxclients + 128`;
//! a client socket numbered past it is never served. Linux hands out the lowest
//! free number, so object fds left low (the fd cache, in-flight SET files) push
//! client sockets past the end. Every object-file open goes through
//! `open_object_file`, which moves the fd above a floor at the end of that range.
//! `place` sets the floor at load and on every `CONFIG SET maxclients`.

use std::ffi::{c_int, c_void, CStr};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicI32, Ordering};

use valkey_module::{raw, Context};

/// Valkey's event loop holds `maxclients + CONFIG_FDSET_INCR` fds (server.h: 32 + 96).
const VALKEY_FDSET_INCR: u64 = 128;

/// Lowest fd number object files are moved to. 0 = not reserved; fds stay where `open` put them.
static FD_FLOOR: AtomicI32 = AtomicI32::new(0);

/// Put object-file fds above Valkey's client range for the current `maxclients`.
/// Tiered only, main thread only. Errs when the floor is not actually past the
/// event loop, or when the open-file hard limit leaves no room above it for every
/// object fd the module can hold: object fds would then take client fd numbers,
/// and clients past them are never served.
pub fn place(ctx: &Context) -> Result<(), String> {
    let maxclients: u64 = ctx
        .config_get("maxclients".to_string())
        .expect("CONFIG GET maxclients")
        .to_string()
        .parse()
        .expect("maxclients is numeric");
    let floor = reserve_fd_range(maxclients)?;
    outside_event_loop(floor)?;
    check_room(crate::max_cached_fds() as u64)?;
    ctx.log_notice(&format!("largeobj: object-file fds placed at {floor}+"));
    Ok(())
}

/// Whether every object fd the module can hold fits between the floor and the
/// open-file hard limit: `cap` cached read fds, plus one in-flight fd per client
/// (a SET's write fd, or a GET's fd evicted mid-read; both block their client, so
/// a client holds at most one). With this room, failing to move an fd above the
/// floor means misprovisioning or a bug.
/// Always Ok before `place` has set a floor (non-Tiered, or still loading).
pub fn check_room(cap: u64) -> Result<(), String> {
    let floor = FD_FLOOR.load(Ordering::Relaxed);
    if floor == 0 {
        return Ok(());
    }
    // The floor tracks the highest maxclients seen; Valkey keeps clients above a
    // lowered maxclients connected, so that is the client bound.
    let maxclients = floor as u64 - VALKEY_FDSET_INCR;
    let needed = cap + maxclients;
    let room = nofile_limit()?.rlim_max.saturating_sub(floor as u64);
    if room >= needed {
        return Ok(());
    }
    Err(format!(
        "only {room} fds above Valkey's client range ({floor}) for \
         max-cached-fds {cap} + maxclients {maxclients}; raise LimitNOFILE \
         (soft and hard) above {}",
        floor as u64 + needed
    ))
}

/// The process's `RLIMIT_NOFILE`.
fn nofile_limit() -> Result<libc::rlimit, String> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit is a valid rlimit out-parameter.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return Err(format!(
            "getrlimit(RLIMIT_NOFILE): {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(limit)
}

/// Raises the soft `RLIMIT_NOFILE` to the hard limit and sets the floor that
/// `open_object_file` moves fds above. Returns the floor; whether the hard limit
/// leaves room above it is `check_room`'s job.
///
/// The floor only rises: Valkey grows its event loop when maxclients grows but
/// does not shrink it.
fn reserve_fd_range(maxclients: u64) -> Result<i32, String> {
    let floor = i32::try_from(maxclients + VALKEY_FDSET_INCR)
        .map_err(|_| format!("maxclients {maxclients} puts the fd floor past i32"))?;
    let mut limit = nofile_limit()?;
    limit.rlim_cur = limit.rlim_max;
    // SAFETY: limit is a valid rlimit; raising soft up to hard needs no privilege.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err(format!(
            "setrlimit(RLIMIT_NOFILE soft = hard {}): {}",
            limit.rlim_max,
            std::io::Error::last_os_error()
        ));
    }
    Ok(FD_FLOOR.fetch_max(floor, Ordering::Relaxed).max(floor))
}

/// One-shot check that `floor` is past the end of Valkey's event loop, so the
/// copied `maxclients + 128` really marks where client fds end. `EventLoopAdd`
/// answers ERANGE exactly when `fd >= setsize`. The probe is an eventfd we own
/// (epoll rejects regular files), placed at `floor`, so no client's event is
/// touched; if it registers, it is removed at once. Main thread only.
fn outside_event_loop(floor: i32) -> Result<(), String> {
    unsafe extern "C" fn noop(_fd: c_int, _data: *mut c_void, _mask: c_int) {}
    // SAFETY: eventfd returns a new fd or -1.
    let low = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    if low < 0 {
        return Err(format!(
            "event-loop probe: eventfd: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: low is a valid fd we own; returns a new fd >= floor or -1.
    let probe = unsafe { libc::fcntl(low, libc::F_DUPFD_CLOEXEC, floor) };
    let dup_err = std::io::Error::last_os_error();
    // SAFETY: low is ours and no longer needed.
    unsafe { libc::close(low) };
    if probe < 0 {
        return Err(format!(
            "event-loop probe: no free fd at {floor}: {dup_err}"
        ));
    }
    let readable = raw::REDISMODULE_EVENTLOOP_READABLE as c_int;
    // SAFETY: the module API is initialized (OnLoad or a server event on the main
    // thread); probe is our own eventfd.
    let added = unsafe {
        raw::RedisModule_EventLoopAdd.expect("EventLoopAdd API available")(
            probe,
            readable,
            Some(noop),
            std::ptr::null_mut(),
        )
    } == raw::REDISMODULE_OK as c_int;
    let add_err = std::io::Error::last_os_error();
    if added {
        // SAFETY: probe was just registered by us.
        unsafe {
            raw::RedisModule_EventLoopDel.expect("EventLoopDel API available")(probe, readable)
        };
    }
    // SAFETY: probe is ours.
    unsafe { libc::close(probe) };
    if added {
        return Err(format!(
            "fd {floor} (maxclients + {VALKEY_FDSET_INCR}) is inside Valkey's event loop; \
             this server sizes its client range differently and object-file fds \
             would take client fd numbers"
        ));
    }
    if add_err.raw_os_error() != Some(libc::ERANGE) {
        return Err(format!(
            "event-loop probe: EventLoopAdd({floor}): {add_err}"
        ));
    }
    Ok(())
}

/// Whether `fd` sits where Valkey could need it for a client socket: below the
/// floor, or anywhere when no floor was reserved.
pub fn in_client_range(fd: RawFd) -> bool {
    let floor = FD_FLOOR.load(Ordering::Relaxed);
    floor == 0 || fd < floor
}

/// `open(2)` an object file, then move the fd above the floor set by `place`.
/// Same contract as `open`: an fd, or -1 with errno set.
/// Panics when no fd is free above the floor: `check_room` guarantees room for
/// every object fd the module can hold, so this is misprovisioning or a bug, and
/// keeping the low fd would take a client's fd number.
pub fn open_object_file(path: &CStr, flags: libc::c_int, mode: libc::mode_t) -> RawFd {
    // SAFETY: path is NUL-terminated; open returns an fd or -1.
    let fd = unsafe { libc::open(path.as_ptr(), flags, mode as libc::c_uint) };
    let floor = FD_FLOOR.load(Ordering::Relaxed);
    if fd < 0 || floor == 0 {
        return fd;
    }
    // SAFETY: fd is a valid fd we own; F_DUPFD_CLOEXEC returns a new fd >= floor or -1.
    let high = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, floor) };
    if high < 0 {
        panic!(
            "largeobj: no free fd above Valkey's client range ({floor}): {}; \
             raise LimitNOFILE (soft and hard)",
            std::io::Error::last_os_error()
        );
    }
    // SAFETY: fd is ours and no longer used; high refers to the same open file.
    unsafe { libc::close(fd) };
    high
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_object_file_places_fd_above_client_range() {
        let floor = reserve_fd_range(100).expect("hard limit has room above 228");
        assert_eq!(floor, 228);
        let path = std::ffi::CString::new("/dev/null").unwrap();
        let fd = open_object_file(&path, libc::O_RDONLY, 0);
        assert!(
            fd >= floor,
            "fd {fd} is inside the client range below {floor}"
        );
        assert!(!in_client_range(fd));
        assert!(in_client_range(floor - 1));
        // SAFETY: fd was just opened by this test.
        unsafe { libc::close(fd) };
        FD_FLOOR.store(0, Ordering::Relaxed);
    }
}
