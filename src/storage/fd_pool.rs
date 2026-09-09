//! File Descriptor Pool — pre-opened fds for NVMe object files.
//!
//! Open once per object (on write), reuse on every read, close on delete.
//! Saves open()/close() syscalls on the hot read path.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::RwLock;

use crate::data_type::ObjectId;

pub struct FdPool {
    fds: RwLock<HashMap<ObjectId, RawFd>>,
}

impl Default for FdPool {
    fn default() -> Self {
        Self::new()
    }
}

impl FdPool {
    pub fn new() -> Self {
        Self {
            fds: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, oid: ObjectId) -> Option<RawFd> {
        self.fds
            .read()
            .expect("FdPool.fds lock unavailable")
            .get(&oid)
            .copied()
    }

    /// Get cached fd or open the file and cache it.
    /// direct-io is IMMUTABLE so fds opened here remain valid for the module's lifetime.
    /// Holds write lock for the entire check+open+insert to prevent TOCTOU fd leak.
    pub fn get_or_open(&self, oid: ObjectId, dir: &str) -> Option<RawFd> {
        let mut fds = self.fds.write().expect("FdPool.fds lock unavailable");

        // Check under write lock — no race.
        if let Some(&fd) = fds.get(&oid) {
            return Some(fd);
        }

        // Open while holding lock — concurrent callers wait.
        let path = oid.file_path(dir);
        let c_path = std::ffi::CString::new(path).expect("file_path null");
        let mut flags = libc::O_RDONLY;
        if crate::direct_io() {
            flags |= super::DIRECT_IO_FLAG;
        }
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd < 0 {
            return None;
        }
        fds.insert(oid, fd);
        Some(fd)
    }

    pub fn insert(&self, oid: ObjectId, fd: RawFd) {
        self.fds
            .write()
            .expect("FdPool.fds lock unavailable")
            .insert(oid, fd);
    }

    pub fn remove(&self, oid: ObjectId) {
        if let Some(fd) = self
            .fds
            .write()
            .expect("FdPool.fds lock unavailable")
            .remove(&oid)
        {
            // SAFETY: fd is a valid file descriptor opened by us via libc::open.
            // We own it exclusively (removed from map) and close exactly once.
            unsafe { libc::close(fd) };
        }
    }
}

impl Drop for FdPool {
    fn drop(&mut self) {
        for (_, fd) in self
            .fds
            .write()
            .expect("FdPool.fds lock unavailable")
            .drain()
        {
            // SAFETY: All fds were opened by us via libc::open and are valid.
            // drain() ensures each fd is closed exactly once during shutdown.
            unsafe { libc::close(fd) };
        }
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::ObjectId;

    #[test]
    fn test_fd_pool_insert_get_remove() {
        let pool = FdPool::new();
        let oid = ObjectId(42);

        // Open a real temp file to get a valid fd.
        let tmp = std::ffi::CString::new("/tmp/fdpool_test_XXXXXX").unwrap();
        let mut buf = tmp.into_bytes_with_nul();
        // SAFETY: mkstemp takes a mutable C string template, returns a valid fd.
        let fd = unsafe { libc::mkstemp(buf.as_mut_ptr() as *mut libc::c_char) };
        assert!(fd >= 0, "mkstemp failed");

        // Insert and retrieve.
        pool.insert(oid, fd);
        assert_eq!(pool.get(oid), Some(fd));

        // Remove closes the fd.
        pool.remove(oid);
        assert_eq!(pool.get(oid), None);

        // Verify fd is actually closed: fcntl should fail with EBADF.
        // SAFETY: fcntl on a closed fd returns -1 (does not crash).
        let ret = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_eq!(ret, -1, "fd should be closed after remove");

        // Clean up the temp file.
        let path = std::ffi::CStr::from_bytes_with_nul(&buf).unwrap();
        // SAFETY: path is a valid C string from mkstemp.
        unsafe { libc::unlink(path.as_ptr()) };
    }
}
