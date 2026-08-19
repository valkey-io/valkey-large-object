//! io_uring Engine — registered buffers, ReadFixed/WriteFixed, CQ poller thread.
//!
//! Architecture:
//!   Main thread: submit(IoRequest) via channel → returns immediately
//!   Poller thread: owns io_uring ring, submits ReadFixed/WriteFixed, polls CQ,
//!                  fires completion callback from CQ thread.
//!
//! Key optimization: buffers are registered with IORING_REGISTER_BUFFERS at startup.
//! Reads use ReadFixed opcode — pages pinned ONCE, eliminates per-read gup_fast_fallback.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::storage::{NvmeEngine, StorageError};

// ─── Request Types ───────────────────────────────────────────────────────────

/// Completion callback type — fired from the CQ poller thread.
use super::buffer::Buffer;

pub type ReadCallback = Box<dyn FnOnce(Buffer, Result<u64, StorageError>) + Send>;
pub type WriteCallback = Box<dyn FnOnce(Buffer, Result<(), StorageError>) + Send>;

pub enum IoRequest {
    Read {
        fd: RawFd,
        buf: Buffer, // owned buffer — travels through io_uring pipeline
        len: u64,
        on_complete: ReadCallback,
    },
    Write {
        fd: RawFd,
        buf: Buffer,
        len: u64,
        on_complete: WriteCallback,
    },
}

// SAFETY: IoRequest contains raw pointers (as usize) and boxed closures.
// The pointers refer to pool-allocated buffers that are stable for module lifetime.
// The closures are Send. The enum is only sent across a bounded channel to the
// poller thread which is the sole consumer.
unsafe impl Send for IoRequest {}

// ─── Pending Operation Tracking ──────────────────────────────────────────────

enum PendingOp {
    Read {
        buf: Buffer,
        on_complete: ReadCallback,
    },
    Write {
        buf: Buffer,
        on_complete: WriteCallback,
        len: u64,
    },
}

// ─── UringNvmeEngine ─────────────────────────────────────────────────────────

pub struct UringNvmeEngine {
    tx: Sender<IoRequest>,
    shutdown: Arc<AtomicBool>,
    poller: Option<thread::JoinHandle<()>>,
}

impl NvmeEngine for UringNvmeEngine {
    fn submit(&self, req: IoRequest) {
        self.tx.send(req).ok();
    }
    fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl UringNvmeEngine {
    /// Create engine and spawn CQ poller thread.
    /// `iovecs` are the pool buffers to register with the kernel.
    pub fn new(iovecs: Vec<libc::iovec>) -> Self {
        let (tx, rx) = bounded::<IoRequest>(4096);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        // Convert to Send-safe (ptr as usize, len) pairs for the thread boundary.
        let buf_info: Vec<(usize, usize)> = iovecs
            .iter()
            .map(|iov| (iov.iov_base as usize, iov.iov_len))
            .collect();

        let poller = thread::Builder::new()
            .name("lo-uring-poller".into())
            .spawn(move || {
                // SAFETY: Reconstruct iovecs inside the poller thread from (usize, usize) pairs.
                // The underlying memory is pool-allocated and stable for module lifetime.
                let iovecs: Vec<libc::iovec> = buf_info
                    .iter()
                    .map(|&(ptr, len)| libc::iovec {
                        iov_base: ptr as *mut libc::c_void,
                        iov_len: len,
                    })
                    .collect();
                Self::poller_loop(rx, shutdown_clone, iovecs);
            })
            .expect("failed to spawn io_uring poller thread");

        Self {
            tx,
            shutdown,
            poller: Some(poller),
        }
    }

    /// Shutdown the engine. Drains pending ops then exits.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.poller.take() {
            handle.join().ok();
        }
    }

    /// The CQ poller loop — owns the io_uring ring.
    fn poller_loop(rx: Receiver<IoRequest>, shutdown: Arc<AtomicBool>, iovecs: Vec<libc::iovec>) {
        // Initialize io_uring.
        let mut ring = match io_uring::IoUring::new(256) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("largeobj: io_uring init failed: {}", e);
                // Fallback: drain requests with errors.
                Self::error_drain_loop(rx, shutdown);
                return;
            }
        };

        // Register buffers — pins pages for ReadFixed/WriteFixed.
        let use_fixed = if !iovecs.is_empty() {
            // SAFETY: iovecs point to pool-allocated, page-aligned memory that is stable
            // for the module's lifetime. The kernel pins these pages for zero-copy I/O.
            unsafe { ring.submitter().register_buffers(&iovecs) }.is_ok()
        } else {
            false
        };

        if !use_fixed && !iovecs.is_empty() {
            eprintln!("largeobj: IORING_REGISTER_BUFFERS failed, using regular Read/Write");
        }

        let mut pending: HashMap<u64, PendingOp> = HashMap::new();
        let mut next_token: u64 = 1;

        loop {
            if shutdown.load(Ordering::Relaxed) && pending.is_empty() {
                break;
            }

            // Phase 1: Drain channel → build SQEs.
            let mut batch = 0;
            while batch < 64 {
                match rx.try_recv() {
                    Ok(req) => {
                        let token = next_token;
                        next_token += 1;

                        let (sqe, op) = match req {
                            IoRequest::Read {
                                fd,
                                buf,
                                len,
                                on_complete,
                            } => {
                                let read_len = Self::align_up(len) as u32;
                                let sqe = if use_fixed {
                                    io_uring::opcode::ReadFixed::new(
                                        io_uring::types::Fd(fd),
                                        buf.ptr(),
                                        read_len,
                                        buf.idx(),
                                    )
                                    .offset(0)
                                    .build()
                                    .user_data(token)
                                } else {
                                    io_uring::opcode::Read::new(
                                        io_uring::types::Fd(fd),
                                        buf.ptr(),
                                        read_len,
                                    )
                                    .offset(0)
                                    .build()
                                    .user_data(token)
                                };
                                (sqe, PendingOp::Read { buf, on_complete })
                            }
                            IoRequest::Write {
                                fd,
                                buf,
                                len,
                                on_complete,
                            } => {
                                let write_len = len as u32;
                                let sqe = if use_fixed {
                                    io_uring::opcode::WriteFixed::new(
                                        io_uring::types::Fd(fd),
                                        buf.ptr() as *const u8,
                                        write_len,
                                        buf.idx(),
                                    )
                                    .offset(0)
                                    .build()
                                    .user_data(token)
                                } else {
                                    io_uring::opcode::Write::new(
                                        io_uring::types::Fd(fd),
                                        buf.ptr() as *const u8,
                                        write_len,
                                    )
                                    .offset(0)
                                    .build()
                                    .user_data(token)
                                };
                                (
                                    sqe,
                                    PendingOp::Write {
                                        buf,
                                        on_complete,
                                        len,
                                    },
                                )
                            }
                        };

                        pending.insert(token, op);

                        // SAFETY: The SQE references stable pool memory. ring.submission()
                        // is only accessed from this single poller thread (no races).
                        unsafe {
                            if ring.submission().is_full() {
                                ring.submit().ok();
                            }
                            ring.submission().push(&sqe).ok();
                        }
                        batch += 1;
                    }
                    Err(_) => break,
                }
            }

            // Phase 2: Submit + wait for at least 1 completion.
            if !pending.is_empty() {
                ring.submit_and_wait(1).ok();
            } else if shutdown.load(Ordering::Relaxed) {
                break;
            } else {
                // No pending work — brief sleep to avoid busy-spin.
                thread::sleep(std::time::Duration::from_micros(50));
                continue;
            }

            // Phase 3: Reap CQEs → fire callbacks.
            let mut completed = Vec::new();
            for cqe in ring.completion() {
                completed.push((cqe.user_data(), cqe.result()));
            }

            for (token, result) in completed {
                if let Some(op) = pending.remove(&token) {
                    match op {
                        PendingOp::Read { buf, on_complete } => {
                            if result >= 0 {
                                on_complete(buf, Ok(result as u64));
                            } else {
                                on_complete(buf, Err(StorageError::IoError { code: -result }));
                            }
                        }
                        PendingOp::Write {
                            buf,
                            on_complete,
                            len,
                        } => {
                            if result >= 0 && result as u64 >= len {
                                on_complete(buf, Ok(()));
                            } else if result < 0 {
                                on_complete(buf, Err(StorageError::IoError { code: -result }));
                            } else {
                                // Short write.
                                on_complete(buf, Err(StorageError::IoError { code: -1 }));
                            }
                        }
                    }
                }
            }
        }
    }

    fn error_drain_loop(rx: Receiver<IoRequest>, shutdown: Arc<AtomicBool>) {
        loop {
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(req) => match req {
                    IoRequest::Read {
                        buf, on_complete, ..
                    } => {
                        on_complete(buf, Err(StorageError::IoError { code: -1 }));
                    }
                    IoRequest::Write {
                        buf, on_complete, ..
                    } => {
                        on_complete(buf, Err(StorageError::IoError { code: -1 }));
                    }
                },
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }

    fn align_up(n: u64) -> u64 {
        (n + 4095) & !4095
    }
}
