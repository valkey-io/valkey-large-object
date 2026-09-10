//! Nvme tiered mode on Linux via io_uring

use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd};

use valkey_module::{ValkeyError, ValkeyValue};

use crate::data_type::{LoValue, ObjectId, LO_TYPE};
use crate::engine::{
    efa_read_from_client, efa_write_to_client, serve_from_dram, DataSource, Transport,
};
use crate::errors;
use crate::storage::{self, uring};
use crate::OperatingMode;

// ─── GET ─────────────────────────────────────────────────────────────────────

/// Tiered GET: check DRAMPool → try promote → fall back to NVMe.
pub fn execute_get(
    object_id: ObjectId,
    obj_len: u64,
    transport: Transport,
    blocked_client: valkey_module::BlockedClient,
) {
    let dram_pool = storage::get_dram_pool();

    // ─── DRAMPool hit ────────────────────────────────────────────────────
    if let Some(obj_ctx) = dram_pool.get_object(&object_id) {
        if obj_ctx.is_ready() {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            serve_from_dram(dram_pool, &obj_ctx, obj_len, transport, thread_ctx);
            return;
        }
        // Filling state: promotion in progress.
        // TODO: coalesce — register as waiter on this ObjectContext.
        // For now: fall through to NVMe read.
    }

    // ─── Try DRAMPool promotion ──────────────────────────────────────────
    // If pool has space and object is eligible, read directly into DRAMPool.
    if let Some(obj_ctx) = dram_pool.try_promote_object(object_id, obj_len) {
        // TODO: Multi-buffer streaming/chunking (STORAGE_DESIGN.md §7.3).
        if obj_ctx.buffers.len() != 1 {
            todo!("streaming and chunking not yet implemented");
        }
        let seg_buf = &obj_ctx.buffers[0];
        let buf_ptr_usize = dram_pool.buffer_ptr(seg_buf) as usize;
        let read_op = uring::UringOp {
            iovec_index: dram_pool.segments()[seg_buf.segment_idx as usize].iovec_index,
            buf_ptr: buf_ptr_usize as *mut u8,
            file_offset: 0,
            len: obj_len,
        };

        let fd_pool = storage::get_fd_pool();
        let fd = match fd_pool.get_or_open(object_id, &crate::nvme_dir()) {
            Some(fd) => fd,
            None => {
                // remove_object drops the map's Arc; obj_ctx drops at end of scope
                // → ObjectContext::Drop frees the buffer automatically.
                dram_pool.remove_object(&object_id);
                let thread_ctx =
                    valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
                return;
            }
        };

        // Read from NVMe directly into DRAMPool buffer, then serve.
        crate::runtime_handle().spawn(async move {
            let result = uring::submit_read(fd, read_op).await;
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

            match result {
                Ok(Ok(_)) => {
                    // NVMe read complete — transition Filling→Ready.
                    // Release ordering ensures buffer data is visible to any
                    // thread that subsequently sees is_ready() == true.
                    obj_ctx.mark_ready();
                    let dram_pool = storage::get_dram_pool();
                    serve_from_dram(dram_pool, &obj_ctx, obj_len, transport, thread_ctx);
                }
                _ => {
                    // Read failed — remove entry. Buffers freed by ObjectContext Drop.
                    storage::get_dram_pool().remove_object(&object_id);
                    thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
                }
            }
        });
        return;
    }

    // ─── NVMePool fallback (DRAMPool full) ───────────────────────────────
    // Transient read: alloc NVMePool buffer, serve, free.
    let nvme_pool = storage::get_nvme_pool();
    let seg_buf = match nvme_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)));
            return;
        }
    };

    let stream_ctx = storage::StreamingContext::new(vec![seg_buf], obj_len, 1);

    let fd_pool = storage::get_fd_pool();
    let fd = match fd_pool.get_or_open(object_id, &crate::nvme_dir()) {
        Some(fd) => fd,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
            return;
        }
    };

    let buf_ptr_usize = nvme_pool.buffer_ptr(&stream_ctx.buffers[0]) as usize;
    // Single-chunk today: one UringOp for the entire object.
    // Streaming (STORAGE_DESIGN.md §7.3) will iterate stream_ctx.buffers and submit per-chunk ops in a loop.
    let read_op = uring::UringOp {
        iovec_index: nvme_pool.segments()[stream_ctx.buffers[0].segment_idx as usize].iovec_index,
        buf_ptr: buf_ptr_usize as *mut u8,
        file_offset: 0,
        len: obj_len,
    };

    // Spawn tokio task for NVMe read + serve (no caching — transient).
    // stream_ctx is moved into the async block so its Drop (which returns the
    // NVMe buffer to the pool) doesn't fire until the task completes.
    crate::runtime_handle().spawn(async move {
        let _keep_alive = stream_ctx;
        let read_result = uring::submit_read(fd, read_op).await;
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        handle_nvme_read_result(read_result, transport, obj_len, buf_ptr_usize, thread_ctx).await;
    });
}

/// Handle NVMe read result — reply to the blocked client based on the io_uring
/// completion result and transport type. Shared across GET paths that read from NVMe.
async fn handle_nvme_read_result(
    read_result: Result<Result<u64, storage::StorageError>, tokio::sync::oneshot::error::RecvError>,
    transport: Transport,
    obj_len: u64,
    buf_ptr_usize: usize,
    thread_ctx: valkey_module::ThreadSafeContext<valkey_module::BlockedClient>,
) {
    match read_result {
        Ok(Ok(_bytes_read)) => match transport {
            Transport::Tcp => {
                if crate::bench_mode() {
                    thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
                } else {
                    let data = unsafe {
                        std::slice::from_raw_parts(buf_ptr_usize as *const u8, obj_len as usize)
                            .to_vec()
                    };
                    thread_ctx.reply(Ok(ValkeyValue::StringBuffer(data)));
                }
            }
            Transport::Efa {
                session,
                rkey,
                remote_addr,
            } => {
                match efa_write_to_client(
                    session,
                    buf_ptr_usize,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                )
                .await
                {
                    Ok(()) => {
                        thread_ctx.reply(Ok(ValkeyValue::Integer(obj_len as i64)));
                    }
                    Err(e) => {
                        thread_ctx.reply(Err(e));
                    }
                }
            }
        },
        // io_uring read completed with an error (EIO, short read, etc.)
        Ok(Err(e)) => {
            thread_ctx.reply(Err(ValkeyError::String(format!(
                "{}: {}",
                errors::ERR_NVME_READ,
                e
            ))));
        }
        // RecvError: io_uring poller thread dropped the oneshot sender.
        // This means the poller panicked or shut down unexpectedly.
        // TODO: Add error metric counter for poller channel failures.
        Err(_) => {
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_READ)));
        }
    }
}

// ─── SET ─────────────────────────────────────────────────────────────────────

/// Tiered SET: write to NVMe (invalidate DRAMPool entry if exists).
pub fn execute_set(
    key_name: Vec<u8>,
    obj_len: u64,
    data_source: DataSource,
    blocked_client: valkey_module::BlockedClient,
    object_id: ObjectId,
) {
    let nvme_pool = storage::get_nvme_pool();

    // TODO (object lifecycle): Overwriting a key leaks old object. See execute_set_dram_efa.

    // Alloc NVMePool buffer for the write.
    let seg_buf = match nvme_pool.alloc(obj_len as usize) {
        Some(b) => b,
        None => {
            let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)));
            return;
        }
    };

    // StreamingContext owns the NVMePool buffer for this SET operation.
    // Single buffer today; multi-batch streaming adds more buffers here.
    let stream_ctx = storage::StreamingContext::new(vec![seg_buf], obj_len, 1);

    let buf_ptr = nvme_pool.buffer_ptr(&stream_ctx.buffers[0]);
    let buf_ptr_usize = buf_ptr as usize;

    match data_source {
        DataSource::Tcp(data) => {
            // TCP: memcpy into NVMePool buffer, then write to NVMe.
            let copy_len = data.len().min(obj_len as usize);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr, copy_len) };
            crate::runtime_handle().spawn(async move {
                do_tiered_nvme_write(
                    buf_ptr_usize,
                    obj_len,
                    stream_ctx,
                    blocked_client,
                    key_name,
                    object_id,
                )
                .await;
            });
        }
        DataSource::Efa {
            session,
            rkey,
            remote_addr,
        } => {
            // EFA: transport.read into NVMePool buffer, then write to NVMe.
            crate::runtime_handle().spawn(async move {
                match efa_read_from_client(
                    session,
                    buf_ptr_usize,
                    obj_len as usize,
                    rkey,
                    remote_addr,
                )
                .await
                {
                    Ok(()) => {
                        do_tiered_nvme_write(
                            buf_ptr_usize,
                            obj_len,
                            stream_ctx,
                            blocked_client,
                            key_name,
                            object_id,
                        )
                        .await;
                    }
                    Err(_) => {
                        let thread_ctx =
                            valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
                        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_EFA_READ)));
                    }
                }
            });
        }
    }
}

/// Shared Tiered NVMe write: CRC → open tmp → WriteFixed → rename → create LoValue.
/// Must be called from within a tokio task (awaits io_uring write).
async fn do_tiered_nvme_write(
    buf_ptr_usize: usize,
    obj_len: u64,
    stream_ctx: storage::StreamingContext,
    blocked_client: valkey_module::BlockedClient,
    key_name: Vec<u8>,
    object_id: ObjectId,
) {
    // Reject if writing this object would exceed nvme-maxmemory.
    // stream_ctx Drop frees the buffer on return.
    if !uring::has_nvme_capacity(obj_len) {
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_POOL_EXHAUSTED)));
        return;
    }
    let buf_ptr = buf_ptr_usize as *mut u8;
    let crc = crc32c::crc32c(unsafe { std::slice::from_raw_parts(buf_ptr, obj_len as usize) });

    let dir = crate::nvme_dir();
    let file_path = object_id.file_path(&dir);

    let c_path = std::ffi::CString::new(file_path.as_str()).expect("file_path null");
    let mut write_flags = libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC;
    if crate::direct_io() {
        write_flags |= storage::DIRECT_IO_FLAG;
    }
    // FdPool intentionally not used on SET path — fd cached lazily on first GET via get_or_open.
    let raw_fd = unsafe { libc::open(c_path.as_ptr(), write_flags, 0o644) };
    if raw_fd < 0 {
        let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);
        thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        return;
    }
    // OwnedFd closes on drop — no leak on early return.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };

    let nvme_pool = storage::get_nvme_pool();
    // Single-chunk today: one UringOp for the entire object.
    // Streaming (STORAGE_DESIGN.md §7.3) will iterate stream_ctx.buffers and submit per-chunk ops in a loop.
    let write_op = uring::UringOp {
        iovec_index: nvme_pool.segments()[stream_ctx.buffers[0].segment_idx as usize].iovec_index,
        buf_ptr: buf_ptr_usize as *mut u8,
        file_offset: 0,
        len: obj_len,
    };

    // Reserve disk usage before the write — decrement on any failure path.
    uring::increase_nvme_disk_usage(obj_len);

    let write_result = uring::submit_write(fd.as_raw_fd(), write_op).await;
    // fd (OwnedFd) drops when out of scope and close() is automatic.

    let thread_ctx = valkey_module::ThreadSafeContext::with_blocked_client(blocked_client);

    match write_result {
        Ok(Ok(())) => {
            // Version check + set_value. No DRAMPool involvement on Tiered SET.
            {
                let ctx = thread_ctx.lock();
                let key_str = ctx.create_string(key_name);
                let key = ctx.open_key_writable(&key_str);
                if let Ok(Some(existing)) = key.get_value::<LoValue>(&LO_TYPE) {
                    if existing.object_id > object_id {
                        // Stale write — a newer SET already completed. Discard silently.
                        uring::decrease_nvme_disk_usage(obj_len);
                        let _ = std::fs::remove_file(&file_path);
                        thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
                        return;
                    }
                }
                let lo_value = LoValue {
                    object_id,
                    len: obj_len,
                    crc32c: crc,
                };
                if key.set_value(&LO_TYPE, lo_value).is_err() {
                    uring::decrease_nvme_disk_usage(obj_len);
                    let _ = std::fs::remove_file(&file_path);
                    thread_ctx.reply(Err(ValkeyError::Str("ERR failed to set key")));
                    return;
                }
            }
            thread_ctx.reply(Ok(ValkeyValue::SimpleStringStatic("OK")));
        }
        Ok(Err(e)) => {
            uring::decrease_nvme_disk_usage(obj_len);
            let _ = std::fs::remove_file(&file_path);
            thread_ctx.reply(Err(ValkeyError::String(format!(
                "{}: {}",
                errors::ERR_NVME_WRITE,
                e
            ))));
        }
        // RecvError: io_uring poller thread dropped the oneshot sender.
        // This means the poller panicked or shut down unexpectedly.
        // TODO: Add error metric counter for poller channel failures.
        Err(_) => {
            uring::decrease_nvme_disk_usage(obj_len);
            let _ = std::fs::remove_file(&file_path);
            thread_ctx.reply(Err(ValkeyError::Str(errors::ERR_NVME_WRITE)));
        }
    }
}

// ─── Data Type Callbacks ─────────────────────────────────────────────────────

/// Tiered mode: copy NVMe file with a fresh OID.
/// Operates at the NVMe level only — DRAMPool promotion is per-key and not carried over.
/// Returns None if nvme-maxmemory would be exceeded.
pub fn create_copy(value: &LoValue) -> Option<LoValue> {
    let data_dir = crate::nvme_dir();
    if !crate::storage::uring::has_nvme_capacity(value.len) {
        return None;
    }
    let new_oid = ObjectId::next();
    let src_path = value.object_id.file_path(&data_dir);
    let dst_path = new_oid.file_path(&data_dir);
    std::fs::copy(&src_path, &dst_path)
        .expect("Tiered COPY: source file missing — key exists implies file exists");
    crate::storage::uring::increase_nvme_disk_usage(value.len);
    Some(LoValue {
        object_id: new_oid,
        len: value.len,
        crc32c: value.crc32c,
    })
}

// FdPool and NVMe files only exist in Tiered mode, which only exists on Linux.
pub fn free(value: &LoValue) {
    crate::storage::get_fd_pool().remove(value.object_id);
    crate::storage::delete_file(value.object_id);
    crate::storage::uring::decrease_nvme_disk_usage(value.len);
}

// ─── io_uring Engine Lifecycle ───────────────────────────────────────────────

/// The engine between construction and commit. `None` outside Tiered mode.
pub type PreparedEngine = Option<uring::UringNvmeEngine>;

/// Build the io_uring NVMe engine, in Tiered mode only. Ring creation and buffer registration
/// happen on the caller's (main) thread so failures return Err rather than panicking in the poller.
pub fn prepare_engine(
    mode: OperatingMode,
    iovecs: Vec<libc::iovec>,
) -> Result<PreparedEngine, String> {
    if OperatingMode::Tiered != mode {
        return Ok(None);
    }
    let engine =
        uring::UringNvmeEngine::new(iovecs).map_err(|e| format!("io_uring engine: {}", e))?;
    Ok(Some(engine))
}

/// Publish the prepared engine. Called only after every other init step has succeeded.
pub fn commit_engine(engine: PreparedEngine) {
    if let Some(engine) = engine {
        uring::set_nvme_engine(engine);
    }
}
