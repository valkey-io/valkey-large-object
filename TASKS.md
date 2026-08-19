Hand written list of tasks to bring the Module to production grade.

ValkeyModule-rs Crate Updates:
- Update the Rust valkeymodule-rs crate - add a reply callback capability on mainthread
- Update the Rust valkeymodule-rs crate - add an API wrapper for getting the cluster node ID

General Functional Completeness in ValkeyLargeObjModule:
- Data type callbacks for COPY, MEMORY USAGE and FREE EFFORT
- Ensure we have a plan for when the object value is larger than the buffer size - reject or allocate new buffer (Define a oversized buffer path)
- Have the reply callback on the main thread
- Handle case when the DRAM exceeds memory usage limit.
- Handle case when the NVMe exceeds memory usage limit - max-bytes enforcement
- Handle case when the number of EFA sessions exceeds limit (add a configurable limit)
- Congestion Control/Rate limiting based on number of inflight requests
- FD pool size config and also implementing an LFU on the FD pool
- Ensure the ObjectID has a uniqueness guarantee - use node id + monotonic counter
- Request State and transition machninery across EFA and TCP cases
- Support multiple buffer sizes in the BufferPool layer.
- Per Client disconnect handling - EFA session tear down
- Shutdown handling - cleanup of buffer pools, fds and also dir/files in the NVMe, cancel inflight ops, etc.
- Module Metrics for Requests, BufferPool, NVMe, Objects, etc.
- Emit KeySpace Events from main thread callbacks
- Review thread boundaries and locking of every data structure for safety and concurrency
- Elimate as much as Rust `unsafe` declared code as possible
- Finalize command customer experience and implement arg parsing
- Finalize on metric names, key space event names, config names, errors
- Module Threads Core Pinning
- Adding unit testing on every interface
- I have already added support for sanity integ tests using valkey-test-framework. We need to complete this.
- CI Setup for full test run - build, unit test, integ test
- CI for performance testing automation runs;

NVMe optimizations / improvements:
- tokio-uring evaluation (replace hand-rolled poller)
- Per-key read coalescing (KeyState/singleflight)
- Older file version cleanup / garbage collection
- It is possible to have the fds pregistered. Check IORING_REGISTER_FILES (pre-register fds, benchmark impact)
- Evaluate Multi-poller (multiple io_uring rings for 16-drive parallelism)
- Storage Retryable errors
- NVMe Disk space accounting +  + eviction trigger
- Multiple stripe directories (beyond LVM) - Any ordering in striping and in encryption
- SQPOLL evaluation
- NVMe SMART monitoring

Transport:
- Transport API interface implementation using libfabric
- Buffer dual-registration (same pages in io_uring + EFA MR)
- Functional validation of EFA with the ValkeyLargeObjModule
- Transport Design
- Aligning and understanding the thread model and runtime sharing across the Module and crate boundary
- Performance benchmarking on EFA with NVMe only
- Performance benchmarking on EFA with DRAM tier

DRAM Tier:
- DRAM Tier design
- DRAM Tier - Have a Tier in the BufferPool to store values which live in DRAM. My idea is that we can have a DRAMBufferPool and NVMeBufferPool. ie, values for objects always are owned by the Module.
- DRAM Tier - Policy for Spilling
- DRAM Tier - Spilling
