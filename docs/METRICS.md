# INFO Metrics

`INFO largeobj` prints the module's statistics (they're also in `INFO modules` and `INFO everything`). Valkey prefixes the module name to every section and field, so the `cached_objects` field of the `dram` section prints as `largeobj_cached_objects` under `# largeobj_dram`. Field names are unique across sections, so a flat key lookup is safe.

- **Counters** are cumulative since module load (the SMART log's are device lifetime values; see its section). Take the difference between two reads for a rate.
- **Gauges** are point-in-time values. Fields ending in `_pct` are percentages to two decimals, such as `99.99`, as Valkey prints its own percentages.
- Sections marked **Tiered** are absent in Dram mode. Every other section prints in both modes, with all of its fields, whether or not anything has happened yet.
- Durations come as a pair of counters: over any interval, the change in `nvme_read_usec_total` divided by the change in `nvme_reads_total` is the mean NVMe read time in that interval.

## `largeobj_dram`

The DRAMPool. In Dram mode it holds every object; in Tiered mode it's the promotion cache in front of NVMe.

| Field | Type | Description |
|---|---|---|
| `dram_live_segments` | gauge | Segments in service. |
| `draining_segments` | gauge | Segments being drained for a shrink. They take no new allocations. |
| `dram_unused_segments` | gauge | Segment slots not in use. |
| `allocated_bytes` | gauge | Bytes allocated in live segments. |
| `dram_fragment_count` | gauge | Free gaps in live segments. Rising while `allocated_bytes` is flat means free space is splitting into small holes. |
| `capacity_bytes` | gauge | `dram_live_segments` × `dram_segment_size_bytes`. All of it counts toward `used_memory`, whether or not it holds data. |
| `utilization_pct` | gauge | `allocated_bytes` × 100 / `capacity_bytes`. |
| `cached_objects` | gauge | Objects in the pool: every object in Dram mode, promoted copies in Tiered mode. |
| `dram_hits_total` | counter | GETs served from the pool, counted at the lookup, so one whose transfer then fails still counts. In Dram mode, every GET of an existing key. |
| `dram_misses_total` | counter | Tiered GETs the pool couldn't serve, counted at the lookup: they go on to NVMe to promote or stream the object, or are refused (`nvme_buffer_exhausted`). A GET of an object still being promoted is a miss. Always 0 in Dram mode. |
| `scaling_expand_total` | counter | Pool expansions (one segment each). |
| `scaling_shrink_total` | counter | Pool shrinks (one segment each). |
| `dram_segment_size_bytes` | gauge | `segment-size`. |
| `efa_registered_segments` | gauge | Segments registered with the fabric, staging segments included. 0 when no fabric is up. |
| `dram_uring_registered_segments` | gauge | Segments in the io_uring fixed-buffer table, draining ones included. Below `dram_live_segments` while a new segment waits to be registered. 0 in Dram mode, which doesn't use io_uring. |

## `largeobj_nvme_staging` (Tiered)

The fixed pool of buffers that Tiered SETs, and GETs that stream from NVMe without promoting, move data through. Promotion reads go straight into DRAMPool buffers.

| Field | Type | Description |
|---|---|---|
| `nvme_live_segments` | gauge | Segments in service: `nvme-staging-size` rounded up to whole segments. |
| `nvme_unused_segments` | gauge | Segment slots not in use. |
| `nvme_fragment_count` | gauge | Free gaps in the pool's segments. |
| `staging_size_bytes` | gauge | `nvme-staging-size`. |
| `staging_utilization_pct` | gauge | Bytes held by in-flight requests × 100 / (`nvme_live_segments` × `nvme_segment_size_bytes`). Requests are refused (`nvme_buffer_exhausted`) once fewer than `min-buffers-per-op` buffers fit. |
| `nvme_segment_size_bytes` | gauge | `segment-size`. |
| `nvme_uring_registered_segments` | gauge | Segments in the io_uring fixed-buffer table. |

## `largeobj_nvme` (Tiered)

Object files on NVMe and the I/O against them.

| Field | Type | Description |
|---|---|---|
| `nvme_disk_used_bytes` | gauge | Disk space held by object files, as counted against `nvme-maxmemory`: each file's 4 KiB header plus each chunk rounded up to a multiple of 4 KiB. Includes files of SETs still in flight and of deleted objects still being read. |
| `nvme_disk_utilization_pct` | gauge | `nvme_disk_used_bytes` × 100 / `nvme-maxmemory`. 0 while `nvme-maxmemory` is 0 (unlimited), and above 100 if `nvme-maxmemory` is lowered below current usage. |
| `live_objects` | gauge | Keys holding an object. A value that's deleted, overwritten, expired or evicted counts until it's freed, which lazyfree may do in the background, so this can briefly exceed the number of keys. |
| `nvme_reads_total` | counter | io_uring reads of object files: data chunks and file headers, on the promotion and streaming paths. |
| `nvme_read_usec_total` | counter | Total time of those reads, from SQE push to CQE reap. |
| `nvme_writes_total` | counter | io_uring writes of object files: data chunks and file headers. |
| `nvme_write_usec_total` | counter | Total time of those writes, from SQE push to CQE reap. |

Failed and short I/Os count like any other I/O: they still occupied the device. COPY duplicates a file with buffered I/O outside io_uring, so it isn't counted.

## `largeobj_smartlog_usage` and `largeobj_smartlog_critical_warnings` (Tiered)

The NVMe SMART log, polled every `smartlog-poll-secs` and summed across controllers. Absent until the first poll, and absent entirely when `smartlog-poll-secs` is 0. The counters here are device lifetime values, not counts since module load, summed over the controllers read in the latest poll, so they drop if a controller can't be read.

| Field | Type | Description |
|---|---|---|
| `snapshot_age_seconds` | gauge | Age of the polled snapshot. |
| `devices` | gauge | Controllers polled. |
| `devices_read_failed` | gauge | Controllers whose SMART log couldn't be read. |
| `data_units_read`, `data_units_written` | counter | Summed device lifetime counters, in the spec's units of 512,000 bytes. |
| `percentage_used_avg` | gauge | Device life used, averaged over the controllers that were read. A whole number. |
| `available_spare_pct_avg` | gauge | Remaining spare capacity, averaged the same way. A whole number. |
| `media_errors` | counter | Summed unrecovered data integrity errors. |
| `unsafe_shutdowns` | counter | Summed unsafe shutdowns. |
| `spare_below_threshold`, `temperature_warning`, `reliability_degraded`, `media_read_only`, `volatile_mem_backup_failed`, `persistent_mem_read_only` | gauge | 1 if any controller reports that critical-warning bit, else 0. |

## `largeobj_efa`

DMA sessions and traffic over the fabric. Present in both modes, and all 0 when no fabric is available. Bytes are the RDMA payload of successful transfers: no transport headers or retransmissions.

| Field | Type | Description |
|---|---|---|
| `efa_sessions` | gauge | Live sessions: connections that ran `BLOB.HELLO` and haven't disconnected. |
| `efa_read_bytes_total` | counter | Bytes pulled from client memory by successful reads (the `BLOB.SET` path). |
| `efa_write_bytes_total` | counter | Bytes pushed into client memory by successful writes (the `BLOB.GET` path). |
| `efa_reads_total` | counter | Successful reads, one per client address of each chunk. |
| `efa_read_usec_total` | counter | Total time of those reads, submission to completion, including the checksum of what landed and any wait behind `fabric-max-in-flight`. The module's view, not wire latency. |
| `efa_writes_total` | counter | Successful writes, one per client address of each chunk. |
| `efa_write_usec_total` | counter | Total time of those writes, submission to completion, including any wait behind `fabric-max-in-flight`. |

Failed transfers count only in `efa_read_errors` and `efa_write_errors`, once per failed request.

## `largeobj_error_metrics`

Requests that failed, by cause. Each counts once per failed request.

| Field | Type | Description |
|---|---|---|
| `nvme_read_errors` | counter | Requests failed reading NVMe, including opening the object file. |
| `nvme_write_errors` | counter | Requests failed writing NVMe, including creating the object file. |
| `efa_read_errors` | counter | SETs failed pulling client memory. |
| `efa_write_errors` | counter | GETs failed pushing into client memory. |
| `dram_pool_exhausted` | counter | Dram-mode SETs refused because the pool was full and couldn't grow. |
| `nvme_buffer_exhausted` | counter | Tiered requests refused because fewer than `min-buffers-per-op` staging buffers were free. |
| `nvme_capacity_exceeded` | counter | Tiered SETs refused because the object wouldn't fit under `nvme-maxmemory`. |
| `set_finalize_stale` | counter | SETs discarded at commit because the key already held a newer object, such as one from a SET that started later. The client still gets `OK`. |
| `set_value_failures` | counter | SETs whose commit to the keyspace failed. |
