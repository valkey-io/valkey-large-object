# Getting started

> **Note:** valkey-large-object is at version 0.x.
> Commands, arguments, replies, and configuration can change before version 1.0.

valkey-large-object is a Valkey module that stores large binary values (for example model data or cache blocks for machine learning inference) in DRAM or in files on a local disk.
Clients send and get the values over the normal Valkey connection, or with RDMA directly to and from client memory.

This page shows how to build the module, load it into Valkey, and run your first commands.
For the command reference, see [COMMANDS.md](COMMANDS.md).

## Contents

- [Terms](#terms)
- [Requirements](#requirements)
- [Build](#build)
- [Load the module](#load-the-module)
- [Verify the module](#verify-the-module)
- [First commands](#first-commands)
- [Behavior to know](#behavior-to-know)
- [Limitations](#limitations)
- [Security](#security)
- [What to watch](#what-to-watch)
- [Troubleshooting](#troubleshooting)
- [Next steps](#next-steps)

## Terms

| Term | Meaning |
|---|---|
| Segment | A large block of DRAM that the module reserves at one time. The module stores objects in segments. All segments have the size `largeobj.segment-size`. |
| `Dram` mode | All objects are in DRAM segments. This is the default `largeobj.operating-mode`. |
| `Tiered` mode | Each object is a file on disk, and DRAM segments are a read cache. |
| Promote | In `Tiered` mode, copy an object from disk into the DRAM cache. |
| Disk staging buffers | DRAM segments that the module uses for disk reads and writes in `Tiered` mode. |
| Fabric | The [libfabric](https://ofiwg.github.io/libfabric/) network layer that the RDMA commands use. A fabric domain is one network device as libfabric names it, for example `lo` (loopback) or an EFA device. The module starts one fabric service for each domain. |

## Requirements

- **Valkey 8.0 or later.** Get it from [valkey.io/download](https://valkey.io/download/), or build it from [source](https://github.com/valkey-io/valkey).
- **Linux on x86_64 or aarch64.** The module has no architecture-specific code.
- **Linux 5.1 or later for `Tiered` mode.** The module uses io_uring for disk I/O, with the operations `IORING_OP_READV`, `IORING_OP_WRITEV`, `IORING_OP_READ_FIXED`, and `IORING_OP_WRITE_FIXED`, and with registered buffers (`IORING_REGISTER_BUFFERS`).
  These are in Linux 5.1 and later.
- **Rust.** A recent stable toolchain with `cargo`, for example from [rustup](https://rustup.rs/).
- **Build tools.** `clang`, `libclang-dev` (for bindgen), `build-essential` (or `gcc` and `make`), and `pkg-config`.
- **libfabric 2.0 or later, with development files.** The build needs libfabric also when you use only the TCP commands.
  The module uses the libfabric 2.0 interface (symbol version `FABRIC_1.8`), and the build needs the header `rdma/fi_collective.h`.
  An older libfabric, for example 1.8.0, fails (see [Troubleshooting](#troubleshooting)).
  The build finds libfabric with `pkg-config`; check with `pkg-config --modversion libfabric`.
- **EFA hardware for production RDMA.** The default fabric provider, `EfaDirect`, uses Elastic Fabric Adapter (EFA) hardware (see [RDMA overview](COMMANDS.md#rdma-overview)).
  Without RDMA hardware, use the TCP commands.
  To develop RDMA client code without RDMA hardware, set `largeobj.fabric-provider` to `Emulated` (see [Try RDMA without RDMA hardware](COMMANDS.md#try-rdma-without-rdma-hardware)).
- **RDMA clients: RMA support and locked memory.** The server reads from and writes into client memory with RDMA, so the libfabric provider of the client must support RMA (`FI_RMA`) with remote read and remote write.
  The client registers its buffers with `fi_mr_reg`, which also counts against the `RLIMIT_MEMLOCK` of the client process.
  Set the client limit to at least the total size of its registered buffers, or to `unlimited`.
- **Locked-memory limit.** The module registers (pins) each segment one time for each fabric domain, and one more time for io_uring in `Tiered` mode.
  This counts against `RLIMIT_MEMLOCK`.
  Set the limit to `unlimited` (for example `ulimit -l unlimited`, or `LimitMEMLOCK=infinity` in a systemd unit).
  Else set it to at least the total segment bytes (DRAM pool and disk staging buffers) times the number of registrations.
- **Tiered mode only: XFS.** XFS is the only supported file system for `largeobj.disk-dir`.
  The module needs `O_DIRECT` and user extended attributes (`user.*`): it stores a small header for each object file in the `user.lobj` extended attribute.
  Other file systems, for example ext4, are not supported, also when they have these features.
  On such a file system, the module loads with a warning in the log.
  The module does not check for `O_DIRECT` and extended attributes at load time.
  On a file system without them (for example tmpfs on older kernels), every write fails with `ERR disk write failed`.
- **Tiered mode only: open-file limit.** For all features, the open-file limit must be at least `2 x maxclients + 128 + largeobj.max-cached-fds`.
  With the defaults (`maxclients` 10000, `largeobj.max-cached-fds` 16384), this is 36512.
  Raise the limit with `ulimit -n` in the shell or `LimitNOFILE` in a systemd unit, or set a lower `maxclients`.
  Below `2 x maxclients + 128`, the module loads with the file descriptor cache off.
  Between that value and the full value, the cache is smaller.
  The server log shows the limit to set (see [Troubleshooting](#troubleshooting)).

## Build

```bash
git clone https://github.com/valkey-io/valkey-large-object
cd valkey-large-object
cargo build --release
```

The build writes the module to `target/release/libvalkey_large_object.so`.

If libfabric is not in a default location, or the default libfabric is older than 2.0, give the `pkg-config` directory of the newer one.
If bindgen cannot find the standard C headers (for example `stddef.h`), add the GCC include directory.
You can set both:

```bash
PKG_CONFIG_PATH=/opt/amazon/efa/lib/pkgconfig \
BINDGEN_EXTRA_CLANG_ARGS="-I$(gcc -print-file-name=include)" \
cargo build --release
```

`./build.sh build` runs the format and lint checks, the release build, and the unit tests.
`./build.sh` with no arguments also runs the integration tests.
See the [README](../README.md#test).

If you set `PKG_CONFIG_PATH`, also set `LD_LIBRARY_PATH` to the `lib` directory of the same libfabric.
You need it when you run `./build.sh`, `valkey-server`, or the test peer `fabric_target`.

## Load the module

Module arguments are pairs of configuration names and values.

- **On the `loadmodule` line, use the short names without the `largeobj.` prefix, for example `segment-size`, and give memory sizes in bytes.**
  For the naming and unit rules, see [Configuration](COMMANDS.md#configuration).
- `largeobj.segment-size` must be at least twice `largeobj.chunk-size` (default 1MB).
- Values such as `Dram`, `Tiered`, `EfaDirect`, and `Emulated` are case-sensitive.

### Dram mode

All objects are in DRAM.
This example uses 64MB (67108864 bytes) segments.
In `Dram` mode an object must fit in one segment (see `largeobj.max-object-size` in [Configuration](COMMANDS.md#configuration)).
For 64MB segments, the largest `largeobj.max-object-size` is 66842624.
This example uses 32MB (33554432 bytes).

```bash
valkey-server --port 6379 \
  --maxmemory 4gb \
  --maxmemory-policy allkeys-lru \
  --loadmodule ./target/release/libvalkey_large_object.so \
    operating-mode Dram \
    segment-size 67108864 \
    max-object-size 33554432
```

The DRAM limit is the core Valkey `maxmemory` setting.
Each pool has at most 16384 segments, so the DRAM pool does not grow past 16384 x `largeobj.segment-size` (1TB with 64MB segments), also when `maxmemory` is larger.
The default `maxmemory-policy` is `noeviction`: when the module is full, a write fails with an `OOM` error.
The examples use `allkeys-lru`, so a write evicts the least recently used large objects instead.
Use `volatile-lru` to let the module evict only keys that have a TTL (see [Eviction](#eviction)).
When a fabric is up, `largeobj.max-rdma-registered-bytes` (default 380GB) also limits the DRAM pool.

### Tiered mode

Each object is stored in a file in `largeobj.disk-dir`, and DRAM is a read cache.
Put `largeobj.disk-dir` on an XFS file system (see [Requirements](#requirements)).
`largeobj.max-promote-size` (default 256MB) must fit in one segment, with the same overhead as `largeobj.max-object-size` (with 64MB segments, at most 66842624).
This example uses 64MB segments, a 32MB maximum object size and promotion size, a 10GB disk limit, and 64MB of disk staging buffers.
Change `largeobj.disk-dir` to a directory on your disk.

```bash
valkey-server --port 6379 \
  --maxmemory 4gb \
  --maxmemory-policy allkeys-lru \
  --loadmodule ./target/release/libvalkey_large_object.so \
    operating-mode Tiered \
    disk-dir /mnt/nvme/largeobj \
    segment-size 67108864 \
    max-object-size 33554432 \
    max-promote-size 33554432 \
    disk-maxmemory 10737418240 \
    disk-staging-size 67108864
```

> **Warning:** In `Tiered` mode, at startup and at shutdown, the module deletes the `largeobj.disk-dir` directory and everything in it, then creates it again, empty.
> It deletes all files and subdirectories, not only object files.
> Use a dedicated subdirectory that is only for this module, for example `/mnt/nvme/largeobj`.
> Do not use a mount point.
> If `largeobj.disk-dir` is a mount point, the module first deletes all contents of the mounted file system, then cannot remove the mount point (`EBUSY`), and the module does not load.
> The parent directory of `largeobj.disk-dir` must be writable by the server, because the module removes and creates `largeobj.disk-dir`.

The module makes `largeobj.disk-dir-shards` subdirectories in `largeobj.disk-dir` when it starts (default 1024, named `0000` to `03ff`).

### Load from valkey.conf

Add a `loadmodule` line to `valkey.conf`:

```
loadmodule /path/to/libvalkey_large_object.so operating-mode Dram segment-size 67108864 max-object-size 33554432
```

You can also put each configuration on its own line after the `loadmodule` line, with the full `largeobj.` name.
Memory sizes can have units there:

```
loadmodule /path/to/libvalkey_large_object.so
largeobj.operating-mode Dram
largeobj.segment-size 64mb
largeobj.max-object-size 32mb
```

### Fabric provider

By default the module tries to start the `EfaDirect` fabric provider.
On a host without EFA, the module still loads, and only the TCP commands work.
The server log then shows:

```
largeobj: fabric unavailable, RDMA path disabled: fabric error: fi_getinfo: No data available (-61). Only Large Object Commands of the TCP variant will be supported
```

To try the RDMA commands on a host without RDMA hardware, add the module arguments `fabric-provider Emulated fabric-interfaces lo`.
See [COMMANDS.md](COMMANDS.md#try-rdma-without-rdma-hardware).

## Verify the module

The module name is `largeobj`.

```
127.0.0.1:6379> MODULE LIST
1) 1) "name"
   2) "largeobj"
   3) "ver"
   4) (integer) 1
   ...
```

All module commands are in the ACL category `largeobj`:

```
127.0.0.1:6379> COMMAND LIST FILTERBY ACLCAT largeobj
1) "BLOB.RDMA_HELLO"
2) "BLOB.TCP_GET"
3) "BLOB.TCP_SET"
4) "BLOB.INFO"
5) "BLOB.RDMA_GET"
6) "BLOB.RDMA_SET"
```

The order of the list can be different.
`ver` is the version number that the module registers: 1 for version 0.1.0.

## First commands

Store a value, read it, and get its metadata:

```
127.0.0.1:6379> BLOB.TCP_SET greeting hello
OK
127.0.0.1:6379> BLOB.TCP_GET greeting
"hello"
127.0.0.1:6379> BLOB.INFO greeting
1) len
2) (integer) 5
3) crc
4) (integer) 2591144780
5) tier
6) dram
127.0.0.1:6379> BLOB.INFO greeting LEN
(integer) 5
127.0.0.1:6379> BLOB.TCP_GET missing
(nil)
```

`crc` is the CRC32C (Castagnoli) checksum of the bytes `hello`.
In `Tiered` mode, `tier` is `disk` until a read promotes the object to DRAM.
With the default `largeobj.promote-min-hits` 1, the first read promotes it:

```
127.0.0.1:6379> BLOB.TCP_SET greeting hello
OK
127.0.0.1:6379> BLOB.INFO greeting TIER
disk
127.0.0.1:6379> BLOB.TCP_GET greeting
"hello"
127.0.0.1:6379> BLOB.INFO greeting TIER
dram
```

To store a file, use the `-x` option of `valkey-cli`.
It reads the last argument from standard input:

```bash
valkey-cli -x BLOB.TCP_SET model:embeddings:v1 < embeddings.bin
```

Delete an object with the core `DEL` command:

```
127.0.0.1:6379> DEL greeting
(integer) 1
127.0.0.1:6379> BLOB.INFO greeting
(nil)
```

See the module metrics with `INFO largeobj`:

```
127.0.0.1:6379> INFO largeobj
# largeobj_core_metrics
largeobj_num_objects:0
largeobj_inflight_requests:0

# largeobj_evictions
largeobj_evictions:0
largeobj_pending_evictions:0
...
```

`INFO largeobj` shows all fields, in sections for evictions, DRAM, disk, RDMA, and errors.
When `largeobj.enabled` is `no` and no large objects are stored, `INFO largeobj` returns no sections.
Some important fields:

| Field | Meaning |
|---|---|
| `largeobj_num_objects` | Number of large objects. |
| `largeobj_evictions` | Number of large object keys evicted. |
| `largeobj_dram_allocated_bytes`, `largeobj_dram_capacity_bytes` | DRAM in use by objects, and DRAM in segments. |
| `largeobj_disk_used_bytes` | `Tiered` mode: disk space in use. |
| `largeobj_disk_metadata_spills` | `Tiered` mode: object files that take disk blocks outside their inode, in addition to their data. For example, a file that has more extents than its inode can list uses one more block. These blocks count in `largeobj_disk_used_bytes`. |
| `largeobj_rdma_sessions` | Open RDMA sessions. |
| `largeobj_disk_write_errors`, `largeobj_rdma_read_errors`, `largeobj_crc_mismatch` | Error counters. |

## Behavior to know

### No persistence

The module does not keep object data across a restart or a reload, in `Dram` or `Tiered` mode.

- `SAVE`, `BGSAVE`, and `SHUTDOWN` with save points succeed, but the RDB file has only the key names, without the object data.
- `BGREWRITEAOF` keeps no object data.
  With `aof-use-rdb-preamble yes` (the default), the rewritten AOF has the key names, as with `SAVE`.
  With `aof-use-rdb-preamble no`, it has no large object keys.
- After the server loads an RDB file, each large object key reads as missing.
  The module starts to delete these keys within `largeobj.scaling-poll-ms` (default 2.5 seconds) after the load.
  With many keys, this can take longer.
  Until then, `EXISTS` and `DBSIZE` still count them.
- `DUMP` returns a value without the object data.
  `RESTORE` of that value fails with `ERR Bad data format`.
  Thus `MIGRATE` of a large object key fails, and the key stays on the source server.
- The module does not replicate large objects.

The server log shows a warning, for example `largeobj: persistence is not supported: blobs are saved to RDB/DUMP without their data`, the first time that each case occurs.

Example after `SAVE`, `SHUTDOWN`, and a restart:

```
127.0.0.1:6379> EXISTS mykey
(integer) 1
127.0.0.1:6379> BLOB.TCP_GET mykey
(nil)
127.0.0.1:6379> EXISTS mykey
(integer) 1
```

A few seconds later:

```
127.0.0.1:6379> EXISTS mykey
(integer) 0
```

### Transactions and scripts

`BLOB.TCP_SET`, `BLOB.TCP_GET`, `BLOB.RDMA_SET`, and `BLOB.RDMA_GET` are not permitted in `MULTI`/`EXEC`, Lua scripts, functions, or module calls.
These are blocking commands: the module blocks the client and does the I/O on a worker thread, and Valkey does not let a command block in these contexts.
In a transaction, only that command fails, and the other commands run.
In this example, the queued `BLOB.TCP_SET` does not run, so `mykey` keeps its 11-byte value:

```
127.0.0.1:6379> BLOB.TCP_SET mykey "hello world"
OK
127.0.0.1:6379> MULTI
OK
127.0.0.1:6379(TX)> BLOB.TCP_SET mykey "new value"
QUEUED
127.0.0.1:6379(TX)> BLOB.INFO mykey LEN
QUEUED
127.0.0.1:6379(TX)> EXEC
1) (error) ERR command not allowed in MULTI, scripts or module calls
2) (integer) 11
127.0.0.1:6379> BLOB.TCP_GET mykey
"hello world"
```

`BLOB.INFO` and `BLOB.RDMA_HELLO` are permitted there.

### Turn off the module without a restart

You cannot unload the module with `MODULE UNLOAD`, because it registers a data type.
To stop clients from using the module, set `largeobj.enabled` to `no`.
Stored keys stay, and core commands such as `TYPE`, `EXISTS`, and `DEL` still work on them:

```
127.0.0.1:6379> BLOB.TCP_SET mykey "hello world"
OK
127.0.0.1:6379> CONFIG SET largeobj.enabled no
OK
127.0.0.1:6379> BLOB.TCP_GET mykey
(error) DISABLED largeobj module is disabled
127.0.0.1:6379> TYPE mykey
largeob-k
127.0.0.1:6379> CONFIG SET largeobj.enabled yes
OK
127.0.0.1:6379> BLOB.TCP_GET mykey
"hello world"
```

To remove the module, stop the server and start it without the `loadmodule` argument.

### Eviction

A write can evict other large objects to make space.
The core `maxmemory-policy` setting controls which objects the module can evict:

| Policy | Objects that the module can evict |
|---|---|
| `allkeys-*` | Any large object. |
| `volatile-*` | Only large object keys with a TTL. |
| `noeviction` | None. The write fails with an `OOM` error. |

The module evicts in least-recently-used (LRU) order for all policies.
A read and a TTL change count as a use.
For the `*-lfu`, `*-random`, and `volatile-ttl` policies, the module also uses LRU order, and the server log shows a warning.

- `Dram` mode: eviction occurs only when `maxmemory` is set.
- `Tiered` mode: eviction removes object files when `largeobj.disk-maxmemory` is larger than 0.
  `maxmemory` is not necessary for disk eviction.
  When the DRAM cache is full, the module drops cached copies; the objects stay on disk.

An evicted key stays in the keyspace until the module deletes it, within a few milliseconds when the backlog is small.
Until then, a read returns nil, but `EXISTS` and `DBSIZE` still count the key.
`largeobj_pending_evictions` in `INFO largeobj` shows how many evicted keys wait to be deleted.
In this example, a write evicts 8 objects, and `obj1` is one of them:

```
127.0.0.1:6379> BLOB.TCP_GET obj1
(nil)
127.0.0.1:6379> EXISTS obj1
(integer) 1
127.0.0.1:6379> INFO largeobj
...
largeobj_pending_evictions:8
...
```

Less than a second later, `EXISTS obj1` returns 0.

## Limitations

### Cluster mode

The module loads on each node of a cluster.
Each command uses one key (or no key for `BLOB.RDMA_HELLO`), so a cluster client routes the commands by slot, as for core commands.

- An RDMA session belongs to one connection to one node.
  Send `BLOB.RDMA_HELLO` on each connection to each node, and add the server addresses of each node to the address vector.
  Each node replies with its own server addresses.
- You cannot move a slot that has large object keys.
  `MIGRATE` uses `DUMP` and `RESTORE`, which do not keep object data (see [No persistence](#no-persistence)).
  The key stays on the source node:

  ```
  127.0.0.1:7395> MIGRATE 127.0.0.1 7394 foo 0 5000
  (error) ERR Target instance replied with error: ERR Bad data format
  ```

  Atomic slot migration (`CLUSTER MIGRATESLOTS`) is refused:

  ```
  127.0.0.1:7395> CLUSTER MIGRATESLOTS SLOTSRANGE 12182 12182 NODE 3f6baa5225d92839763069e7ec333d742c40d629
  (error) ERR The module largeobj does not support atomic slot migrations. Please ensure all modules have declared support for atomic slot migration and try again
  ```

- To add capacity, you have these options:
  - Raise `maxmemory` (`Dram` mode) or `largeobj.disk-maxmemory` (`Tiered` mode) on the nodes, if the hosts have the memory or disk. Both are mutable with `CONFIG SET`.
  - Move a slot to a new node only when the slot has no large object keys. Delete the large object keys in the slot first (or let them expire), move the slot, and then write the objects again.
  - Make a new, larger cluster and write the objects to it.
- After a `MOVED` or `ASK` redirect, the client sends the command on a connection to the other node.
  If that connection has no RDMA session, `BLOB.RDMA_SET` and `BLOB.RDMA_GET` fail with `ERR no RDMA session (call BLOB.RDMA_HELLO first)`.
  Send `BLOB.RDMA_HELLO` on that connection first.

### Replication

The module does not replicate large objects.
Replicas are not supported for large object data.

- A large object write on the primary is not sent to the replica.
  The replica does not have the key.
- At a full synchronization, the replica loads the large object keys without their data.
  They read as missing, and the replica deletes them a few seconds later (as after an RDB load, see [No persistence](#no-persistence)).
- In that time, a `COPY` from the primary of such a key fails on the replica.
  The replica log then shows `This replica is sending an error to its primary: 'module key failed to copy'`.
- If a replica becomes the primary, it has no large objects.

### Memory reports and defragmentation

- `MEMORY USAGE` of a large object key counts the object data only when the data is in DRAM.
  In `Tiered` mode, a key that is only on disk shows a small value.
- Active defragmentation does not move large object memory.

### Persistence, transactions, and scripts

- The module does not keep object data across a restart (see [No persistence](#no-persistence)).
- The data commands are not permitted in transactions, scripts, or module calls (see [Transactions and scripts](#transactions-and-scripts)).

### Core commands on large object keys

These results are the same in `Dram` and `Tiered` mode.

| Command | Result |
|---|---|
| `COPY` | Works. The copy has its own data. |
| `RENAME`, `RENAMENX` | Work. |
| `MOVE`, `SWAPDB` | Work. |
| `EXPIRE`, `TTL`, `PERSIST` | Work. |
| `TYPE` | Returns `largeob-k`. |
| `OBJECT ENCODING` | Returns `raw`. |
| `DUMP` | Returns a value without the object data. |
| `RESTORE` | Fails with `ERR Bad data format`. |
| `DEL`, `UNLINK` | Work. The module frees the storage in the background. |
| `FLUSHALL`, `FLUSHALL ASYNC` | Work. |

```
127.0.0.1:6379> BLOB.TCP_SET a "hello world"
OK
127.0.0.1:6379> COPY a b
(integer) 1
127.0.0.1:6379> BLOB.TCP_GET b
"hello world"
127.0.0.1:6379> RENAME a c
OK
127.0.0.1:6379> MOVE c 1
(integer) 1
127.0.0.1:6379> TYPE b
largeob-k
127.0.0.1:6379> OBJECT ENCODING b
"raw"
```

## Security

### ACL categories

All commands are in the ACL category `@largeobj`.
They are also in these core categories:

| Command | `@read` | `@write` | `@fast` | `@connection` | `@largeobj` |
|---|---|---|---|---|---|
| `BLOB.INFO` | yes | | yes | | yes |
| `BLOB.RDMA_GET` | yes | | yes | | yes |
| `BLOB.RDMA_HELLO` | | | yes | yes | yes |
| `BLOB.RDMA_SET` | | yes | yes | | yes |
| `BLOB.TCP_GET` | yes | | yes | | yes |
| `BLOB.TCP_SET` | | yes | yes | | yes |

`@largeobj` has only the `BLOB.*` commands.
`DEL` and `UNLINK` are not in `@largeobj`, so a user that must delete large object keys also needs these commands.
This example makes a user for the module, and a read-only user:

```
127.0.0.1:6379> ACL SETUSER app on >app-secret ~* +@largeobj +del +unlink
OK
127.0.0.1:6379> ACL SETUSER reader on >reader-secret ~* +@largeobj -@write
OK
```

The read-only user can read objects and open an RDMA session, but cannot write:

```
127.0.0.1:6379> AUTH reader reader-secret
OK
127.0.0.1:6379> BLOB.TCP_GET greeting
"hello"
127.0.0.1:6379> BLOB.TCP_SET b x
(error) NOPERM User reader has no permissions to run the 'BLOB.TCP_SET' command
```

### How data moves

- The TCP commands carry the object data on the normal Valkey connection.
  If you use Valkey TLS, TLS also protects this data.
- The RDMA commands carry only the command, the key, the client memory regions, and the reply on the Valkey connection.
  The object data moves directly between client memory and server memory over the fabric, not through the Valkey connection.
  Valkey TLS does not protect this data, and the module does not encrypt it.
- In `Tiered` mode, the module does not encrypt the object files in `largeobj.disk-dir`.
  If you need encryption at rest, use file system or disk encryption.
- For an RDMA transfer, the server reads or writes only the client memory regions that the command names, at the client endpoint of the session of that connection.
- The server registers its own memory for local access only.
  A client cannot read or write server memory directly; the server starts every transfer.

## What to watch

| `INFO largeobj` field | What it tells you | Action |
|---|---|---|
| `largeobj_evictions` | Large object keys evicted to make space. | If it grows fast, add memory or disk, or store fewer objects. |
| `largeobj_pending_evictions` | Evicted keys that the module did not delete yet. | If it stays high, the module cannot delete keys as fast as it evicts them. |
| `largeobj_dram_pool_exhausted`, `largeobj_disk_capacity_exceeded` | Writes that the module failed with `OOM`. A write that core Valkey rejects with `OOM` before the module runs is not counted. | Raise `maxmemory` or `largeobj.disk-maxmemory`, or use a `maxmemory-policy` that permits eviction. |
| `largeobj_disk_staging_buffer_exhausted` | `Tiered` disk operations that failed because of too much concurrency. | Raise `largeobj.disk-staging-size`, lower `largeobj.max-buffers-per-op`, or send fewer concurrent operations. |
| `largeobj_disk_write_errors`, `largeobj_disk_read_errors` | Failed disk I/O. | Look at the server log and the disk health. |
| `largeobj_rdma_read_errors`, `largeobj_rdma_write_errors`, `largeobj_crc_mismatch` | Failed RDMA transfers. | Look at the client: registered regions, endpoint, and checksum. |

The core `evicted_keys` field in `INFO stats` does not count large objects that the module evicts.
The module deletes the evicted keys itself (with an unlink), not through core eviction.
Use `largeobj_evictions` instead.

## Troubleshooting

### Errors at run time

| Error | Cause and fix |
|---|---|
| `OOM command not allowed when used memory > 'maxmemory'.` | The DRAM pool is full, and eviction cannot free space. With `maxmemory-policy noeviction` (the default), the module evicts nothing. Use `allkeys-lru` or `volatile-lru`, or raise `maxmemory`. |
| `OOM command not allowed when used disk space > 'largeobj.disk-maxmemory'.` | `Tiered` mode: the disk limit is reached, and eviction cannot free space. Use a policy that permits eviction, or raise `largeobj.disk-maxmemory`. |
| `ERR disk staging buffer pool exhausted` | `Tiered` mode: too many concurrent disk operations for the disk staging buffers, not a lack of DRAM for data. A retry later can succeed. See [COMMANDS.md](COMMANDS.md#blobtcp_set). |
| `ERR max object size exceeded` | The object is larger than `largeobj.max-object-size`. |
| `ERR failed to set key` | Rare: the server could not store the key after the write. Retry. |
| `ERR Protocol error: invalid bulk length`, and the connection closes | A `BLOB.TCP_SET` value is larger than the core `proto-max-bulk-len` (default 512MB). Raise `proto-max-bulk-len`, or use `BLOB.RDMA_SET`. |
| `ERR no RDMA session (call BLOB.RDMA_HELLO first)` | The connection has no RDMA session, for example a new connection after a cluster redirect. Send `BLOB.RDMA_HELLO` on it. |
| `ERR RDMA read failed`, `ERR RDMA write failed` | The transfer to or from client memory failed, for example because the client endpoint is gone or a region is not registered. |
| `ERR disk write failed`, `ERR disk read failed` | `Tiered` mode: disk I/O failed. Look at the server log. If every write fails, see the file system rows in [At load time](#at-load-time). |
| `ERR command not allowed in MULTI, scripts or module calls` | See [Transactions and scripts](#transactions-and-scripts). |
| `DISABLED largeobj module is disabled` | `largeobj.enabled` is `no`. |

### At load time

| Symptom | Cause and fix |
|---|---|
| Build fails. Log: `fatal error: fi_collective.h: No such file or directory` | The libfabric that `pkg-config` finds is older than 2.0. Install libfabric 2.0 or later, and set `PKG_CONFIG_PATH` to its `lib/pkgconfig` directory (see [Build](#build)). |
| Server stops at load, or `./build.sh` or `fabric_target` fails. Log: ``libfabric.so.1: version `FABRIC_1.8' not found`` | An older libfabric is loaded at run time. Set `LD_LIBRARY_PATH` to the `lib` directory of the libfabric that you built with. |
| Server stops at load. Log: `invalid digit found in string` | A module argument has a unit, for example `64mb`. Give the size in bytes. |
| Server stops at load. Log: `Enum 'dram' not exists` | Enum values are case-sensitive. Use `Dram`, `Tiered`, `EfaDirect`, or `Emulated`. |
| Server stops at load. Log: `largeobj.segment-size is insufficient for largeobj.max-object-size` | `Dram` mode: an object of `largeobj.max-object-size` does not fit in one segment (default `largeobj.max-object-size` is 512MB). Make `largeobj.max-object-size` smaller or `largeobj.segment-size` larger (see [Dram mode](#dram-mode)). |
| Server stops at load. Log: `largeobj.segment-size must be >= 2 * largeobj.chunk-size` | `largeobj.segment-size` is less than twice `largeobj.chunk-size` (default 1MB). Make `largeobj.segment-size` larger, for example 2MB (2097152) or more. |
| Server stops at load. Log: `largeobj: config validation failed: ERR largeobj.segment-size is insufficient for largeobj.max-promote-size` | `Tiered` mode: an object of `largeobj.max-promote-size` (default 256MB) does not fit in one segment. With 64MB segments, `largeobj.max-promote-size` can be at most 66842624. Make `largeobj.max-promote-size` smaller. |
| Server stops at load. Log: `largeobj: config validation failed: ERR largeobj.disk-maxmemory must be >= largeobj.max-object-size in Tiered mode` | `Tiered` mode: `largeobj.disk-maxmemory` is smaller than `largeobj.max-object-size` (default 512MB). Make `largeobj.disk-maxmemory` larger, set it to `0`, or make `largeobj.max-object-size` smaller. |
| Server stops at load. Log: `largeobj: config validation failed: ERR largeobj.disk-staging-size must be >= largeobj.segment-size` | `Tiered` mode: `largeobj.disk-staging-size` is smaller than `largeobj.segment-size`. Make `largeobj.disk-staging-size` larger. |
| Server stops at load. Log: `largeobj: config validation failed: ERR largeobj.chunk-size must be a multiple of 4096` | Set `largeobj.chunk-size` to a multiple of 4096. |
| Server stops at load. Log: `largeobj: config validation failed: ERR largeobj.max-buffers-per-op must be >= largeobj.min-buffers-per-op` | Make `largeobj.max-buffers-per-op` equal to or larger than `largeobj.min-buffers-per-op`. |
| Server stops at load. Log: `largeobj: storage init failed: too many segments (DRAM <n>, NVMe <m>). Max 16384 per pool. Increase segment-size or decrease disk-staging-size` | `Tiered` mode: `largeobj.disk-staging-size` divided by `largeobj.segment-size` is more than 16384. Make `largeobj.segment-size` larger or `largeobj.disk-staging-size` smaller. |
| Server stops at load. Log: `IORING_REGISTER_BUFFERS failed: Cannot allocate memory (os error 12)` | `Tiered` mode: the locked-memory limit is too low. Raise `RLIMIT_MEMLOCK` (see [Requirements](#requirements)). |
| Server aborts at load or when the DRAM pool grows. Log: `largeobj: fatal panic — aborting server: ... EFA registration of segment at ... failed (fatal)` | A fabric is up, and the module cannot register a segment. Raise `RLIMIT_MEMLOCK` (see [Requirements](#requirements)), or set a lower `largeobj.max-rdma-registered-bytes`. |
| Log: `fabric unavailable, RDMA path disabled`. `BLOB.RDMA_HELLO` returns `ERR RDMA provider unavailable on this instance` | The fabric provider did not start. Use the TCP commands, or check the EFA setup. For tests without RDMA hardware, set `largeobj.fabric-provider` to `Emulated`. |
| Log: `fd-reserve Lenient: open-file limit ... is too low, so fd placement and the fd cache are off; set LimitNOFILE to at least <n> to turn them on` | `Tiered` mode: raise the open-file limit (`ulimit -n` or `LimitNOFILE`) to the value in the message, or set a lower `maxclients`. With `largeobj.fd-reserve` set to `Strict`, the module does not load in this case. |
| Log: `open-file limit ... is too low for max-cached-fds ...: the fd cache holds <m>; set LimitNOFILE to at least <n> to cache all` | `Tiered` mode: the file descriptor cache is smaller than `largeobj.max-cached-fds`. Raise the open-file limit, or set a lower `largeobj.max-cached-fds`. |
| Server stops at load. Log: `largeobj: startup failed to reset disk-dir <dir>: Device or resource busy (os error 16); aborting module load` | `Tiered` mode: `largeobj.disk-dir` is a mount point. The module deletes the contents of the mounted file system, then cannot remove the mount point. Use a dedicated subdirectory of the mount for `largeobj.disk-dir`. |
| Server stops at load. Log: `largeobj: startup failed to reset disk-dir <dir>: <error>; aborting module load`, with an error such as `Permission denied` or `Read-only file system` | `Tiered` mode: the module deletes and creates `largeobj.disk-dir` at startup, and it could not. Make the parent directory of `largeobj.disk-dir` and the file system writable by the server user. |
| Server stops at load. Log: `largeobj: startup failed to reset disk-dir : disk-dir is required in Tiered operating mode; aborting module load` | `Tiered` mode: `largeobj.disk-dir` is not set. Set `largeobj.disk-dir` to a directory on the disk. |
| Server stops at load. Log: `largeobj: storage init failed: failed to create disk-dir shards under <dir>: <error>` | `Tiered` mode: the module could not create the `largeobj.disk-dir-shards` subdirectories, for example because the file system is full or read-only. Free space or fix the mount. |
| Server stops at load. Log: `largeobj: storage init failed: disk-dir <dir> has a block size of <N> B, which does not divide the 4096 B I/O alignment; ...` | `Tiered` mode: the file system block size does not divide 4096, for example 64KB blocks. The module cannot count disk use exactly, so it does not load. Put `largeobj.disk-dir` on a file system with a block size of 4096 B or less. |
| Server stops at load. Log: `largeobj: storage init failed: cannot read disk-dir <dir> XFS geometry: <error>`, or `cannot open disk-dir <dir>: <error>`, or `cannot statfs disk-dir <dir>: <error>` | `Tiered` mode: the module could not read the file system geometry that it needs to count disk use, so it does not load. Make sure that the mount is healthy and that the server can read `largeobj.disk-dir`. |
| Every `Tiered` write fails with `ERR disk write failed` | `largeobj.disk-dir` does not support `O_DIRECT` or user extended attributes (the module stores a header in the `user.lobj` attribute of each object file). The module does not check this at load time. Use XFS. |
| Log at load: `largeobj: disk-dir <dir> is not on XFS; disk usage is charged in 4096 B blocks` | `largeobj.disk-dir` is not on XFS. The module loads, but only XFS is supported. Use XFS. |
| A configuration on the `loadmodule` line has no effect | The argument has the `largeobj.` prefix. In module arguments, use the name without the prefix (see [Load the module](#load-the-module)). |

## Next steps

- [COMMANDS.md](COMMANDS.md): all commands, the RDMA flow, and the configurations that you are most likely to change.
- [README](../README.md): architecture, the full configuration table, tests, and benchmarks.
- [STORAGE_DESIGN.md](STORAGE_DESIGN.md) and [CACHE_POLICY_DESIGN.md](CACHE_POLICY_DESIGN.md): design details.
