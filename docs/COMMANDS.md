# Command reference

> **Note:** valkey-large-object is at version 0.x.
> Commands, arguments, replies, and configuration can change before version 1.0.

valkey-large-object is a Valkey module that stores large binary values in DRAM or on a local disk, and can move them with RDMA directly to and from client memory.
This page describes its six commands and the configurations that you are most likely to change.

The module adds six commands.
All of them are in the ACL category `@largeobj`.
Each command uses one key, or no key for `BLOB.RDMA_HELLO`.

To start, see [GETTING_STARTED.md](GETTING_STARTED.md).

## Contents

- [Common rules](#common-rules)
- [BLOB.TCP_SET](#blobtcp_set)
- [BLOB.TCP_GET](#blobtcp_get)
- [BLOB.INFO](#blobinfo)
- [RDMA overview](#rdma-overview)
- [BLOB.RDMA_HELLO](#blobrdma_hello)
- [BLOB.RDMA_SET](#blobrdma_set)
- [BLOB.RDMA_GET](#blobrdma_get)
- [Try RDMA without RDMA hardware](#try-rdma-without-rdma-hardware)
- [Configuration](#configuration)

## Common rules

These rules apply to all commands on this page:

- If a command takes a key and the key holds a value of a different type, the command returns `WRONGTYPE Operation against a key holding the wrong kind of value`.
- If `largeobj.enabled` is `no`, the command returns `DISABLED largeobj module is disabled`.
- `BLOB.TCP_SET`, `BLOB.TCP_GET`, `BLOB.RDMA_SET`, and `BLOB.RDMA_GET` are not permitted in transactions, scripts, or module calls (see [Transactions and scripts](GETTING_STARTED.md#transactions-and-scripts)).
- Most core commands work on large object keys; see [Core commands on large object keys](GETTING_STARTED.md#core-commands-on-large-object-keys).
  `DEL`, `UNLINK`, overwrite, expiry, and eviction free the storage in the background.
  `EXPIRE` and `PEXPIRE` set a TTL.
- When `notify-keyspace-events` includes module key type events (`d`), a successful write sends `largeobj.create` (new key) or `largeobj.update` (replaced key).

## BLOB.TCP_SET

```
BLOB.TCP_SET key value
```

Stores `value` as a large object at `key`.
The value goes inline in the command, over the normal Valkey connection.
If `key` already holds a large object, the command replaces it.

The value must be at least 1 byte and not larger than `largeobj.max-object-size` (default 512MB).
The value is also limited by the core `proto-max-bulk-len` (default 512MB).
For a larger value, the server replies `ERR Protocol error: invalid bulk length` and closes the connection.
The server calculates a CRC32C (Castagnoli) checksum of the value and stores it with the object.
In `Tiered` mode, the server replies only after the disk write is complete.

**Reply:** `OK`.

**Errors:**

| Error | Cause |
|---|---|
| `ERR object length must be > 0` | The value is empty. |
| `ERR max object size exceeded` | The value is larger than `largeobj.max-object-size`. |
| `OOM command not allowed when used memory > 'maxmemory'.` | No DRAM space, and eviction cannot free space. |
| `OOM command not allowed when used disk space > 'largeobj.disk-maxmemory'.` | `Tiered` mode: no disk space, and eviction cannot free space. |
| `ERR disk write failed` | `Tiered` mode: the disk write failed. |
| `ERR failed to set key` | Rare: the server could not store the key after the write. Retry. |
| `ERR disk staging buffer pool exhausted` | `Tiered` mode: no free disk staging buffers for the write. This is a limit on concurrent disk operations, not a lack of DRAM for data. The error is temporary: a retry later can succeed, after operations in progress finish. Each disk read or write holds `largeobj.min-buffers-per-op` to `largeobj.max-buffers-per-op` buffers of `largeobj.chunk-size` from the `largeobj.disk-staging-size` pool while it runs. Raise `largeobj.disk-staging-size` (needs a restart), lower `largeobj.max-buffers-per-op` so that each operation holds fewer buffers, or send fewer concurrent `Tiered` disk operations. The `largeobj_disk_staging_buffer_exhausted` counter in `INFO largeobj` counts these errors. |

**Example:**

```
127.0.0.1:6379> BLOB.TCP_SET greeting hello
OK
127.0.0.1:6379> SET mystring abc
OK
127.0.0.1:6379> BLOB.TCP_SET mystring hello
(error) WRONGTYPE Operation against a key holding the wrong kind of value
```

## BLOB.TCP_GET

```
BLOB.TCP_GET key
```

Returns the object at `key` inline in the reply.
The reply does not include the checksum; use [`BLOB.INFO`](#blobinfo) for it.
In `Tiered` mode, the server reads from the DRAM cache if a cached copy is ready, else from disk.

**Reply:** a bulk string with the object, or nil if the key does not exist.

**Errors:**

| Error | Cause |
|---|---|
| `ERR disk read failed` | `Tiered` mode: the disk read failed. |
| `ERR disk staging buffer pool exhausted` | `Tiered` mode: no free disk staging buffers for the read. This is a temporary concurrency limit; a retry later can succeed. See [BLOB.TCP_SET](#blobtcp_set). |

`BLOB.TCP_GET` can also return the errors in [Common rules](#common-rules).

**Example:**

```
127.0.0.1:6379> BLOB.TCP_GET greeting
"hello"
127.0.0.1:6379> BLOB.TCP_GET missing
(nil)
```

## BLOB.INFO

```
BLOB.INFO key [LEN | CRC | TIER]
```

Returns metadata about the object at `key`.
The command does not transfer the object.
The field name is not case-sensitive.

- `LEN`: the object length in bytes, as the client stored it.
  It does not include DRAM allocation overhead (`Dram` mode) or file system overhead (`Tiered` mode).
- `CRC`: the CRC32C checksum of the object, as an unsigned 32-bit integer.
- `TIER`: `dram` or `disk`.
  In `Dram` mode it is always `dram`.
  In `Tiered` mode it is `dram` only when a cached copy is ready in DRAM.

**Reply:** with no field, a flat array of field names and values.
With one field, only that value.
Nil if the key does not exist.

**Errors:** `ERR invalid information field` for an unknown field.

**Example:**

```
127.0.0.1:6379> BLOB.INFO greeting
1) len
2) (integer) 5
3) crc
4) (integer) 2591144780
5) tier
6) dram
127.0.0.1:6379> BLOB.INFO greeting crc
(integer) 2591144780
127.0.0.1:6379> BLOB.INFO greeting SIZE
(error) ERR invalid information field
```

## RDMA overview

With the RDMA commands, the object bytes do not go through the Valkey connection.
The connection carries only the command, the key, a list of client memory regions, and a short reply.
The server moves the bytes with [libfabric](https://ofiwg.github.io/libfabric/):

- For `BLOB.RDMA_SET`, the server reads from client memory.
- For `BLOB.RDMA_GET`, the server writes into client memory.

This repository has no RDMA client library.
To use the RDMA commands, a client must be a libfabric program that registers its own memory and follows the flow in this section.
The libfabric provider of the client must support RMA (`FI_RMA`) with remote read and remote write, and the client needs a locked-memory limit (`RLIMIT_MEMLOCK`) for its registered buffers.
`valkey-cli` can send the commands, but it cannot move the data.

**Session.** An RDMA session belongs to one Valkey connection.
In cluster mode, a client that follows a `MOVED` or `ASK` redirect to another node must send `BLOB.RDMA_HELLO` on that connection first.
The client sends [`BLOB.RDMA_HELLO`](#blobrdma_hello) one time on the connection, with its libfabric endpoint address.
The reply gives one server address for each server fabric service.
The client must add all of them to its address vector (`fi_av_insert`).
The session ends when the connection closes.

**Region triples.** `BLOB.RDMA_SET` and `BLOB.RDMA_GET` describe client memory as one or more triples, `rkey addr len`:

- `rkey`: the remote key from the client memory registration (`fi_mr_reg`).
- `addr`: the address in the region.
  This is a virtual address, or an offset in the registered buffer when the provider does not use virtual addresses.
  `0` is a valid value.
- `len`: the region length in bytes, larger than 0.

All values are unsigned decimal integers.
A command can have a maximum of 256 triples.
The server fills or reads the regions in the order of the command.
The total region length must be equal to or larger than the object length.

**CRC32C.** The server calculates a CRC32C checksum of every stored object.
`BLOB.RDMA_SET` can send `CRC <crc32c>`; the server then rejects the write if the transferred bytes have a different checksum.
`BLOB.RDMA_GET` returns the stored checksum, so the client can check the received bytes.

**Providers.** The `largeobj.fabric-provider` configuration selects the libfabric provider at load time:

| Provider | Use | Client address for `BLOB.RDMA_HELLO` |
|---|---|---|
| `EfaDirect` (default) | [EFA](https://aws.amazon.com/hpc/efa/) hardware RDMA. Use this in production. | 32 bytes |
| `Emulated` | libfabric over TCP sockets. | A socket address: 16 bytes (IPv4) or 28 bytes (IPv6) |

`Emulated` is only for developing and testing RDMA client code on a host without RDMA hardware.
It is not faster than `BLOB.TCP_SET` and `BLOB.TCP_GET`.
On hosts without RDMA hardware, use the TCP commands.

If the server cannot post a transfer for 1 second (for example, because the client endpoint is gone), the command fails with `ERR RDMA read failed` or `ERR RDMA write failed`.
There is no other server-side timeout for a transfer.
To stop a transfer, close the connection.
The client must keep its regions registered until it gets the reply.

## BLOB.RDMA_HELLO

```
BLOB.RDMA_HELLO client-rdma-address
```

Opens an RDMA session on the connection.
`client-rdma-address` is the client libfabric endpoint address (from `fi_getname`) as hexadecimal text.
The command takes exactly one argument.

**Reply:** an array with one hexadecimal server address for each server fabric service.

**Errors:**

| Error | Cause |
|---|---|
| `ERR wrong number of arguments for 'BLOB.RDMA_HELLO' command` | No argument, or more than one argument. |
| `ERR RDMA provider unavailable on this instance` | The fabric did not start. Only the TCP commands work. |
| `ERR invalid rdma address hex` | The address is not valid hexadecimal text. |
| `ERR rdma address must not be empty` | `Emulated`: the address is empty. |
| `ERR rdma address must be 32 bytes` | `EfaDirect`: the address has the wrong length. |
| `ERR rdma address is not a valid sockaddr` | `Emulated`: the address is not a 16-byte or 28-byte socket address. |
| `ERR RDMA session already established` | A second `BLOB.RDMA_HELLO` on the same connection. To use a new endpoint, open a new connection. |
| `ERR session create` | The server could not add the client address to its fabric services. |

**Example** (`Emulated` provider):

```
127.0.0.1:6379> BLOB.RDMA_HELLO 02009aa57f0000010000000000000000 extra
(error) ERR wrong number of arguments for 'BLOB.RDMA_HELLO' command
127.0.0.1:6379> BLOB.RDMA_HELLO zz
(error) ERR invalid rdma address hex
127.0.0.1:6379> BLOB.RDMA_HELLO 0a1b
(error) ERR rdma address is not a valid sockaddr
127.0.0.1:6379> BLOB.RDMA_HELLO 02009aa57f0000010000000000000000
1) "0200a47f7f0000010000000000000000"
127.0.0.1:6379> BLOB.RDMA_HELLO 02009aa57f0000010000000000000000
(error) ERR RDMA session already established
```

## BLOB.RDMA_SET

```
BLOB.RDMA_SET key total-len [CRC crc32c] rkey addr len [rkey addr len ...]
```

Stores an object of `total-len` bytes at `key`.
The server reads the bytes from the client memory regions with RDMA.
`CRC`, if given, must come directly after `total-len`.
`total-len` must be larger than 0 and not larger than `largeobj.max-object-size`.
The server replies only after it has read all bytes and stored the object (in `Tiered` mode, after the disk write).

**Reply:** `OK`.

**Errors:**

| Error | Cause |
|---|---|
| `ERR no RDMA session (call BLOB.RDMA_HELLO first)` | The connection has no session. |
| `ERR invalid len` | `total-len` is not an unsigned decimal integer. |
| `ERR object length must be > 0`, `ERR max object size exceeded` | `total-len` is not valid. |
| `ERR invalid CRC value` | The `CRC` value is not an unsigned 32-bit decimal integer. |
| `ERR memory address args must be rkey, addr, len triples` | The region arguments are not a multiple of 3. |
| `ERR too many memory addresses (max 256)` | More than 256 triples. |
| `ERR invalid rkey`, `ERR invalid memory address`, `ERR invalid memory address len` | An `rkey`, `addr`, or `len` value is not an unsigned decimal integer, or a `len` is 0. |
| `ERR client memory address length <sum> smaller than object length <len>` | The regions are smaller than `total-len`. |
| `ERR CRC mismatch after transfer` | The transferred bytes have a different checksum. Nothing is stored. |
| `ERR failed to set key` | Rare: the server could not store the key after the transfer. Retry. |
| `ERR RDMA read failed` | The RDMA read from client memory failed. |

`BLOB.RDMA_SET` can also return the `OOM` and disk errors of [`BLOB.TCP_SET`](#blobtcp_set).

**Example** (1MB object in one region; see [Try RDMA without RDMA hardware](#try-rdma-without-rdma-hardware)):

```
127.0.0.1:6379> BLOB.RDMA_SET kv:block:1 1048576 0 0 1048576
OK
127.0.0.1:6379> BLOB.RDMA_SET kv:block:1 1048576 CRC 714977837 0 0 1048576
OK
127.0.0.1:6379> BLOB.RDMA_SET kv:block:1 1048576 CRC 12345 0 0 1048576
(error) ERR CRC mismatch after transfer
127.0.0.1:6379> BLOB.RDMA_SET kv:block:1 1048576 0 0 1048576 7
(error) ERR memory address args must be rkey, addr, len triples
```

## BLOB.RDMA_GET

```
BLOB.RDMA_GET key rkey addr len [rkey addr len ...]
```

Writes the object at `key` into the client memory regions with RDMA.
The server replies only after all RDMA writes are complete.
The regions can be larger than the object; the reply gives the object length.

If the key does not exist, the command returns nil.
The server looks up the key first, so a missing key returns nil also on a connection without a session.

**Reply:** a two-element array, the object length and the CRC32C checksum, or nil if the key does not exist.

**Errors:**

| Error | Cause |
|---|---|
| `ERR no RDMA session (call BLOB.RDMA_HELLO first)` | The connection has no session. |
| `ERR client memory address length <sum> smaller than object length <len>` | The regions are too small for the object. |
| `ERR memory address args must be rkey, addr, len triples` | The region arguments are not a multiple of 3. |
| `ERR too many memory addresses (max 256)` | More than 256 triples. |
| `ERR invalid rkey`, `ERR invalid memory address`, `ERR invalid memory address len` | An `rkey`, `addr`, or `len` value is not an unsigned decimal integer, or a `len` is 0. |
| `ERR RDMA write failed` | The RDMA write into client memory failed. |

`BLOB.RDMA_GET` can also return the `Tiered` mode errors of [`BLOB.TCP_GET`](#blobtcp_get).

**Example:**

```
127.0.0.1:6379> BLOB.RDMA_GET kv:block:1 0 0 1048576
1) (integer) 1048576
2) (integer) 714977837
127.0.0.1:6379> BLOB.RDMA_GET kv:block:1 0 0 4096
(error) ERR client memory address length 4096 smaller than object length 1048576
127.0.0.1:6379> BLOB.RDMA_GET nosuchkey 0 0 1048576
(nil)
```

## Try RDMA without RDMA hardware

A real RDMA client registers its own memory and sends the commands.
To see the flow on one host without RDMA hardware, you can use the test peer `fabric_target` from this repository.
It registers a buffer, prints its address and region, and waits while the server reads or writes the buffer.
It is a test tool, not a client library.
It is not part of a release, and it can change without notice.
To build it, you need a source checkout of this repository.

1. Load the module with the `Emulated` provider on the loopback interface:

   ```bash
   valkey-server --port 6379 --maxmemory 4gb \
     --loadmodule ./target/release/libvalkey_large_object.so \
       operating-mode Dram segment-size 67108864 max-object-size 33554432 \
       fabric-provider Emulated fabric-interfaces lo
   ```

2. Build the test peer and start it.
   `--read` fills the buffer with a test pattern for `BLOB.RDMA_SET`.
   Without `--read`, the peer waits for `BLOB.RDMA_GET` to write into the buffer, and then checks the bytes.

   ```bash
   cargo build --release --features test-harness --bin fabric_target
   ./target/release/fabric_target 127.0.0.1 --read --len=1048576
   ```

   Use the same `PKG_CONFIG_PATH`, `BINDGEN_EXTRA_CLANG_ARGS`, and `LD_LIBRARY_PATH` as for the module (see [GETTING_STARTED.md](GETTING_STARTED.md#build)).
   The first argument is the local IP address of the peer endpoint.
   Use an address of the interface in `largeobj.fabric-interfaces`, here `127.0.0.1` for `lo`.

   With `--read`, the peer prints two lines.
   The first line gives its address, then `rkey`, `addr`, and `len` of the region.
   With the `Emulated` provider, `rkey` and `addr` are `0`.
   The second line gives the CRC32C checksum of the test pattern, in hexadecimal.
   The `CRC` argument of `BLOB.RDMA_SET` must be decimal: `0x2a9db22d` is 714977837 (`printf '%d\n' 0x2a9db22d`).

   ```
   advertisement: 02009aa57f0000010000000000000000 0 0 1048576
   serving 1048576 bytes (position pattern) across 1 region(s) for a remote read, crc 0x2a9db22d — the initiator verifies
   ```

   The peer exits after 300 seconds (5 minutes).
   Without `--read`, it exits earlier when its standard input closes after it has checked the bytes.
   After it exits, a transfer to its address fails with `ERR RDMA read failed` (or `ERR RDMA write failed` for `BLOB.RDMA_GET`).

3. In one `valkey-cli` session, open the session with the peer address, then transfer:

   ```
   127.0.0.1:6379> BLOB.RDMA_HELLO 02009aa57f0000010000000000000000
   1) "0200a47f7f0000010000000000000000"
   127.0.0.1:6379> BLOB.RDMA_SET kv:block:1 1048576 0 0 1048576
   OK
   127.0.0.1:6379> BLOB.INFO kv:block:1
   1) len
   2) (integer) 1048576
   3) crc
   4) (integer) 714977837
   5) tier
   6) dram
   ```

For `BLOB.RDMA_GET`, start a second peer without `--read`, open a session with its address on a new connection, and send `BLOB.RDMA_GET kv:block:1 0 0 1048576`.
The peer then prints `received 1048576 bytes, crc 0x2a9db22d — payload verified`.

## Configuration

The configuration names have the `largeobj.` prefix, for example `largeobj.max-object-size`.
These documents always use the full name with the prefix, except on `loadmodule` lines and after `--loadmodule`.

- **Module arguments** (on the `loadmodule` line or after `--loadmodule`) must use the short name without the prefix, for example `max-object-size 33554432`.
  The module ignores a prefixed module argument, such as `largeobj.segment-size 67108864`, without an error, and the configuration keeps its default value.
  Give memory sizes in bytes: a unit such as `64mb` stops the load with `invalid digit found in string`.
- **Config file lines** after the `loadmodule` line must use the full name with the prefix; there, sizes can have units, for example `largeobj.segment-size 64mb`.
  A line with the short name, for example `segment-size 64mb`, stops the server with `Bad directive or wrong number of arguments`.
- **At run time**, use `CONFIG GET` and `CONFIG SET` with the full name.
  `CONFIG SET` accepts units such as `16mb`; `CONFIG GET` returns bytes:

```
127.0.0.1:6379> CONFIG SET largeobj.max-object-size 16mb
OK
127.0.0.1:6379> CONFIG GET largeobj.max-object-size
1) "largeobj.max-object-size"
2) "16777216"
127.0.0.1:6379> CONFIG SET largeobj.segment-size 1048576
(error) ERR CONFIG SET failed (possibly related to argument 'largeobj.segment-size') - can't set immutable config
```

These are the configurations that you are most likely to change.
The full list, with ranges and validation, is in [`src/configs.rs`](../src/configs.rs) and in the [README](../README.md#configuration).

| Name | Default | Mutable | Description |
|---|---|---|---|
| `largeobj.operating-mode` | `Dram` | No | `Dram`: objects in DRAM only. `Tiered`: objects on disk, DRAM is a read cache. |
| `largeobj.disk-dir` | `""` | No | Required in `Tiered` mode. A dedicated subdirectory for object files on XFS (the only supported file system), not a mount point, with a writable parent. Must support `O_DIRECT` and user extended attributes. At startup and shutdown, the module deletes this directory and all its contents, then creates it again, empty. |
| `largeobj.disk-dir-shards` | `1024` | No | `Tiered`: number of subdirectories of `largeobj.disk-dir` for object files. |
| `largeobj.segment-size` | 1GB | No | Size of each DRAM pool segment and disk staging segment. 1MB to 1GB. Must be at least twice `largeobj.chunk-size`. Each pool has at most 16384 segments: the DRAM pool stops at 16384 x `largeobj.segment-size`, and `largeobj.disk-staging-size` / `largeobj.segment-size` must not be more than 16384. |
| `largeobj.disk-staging-size` | 1GB | No | `Tiered`: total size of the DRAM staging buffers for disk I/O. |
| `largeobj.disk-maxmemory` | `0` | Yes | `Tiered`: maximum disk use. Each file counts as its length rounded up to 4KB, or, on a striped XFS, to the stripe unit for files of at least one stripe unit. On XFS, each file also counts its inode. A file also counts any metadata blocks outside its inode (`largeobj_disk_metadata_spills`). `0` is unlimited. |
| `largeobj.max-object-size` | 512MB | Yes | Maximum object size. `BLOB.TCP_SET` is also limited by the core `proto-max-bulk-len` (default 512MB). In `Dram` mode an object must fit in one segment, with 4KB of overhead for each `largeobj.chunk-size` part, plus 4KB. With 64MB segments and the default `largeobj.chunk-size`, the largest value is 66842624. |
| `largeobj.max-promote-size` | 256MB | Yes | `Tiered`: largest object that is promoted to DRAM. Must fit in one segment, with the same overhead as `largeobj.max-object-size` (with 64MB segments, at most 66842624). `0` disables promotion. |
| `largeobj.chunk-size` | 1MB | No | Chunk size for streaming I/O. A multiple of 4096. |
| `largeobj.max-buffers-per-op` | `64` | Yes | Maximum number of `largeobj.chunk-size` buffers that one streaming operation holds. 2 to 1024. In `Tiered` mode, a lower value lets more disk operations share the disk staging buffers. |
| `largeobj.worker-threads` | `8` | No | Worker threads for asynchronous I/O. |
| `largeobj.scaling-shrink-watermark` | `90` | Yes | Percent of `maxmemory` above which the DRAM pool does not grow. In `Tiered` mode, the pool also releases segments above it. |
| `largeobj.promote-min-hits` | `1` | Yes | `Tiered`: number of disk reads of an object; the read that reaches this number promotes it. `1` promotes on the first read. |
| `largeobj.fabric-provider` | `EfaDirect` | No | `EfaDirect` or `Emulated` (case-sensitive). See [RDMA overview](#rdma-overview). |
| `largeobj.fabric-interfaces` | `""` | No | Comma-separated fabric domains (network devices as libfabric names them, for example `lo`). Empty uses all domains that the provider finds. |
| `largeobj.fabric-crc-pool-threads` | `2` | No | Threads that calculate checksums for `BLOB.RDMA_SET`. |
| `largeobj.max-rdma-registered-bytes` | 380GB | Yes | Maximum bytes of segments registered with the fabric. When a fabric is up, the DRAM pool does not grow past it, as with `maxmemory`. `0` is unlimited. |
| `largeobj.enabled` | `yes` | Yes | `no` rejects all `BLOB.*` commands from clients. Keys stay. |
