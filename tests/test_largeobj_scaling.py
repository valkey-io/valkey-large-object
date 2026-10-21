"""
Integration tests for DRAMPool expand/shrink scaling behavior, and eviction.

Tests cover:
  - Dram mode: reactive expand (TCP and EFA SET) and proactive expand
  - Dram mode: expansion gated by server maxmemory watermark
  - Dram mode: shrink deletes the victim segment's keys and frees memory
  - Dram mode: expand, shrink to zero segments, expand again
  - Tiered mode: reactive expand on promotion
  - Tiered mode: shrink drops cached copies, NVMe data survives
  - Tiered mode: expand, shrink to zero segments, expand again
  - Tiered mode: shrink releases an EFA-registered segment
  - Eviction, both modes: a SET or COPY past the budget evicts other objects; their keys read as
    misses until the scaling cron deletes them. Tiered stays exact against concurrent GET, SET,
    overwrite, COPY and DEL
"""

import os
import random
import re
import subprocess
import threading
import time
from contextlib import contextmanager

import pytest
from valkey import OutOfMemoryError, ResponseError
from valkeytestframework.util.waiters import wait_for_true
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj

# 900KB in a 1MB segment: one object of this size fills a segment by itself.
SEGMENT_FILLING_SIZE = 900 * 1024


def wait_uring_registered_matches_segments(client, timeout=10):
    """Tiered: each pool's io_uring ring registers all its segments
    (dram_uring==dram_segments, nvme_uring==nvme_segments). wait_for because the
    re-register on expand/shrink is fire-and-forget on the poller."""
    def _match():
        i = info_largeobj(client)
        return (i['largeobj_dram_uring_registered_segments'] == i['largeobj_dram_segments']
                and i['largeobj_disk_staging_uring_registered_segments'] == i['largeobj_disk_staging_segments'])
    wait_for_true(_match, timeout=timeout)


def apply_shrink_pressure(client, policy='noeviction'):
    """Cap maxmemory at 85% of used_memory (ratio ~1.18, above the shrink
    watermark). With no TTL keys, volatile-lru evicts nothing in core, like
    noeviction, but allows Dram shrink."""
    client.config_set('maxmemory-policy', policy)
    client.config_set('maxmemory', int(client.info('memory')['used_memory'] * 0.85))


def apply_full_shrink_pressure(client, policy='noeviction'):
    """maxmemory 1 byte: pressure never lifts, so shrink releases every segment."""
    client.config_set('maxmemory-policy', policy)
    client.config_set('maxmemory', 1)


def shrink_completed(client):
    """No segment draining and every reclaimed key deleted."""
    info = info_largeobj(client)
    return info['largeobj_dram_draining_segments'] == 0 and info['largeobj_pending_evictions'] == 0


def promote(client, keys):
    """Tiered (promote-min-hits 1): one GET per key caches it in DRAM."""
    for key in keys:
        client.execute_command('BLOB.TCP_GET', key)
    wait_for_true(lambda: info_largeobj(client)['largeobj_dram_objects'] == len(keys))


def set_policy(client, policy='allkeys-lru'):
    """Let the module evict: it needs maxmemory > 0 and a policy other than noeviction. Also freezes
    the pool, so a full segment can only be served by evicting: with the shrink watermark at its 50%
    floor, a maxmemory about two segments above usage stops `try_expand` (any tighter and core
    evicts or refuses the command itself)."""
    used = int(client.info('memory')['used_memory'])
    segment = info_largeobj(client)['largeobj_dram_segment_size_bytes']
    client.config_set('largeobj.scaling-shrink-watermark', 50)
    client.config_set('maxmemory', 2 * (used + segment) - 512 * 1024)
    client.config_set('maxmemory-policy', policy)


def set_objects(client, prefix, count, size):
    """SET `count` objects of `size` bytes, each filled with its own byte; return them by key."""
    payloads = {f'{prefix}{i}': bytes([i % 251 + 1]) * size for i in range(count)}
    for key, payload in payloads.items():
        assert client.execute_command('BLOB.TCP_SET', key, payload) == b'OK'
    return payloads


def surviving(client, payloads):
    """The objects that survive, by key, read back. `EXISTS` is no judge: an evicted key outlives
    its object until the scaling cron deletes it."""
    got = {key: client.execute_command('BLOB.TCP_GET', key) for key in payloads}
    return {key: value for key, value in got.items() if value is not None}


def pending_evictions(client):
    return info_largeobj(client)['largeobj_pending_evictions']


# ─── Dram Mode Scaling ────────────────────────────────────────────────────────

class TestDramReactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows reactively when a segment fills (SET path).

    scaling-poll-ms is set very high (60s) so the scaling cron cannot fire
    during the test. Any expand observed must be from the reactive SET path.
    """

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB → pool starts with 1 segment, grows on demand.
        # scaling-poll-ms=60000 → cron fires at most once per minute, won't interfere.
        # fabric-provider Emulated on loopback for EFA reactive expand test.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-poll-ms 60000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags):
        """Launch the fabric_target peer process and return (process, address, rkey, remote_addr, length)."""
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'fabric_target')
        process = subprocess.Popen(
            [target, '127.0.0.1', *flags],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        line = process.stdout.readline()
        assert line.startswith('advertisement: '), line
        address, rkey, remote_addr, length = line.split()[1:]
        return process, address, int(rkey), int(remote_addr), int(length)

    def test_expand_on_segment_full(self):
        """A SET that does not fit the one segment expands the pool."""
        client = self.server.get_new_client()
        before = info_largeobj(client)

        assert client.execute_command('BLOB.TCP_SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE) == b'OK'
        assert client.execute_command('BLOB.TCP_SET', 'key_b', b'B' * SEGMENT_FILLING_SIZE) == b'OK'

        after = info_largeobj(client)
        assert after['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 1
        assert after['largeobj_dram_segments'] == 2
        # Dram mode has no io_uring ring, so nothing is ever io_uring-registered.
        assert after['largeobj_dram_uring_registered_segments'] == 0
        # Fabric up: both segments, the startup one and the expanded one, are RDMA-registered.
        assert after['largeobj_dram_rdma_registered_segments'] == 2

    def test_copy_lands_in_one_segment(self):
        """Dram COPY allocates the whole copy in one segment, not chunk by chunk.

        Source is 600KB in segment A (1MB), A chunk-by-chunk copy would put about
        6 chunks in A and the rest in a new segment B. After DEL of the source, A 
        would still hold part of the copy, so neither segment has room for a 900KB
        object and a third segment is needed. With a one-segment copy, A is empty
        after the DEL and takes it.
        """
        client = self.server.get_new_client()
        src = b'S' * (600 * 1024)
        assert client.execute_command('BLOB.TCP_SET', 'src', src) == b'OK'
        assert client.execute_command('COPY', 'src', 'dst') == 1
        assert client.execute_command('BLOB.TCP_GET', 'dst') == src
        assert info_largeobj(client)['largeobj_dram_segments'] == 2
        client.execute_command('DEL', 'src')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_objects'] == 1)
        assert client.execute_command('BLOB.TCP_SET', 'big', b'B' * (900 * 1024)) == b'OK'
        assert info_largeobj(client)['largeobj_dram_segments'] == 2, \
            "copy was split across segments, so a 900KB SET needed a third"

    def test_expand_data_integrity(self):
        """Objects spread over expanded segments all read back correctly."""
        client = self.server.get_new_client()
        before = info_largeobj(client)['largeobj_dram_scaling_expands']
        payloads = {f'key_{i}': bytes([i]) * (800 * 1024) for i in range(4)}

        for key, payload in payloads.items():
            client.execute_command('BLOB.TCP_SET', key, payload)

        assert info_largeobj(client)['largeobj_dram_scaling_expands'] == before + 3
        for key, payload in payloads.items():
            assert client.execute_command('BLOB.TCP_GET', key) == payload

    def test_efa_set_triggers_reactive_expand(self):
        """An EFA SET that fits no segment expands the pool itself.

        Fill segment 1 with 4KB TCP objects until one more expands the pool,
        then give segment 2 the same number: it is now as full as segment 1,
        so the next 4KB object -- the EFA SET -- fits neither.
        """
        client = self.server.get_new_client()
        expands = lambda: info_largeobj(client)['largeobj_dram_scaling_expands']
        efa_len = 4096
        # fabric_target --read serves 0x00..0xFF repeating (its generate_pattern()).
        efa_payload = bytes(i % 256 for i in range(efa_len))
        small = b'F' * efa_len
        start = expands()

        sets = 0
        while expands() == start:
            client.execute_command('BLOB.TCP_SET', f'filler_{sets}', small)
            sets += 1
            assert sets < 1024, "4KB objects never filled a 1MB segment"
        per_segment = sets - 1  # the last SET went to segment 2
        for i in range(per_segment - 1):
            client.execute_command('BLOB.TCP_SET', f'filler_{sets + i}', small)
        assert expands() == start + 1
        assert info_largeobj(client)['largeobj_dram_segments'] == 2

        process, address, rkey, remote_addr, length = self.start_target('--read')
        try:
            client.execute_command('BLOB.RDMA_HELLO', address)
            result = client.execute_command('BLOB.RDMA_SET', 'efa_key', efa_len, rkey, remote_addr, length)
            assert result == b'OK', f"EFA SET failed: {result}"
            assert client.execute_command('BLOB.TCP_GET', 'efa_key') == efa_payload
        finally:
            process.kill()
        assert expands() == start + 2
        assert info_largeobj(client)['largeobj_dram_segments'] == 3


class TestDramProactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows proactively when the scaling cron sees utilization > watermark.

    scaling-poll-ms=1000 so the cron fires every second. The test writes enough
    data to push utilization above the expand watermark, then stops writing and
    waits for the cron to add a segment.
    """

    EXPAND_TIMEOUT_S = 15

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB; no server maxmemory, so shrink never fires.
        # scaling-expand-watermark=50 so filling half a segment triggers proactive expand.
        # scaling-poll-ms=1000 so the cron fires frequently.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-expand-watermark 50"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def test_proactive_expand_fires_when_watermark_exceeded(self):
        """The scaling cron adds a segment when utilization exceeds the expand watermark.

        One 600KB object puts one 1MB segment at ~59% > 50%. No SET follows, so
        any expand is the cron's.
        """
        client = self.server.get_new_client()
        expand_before = info_largeobj(client)['largeobj_dram_scaling_expands']

        assert client.execute_command('BLOB.TCP_SET', 'probe', b'P' * (600 * 1024)) == b'OK'
        assert info_largeobj(client)['largeobj_dram_scaling_expands'] == expand_before

        wait_for_true(
            lambda: info_largeobj(client)['largeobj_dram_scaling_expands'] > expand_before,
            timeout=self.EXPAND_TIMEOUT_S,
        )

        # The next 600KB object doesn't fit the first segment, so it must land in
        # the cron's new one. Raising the watermark to 95 (~59% used after it)
        # stops cron expands, so any expand from here would be the SET's own.
        client.config_set('largeobj.scaling-expand-watermark', 95)
        expanded = info_largeobj(client)['largeobj_dram_scaling_expands']
        second = b'S' * (600 * 1024)
        assert client.execute_command('BLOB.TCP_SET', 'second', second) == b'OK'
        assert info_largeobj(client)['largeobj_dram_scaling_expands'] == expanded
        assert client.execute_command('BLOB.TCP_GET', 'second') == second


class TestDramServerMaxMemoryCap(ValkeyLargeObjTestCaseBase):
    """Dram mode: expansion is gated by the SERVER maxmemory watermark.

    There is no module-local DRAM budget — the pool grows on demand and the
    only ceiling is Valkey's own `maxmemory` (via would_cross_memory_watermark).
    """

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def test_expansion_capped_by_server_maxmemory(self):
        """With less than one segment of headroom under maxmemory, an object
        that needs a new segment is rejected by the module and the pool does
        not grow. The object is small enough that core's own OOM check passes."""
        client = self.server.get_new_client()
        client.config_set('maxmemory-policy', 'noeviction')
        client.execute_command('BLOB.TCP_SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE)

        used = int(client.info('memory')['used_memory'])
        # 600KB under maxmemory: core's OOM check lets a 200KB SET through, but
        # the module can't add a 1MB segment.
        client.config_set('maxmemory', used + 600 * 1024)
        before = info_largeobj(client)

        # 200KB does not fit the ~124KB left in the segment, so it needs a new
        # segment, and the module rejects it.
        with pytest.raises(OutOfMemoryError):
            client.execute_command('BLOB.TCP_SET', 'key_b', b'B' * (200 * 1024))
        after = info_largeobj(client)
        assert after['largeobj_dram_segments'] == before['largeobj_dram_segments']
        assert after['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands']


class TestDramShrink(ValkeyLargeObjTestCaseBase):
    """Dram mode: under server memory pressure and an evicting maxmemory-policy,
    the scaling cron reclaims a segment by deleting the keys whose objects live
    on it, so used_memory drops and writes fit again.
    """

    TIMEOUT_S = 20
    # One object per segment, so each shrink reclaims exactly one key.
    PAYLOADS = {f'dshrink_{i}': bytes([i]) * SEGMENT_FILLING_SIZE for i in range(4)}

    def get_module_args(self, data_dir, direct_io):
        # Expand watermark 95 (above one object's ~88% of a segment): no proactive
        # expand, so the pool holds exactly one segment per object and no empty
        # segment the shrink could pick instead.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-poll-ms 1000"
            f" scaling-expand-watermark 95"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def _write_payloads(self, client):
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.TCP_SET', key, payload) == b'OK'

    def test_shrink_deletes_victim_keys_and_frees_memory(self):
        """Under maxmemory pressure with an evicting policy, a shrink releases a
        segment and deletes its keys, which frees enough memory for core writes
        to succeed again. Every deleted key fires an 'evicted' keyspace event,
        and every surviving key still reads back correctly."""
        client = self.server.get_new_client()
        self._write_payloads(client)
        before = info_largeobj(client)
        # Subscribe before the shrink so no 'evicted' event is missed.
        client.config_set('notify-keyspace-events', 'Ee')
        events = client.pubsub()
        events.subscribe('__keyevent@0__:evicted')
        assert events.get_message(timeout=1)['type'] == 'subscribe'

        apply_shrink_pressure(client, 'volatile-lru')
        # Over maxmemory: core rejects the write before any shrink has run.
        with pytest.raises(OutOfMemoryError):
            client.set('core_key', 'v')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_scaling_shrinks']
                      > before['largeobj_dram_scaling_shrinks'], timeout=self.TIMEOUT_S)
        # Retry SET on a standard (core) Valkey string key until it returns OK:
        # it raises OOM until the shrink has freed enough memory. The client
        # maps an OK reply to True.
        wait_for_true(lambda: client.set('core_key', 'v') is True,
                      ignore_exception=OutOfMemoryError, timeout=self.TIMEOUT_S)

        # Lift the pressure so no further shrink starts, then wait for the last
        # one to finish: segment released and its keys deleted.
        client.config_set('maxmemory', 0)
        wait_for_true(lambda: shrink_completed(client), timeout=self.TIMEOUT_S)

        after = info_largeobj(client)
        # Every shrink releases exactly one segment.
        num_shrinks = after['largeobj_dram_scaling_shrinks'] - before['largeobj_dram_scaling_shrinks']
        assert num_shrinks > 0
        assert after['largeobj_dram_segments'] == before['largeobj_dram_segments'] - num_shrinks
        # Every key the shrinks deleted is counted as one reclaim.
        surviving_keys = [k for k in self.PAYLOADS if client.exists(k)]
        deleted_keys = set(self.PAYLOADS) - set(surviving_keys)
        assert deleted_keys
        assert after['largeobj_evictions'] == before['largeobj_evictions'] + len(deleted_keys)
        # Keys whose segments were not released read back unchanged.
        for key in surviving_keys:
            assert client.execute_command('BLOB.TCP_GET', key) == self.PAYLOADS[key]
        # Each deleted key fires the same 'evicted' event as a core eviction.
        evicted_keys = []
        while (msg := events.get_message(timeout=1)) is not None:
            if msg['type'] == 'message':
                evicted_keys.append(msg['data'].decode())
        assert sorted(evicted_keys) == sorted(deleted_keys)

    def test_reclaimed_key_reads_as_missing(self):
        """Between the shrink and its key deletion, the victim key still exists
        but every module command treats it as missing. A user DEL in that window
        takes it off the reclaim list."""
        client = self.server.get_new_client()
        # The shrink tick re-arms the cron 60s out, holding the window open.
        client.config_set('largeobj.reaper-poll-ms', 60000)
        self._write_payloads(client)

        apply_shrink_pressure(client, 'volatile-lru')
        wait_for_true(lambda: info_largeobj(client)['largeobj_pending_evictions'] == 1,
                      timeout=self.TIMEOUT_S)
        client.config_set('maxmemory', 0)  # let COPY (denyoom) reach the module

        victims = [k for k in self.PAYLOADS if client.execute_command('BLOB.TCP_GET', k) is None]
        assert len(victims) == 1
        victim = victims[0]
        assert client.exists(victim) == 1
        assert client.execute_command('BLOB.INFO', victim) is None
        with pytest.raises(ResponseError, match='module key failed to copy'):
            client.execute_command('COPY', victim, 'copy_dst')

        client.delete(victim)
        wait_for_true(lambda: info_largeobj(client)['largeobj_pending_evictions'] == 0)

    def test_no_shrink_under_noeviction(self):
        """Dram shrink deletes keys, so noeviction disables it."""
        client = self.server.get_new_client()
        self._write_payloads(client)
        shrinks = info_largeobj(client)['largeobj_dram_scaling_shrinks']

        apply_shrink_pressure(client)
        time.sleep(3)  # three cron ticks at scaling-poll-ms 1000

        assert info_largeobj(client)['largeobj_dram_scaling_shrinks'] == shrinks
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.TCP_GET', key) == payload

    def test_no_shrink_without_maxmemory(self):
        """maxmemory 0 gives no pressure signal, so no shrink under any policy."""
        client = self.server.get_new_client()
        self._write_payloads(client)
        shrinks = info_largeobj(client)['largeobj_dram_scaling_shrinks']

        client.config_set('maxmemory-policy', 'volatile-lru')
        time.sleep(3)  # three cron ticks at scaling-poll-ms 1000

        assert info_largeobj(client)['largeobj_dram_scaling_shrinks'] == shrinks
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.TCP_GET', key) == payload

    def test_expand_shrink_to_zero_expand(self):
        """The pool expands, shrinks to zero segments (every key reclaimed),
        and expands again for the next SET."""
        client = self.server.get_new_client()
        before = info_largeobj(client)
        self._write_payloads(client)
        expanded = info_largeobj(client)
        assert expanded['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 3
        assert expanded['largeobj_dram_segments'] == 4

        apply_full_shrink_pressure(client, 'volatile-lru')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_segments'] == 0
                      and shrink_completed(client), timeout=self.TIMEOUT_S)
        shrunk = info_largeobj(client)
        assert shrunk['largeobj_dram_scaling_shrinks'] == expanded['largeobj_dram_scaling_shrinks'] + 4
        assert shrunk['largeobj_evictions'] == expanded['largeobj_evictions'] + 4
        assert shrunk['largeobj_num_objects'] == 0
        assert client.dbsize() == 0

        client.config_set('maxmemory', 0)
        assert client.execute_command('BLOB.TCP_SET', 'after', b'Z' * SEGMENT_FILLING_SIZE) == b'OK'
        regrown = info_largeobj(client)
        assert regrown['largeobj_dram_scaling_expands'] == shrunk['largeobj_dram_scaling_expands'] + 1
        assert regrown['largeobj_dram_segments'] == 1
        assert client.execute_command('BLOB.TCP_GET', 'after') == b'Z' * SEGMENT_FILLING_SIZE


# ─── Tiered Mode Scaling ──────────────────────────────────────────────────────

TIERED_ARGS = (
    "operating-mode Tiered"
    " disk-dir {data_dir}"
    " disk-staging-size 4194304"
    " segment-size 1048576"
    " max-promote-size 983040"
    " promote-min-hits 1"
    " scaling-poll-ms 1000"
    " chunk-size 65536"
    " bench-mode no"
    " direct-io no"
    " fabric-provider Emulated"
)


class TestTieredExpand(ValkeyLargeObjTestCaseBase):
    """Tiered mode: SET writes to NVMe, so the DRAMPool grows on promotion (GET)."""

    def get_module_args(self, data_dir, direct_io):
        # Expand watermark 95: no proactive expand, only the promotion's own.
        return TIERED_ARGS.format(data_dir=data_dir) + " scaling-expand-watermark 95"

    def test_tiered_expand_on_promotion(self):
        """Promoting an object that fits no segment expands the pool."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.TCP_SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE)
        client.execute_command('BLOB.TCP_SET', 'key_b', b'B' * SEGMENT_FILLING_SIZE)
        before = info_largeobj(client)

        promote(client, ['key_a', 'key_b'])

        after = info_largeobj(client)
        assert after['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 1
        assert after['largeobj_dram_segments'] == 2
        assert client.execute_command('BLOB.TCP_GET', 'key_a') == b'A' * SEGMENT_FILLING_SIZE
        assert client.execute_command('BLOB.TCP_GET', 'key_b') == b'B' * SEGMENT_FILLING_SIZE
        # The expanded DRAM segment joins its ring's io_uring table (per-pool).
        wait_uring_registered_matches_segments(client)
        # With the fabric up, every segment of each pool is RDMA-registered, the expanded one too.
        assert after['largeobj_dram_rdma_registered_segments'] == 2
        assert (after['largeobj_disk_staging_rdma_registered_segments']
                == after['largeobj_disk_staging_segments'])


class TestTieredShrink(ValkeyLargeObjTestCaseBase):
    """Tiered mode: under server memory pressure the scaling cron drops cached
    DRAM copies a segment at a time; the data stays on NVMe. Tiered shrink
    deletes no keys, so it runs under noeviction too."""

    TIMEOUT_S = 20
    PAYLOADS = {f'tshrink_{i}': bytes([i]) * SEGMENT_FILLING_SIZE for i in range(4)}

    def get_module_args(self, data_dir, direct_io):
        # Expand watermark 95: one segment per cached object and no empty
        # segment the shrink could pick instead.
        return TIERED_ARGS.format(data_dir=data_dir) + " scaling-expand-watermark 95"

    def _write_and_promote(self, client):
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.TCP_SET', key, payload) == b'OK'
        promote(client, self.PAYLOADS)

    def _assert_reads_from_nvme(self, client):
        for key, payload in self.PAYLOADS.items():
            assert client.execute_command('BLOB.TCP_GET', key) == payload

    def test_shrink_drops_cached_copies_keeps_nvme_data(self):
        client = self.server.get_new_client()
        self._write_and_promote(client)
        before = info_largeobj(client)
        assert before['largeobj_dram_segments'] == 4

        apply_shrink_pressure(client, 'noeviction')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_scaling_shrinks']
                      > before['largeobj_dram_scaling_shrinks'], timeout=self.TIMEOUT_S)
        client.config_set('maxmemory', 0)
        wait_for_true(lambda: shrink_completed(client), timeout=self.TIMEOUT_S)

        after = info_largeobj(client)
        shrinks = after['largeobj_dram_scaling_shrinks'] - before['largeobj_dram_scaling_shrinks']
        assert after['largeobj_dram_segments'] == 4 - shrinks
        assert after['largeobj_dram_objects'] == 4 - shrinks
        assert after['largeobj_evictions'] == before['largeobj_evictions']
        assert after['largeobj_cache_demotions'] == before['largeobj_cache_demotions'] + shrinks
        assert client.dbsize() == 4
        self._assert_reads_from_nvme(client)
        # The released segment left its ring's io_uring table too.
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)

    def test_no_shrink_without_maxmemory(self):
        """maxmemory 0 gives no pressure signal, so cached copies stay."""
        client = self.server.get_new_client()
        self._write_and_promote(client)
        before = info_largeobj(client)

        time.sleep(3)  # three cron ticks at scaling-poll-ms 1000

        after = info_largeobj(client)
        assert after['largeobj_dram_scaling_shrinks'] == before['largeobj_dram_scaling_shrinks']
        assert after['largeobj_dram_objects'] == before['largeobj_dram_objects']

    def test_expand_shrink_to_zero_expand(self):
        """Promotion expands the pool, shrink drops every cached copy down to
        zero segments, and the next promotion expands again."""
        client = self.server.get_new_client()
        before = info_largeobj(client)
        self._write_and_promote(client)
        expanded = info_largeobj(client)
        assert expanded['largeobj_dram_scaling_expands'] == before['largeobj_dram_scaling_expands'] + 3
        assert expanded['largeobj_dram_segments'] == 4

        apply_full_shrink_pressure(client, 'noeviction')
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_segments'] == 0
                      and shrink_completed(client), timeout=self.TIMEOUT_S)
        shrunk = info_largeobj(client)
        assert shrunk['largeobj_dram_scaling_shrinks'] == expanded['largeobj_dram_scaling_shrinks'] + 4
        assert shrunk['largeobj_dram_objects'] == 0
        # Still under pressure, so no segment can be added: reads come from NVMe.
        self._assert_reads_from_nvme(client)
        assert info_largeobj(client)['largeobj_dram_segments'] == 0
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)

        client.config_set('maxmemory', 0)
        promote(client, ['tshrink_0'])
        regrown = info_largeobj(client)
        assert regrown['largeobj_dram_scaling_expands'] == shrunk['largeobj_dram_scaling_expands'] + 1
        assert regrown['largeobj_dram_segments'] == 1
        assert client.execute_command('BLOB.INFO', 'tshrink_0', 'TIER') == b'dram'
        assert client.execute_command('BLOB.TCP_GET', 'tshrink_0') == self.PAYLOADS['tshrink_0']
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)


class TestTieredShrinkReleasesEfaRegisteredSegment(ValkeyLargeObjTestCaseBase):
    """Tiered mode, fabric UP: a segment added by expansion is EFA-registered, and shrinking it
    must tear that registration down (Segment::drop -> efa_release_segment) BEFORE the segment
    memory is freed — a broken teardown (registration outliving freed pages) would crash here."""

    TIMEOUT_S = 20

    def get_module_args(self, data_dir, direct_io):
        # Fabric up (Emulated on loopback) so expanded segments are actually EFA-registered.
        return (TIERED_ARGS.format(data_dir=data_dir)
                + " fabric-interfaces lo")

    def test_string_configs_read_back(self):
        """The string configs are registered as ValkeyGILGuard<ValkeyString> and mirrored into
        Rust-side state by their set handler: CONFIG GET returns what was loaded, every time,
        and the module is using it (the fabric came up on `lo`)."""
        client = self.server.get_new_client()
        for _ in range(3):
            assert client.config_get('largeobj.fabric-interfaces') == {
                'largeobj.fabric-interfaces': 'lo'}
            assert client.config_get('largeobj.disk-dir') == {
                'largeobj.disk-dir': self.data_dir}
        assert self.server.verify_string_in_logfile('fabric service 0 rdma address')

    @pytest.mark.parametrize('name', ['disk-dir', 'fabric-interfaces'])
    def test_string_config_rejects_invalid_utf8(self, name):
        """A string config set outside the module arguments (as `largeobj.<name>`, the way a
        config file does) that is not valid UTF-8 fails the load. A lossy copy would make the
        value the module uses differ from what CONFIG GET reports."""
        testdir = os.path.abspath(os.path.join(self.testdir, f'bad-utf8-{name}'))
        os.makedirs(testdir, exist_ok=True)
        data_dir = os.path.join(testdir, 'nvme')
        module_args = TIERED_ARGS.format(data_dir=data_dir)
        if name == 'disk-dir':
            module_args = module_args.replace(f' disk-dir {data_dir}', '')
        server, _ = self.create_server(
            testdir=testdir,
            server_path=self.server.valkey_path,
            args={'loadmodule': f"{os.environ['MODULE_PATH']} {module_args}",
                  f'largeobj.{name}': 'bad\udcffvalue'},  # \udcff is the byte 0xff
            connect_client=False)
        wait_for_true(lambda: server.server.poll() is not None)
        assert server.server.returncode != 0
        assert server.verify_string_in_logfile("Couldn't parse as UTF-8 string")

    def test_shrink_releases_efa_registered_expanded_segment(self):
        client = self.server.get_new_client()
        expand_before = info_largeobj(client)['largeobj_dram_scaling_expands']

        # Promoting the second ~full-segment object forces a reactive expand, and with the
        # fabric up try_expand EFA-registers that new segment (the path under test).
        client.execute_command('BLOB.TCP_SET', 'key_a', b'A' * SEGMENT_FILLING_SIZE)
        client.execute_command('BLOB.TCP_SET', 'key_b', b'B' * SEGMENT_FILLING_SIZE)
        promote(client, ['key_a', 'key_b'])

        expanded = info_largeobj(client)
        assert expanded['largeobj_dram_scaling_expands'] > expand_before
        dram_segments = expanded['largeobj_dram_segments']
        assert dram_segments > 1
        # Each pool reports its own RDMA-registered segments: every DRAM segment (the startup one
        # and the expanded ones) and every NVMe staging segment.
        assert expanded['largeobj_dram_rdma_registered_segments'] == dram_segments
        assert (expanded['largeobj_disk_staging_rdma_registered_segments']
                == expanded['largeobj_disk_staging_segments'])
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)

        # In Tiered mode shrink releases a live-data segment (data persists on NVMe), and with
        # the fabric up the released segment carries an EFA registration Segment::drop tears down.
        apply_shrink_pressure(client)
        wait_for_true(lambda: info_largeobj(client)['largeobj_dram_scaling_shrinks']
                      > expanded['largeobj_dram_scaling_shrinks'], timeout=self.TIMEOUT_S)
        client.config_set('maxmemory', 0)
        wait_for_true(lambda: shrink_completed(client), timeout=self.TIMEOUT_S)

        assert client.execute_command('PING')
        after = info_largeobj(client)
        # Only the DRAM count falls; the staging pool is untouched by a DRAM shrink.
        assert after['largeobj_dram_rdma_registered_segments'] < dram_segments
        assert after['largeobj_dram_rdma_registered_segments'] == after['largeobj_dram_segments']
        assert (after['largeobj_disk_staging_rdma_registered_segments']
                == expanded['largeobj_disk_staging_rdma_registered_segments'])
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)
        assert client.execute_command('BLOB.TCP_GET', 'key_a') == b'A' * SEGMENT_FILLING_SIZE
        assert client.execute_command('BLOB.TCP_GET', 'key_b') == b'B' * SEGMENT_FILLING_SIZE


# ─── Eviction ─────────────────────────────────────────────────────────────────

def hook(client, name, value):
    """Set a hidden test-only hook: a pause or an injected failure in the production path."""
    client.config_set(f'largeobj.test-{name}', value)


@contextmanager
def paused_set(server, client, key, payload):
    """A Tiered SET paused after writing its file and before its key commits. Other commands run
    meanwhile; the SET commits when the block ends. Yields its outcome."""
    outcome = {}

    def run():
        try:
            outcome['reply'] = server.get_new_client().execute_command('BLOB.TCP_SET', key, payload)
        except ResponseError as e:
            outcome['error'] = str(e)

    hook(client, 'pause-before-finalize-set-ms', 3000)
    thread = threading.Thread(target=run)
    thread.start()
    time.sleep(0.5)  # the file is written and the task is paused
    hook(client, 'pause-before-finalize-set-ms', 0)
    try:
        yield outcome
    finally:
        thread.join(timeout=15)


OBJECT_FILE = re.compile(r'^[0-9a-f]{16}\.dat$')


class Budget(ValkeyLargeObjTestCaseBase):
    """A node whose only room is `BUDGET` bytes: past it, a write succeeds only by evicting.
    `POLL_MS` keeps the scaling cron away, so victims' keys stay on the reclaim list."""

    POLL_MS = 60_000

    def evictions(self, client):
        return info_largeobj(client)['largeobj_evictions']

    def assert_refused(self, client, key, payload):
        with pytest.raises(ResponseError, match=self.FULL):
            client.execute_command('BLOB.TCP_SET', key, payload)


class DramBudget(Budget):
    """One 1MB segment that `set_policy` freezes."""

    BUDGET = 1024 * 1024
    OBJ = BUDGET // 4
    PER_BUDGET = 3  # a fourth would fill the segment exactly, but talc's metadata lives in it
    FULL = 'maxmemory'

    def get_module_args(self, data_dir, direct_io):
        return (f"operating-mode Dram segment-size {self.BUDGET} max-object-size {self.BUDGET - 64 * 1024}"
                f" chunk-size 65536 scaling-poll-ms {self.POLL_MS} bench-mode no direct-io no")

    def new_client(self, policy='allkeys-lru'):
        client = self.server.get_new_client()
        set_policy(client, policy)
        return client

    def first_in_line(self, payloads):
        return next(iter(payloads))

    def settle(self, client, live, timeout=10):
        """A Dram eviction has nothing asynchronous to wait for."""


class TieredBudget(Budget):
    """`disk-maxmemory` of 8 objects, with pools bigger than any object: only the ledger can refuse
    a write, and it is exact. Victims are the directory's first files in readdir order, the
    filesystem's choice: tests assert counts and the ledger, not names, unless they pin one down."""

    OBJ = 512 * 1024
    DISK_PER_OBJ = OBJ + 4096  # the ledger also charges the file's one-page header
    PER_BUDGET = 8
    BUDGET = PER_BUDGET * DISK_PER_OBJ
    FULL = 'disk-maxmemory'

    def get_module_args(self, data_dir, direct_io):
        return (f"operating-mode Tiered disk-dir {data_dir} disk-maxmemory {self.BUDGET}"
                f" disk-staging-size {2 * self.BUDGET} segment-size {2 * self.BUDGET}"
                f" max-object-size {self.BUDGET} max-promote-size {self.BUDGET}"
                f" scaling-poll-ms {self.POLL_MS} bench-mode no direct-io no")

    def new_client(self, policy='allkeys-lru'):
        """maxmemory far above usage: core never evicts, so every eviction seen is the module's."""
        client = self.server.get_new_client()
        used = int(client.info('memory')['used_memory'])
        client.config_set('maxmemory', used + 256 * 1024 * 1024)
        client.config_set('maxmemory-policy', policy)
        return client

    def object_files(self):
        """In readdir order, the order eviction takes them in."""
        return [name for name in os.listdir(self.data_dir) if OBJECT_FILE.match(name)]

    def first_in_line(self, payloads):
        """The key eviction takes first, read back from the first file's payload byte."""
        with open(os.path.join(self.data_dir, self.object_files()[0]), 'rb') as f:
            f.seek(4096)
            byte = f.read(1)[0]
        return next(key for key, payload in payloads.items() if payload[0] == byte)

    def settle(self, client, live, timeout=10):
        """Every unevictable-list entry removed and every unlink done: the ledger equals the bytes
        on disk, and the directory holds `live` objects."""
        def settled():
            info = info_largeobj(client)
            files = self.object_files()
            on_disk = sum(os.path.getsize(os.path.join(self.data_dir, name)) for name in files)
            return (info['largeobj_disk_unevictable_objects'] == 0
                    and info['largeobj_disk_used_bytes'] == on_disk
                    and (live is None or len(files) == live))
        wait_for_true(settled, timeout=timeout)


class EvictionTests:
    """Run against both budgets."""

    def test_writing_past_the_budget_evicts_and_victims_read_as_misses(self):
        """A victim's key stays in the keyspace, but reads as missing until the cron deletes it."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.PER_BUDGET, self.OBJ)
        survivors = surviving(client, payloads)
        assert len(survivors) == self.PER_BUDGET
        assert f'obj_{len(payloads) - 1}' in survivors, "the last write is nobody's victim"
        assert all(value == payloads[key] for key, value in survivors.items())
        assert self.evictions(client) == pending_evictions(client) == len(payloads) - self.PER_BUDGET
        assert client.execute_command('EXISTS', *payloads) == len(payloads)
        self.settle(client, self.PER_BUDGET)

    def test_noeviction_refuses_and_destroys_nothing(self):
        client = self.new_client('noeviction')
        payloads = set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        self.assert_refused(client, 'refused', b'R' * self.OBJ)
        assert self.evictions(client) == 0
        assert surviving(client, payloads) == payloads
        self.settle(client, self.PER_BUDGET)

    def test_copy_evicts_other_keys_and_never_its_source(self):
        """A COPY whose only possible victim is its source fails and destroys nothing. Otherwise each
        COPY evicts another key, even when the source is first in line."""
        client = self.new_client()
        alone = b'A' * int(self.BUDGET * 0.6)
        assert client.execute_command('BLOB.TCP_SET', 'alone', alone) == b'OK'
        with pytest.raises(ResponseError):
            client.execute_command('COPY', 'alone', 'dst')
        assert client.execute_command('BLOB.TCP_GET', 'alone') == alone
        assert self.evictions(client) == 0
        client.execute_command('FLUSHALL')
        self.settle(client, 0)

        payloads = set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        source = self.first_in_line(payloads)
        for i in range(2 * self.PER_BUDGET):
            assert client.execute_command('COPY', source, f'dst_{i}') in (1, True)
            assert client.execute_command('BLOB.TCP_GET', source) == payloads[source]
        assert self.evictions(client) == 2 * self.PER_BUDGET
        self.settle(client, self.PER_BUDGET)

    def test_a_victims_key_cannot_be_copied_and_leaves_the_list_when_overwritten_or_deleted(self):
        """Any policy but noeviction evicts: the policy only decides whether to."""
        client = self.new_client('volatile-ttl')
        payloads = set_objects(client, 'obj_', 2 * self.PER_BUDGET, self.OBJ)
        overwritten, deleted = [key for key in payloads if key not in surviving(client, payloads)][:2]
        with pytest.raises(ResponseError):
            client.execute_command('COPY', overwritten, 'copy')
        assert client.execute_command('EXISTS', 'copy') == 0

        assert client.execute_command('BLOB.TCP_SET', overwritten, b'fresh' * 1000) == b'OK'
        assert client.execute_command('BLOB.TCP_GET', overwritten) == b'fresh' * 1000
        assert client.execute_command('DEL', deleted) == 1
        wait_for_true(lambda: pending_evictions(client) == self.evictions(client) - 2)
        client.execute_command('FLUSHALL', 'ASYNC')
        wait_for_true(lambda: pending_evictions(client) == 0)
        self.settle(client, 0)


class EvictionCronTests:
    """The scaling cron deletes victims' keys, one tick a second here."""

    POLL_MS = 1000

    def test_the_cron_deletes_the_victims_keys_wherever_they_went(self):
        """Renamed, or moved to another db."""
        client = self.new_client()
        set_objects(client, 'first_', 2, self.OBJ)
        client.execute_command('RENAME', 'first_0', 'renamed')
        client.execute_command('MOVE', 'first_1', 2)
        payloads = set_objects(client, 'obj_', 3 * self.PER_BUDGET, self.OBJ)
        client.config_set('maxmemory', 1 << 40)  # Dram: else the tick after the sweep shrinks
        wait_for_true(lambda: pending_evictions(client) == 0, timeout=15)

        evictions = self.evictions(client)
        assert evictions > 0
        keys = 0
        for db in range(3):
            client.execute_command('SELECT', db)
            keys += client.dbsize()
        client.execute_command('SELECT', 0)
        assert keys == 2 + len(payloads) - evictions, "only victims' keys were deleted"
        # 'renamed' is in db 0 too, and Tiered readdir order may leave it alive.
        live = surviving(client, {**payloads, 'renamed': None})
        assert len(live) == client.dbsize(), "and all of them"
        self.settle(client, keys)


class TestDramEviction(EvictionTests, DramBudget):

    def test_a_read_object_outlives_unread_ones(self):
        """A GET ranks the object by use, so eviction takes the objects never read first."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        read = self.first_in_line(payloads)
        # Twice: a minute's decay can cost the first GET its gain.
        for _ in range(2):
            assert client.execute_command('BLOB.TCP_GET', read) == payloads[read]
        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        survivors = surviving(client, payloads)
        assert read in survivors
        assert len(survivors) == self.PER_BUDGET - 1


class TestDramEvictionCron(EvictionCronTests, DramBudget):
    pass


class TestTieredEviction(EvictionTests, TieredBudget):

    def test_more_than_the_budget_or_no_maxmemory_is_refused(self):
        """No eviction makes room for more than the cap, and without a server maxmemory the node
        never evicts, disk-maxmemory or not."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        self.assert_refused(client, 'huge', b'H' * self.BUDGET)
        client.config_set('maxmemory', 0)
        self.assert_refused(client, 'overflow', b'O' * self.OBJ)
        info = info_largeobj(client)
        assert info['largeobj_evictions'] == 0
        assert info['largeobj_disk_capacity_exceeded'] == 2
        assert surviving(client, payloads) == payloads

    def test_a_write_that_needs_more_than_16_victims_is_refused_but_they_stay_evicted(self):
        """A claim is never undone."""
        client = self.new_client()
        set_objects(client, 's_', 64, 60 * 1024)  # 64KB on disk each: they fill the budget
        self.assert_refused(client, 'big', b'B' * int(self.BUDGET * 0.6))
        info = info_largeobj(client)
        assert info['largeobj_evictions'] == 16
        assert info['largeobj_disk_capacity_exceeded'] == 1
        self.settle(client, 64 - 16)

    def test_files_that_are_not_objects_are_never_victims(self):
        client = self.new_client()
        strays = ['README', 'ffffffffffffffff.txt', 'abc.dat', 'FFFFFFFFFFFFFFFF.dat']
        for name in strays:
            with open(os.path.join(self.data_dir, name), 'wb') as f:
                f.write(b'x' * 4096)
        set_objects(client, 'obj_', 3 * self.PER_BUDGET, self.OBJ)
        assert all(os.path.exists(os.path.join(self.data_dir, name)) for name in strays)
        self.settle(client, self.PER_BUDGET)

    # Disabled: test-pause-before-finalize-set-ms is immutable, so paused_set cannot toggle it.
    # @pytest.mark.parametrize('overwrite', [False, True])
    # def test_a_set_in_flight_is_not_evicted(self, overwrite):
    #     """A SET's file exists before its key does, and the SET keeps the object it overwrites in the
    #     unevictable list too. Writes that need room while it is paused before its commit must pass
    #     over both. The overwritten object is first in line."""
    #     client = self.new_client()
    #     payloads = set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
    #     key = self.first_in_line(payloads) if overwrite else 'slow'
    #     with paused_set(self.server, client, key, b'S' * self.OBJ) as outcome:
    #         set_objects(client, 'other_', 3 * self.PER_BUDGET, self.OBJ)
    #         if overwrite:
    #             assert client.execute_command('BLOB.TCP_GET', key) == payloads[key]
    #     assert outcome.get('reply') == b'OK', outcome
    #     assert client.execute_command('BLOB.TCP_GET', key) == b'S' * self.OBJ
    #     self.settle(client, self.PER_BUDGET - (1 if overwrite else 0))

    def test_overlapping_writes_each_evict_only_for_themselves(self):
        """Each write credits its victims in the same update that reserves its own room, so none
        evicts for another and none is refused."""
        client = self.new_client()
        set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        threads, writes = 8, 20
        errors = []

        def writer(n):
            c = self.server.get_new_client()
            for i in range(writes):
                try:
                    c.execute_command('BLOB.TCP_SET', f'w{n}_{i}', b'W' * self.OBJ)
                except ResponseError as e:
                    errors.append(e)

        pool = [threading.Thread(target=writer, args=(n,)) for n in range(threads)]
        for t in pool:
            t.start()
        for t in pool:
            t.join(timeout=120)
        assert errors == []
        assert self.evictions(client) == threads * writes, "one victim per write"
        self.settle(client, self.PER_BUDGET)

    def test_an_evicted_object_releases_its_cached_copy_at_once(self):
        """The victim's key waits for the cron, but its DRAM copy must not."""
        client = self.new_client()
        client.config_set('largeobj.promote-min-hits', 1)
        promote(client, set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ))
        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        info = info_largeobj(client)
        assert info['largeobj_evictions'] == info['largeobj_pending_evictions'] == 1
        assert info['largeobj_dram_objects'] == self.PER_BUDGET - 1


class TestTieredEvictionCron(EvictionCronTests, TieredBudget):
    pass


class TestTieredUnlinkFailure(TieredBudget):
    """Every unlink fails transiently except the cron's retry, which runs once a second here. A
    file left in place stays charged and unevictable until the retry unlinks it."""

    POLL_MS = 1000

    def get_module_args(self, data_dir, direct_io):
        return super().get_module_args(data_dir, direct_io) + " test-fail-unlink yes"

    def assert_no_orphans(self, client):
        info = info_largeobj(client)
        assert info['largeobj_disk_orphaned_files'] == info['largeobj_disk_orphaned_bytes'] == 0

    def test_a_victim_whose_unlink_fails_frees_nothing_until_the_retry(self):
        client = self.new_client()
        payloads = set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        self.assert_refused(client, 'refused', b'R' * self.OBJ)
        assert self.evictions(client) == 1
        assert len(surviving(client, payloads)) == self.PER_BUDGET - 1
        self.settle(client, self.PER_BUDGET - 1)
        self.assert_no_orphans(client)
        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        assert self.evictions(client) == 1, "the retry's credit made the room"

    def test_a_deleted_object_whose_unlink_fails_stays_charged_until_the_retry(self):
        client = self.new_client()
        set_objects(client, 'obj_', 2, self.OBJ)
        client.delete('obj_0')
        self.settle(client, 1)
        self.assert_no_orphans(client)


class TestTieredOrphanHeld(TieredBudget):
    """Unlinks fail as above, but the cron never ticks during a test, so orphans stay put."""

    def get_module_args(self, data_dir, direct_io):
        return super().get_module_args(data_dir, direct_io) + " test-fail-unlink yes"

    def test_an_orphan_stays_charged_and_is_never_evicted(self):
        client = self.new_client()
        payloads = set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        orphan = self.first_in_line(payloads)
        client.delete(orphan)
        wait_for_true(lambda: info_largeobj(client)['largeobj_disk_orphaned_files'] == 1)
        info = info_largeobj(client)
        assert info['largeobj_disk_orphaned_bytes'] == self.DISK_PER_OBJ
        assert info['largeobj_disk_used_bytes'] == self.BUDGET
        assert info['largeobj_disk_unevictable_objects'] == 1
        assert len(self.object_files()) == self.PER_BUDGET
        self.assert_refused(client, 'newcomer', b'N' * self.OBJ)
        assert len(surviving(client, payloads)) == self.PER_BUDGET - 2, "the victim was not the orphan"


class TestTieredEvictionMixed(TieredBudget):
    """GET, SET, overwrite, COPY, DEL and UNLINK from many clients at once, all at the cap, so
    nearly every write evicts. Each client owns its keys, so it knows what each may read back.
    Afterwards the ledger, the directory and the keyspace must agree exactly."""

    POLL_MS = 1000
    SECONDS = 8

    def test_mixed_workload_keeps_every_invariant(self):
        client = self.new_client()
        set_objects(client, 'obj_', self.PER_BUDGET, self.OBJ)
        stop = time.time() + self.SECONDS
        failures = []
        expected_errors = ('disk-maxmemory', 'failed to copy', 'no such key')

        def worker(n):
            c = self.server.get_new_client()
            rng = random.Random(n)
            expected = {}  # key -> the payload it last committed
            version = 0
            while time.time() < stop:
                key = f'c{n}_{rng.randrange(6)}'
                op = rng.random()
                try:
                    if op < 0.45:
                        version += 1
                        payload = bytes([n % 200 + 1, version % 256]) * (self.OBJ // 2)
                        if c.execute_command('BLOB.TCP_SET', key, payload) == b'OK':
                            expected[key] = payload
                    elif op < 0.75:
                        got = c.execute_command('BLOB.TCP_GET', key)
                        if got is not None and got != expected.get(key):
                            failures.append(f'{key}: read a payload it never wrote')
                    elif op < 0.85:
                        dst = f'c{n}_{rng.randrange(6)}'
                        if dst != key and c.execute_command('COPY', key, dst, 'REPLACE') == 1:
                            if key in expected:
                                expected[dst] = expected[key]
                    else:
                        c.execute_command('DEL' if op < 0.925 else 'UNLINK', key)
                        expected.pop(key, None)
                except ResponseError as e:
                    if not any(m in str(e).lower() for m in expected_errors):
                        failures.append(f'{key}: {e}')

        pool = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in pool:
            t.start()
        for t in pool:
            t.join(timeout=60)
        assert failures == []
        assert client.execute_command('PING')  # no accounting panic took the server down
        assert self.evictions(client) > 0

        # The cron deletes the ghosts, then ledger == directory == live keys.
        wait_for_true(lambda: pending_evictions(client) == 0, timeout=20)
        wait_for_true(lambda: client.info('memory').get('lazyfree_pending_objects', 0) == 0)
        self.settle(client, None, timeout=15)
        assert len(self.object_files()) == client.dbsize()
        assert info_largeobj(client)['largeobj_disk_used_bytes'] == client.dbsize() * self.DISK_PER_OBJ
