"""
Integration tests for DRAMPool expand/shrink scaling behavior, and Dram eviction.

Tests cover:
  - Dram mode: reactive expand (TCP and EFA SET) and proactive expand
  - Dram mode: expansion gated by server maxmemory watermark
  - Dram mode: shrink deletes the victim segment's keys and frees memory
  - Dram mode: expand, shrink to zero segments, expand again
  - Tiered mode: reactive expand on promotion
  - Tiered mode: shrink drops cached copies, NVMe data survives
  - Tiered mode: expand, shrink to zero segments, expand again
  - Tiered mode: shrink releases an EFA-registered segment
  - Dram mode: a SET or COPY that finds the arena full evicts other objects; their keys read as
    misses until the scaling cron deletes them
  - Tiered mode: a SET or COPY that would pass `disk-maxmemory` evicts other objects' files, and
    stays exact against concurrent GET, SET, overwrite, COPY and DEL
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
                and i['largeobj_nvme_uring_registered_segments'] == i['largeobj_nvme_segments'])
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
    return info['largeobj_dram_draining_segments'] == 0 and info['largeobj_pending_reclaims'] == 0


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


def pending_reclaims(client):
    return info_largeobj(client)['largeobj_pending_reclaims']


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
        with pytest.raises(ResponseError, match='pool exhausted'):
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
        assert after['largeobj_reclaims'] == before['largeobj_reclaims'] + len(deleted_keys)
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
        takes it off the reclaim list but is not counted as a reclaim."""
        client = self.server.get_new_client()
        # The shrink tick re-arms the cron 60s out, holding the window open.
        client.config_set('largeobj.reclaim-poll-ms', 60000)
        self._write_payloads(client)

        apply_shrink_pressure(client, 'volatile-lru')
        wait_for_true(lambda: info_largeobj(client)['largeobj_pending_reclaims'] == 1,
                      timeout=self.TIMEOUT_S)
        client.config_set('maxmemory', 0)  # let COPY (denyoom) reach the module

        victims = [k for k in self.PAYLOADS if client.execute_command('BLOB.TCP_GET', k) is None]
        assert len(victims) == 1
        victim = victims[0]
        assert client.exists(victim) == 1
        with pytest.raises(ResponseError, match='not found'):
            client.execute_command('BLOB.INFO', victim)
        with pytest.raises(ResponseError, match='module key failed to copy'):
            client.execute_command('COPY', victim, 'copy_dst')

        reclaims = info_largeobj(client)['largeobj_reclaims']
        client.delete(victim)
        wait_for_true(lambda: info_largeobj(client)['largeobj_pending_reclaims'] == 0)
        # The user deleted it, not the module, so it isn't counted.
        assert info_largeobj(client)['largeobj_reclaims'] == reclaims

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
        assert shrunk['largeobj_reclaims'] == expanded['largeobj_reclaims'] + 4
        assert shrunk['largeobj_num_objects'] == 0
        assert client.dbsize() == 0

        client.config_set('maxmemory', 0)
        assert client.execute_command('BLOB.TCP_SET', 'after', b'Z' * SEGMENT_FILLING_SIZE) == b'OK'
        regrown = info_largeobj(client)
        assert regrown['largeobj_dram_scaling_expands'] == shrunk['largeobj_dram_scaling_expands'] + 1
        assert regrown['largeobj_dram_segments'] == 1
        assert client.execute_command('BLOB.TCP_GET', 'after') == b'Z' * SEGMENT_FILLING_SIZE


# ─── Dram Mode Eviction ───────────────────────────────────────────────────────
class DramArena(ValkeyLargeObjTestCaseBase):
    """Dram mode with one 1MB segment that `set_policy` freezes, so eviction is the only way a SET
    succeeds once it is full. `POLL_MS` keeps the scaling cron out of the way: a victim's key stays
    on the reclaim list for the test to see."""

    SEGMENT_SIZE = 1024 * 1024
    OBJ = SEGMENT_SIZE // 4
    OBJECTS_PER_CAP = 3  # a fourth would fill the segment exactly, but talc's metadata lives in it
    POLL_MS = 60_000

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Dram"
            f" segment-size {self.SEGMENT_SIZE}"
            f" max-object-size {self.SEGMENT_SIZE - 64 * 1024}"
            f" chunk-size 65536"
            f" scaling-poll-ms {self.POLL_MS}"
            f" bench-mode no"
            f" direct-io no"
        )

    def new_client(self, policy='allkeys-lru'):
        client = self.server.get_new_client()
        set_policy(client, policy)
        return client


class TestDramEviction(DramArena):

    def test_writing_past_the_segment_evicts_and_victims_read_as_misses(self):
        """Three segments' worth cannot be resident at once, so these SETs only succeed by evicting.
        A victim's key, still in the keyspace, reads as a miss until the cron deletes it."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)

        info = info_largeobj(client)
        survivors = surviving(client, payloads)
        victims = len(payloads) - len(survivors)
        assert info['largeobj_dram_evictions'] == victims > 0
        assert info['largeobj_dram_segments'] == 1
        assert 'obj_8' in survivors, "the last write cannot have been anyone's victim"
        assert all(survivors[key] == payloads[key] for key in survivors)
        assert pending_reclaims(client) == victims
        assert client.execute_command('EXISTS', *payloads) == len(payloads), "no key is gone yet"

    def test_a_full_arena_is_refused_not_evicted_under_noeviction(self):
        client = self.new_client('noeviction')
        payloads = set_objects(client, 'obj_', self.OBJECTS_PER_CAP, self.OBJ)

        with pytest.raises(ResponseError, match='pool exhausted'):
            client.execute_command('BLOB.TCP_SET', 'refused', b'R' * self.OBJ)
        assert info_largeobj(client)['largeobj_dram_evictions'] == 0
        assert surviving(client, payloads) == payloads

    def test_copy_evicts_another_key_and_never_its_source(self):
        """COPY pins its source: alone in the segment it fails cleanly, and with another key
        resident that key is the victim."""
        client = self.new_client()
        big = b'S' * (640 * 1024)
        assert client.execute_command('BLOB.TCP_SET', 'src', big) == b'OK'
        with pytest.raises(ResponseError):
            client.execute_command('COPY', 'src', 'dst')
        assert client.execute_command('BLOB.TCP_GET', 'src') == big
        assert info_largeobj(client)['largeobj_dram_evictions'] == 0

        client.execute_command('FLUSHALL')
        obj = b'A' * (384 * 1024)
        assert client.execute_command('BLOB.TCP_SET', 'src', obj) == b'OK'
        assert client.execute_command('BLOB.TCP_SET', 'other', b'O' * len(obj)) == b'OK'
        assert client.execute_command('COPY', 'src', 'dst') in (1, True)
        assert info_largeobj(client)['largeobj_dram_evictions'] == 1
        assert client.execute_command('BLOB.TCP_GET', 'other') is None
        assert client.execute_command('BLOB.TCP_GET', 'src') == obj
        assert client.execute_command('BLOB.TCP_GET', 'dst') == obj

    def test_deleting_a_victim_takes_it_off_the_list(self):
        """Deleting an evicted key frees its object, which clears the oid: nothing stays listed
        for a key that no longer exists."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        victims = [key for key in payloads if client.execute_command('BLOB.TCP_GET', key) is None]
        assert victims and pending_reclaims(client) == len(victims)

        assert client.execute_command('DEL', *victims) == len(victims)
        wait_for_true(lambda: pending_reclaims(client) == 0)
        assert info_largeobj(client)['largeobj_reclaims'] == 0, "the cron deleted nothing"

    def test_flushall_async_clears_the_list(self):
        """Objects freed in bulk on a background thread can include listed victims."""
        client = self.new_client()
        set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        assert pending_reclaims(client) > 0

        client.execute_command('FLUSHALL', 'ASYNC')
        wait_for_true(lambda: pending_reclaims(client) == 0)


class TestDramEvictionReclaim(DramArena):
    """The scaling cron deletes the keys of evicted objects, a tick (`scaling-poll-ms`) at a time."""

    POLL_MS = 1000

    def test_the_cron_deletes_the_keys_eviction_took(self):
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        live = surviving(client, payloads)
        assert 0 < len(live) < len(payloads)

        # Lift the pressure: the tick after the sweep would otherwise shrink the arena the
        # survivors are in.
        client.config_set('maxmemory', 1 << 40)
        wait_for_true(lambda: pending_reclaims(client) == 0, timeout=15)
        assert client.execute_command('DBSIZE') == len(live), "only live keys are left"
        assert surviving(client, payloads) == live
        info = info_largeobj(client)
        assert info['largeobj_reclaims'] == info['largeobj_dram_evictions'] == len(payloads) - len(live)


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
        assert after['largeobj_reclaims'] == before['largeobj_reclaims']  # no key deleted
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
        registered = expanded['largeobj_efa_registered_segments']
        assert dram_segments > 1
        # EFA registration spans BOTH pools: every live segment (DRAM + NVMe staging) is registered.
        assert registered == dram_segments + expanded['largeobj_nvme_segments']
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
        assert after['largeobj_efa_registered_segments'] < registered
        assert after['largeobj_efa_registered_segments'] == (
            after['largeobj_dram_segments'] + after['largeobj_nvme_segments'])
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)
        assert client.execute_command('BLOB.TCP_GET', 'key_a') == b'A' * SEGMENT_FILLING_SIZE
        assert client.execute_command('BLOB.TCP_GET', 'key_b') == b'B' * SEGMENT_FILLING_SIZE


# ─── Tiered Eviction ──────────────────────────────────────────────────────────

def hook(client, name, value):
    """Set a test-only pause or failure hook, which opens a window in the production path."""
    client.config_set(f'largeobj.test-{name}', value)


def background(server, *command):
    """Run `command` on a connection of its own, in a thread. Returns the thread and its outcome:
    'reply' or 'error' appears when it finishes."""
    outcome = {}

    def run():
        try:
            outcome['reply'] = server.get_new_client().execute_command(*command)
        except ResponseError as e:
            outcome['error'] = str(e)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


@contextmanager
def paused_set(server, client, key, payload):
    """A SET paused after writing its file and before its key commits. The pause is lifted at once,
    so other commands run meanwhile; the SET commits when the block ends. Yields its outcome."""
    hook(client, 'pause-before-finalize-set-ms', 3000)
    thread, outcome = background(server, 'BLOB.TCP_SET', key, payload)
    time.sleep(0.5)  # the file is written and the task is paused
    hook(client, 'pause-before-finalize-set-ms', 0)
    try:
        yield outcome
    finally:
        thread.join(timeout=15)


OBJECT_FILE = re.compile(r'^[0-9a-f]{16}\.dat$')


class TieredCap(ValkeyLargeObjTestCaseBase):
    """Tiered mode against a small `disk-maxmemory`, the only budget eviction can free. The pools
    are bigger than any object, so only the ledger can refuse a SET, and it is exact. `POLL_MS`
    keeps the scaling cron away: a victim's key outlives its file on the reclaim list.

    Victims come from the directory in readdir order, so which objects survive is the filesystem's
    choice: the tests assert counts and the ledger, not names, unless they pin a name down."""

    OBJ = 512 * 1024
    DISK_PER_OBJ = OBJ + 4096  # the ledger also charges the file's one-page header
    OBJECTS_PER_CAP = 8
    CAP = OBJECTS_PER_CAP * DISK_PER_OBJ  # a whole number of objects, so filling it evicts nothing
    POLL_MS = 60_000

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-maxmemory {self.CAP}"
            f" disk-staging-size {2 * self.CAP}"
            f" segment-size {2 * self.CAP}"
            f" max-object-size {self.CAP}"
            f" max-promote-size {self.CAP}"
            f" scaling-poll-ms {self.POLL_MS}"
            f" bench-mode no"
            f" direct-io no"
        )

    def new_client(self, policy='allkeys-lru'):
        """Let the module evict. maxmemory sits far above usage, so core never evicts and every
        eviction seen is the module's."""
        client = self.server.get_new_client()
        used = int(client.info('memory')['used_memory'])
        client.config_set('maxmemory', used + 256 * 1024 * 1024)
        client.config_set('maxmemory-policy', policy)
        return client

    def fill_cap(self, client):
        return set_objects(client, 'fill_', self.OBJECTS_PER_CAP, self.OBJ)

    def object_files(self):
        """Object files in readdir order, the order eviction takes them in."""
        return [name for name in os.listdir(self.data_dir) if OBJECT_FILE.match(name)]

    def disk_bytes(self):
        return sum(os.path.getsize(os.path.join(self.data_dir, name)) for name in self.object_files())

    def keys_in_eviction_order(self, payloads):
        """`payloads`' keys by their file's place in readdir order. Each payload is one repeated
        byte, read back from just past its file's header."""
        by_byte = {payload[0]: key for key, payload in payloads.items()}
        keys = []
        for name in self.object_files():
            with open(os.path.join(self.data_dir, name), 'rb') as f:
                f.seek(4096)
                keys.append(by_byte.get(f.read(1)[0]))
        return [key for key in keys if key is not None]

    def wait_settled(self, client, live=None, timeout=10):
        """Every hold released and every unlink done: the ledger equals the bytes on disk, and the
        directory holds exactly `live` objects."""
        def settled():
            info = info_largeobj(client)
            return (info['largeobj_nvme_unevictable_objects'] == 0
                    and info['largeobj_nvme_used_bytes'] == self.disk_bytes()
                    and (live is None or len(self.object_files()) == live))
        wait_for_true(settled, timeout=timeout)

    def assert_refused(self, client, key, payload):
        with pytest.raises(ResponseError, match='capacity exceeded'):
            client.execute_command('BLOB.TCP_SET', key, payload)


class TestTieredEviction(TieredCap):

    def test_writing_past_the_cap_evicts_exactly_and_stays_bounded(self):
        """Three caps' worth: the ledger, counters, survivors, reclaim list and directory agree on
        what died, one victim per write."""
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)

        info = info_largeobj(client)
        victims = info['largeobj_nvme_evictions']
        assert victims == len(payloads) - self.OBJECTS_PER_CAP
        assert info['largeobj_nvme_eviction_failures'] == 0
        assert info['largeobj_dram_evictions'] == 0, "a Tiered node never evicts for DRAM"
        survivors = surviving(client, payloads)
        assert f'obj_{len(payloads) - 1}' in survivors
        assert len(survivors) == self.OBJECTS_PER_CAP
        assert all(value == payloads[key] for key, value in survivors.items())
        assert info['largeobj_nvme_used_bytes'] == self.CAP
        assert info['largeobj_pending_reclaims'] == victims, "every victim's key waits to be deleted"
        assert client.dbsize() == len(payloads), "the victims' keys outlive their files"
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    def test_requests_the_cap_cannot_serve_destroy_nothing(self):
        """One bigger than the cap itself, and any write under `noeviction` or without a
        `maxmemory`."""
        client = self.new_client()
        payloads = self.fill_cap(client)

        self.assert_refused(client, 'huge', b'H' * self.CAP)  # the payload alone exceeds the cap
        client.config_set('maxmemory-policy', 'noeviction')
        self.assert_refused(client, 'overflow', b'O' * self.OBJ)
        client.config_set('maxmemory-policy', 'allkeys-lru')
        client.config_set('maxmemory', 0)
        self.assert_refused(client, 'overflow', b'O' * self.OBJ)

        info = info_largeobj(client)
        assert info['largeobj_nvme_evictions'] == 0
        assert info['largeobj_nvme_eviction_failures'] == 0, "refused without evicting"
        assert info['largeobj_pending_reclaims'] == 0
        assert surviving(client, payloads) == payloads
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    def test_copy_evicts_another_key_and_never_its_source(self):
        """Every COPY at the cap evicts, on the main thread. The source is the first file in
        readdir order, so without its hold it would be the first victim."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        source = self.keys_in_eviction_order(payloads)[0]
        for i in range(2 * self.OBJECTS_PER_CAP):
            assert client.execute_command('COPY', source, f'dst_{i}') == 1
            assert client.execute_command('BLOB.TCP_GET', source) == payloads[source], \
                "the source is never a victim"
        assert info_largeobj(client)['largeobj_nvme_evictions'] == 2 * self.OBJECTS_PER_CAP
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    def test_copy_of_a_victim_fails_and_destroys_nothing(self):
        client = self.new_client()
        payloads = self.fill_cap(client)
        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        victim = next(key for key in payloads if key not in surviving(client, payloads))
        with pytest.raises(ResponseError):
            client.execute_command('COPY', victim, 'copy')
        assert client.execute_command('EXISTS', 'copy') == 0
        assert info_largeobj(client)['largeobj_nvme_evictions'] == 1
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    @pytest.mark.parametrize('overwrite', [False, True])
    def test_a_set_in_flight_is_not_evicted(self, overwrite):
        """A SET's file exists before its key does, and a SET holds the object it overwrites too.
        While it is paused before its commit, writes that need room must pass over both, or the SET
        loses its file mid-flight. The overwritten object is first in readdir order."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        key = self.keys_in_eviction_order(payloads)[0] if overwrite else 'slow'
        with paused_set(self.server, client, key, b'S' * self.OBJ) as outcome:
            set_objects(client, 'other_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
            if overwrite:
                assert client.execute_command('BLOB.TCP_GET', key) == payloads[key]

        assert outcome.get('reply') == b'OK', outcome
        assert client.execute_command('BLOB.TCP_GET', key) == b'S' * self.OBJ
        self.wait_settled(client, self.OBJECTS_PER_CAP - (1 if overwrite else 0))

    def test_a_write_discarded_as_stale_leaves_nothing_behind(self):
        """A SET slow to commit finds a newer object under its key and is dropped: its file is
        unlinked, its bytes come back and nothing stays held or listed."""
        client = self.new_client()
        with paused_set(self.server, client, 'key', b'L' * self.OBJ):  # holds the older object id
            assert client.execute_command('BLOB.TCP_SET', 'key', b'N' * self.OBJ) == b'OK'
        assert client.execute_command('BLOB.TCP_GET', 'key') == b'N' * self.OBJ
        self.wait_settled(client, 1)
        assert info_largeobj(client)['largeobj_pending_reclaims'] == 0

    def test_a_failed_eviction_unlink_leaks_the_victim(self):
        """The documented hazard: a victim whose unlink fails stays listed and on disk. Its key reads
        as missing, its bytes stay charged, and its teardown leaves the file alone, so it leaks."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        hook(client, 'fail-evict-unlink', 'yes')
        self.assert_refused(client, 'newcomer', b'N' * self.OBJ)
        hook(client, 'fail-evict-unlink', 'no')

        info = info_largeobj(client)
        assert info['largeobj_nvme_evictions'] == 0
        assert info['largeobj_nvme_eviction_failures'] == 1
        assert info['largeobj_pending_reclaims'] == 1
        assert info['largeobj_nvme_unevictable_objects'] == 0
        assert info['largeobj_nvme_used_bytes'] == self.CAP
        survivors = surviving(client, payloads)
        assert len(survivors) == self.OBJECTS_PER_CAP - 1
        victim = next(key for key in payloads if key not in survivors)

        assert client.execute_command('DEL', victim) == 1
        wait_for_true(lambda: pending_reclaims(client) == 0, timeout=10)
        assert len(self.object_files()) == self.OBJECTS_PER_CAP, "the victim's file leaked"
        assert info_largeobj(client)['largeobj_nvme_used_bytes'] == self.CAP, "and stays charged"

        # Eviction still works around the leak: the other objects give way.
        set_objects(client, 'more_', 2 * self.OBJECTS_PER_CAP, self.OBJ)
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    def test_overlapping_writes_each_evict_only_for_themselves(self):
        """Many SETs at the cap at once: each claims its own victims under the server lock and
        credits them in the same update that reserves its own room, so none evicts for another and
        none is refused."""
        client = self.new_client()
        self.fill_cap(client)
        threads, writes = 8, 20
        errors = []

        def writer(n):
            c = self.server.get_new_client()
            for i in range(writes):
                try:
                    assert c.execute_command('BLOB.TCP_SET', f'w{n}_{i}', b'W' * self.OBJ) == b'OK'
                except Exception as e:  # noqa: BLE001
                    errors.append(e)

        pool = [threading.Thread(target=writer, args=(n,)) for n in range(threads)]
        for t in pool:
            t.start()
        for t in pool:
            t.join(timeout=120)
        assert errors == []
        assert info_largeobj(client)['largeobj_nvme_evictions'] == threads * writes, "one victim per write"
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    @pytest.mark.parametrize('policy', ['allkeys-lru', 'volatile-ttl'])
    def test_any_policy_but_noeviction_evicts(self, policy):
        """A directory has no hit counts or access times, so the policy only decides whether to
        evict."""
        client = self.new_client(policy)
        payloads = self.fill_cap(client)
        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        assert info_largeobj(client)['largeobj_nvme_evictions'] == 1
        assert len(surviving(client, payloads)) == self.OBJECTS_PER_CAP - 1

    def test_an_evicted_object_releases_its_cached_copy_at_once(self):
        """The victim's key waits for the cron, but its DRAM copy must not."""
        client = self.new_client()
        client.config_set('largeobj.promote-min-hits', 1)
        promote(client, self.fill_cap(client))
        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        info = info_largeobj(client)
        assert info['largeobj_nvme_evictions'] == 1 and info['largeobj_pending_reclaims'] == 1
        assert info['largeobj_dram_objects'] == self.OBJECTS_PER_CAP - 1

    def test_an_evicted_key_reads_as_a_miss_until_it_is_overwritten(self):
        client = self.new_client()
        key = 'victim'
        assert client.execute_command('BLOB.TCP_SET', key, b'V' * ((self.OBJECTS_PER_CAP - 1) * self.OBJ)) == b'OK'
        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * (2 * self.OBJ)) == b'OK'  # room for only one
        assert client.execute_command('EXISTS', key) == 1, "the key outlives its data"
        assert pending_reclaims(client) == 1

        assert client.execute_command('BLOB.TCP_GET', key) is None
        with pytest.raises(ResponseError, match='not found'):
            client.execute_command('BLOB.INFO', key)
        assert client.execute_command('DEBUG', 'DIGEST-VALUE', key)

        # The reclaim is the old object's, not the name's: a new write under the name is no miss.
        assert client.execute_command('BLOB.TCP_SET', key, b'fresh' * 1000) == b'OK'
        assert client.execute_command('BLOB.TCP_GET', key) == b'fresh' * 1000
        wait_for_true(lambda: pending_reclaims(client) == 0, timeout=10)  # the overwrite's teardown unlisted it
        self.wait_settled(client, 2)

    def test_files_that_are_not_objects_are_left_alone(self):
        """Only `{id:016x}.dat` names an object: anything else in the directory is never a victim."""
        client = self.new_client()
        strays = ['README', 'ffffffffffffffff.txt', 'abc.dat', 'zzzzzzzzzzzzzzzz.dat']
        for name in strays:
            with open(os.path.join(self.data_dir, name), 'wb') as f:
                f.write(b'x' * 4096)
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        assert len(surviving(client, payloads)) == self.OBJECTS_PER_CAP
        assert all(os.path.exists(os.path.join(self.data_dir, name)) for name in strays)
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    def test_a_write_whose_directory_vanished_destroys_nothing(self):
        """At the cap, a SET that cannot read `disk-dir` finds no victims: it is refused, and
        nothing is claimed or charged."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        away = self.data_dir + '.away'
        os.rename(self.data_dir, away)
        try:
            self.assert_refused(client, 'at_cap', b'N' * self.OBJ)
        finally:
            os.rename(away, self.data_dir)
        info = info_largeobj(client)
        assert info['largeobj_nvme_evictions'] == 0 and info['largeobj_pending_reclaims'] == 0
        assert surviving(client, payloads) == payloads
        self.wait_settled(client, self.OBJECTS_PER_CAP)


class TestTieredEvictionMixed(TieredCap):
    """GET, SET, overwrite, COPY and DEL from many clients at once, all at the cap so nearly every
    write evicts. Each client owns its keys, so it knows what each may read back. Afterwards the
    ledger, the directory and the keyspace must agree exactly."""

    POLL_MS = 1000
    SECONDS = 8

    def test_mixed_workload_keeps_every_invariant(self):
        client = self.new_client()
        self.fill_cap(client)
        stop = time.time() + self.SECONDS
        failures = []
        expected_errors = ('capacity exceeded', 'failed to copy', 'no such key')

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
        assert info_largeobj(client)['largeobj_nvme_evictions'] > 0

        # The cron deletes the ghosts; every hold drains; ledger == directory == live keys.
        wait_for_true(lambda: pending_reclaims(client) == 0, timeout=20)
        wait_for_true(lambda: client.info('stats').get('lazyfree_pending_objects', 0) == 0)
        self.wait_settled(client, timeout=15)
        assert len(self.object_files()) == client.dbsize()
        assert info_largeobj(client)['largeobj_nvme_used_bytes'] == client.dbsize() * self.DISK_PER_OBJ


class TestTieredEvictionReclaim(TieredCap):
    """The scaling cron deletes the keys eviction took: one tick a second here."""

    POLL_MS = 1000

    def test_the_cron_deletes_the_keys_eviction_took(self):
        client = self.new_client()
        payloads = set_objects(client, 'obj_', 3 * self.OBJECTS_PER_CAP, self.OBJ)
        wait_for_true(lambda: pending_reclaims(client) == 0, timeout=15)
        assert client.dbsize() == self.OBJECTS_PER_CAP
        info = info_largeobj(client)
        assert info['largeobj_reclaims'] == info['largeobj_nvme_evictions'] == 2 * self.OBJECTS_PER_CAP
        assert len(surviving(client, payloads)) == self.OBJECTS_PER_CAP
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    def test_victims_in_every_db_and_renamed_or_moved_are_still_reclaimed(self):
        client = self.new_client()
        for db in range(3):
            client.execute_command('SELECT', db)
            set_objects(client, f'db{db}_', self.OBJECTS_PER_CAP // 2, self.OBJ)
        client.execute_command('SELECT', 0)
        client.execute_command('RENAME', 'db0_0', 'renamed')
        client.execute_command('MOVE', 'db0_1', 5)
        set_objects(client, 'new_', 2 * self.OBJECTS_PER_CAP, self.OBJ)
        wait_for_true(lambda: pending_reclaims(client) == 0, timeout=15)
        total = 0
        for db in range(16):
            client.execute_command('SELECT', db)
            total += client.dbsize()
        client.execute_command('SELECT', 0)
        assert total == self.OBJECTS_PER_CAP
        assert info_largeobj(client)['largeobj_reclaims'] == info_largeobj(client)['largeobj_nvme_evictions']
        self.wait_settled(client, self.OBJECTS_PER_CAP)

    @pytest.mark.xfail(strict=True, reason="bug: an evicted file with no key stays listed forever")
    def test_a_leaked_file_evicted_later_leaves_nothing_listed(self):
        """A victim whose unlink failed is left on disk once the cron deletes its key: no key, not
        listed, still charged. A later eviction takes it like any other file, and must not leave it
        listed, or the sweep scans for a key that will never come."""
        client = self.new_client()
        payloads = self.fill_cap(client)
        leaked = self.object_files()[0]
        hook(client, 'fail-evict-unlink', 'yes')
        self.assert_refused(client, 'refused', b'R' * self.OBJ)
        hook(client, 'fail-evict-unlink', 'no')
        wait_for_true(lambda: pending_reclaims(client) == 0, timeout=15)
        assert client.dbsize() == self.OBJECTS_PER_CAP - 1
        assert self.object_files()[0] == leaked
        assert info_largeobj(client)['largeobj_nvme_used_bytes'] == self.CAP

        assert client.execute_command('BLOB.TCP_SET', 'newcomer', b'N' * self.OBJ) == b'OK'
        assert leaked not in self.object_files()
        self.wait_settled(client, self.OBJECTS_PER_CAP)
        assert client.dbsize() == self.OBJECTS_PER_CAP
        assert len(surviving(client, payloads)) == self.OBJECTS_PER_CAP - 1
        time.sleep(3)
        assert pending_reclaims(client) == 0
