"""
Integration tests for DRAMPool expand/shrink scaling behavior.

Tests cover:
  - Dram mode: reactive expand (TCP and EFA SET) and proactive expand
  - Dram mode: expansion gated by server maxmemory watermark
  - Dram mode: shrink deletes the victim segment's keys and frees memory
  - Dram mode: expand, shrink to zero segments, expand again
  - Tiered mode: reactive expand on promotion
  - Tiered mode: shrink drops cached copies, NVMe data survives
  - Tiered mode: expand, shrink to zero segments, expand again
  - Tiered mode: shrink releases an EFA-registered segment
"""

import os
import subprocess
import time

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
    return info['largeobj_dram_draining_segments'] == 0 and info['largeobj_pending_reclaims'] == 0


def promote(client, keys):
    """Tiered (promote-min-hits 1): one GET per key caches it in DRAM."""
    for key in keys:
        client.execute_command('BLOB.TCP_GET', key)
    wait_for_true(lambda: info_largeobj(client)['largeobj_dram_objects'] == len(keys))


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
        assert client.execute_command('BLOB.INFO', victim) is None
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
                + " fabric-provider Emulated fabric-interfaces lo")

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
        registered = expanded['largeobj_rdma_registered_segments']
        assert dram_segments > 1
        # EFA registration spans BOTH pools: every live segment (DRAM + NVMe staging) is registered.
        assert registered == dram_segments + expanded['largeobj_disk_staging_segments']
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
        assert after['largeobj_rdma_registered_segments'] < registered
        assert after['largeobj_rdma_registered_segments'] == (
            after['largeobj_dram_segments'] + after['largeobj_disk_staging_segments'])
        wait_uring_registered_matches_segments(client, timeout=self.TIMEOUT_S)
        assert client.execute_command('BLOB.TCP_GET', 'key_a') == b'A' * SEGMENT_FILLING_SIZE
        assert client.execute_command('BLOB.TCP_GET', 'key_b') == b'B' * SEGMENT_FILLING_SIZE
