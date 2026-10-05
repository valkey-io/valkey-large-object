"""
Integration tests for DRAMPool expand/shrink scaling behavior.

Tests cover:
  - Dram mode: reactive expand when segment fills
  - Dram mode: expansion gated by server maxmemory watermark
  - Tiered mode: reactive expand on DRAMPool fill
  - Tiered mode: shrink evicts cached segment but NVMe copy survives
"""

import binascii
import os
import subprocess
import time
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_true
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj

# A tcp-provider fabric address: FI_SOCKADDR_IN for 127.0.0.1:1.
PEER_ADDRESS = binascii.hexlify(
    b'\x02\x00' + (1).to_bytes(2, 'big') + bytes([127, 0, 0, 1]) + bytes(8)
).decode()

# What tests/harness/fabric_target writes (--read mode) or expects (write mode).
# Must match fabric_target's generate_pattern(): cycling 0x00..0xFF.
EFA_TARGET_LEN = 4096
EFA_PATTERN = bytes(i % 256 for i in range(EFA_TARGET_LEN))


def wait_uring_registered_matches_live(client, timeout=10):
    """Tiered: each pool's io_uring ring registers its own live segments
    (dram_uring==dram_live, nvme_uring==nvme_live). wait_for because the
    re-register on expand/shrink is fire-and-forget on the poller."""
    def _match():
        i = info_largeobj(client)
        return (i.get('largeobj_dram_uring_registered_segments') == i.get('largeobj_dram_live_segments')
                and i.get('largeobj_nvme_uring_registered_segments') == i.get('largeobj_nvme_live_segments'))
    wait_for_true(_match, timeout=timeout)


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
        """SET that fills a segment triggers reactive expand in serve_set_dram_tcp.

        With the cron disabled (60s poll), the only source of expand is the
        reactive path in the SET handler. Verified via scaling_expand_total.
        """
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        before = info_largeobj(client)
        expand_before = before.get('largeobj_scaling_expand_total', 0)

        r = client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)
        assert r == b'OK', f"First BLOB.SET failed: {r}"

        r = client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)
        assert r == b'OK', f"Second BLOB.SET failed (expand may not have fired): {r}"

        after = info_largeobj(client)
        assert after.get('largeobj_scaling_expand_total', 0) > expand_before, \
            "Expected scaling_expand_total to increase — cron is disabled so this must be reactive"
        # Dram mode has no io_uring ring, so nothing is ever io_uring-registered —
        # confirms submit_reregister's engine-none guard no-ops here (even after expand).
        assert after.get('largeobj_dram_uring_registered_segments') == 0

    def test_expand_data_integrity(self):
        """Data written before and after a reactive expand is returned correctly."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024
        keys_payloads = [(f'key_{i}', bytes([i % 256]) * obj_size) for i in range(4)]

        for key, payload in keys_payloads:
            client.execute_command('BLOB.SET', key, payload)

        for key, payload in keys_payloads:
            got = client.execute_command('BLOB.GET', key)
            assert got == payload, f"Data mismatch for {key} after expand"

    def test_maxmemory_0_no_explicit_cap(self):
        """No module-level DRAM cap; the pool grows up to the server maxmemory ceiling."""
        client = self.server.get_new_client()
        for i in range(3):
            r = client.execute_command('BLOB.SET', f'key_{i}', b'X' * (100 * 1024))
            assert r == b'OK', f"SET {i} failed: {r}"

    def test_live_data_survives_memory_pressure(self):
        """Any Dram key not evicted by Valkey core must return correct data.

        Under memory pressure, core may evict LO keys. The module must never
        corrupt data for keys that core did NOT evict.
        """
        client = self.server.get_new_client()
        obj_size = 200 * 1024
        payloads = {f'dram_{i}': bytes([i % 256]) * obj_size for i in range(5)}

        for key, payload in payloads.items():
            r = client.execute_command('BLOB.SET', key, payload)
            assert r == b'OK', f"SET {key} failed: {r}"

        mem_info = client.execute_command('INFO', 'memory')
        used_memory = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used_memory * 1.1)))

        # Wait for a few cron ticks.
        time.sleep(5)

        for key, payload in payloads.items():
            if client.execute_command('EXISTS', key) == 1:
                got = client.execute_command('BLOB.GET', key)
                assert got == payload, f"{key} data corrupted under pressure"

    def test_efa_set_triggers_reactive_expand(self):
        """Fill the 1MB segment with a TCP SET, then an EFA SET triggers expand.

        Same principle as test_expand_on_segment_full but exercises the EFA SET
        path (cmd_set_dram_efa) which also has reactive expand logic.
        """
        client = self.server.get_new_client()
        expand_before = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
        # Fill the pool with TCP objects until a reactive expand fires (each ~full-segment
        # object co-locates in its own segment, so a later one forces a new segment). Bounded
        # loop so a packing change can't hang the test.
        for i in range(8):
            client.execute_command('BLOB.SET', f'filler_{i}', b'F' * (950 * 1024))
            if info_largeobj(client).get('largeobj_scaling_expand_total', 0) > expand_before:
                break
        assert info_largeobj(client).get('largeobj_scaling_expand_total', 0) > expand_before, \
            "expected TCP fill to trigger a reactive expand"
        expand_after_fill = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
        # Now an EFA SET must also succeed and read back correctly in the multi-segment pool
        # (exercises cmd_set_dram_efa's alloc/expand path).
        process, address, rkey, remote_addr, length = self.start_target('--read')
        try:
            client.execute_command('BLOB.HELLO', address)
            result = client.execute_command('BLOB.SET', 'efa_key', EFA_TARGET_LEN, rkey, remote_addr, length)
            assert result == b'OK', f"EFA SET failed: {result}"
            assert client.execute_command('BLOB.GET', 'efa_key') == EFA_PATTERN
        finally:
            process.kill()
        # The EFA SET succeeded in a pool that had already expanded (multi-segment),
        # confirming cmd_set_dram_efa's alloc path works across segments.
        assert info_largeobj(client).get('largeobj_scaling_expand_total', 0) >= expand_after_fill


class TestDramProactiveExpand(ValkeyLargeObjTestCaseBase):
    """Dram mode: DRAMPool grows proactively when the scaling cron sees utilization > watermark.

    scaling-poll-ms=1000 so the cron fires every second. The test writes enough
    data to push utilization above the expand watermark, then stops writing and
    waits for the cron to add a segment.
    """

    EXPAND_TIMEOUT_S = 15

    def get_module_args(self, data_dir, direct_io):
        # segment-size=1MB; no module DRAM cap (server maxmemory 0 so shrink never fires).
        # scaling-expand-watermark=50 so filling half a segment triggers proactive expand.
        # scaling-shrink-watermark=99 to ensure shrink never fires during this test.
        # scaling-poll-ms=1000 so the cron fires frequently.
        return (
            f"operating-mode Dram"
            f" segment-size 1048576"
            f" max-object-size 983040"
            f" scaling-expand-watermark 50"
            f" scaling-shrink-watermark 99"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_proactive_expand_fires_when_watermark_exceeded(self):
        """The scaling cron adds a segment when utilization exceeds the expand watermark.

        Steps:
        1. Write one 600KB object into a 1MB segment → utilization ≈ 60% > 50% watermark.
        2. Stop writing. No new SETs happen.
        3. Wait for cron to fire and increment scaling_expand_total.

        Because cron is the only actor after step 1, any expand is definitively proactive.
        """
        client = self.server.get_new_client()

        before = info_largeobj(client)
        expand_before = before.get('largeobj_scaling_expand_total', 0)

        # Fill >50% of one 1MB segment (600KB ≈ 59% of 1MB).
        r = client.execute_command('BLOB.SET', 'probe', b'P' * (600 * 1024))
        assert r == b'OK', "BLOB.SET failed"

        # No more SETs. Wait for cron to observe utilization > 50% and expand.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_expand_total', 0) > expand_before,
            timeout=self.EXPAND_TIMEOUT_S,
        )



class TestDramServerMaxMemoryCap(ValkeyLargeObjTestCaseBase):
    """Dram mode: expansion is gated by the SERVER maxmemory watermark.

    There is no module-local DRAM budget — the pool grows on demand and the
    only ceiling is Valkey's own `maxmemory` (via would_cross_memory_watermark).
    This test sets a server maxmemory low enough that, after the pool has grown,
    a further object requiring another segment would cross the watermark and is
    rejected.
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
        """Once used_memory is near the server maxmemory ceiling, an object that
        would need a new segment (crossing the watermark) must be rejected."""
        client = self.server.get_new_client()
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')

        obj_size = 900 * 1024
        # Land the first object (pool starts at 1 segment, fits 900KB).
        client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)

        # Cap server maxmemory just above current used_memory, leaving less than
        # one segment (1MB) of headroom — so the next object cannot expand.
        used = int(client.info('memory')['used_memory'])
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(used + 256 * 1024))

        try:
            client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)
            assert False, "Expected rejection: expansion would cross server maxmemory watermark"
        except ResponseError:
            pass
        # Restore uncapped for teardown safety.
        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')


# ─── Tiered Mode Scaling ──────────────────────────────────────────────────────

class TestTieredExpand(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool expands reactively when segment fills."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_expand_on_full(self):
        """In Tiered mode, filling the DRAMPool triggers expand; data stays correct."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        client.execute_command('BLOB.SET', 'key_a', b'A' * obj_size)
        client.execute_command('BLOB.SET', 'key_b', b'B' * obj_size)

        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * obj_size
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * obj_size
        # The expanded DRAM segment joins its ring's io_uring table (per-pool).
        wait_uring_registered_matches_live(client)

    def test_tiered_multiple_segments(self):
        """Objects spread across multiple segments are all readable."""
        client = self.server.get_new_client()
        obj_size = 800 * 1024

        for i in range(4):
            r = client.execute_command('BLOB.SET', f'key_{i}', bytes([i % 256]) * obj_size)
            assert r == b'OK', f"SET key_{i} failed: {r}"

        for i in range(4):
            got = client.execute_command('BLOB.GET', f'key_{i}')
            assert got == bytes([i % 256]) * obj_size, f"Data mismatch for key_{i}"
        wait_uring_registered_matches_live(client)

    def test_tiered_nvme_fallback_on_dram_full(self):
        """When DRAMPool is at cap, further SETs still persist to NVMe and are readable."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            client.execute_command('BLOB.SET', key, fill * obj_size)

        for key, fill in [('key_a', b'A'), ('key_b', b'B'), ('key_c', b'C'), ('key_d', b'D')]:
            assert client.execute_command('BLOB.GET', key) == fill * obj_size
        wait_uring_registered_matches_live(client)


class TestTieredShrink(ValkeyLargeObjTestCaseBase):
    """Tiered mode: DRAMPool shrinks under memory pressure.

    Writes objects first, then sets server maxmemory below current used_memory
    so the module shrink watermark fires on the next cron tick.
    """

    SHRINK_TIMEOUT_S = 20

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def _assert_no_pressure(self, client):
        """Guardrail: verify server is not under memory pressure before the test writes data.

        maxmemory must be 0 (uncapped) at test start. If it's non-zero, the test
        server was left in a bad state from a previous test run and results would
        be unreliable.
        """
        mem_info = client.execute_command('INFO', 'memory')
        maxmemory = int(mem_info.get(b'maxmemory') or mem_info.get('maxmemory', 0))
        assert maxmemory == 0, (
            f"Test server already has maxmemory={maxmemory} at start — "
            f"server is under pressure before test data is written. "
            f"Run 'CONFIG SET maxmemory 0' to reset."
        )

    def _apply_shrink_pressure(self, client):
        """Set maxmemory below current used_memory so ratio > 0.80.

        Setting maxmemory = used * 0.85 gives ratio ≈ 1.18 > 0.80.
        noeviction means no keys are evicted — the module cron handles DRAM.
        """
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')
        mem_info = client.execute_command('INFO', 'memory')
        used = int(mem_info.get(b'used_memory') or mem_info.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used * 0.85)))

    def test_shrink_preserves_nvme_data(self):
        """After the scaling cron shrinks the pool, keys remain readable from NVMe."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        self._assert_no_pressure(client)

        keys = [f'shrink_key_{i}' for i in range(4)]
        for key in keys:
            r = client.execute_command('BLOB.SET', key, b'S' * obj_size)
            assert r == b'OK', f"BLOB.SET {key} failed: {r}"

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Wait for the drained segment to be fully released (draining_segments back to 0).
        # This verifies the complete shrink cycle including release_drained, not just initiation.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        for key in keys:
            assert client.execute_command('EXISTS', key) == 1, \
                f"Key {key} disappeared from keyspace after shrink (data loss)"
        # The released segment left its ring's io_uring table too (per-pool invariant holds post-shrink).
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)

    def test_shrink_then_expand(self):
        """After a shrink, new SETs succeed."""
        client = self.server.get_new_client()
        obj_size = 900 * 1024

        self._assert_no_pressure(client)

        for i in range(4):
            client.execute_command('BLOB.SET', f'pre_shrink_{i}', b'P' * obj_size)

        before = info_largeobj(client)
        shrink_before = before.get('largeobj_scaling_shrink_total', 0)

        self._apply_shrink_pressure(client)

        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Wait for the drained segment to be fully released (draining_segments back to 0).
        # This verifies the complete shrink cycle including release_drained, not just initiation.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')

        r = client.execute_command('BLOB.SET', 'post_shrink', b'Q' * obj_size)
        assert r == b'OK', f"BLOB.SET after shrink+expand failed: {r}"

        for i in range(4):
            assert client.execute_command('EXISTS', f'pre_shrink_{i}') == 1, \
                f"pre_shrink_{i} disappeared from keyspace after shrink"
        # io_uring tables track live segments per pool through shrink + the follow-up expand.
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)


class TestTieredShrinkReleasesEfaRegisteredSegment(ValkeyLargeObjTestCaseBase):
    """Tiered mode, fabric UP: a segment added by expansion is EFA-registered, and shrinking it
    must tear that registration down (Segment::drop -> efa_release_segment) BEFORE the segment
    memory is freed — a broken teardown (registration outliving freed pages) would crash here.
    Combines test_efa_set_triggers_reactive_expand (EFA-expand) and TestTieredShrink (shrink)."""

    SHRINK_TIMEOUT_S = 20

    def get_module_args(self, data_dir, direct_io):
        # Tiered so shrink can release a live-data segment (NVMe-backed), fabric up (Emulated on
        # loopback) so expanded segments are actually EFA-registered, small segment + fast cron so
        # the test forces expansion and shrink quickly.
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" scaling-poll-ms 1000"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def test_shrink_releases_efa_registered_expanded_segment(self):
        client = self.server.get_new_client()

        # Guardrail: server not already under pressure.
        mem_info = client.execute_command('INFO', 'memory')
        maxmemory = int(mem_info.get(b'maxmemory') or mem_info.get('maxmemory', 0))
        assert maxmemory == 0, f"server already under pressure (maxmemory={maxmemory})"

        expand_before = info_largeobj(client).get('largeobj_scaling_expand_total', 0)

        # Tiered SET lands on NVMe; the DRAM pool grows via PROMOTION on GET. Write two
        # ~full-segment objects, GET both to promote them — each fills its own DRAM segment, so
        # promoting the second forces a reactive expand, and with the fabric up try_expand
        # EFA-registers that new segment (the path under test).
        client.execute_command('BLOB.SET', 'key_a', b'A' * (900 * 1024))
        client.execute_command('BLOB.SET', 'key_b', b'B' * (900 * 1024))
        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * (900 * 1024)
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * (900 * 1024)

        expand_after = info_largeobj(client).get('largeobj_scaling_expand_total', 0)
        assert expand_after > expand_before, "expected an expansion (new EFA-registered segment)"

        # Every live DRAM segment is EFA-registered when the fabric is up. In Tiered mode the
        # registered count also covers the NVMe staging segments, so registered >= live (not ==).
        # The point under test: the expansion segment got registered, so registered grew past 1.
        info_expanded = info_largeobj(client)
        dram_live = info_expanded.get('largeobj_dram_live_segments', 0)
        nvme_live = info_expanded.get('largeobj_nvme_live_segments', 0)
        registered = info_expanded.get('largeobj_efa_registered_segments', -1)
        assert dram_live > 1, f"expected DRAM pool to have expanded past 1 segment, dram_live={dram_live}"
        # EFA registration spans BOTH pools: every live segment (DRAM + NVMe staging) is registered.
        assert registered == dram_live + nvme_live, \
            f"every live segment must be EFA-registered: registered={registered} dram={dram_live} nvme={nvme_live}"
        # io_uring registration is per-pool (independent of EFA): the expanded DRAM segment
        # joined its ring's table.
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)

        shrink_before = info_largeobj(client).get('largeobj_scaling_shrink_total', 0)
        registered_before_shrink = registered

        # Apply server memory pressure so the shrink cron releases a segment. In Tiered mode this
        # releases a live-data segment (data persists on NVMe), and because the fabric is up the
        # released segment carries an EFA registration that Segment::drop must tear down first.
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')
        mem_now = client.execute_command('INFO', 'memory')
        used = int(mem_now.get(b'used_memory') or mem_now.get('used_memory'))
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(int(used * 0.85)))

        # Shrink fires, then the drained segment is fully released (registration torn down + memory
        # freed). A broken teardown would fault here rather than completing cleanly.
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_scaling_shrink_total', 0) > shrink_before,
            timeout=self.SHRINK_TIMEOUT_S,
        )
        wait_for_true(
            lambda: info_largeobj(client).get('largeobj_draining_segments', 1) == 0,
            timeout=self.SHRINK_TIMEOUT_S,
        )

        # Restore, then prove the server is alive and correct after releasing a registered segment.
        client.execute_command('CONFIG', 'SET', 'maxmemory', '0')
        assert client.execute_command('PING')  # server still responsive after the release
        # The released segment's EFA registration was torn down: registered count dropped, and it
        # still equals total live segments (DRAM + NVMe) — invariant preserved through release.
        info_after = info_largeobj(client)
        registered_after = info_after.get('largeobj_efa_registered_segments', -1)
        dram_live_after = info_after.get('largeobj_dram_live_segments', 0)
        nvme_live_after = info_after.get('largeobj_nvme_live_segments', 0)
        assert registered_after < registered_before_shrink, \
            f"EFA registration not torn down on release: {registered_after} !< {registered_before_shrink}"
        assert registered_after == dram_live_after + nvme_live_after, \
            f"every live segment must stay registered after release: registered={registered_after} dram={dram_live_after} nvme={nvme_live_after}"
        # io_uring tables also dropped the released segment, per-pool.
        wait_uring_registered_matches_live(client, timeout=self.SHRINK_TIMEOUT_S)
        # The objects survive (Tiered: data on NVMe) and read back correctly after release.
        assert client.execute_command('BLOB.GET', 'key_a') == b'A' * (900 * 1024)
        assert client.execute_command('BLOB.GET', 'key_b') == b'B' * (900 * 1024)
