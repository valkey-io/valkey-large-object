import os
import glob
import time
import resource
import threading
import subprocess
import collections
import pytest
from valkey import ResponseError, Valkey
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj
from valkeytestframework.util.waiters import wait_for_equal, wait_for_true


# One advertised client memory address: the fabric address to HELLO with, plus the
# (rkey, addr, len) triple that BLOB.RDMA_GET / BLOB.RDMA_SET carries per address.
Region = collections.namedtuple('Region', 'address rkey addr len')


def address_args(regions):
    """The addresses as the EFA commands carry them: a (rkey, addr, len) triple each."""
    args = []
    for region in regions:
        args += [region.rkey, region.addr, region.len]
    return args


class TestLargeObjTieredPromotion(ValkeyLargeObjTestCaseBase):
    """Tiered mode with DRAMPool promotion enabled (default max-promote-size).
    promote-min-hits 1 so the first GET promotes and these tests observe promotion
    directly; second-touch admission has its own classes below."""

    def get_module_args(self, data_dir, direct_io):
        # max-promote-size must fit in one segment after talc overhead.
        # With seg=4M and chunk=4K the max is 2093056 (~2044 KiB).
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 4194304"
            f" promote-min-hits 1"
            f" max-promote-size 2093056"
            f" max-object-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
        )

    def test_set_creates_nvme_file(self):
        """In Tiered mode, BLOB.TCP_SET persists to NVMe."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('BLOB.TCP_SET', 'tiered_key', payload)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 1, "Tiered SET should create an NVMe .dat file"

    def test_keyspace_events(self):
        """Async NVMe commit publishes largeobj.create then largeobj.update."""
        client = self.server.get_new_client()
        pubsub = self.subscribe_keyspace_events(client)
        client.execute_command('BLOB.TCP_SET', 'eventkey', b'A' * 4096)
        client.execute_command('BLOB.TCP_SET', 'eventkey', b'B' * 8192)
        assert self.read_keyspace_events(pubsub, 2) == [
            ('largeobj.create', 'eventkey'),
            ('largeobj.update', 'eventkey'),
        ]

    def _dat_count(self):
        return len(glob.glob(os.path.join(self.data_dir, "*.dat")))

    def _wait_free_settled(self, client):
        """Wait for Valkey lazyfree to drain AND teardown to unlink the file."""
        wait_for_equal(
            lambda: client.info("memory").get("lazyfree_pending_objects", 0), 0
        )

    def test_get_after_set_roundtrip(self):
        """Tiered mode: SET then GET returns correct data, including multi-chunk objects.
        Validates FileHeader presence and data offset in the .dat file."""
        client = self.server.get_new_client()
        # chunk-size is 4096 in this class.
        # Single chunk: 4096 bytes.
        payload_single = b'A' * 4096
        client.execute_command('BLOB.TCP_SET', 'rt_single', payload_single)
        assert client.execute_command('BLOB.TCP_GET', 'rt_single') == payload_single
        # Multi-chunk with partial last chunk: 8192 + 1 = 8193 → 3 chunks.
        payload_multi = b'M' * 8193
        client.execute_command('BLOB.TCP_SET', 'rt_multi', payload_multi)
        # GET via serve-and-discard (first GET triggers promotion, verify data).
        result = client.execute_command('BLOB.TCP_GET', 'rt_multi')
        assert result == payload_multi
        # Second GET from DRAM after promotion.
        assert client.execute_command('BLOB.TCP_GET', 'rt_multi') == payload_multi
        # Validate FileHeader on disk: first 4 bytes should be b"LOBJ",
        # data starts at offset 4096.
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected at least one .dat file"
        # Check the most recent file (highest OID = last in sorted hex names).
        with open(dat_files[-1], 'rb') as f:
            header_page = f.read(4096)
            assert header_page[:4] == b'LOBJ', "FileHeader magic mismatch"
            assert header_page[4] == 1, "FileHeader version mismatch"
            # Data starts at offset 4096.
            f.seek(4096)
            data_on_disk = f.read(len(payload_multi))
            assert data_on_disk == payload_multi, "On-disk data at offset 4096 mismatch"

    def test_promotion_caches_in_dram(self):
        """After a GET miss, the object is promoted to DRAMPool.
        BLOB.INFO TIER observes the transition: nvme after SET, dram after the first GET."""
        client = self.server.get_new_client()
        payload = b'B' * 4096
        client.execute_command('BLOB.TCP_SET', 'promo_key', payload)

        # Freshly SET: on NVMe only, nothing cached yet.
        assert client.execute_command('BLOB.INFO', 'promo_key', 'TIER') == b'disk'

        # First GET: DRAMPool miss -> NVMe read -> promote to DRAMPool.
        result1 = client.execute_command('BLOB.TCP_GET', 'promo_key')
        assert result1 == payload
        assert client.execute_command('BLOB.INFO', 'promo_key', 'TIER') == b'dram'

        # Second GET: served from DRAMPool (promotion happened).
        result2 = client.execute_command('BLOB.TCP_GET', 'promo_key')
        assert result2 == payload
        assert client.execute_command('BLOB.INFO', 'promo_key', 'TIER') == b'dram'

    def test_delete_removes_nvme_file(self):
        """DEL removes the NVMe file."""
        client = self.server.get_new_client()
        payload = b'D' * 4096
        client.execute_command('BLOB.TCP_SET', 'del_key', payload)
        dat_files_before = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_before) >= 1
        client.execute_command('DEL', 'del_key')
        wait_for_equal(lambda: client.info('memory').get('lazyfree_pending_objects', 0), 0)
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)

    # ─── COPY callback tests ─────────────────────────────────────────────

    def test_copy(self):
        """COPY in Tiered mode: independent NVMe file, digest differs, delete independence."""
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('BLOB.TCP_SET', 'srckey', payload)
        # COPY creates an independent object with its own NVMe file
        result = client.execute_command('COPY', 'srckey', 'dstkey')
        assert result == 1 or result is True
        assert client.execute_command('BLOB.TCP_GET', 'srckey') == payload
        assert client.execute_command('BLOB.TCP_GET', 'dstkey') == payload
        # COPY carries the CRC over unchanged.
        assert (client.execute_command('BLOB.INFO', 'srckey', 'CRC')
                == client.execute_command('BLOB.INFO', 'dstkey', 'CRC'))
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 2, f"Expected at least 2 .dat files, got {len(dat_files)}"
        # COPY gets a new OID so digests differ
        src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'srckey')
        dst_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'dstkey')
        assert src_digest != dst_digest
        # Deleting source does not affect the copy
        client.execute_command('DEL', 'srckey')
        wait_for_equal(lambda: client.info('memory').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('BLOB.TCP_GET', 'dstkey') == payload
        # Deleting copy does not affect the source
        client.execute_command('BLOB.TCP_SET', 'srckey2', payload)
        client.execute_command('COPY', 'srckey2', 'dstkey2')
        client.execute_command('DEL', 'dstkey2')
        wait_for_equal(lambda: client.info('memory').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('BLOB.TCP_GET', 'srckey2') == payload
        #  The copy starts un-promoted even if the source is cached
        client.execute_command('BLOB.TCP_GET', 'srckey2')  # promote source
        assert client.execute_command('BLOB.INFO', 'srckey2', 'TIER') == b'dram'
        client.execute_command('COPY', 'srckey2', 'dstkey3')
        assert client.execute_command('BLOB.INFO', 'dstkey3', 'TIER') == b'disk'

    # ─── MEMORY USAGE callback tests ──────────────────────────────────────

    def test_memory_usage(self):
        """MEMORY USAGE after promotion returns a non-zero value for the key."""
        client = self.server.get_new_client()
        payload_size = 4096
        client.execute_command('BLOB.TCP_SET', 'memkey', b'M' * payload_size)
        # Single GET promotes into DRAMPool (promote-on-first-GET policy).
        client.execute_command('BLOB.TCP_GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        assert mem > 0, f"Expected non-zero MEMORY USAGE after promotion, got {mem}"

    # ─── DEBUG DIGEST callback tests ──────────────────────────────────────

    def test_debug_digest(self):
        """DEBUG DIGEST-VALUE is deterministic; nonexistent key returns nil digest."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.TCP_SET', 'digkey', b'G' * 4096)
        d1 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        d2 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        assert d1 == d2
        # Nonexistent key returns nil digest
        nil_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'noexist')
        assert nil_digest == [b'0' * 40]

    # ─── Streaming tests ─────────────────────────────────────────────────
    # CRC integrity, delete-during-SET, disk-maxmemory.
    # These use the same config as the promotion tests above.

    def test_zero_length_object_rejected(self):
        """BLOB.TCP_SET with zero-length payload is rejected."""
        client = self.server.get_new_client()
        try:
            client.execute_command('BLOB.TCP_SET', 'empty_key', b'')
            assert False, "Expected error for zero-length object"
        except ResponseError as e:
            assert 'object length must be > 0' in str(e).lower(), f"Unexpected: {e}"

    def test_nvme_maxmemory_exhaustion(self):
        """SET that would exceed disk-maxmemory is rejected."""
        client = self.server.get_new_client()
        # disk-maxmemory minimum is 1MB. Set to 1MB.
        client.execute_command('CONFIG', 'SET', 'largeobj.disk-maxmemory', '1048576')
        # Each 256KB object has disk_len = 4096 (header) + 256KB = 266240 bytes.
        # Three fit (798720 < 1MB), fourth exceeds (1064960 > 1MB).
        payload = b'A' * (256 * 1024)
        for i in range(3):
            result = client.execute_command('BLOB.TCP_SET', f'nvme_cap_{i}', payload)
            assert result == b'OK'
        # Fourth SET should fail (would exceed 1MB).
        try:
            client.execute_command('BLOB.TCP_SET', 'nvme_cap_3', payload)
            assert False, "Expected capacity exceeded error from disk-maxmemory"
        except ResponseError as e:
            assert 'used disk space' in str(e).lower(), f"Unexpected error: {e}"

    # ─── DEL semantics ────────────────────────────────────────────────────

    def test_delete_semantics(self):
        """DEL resolves a subsequent GET to nil and unlinks the .dat file -- both
        for a cold object and one already promoted into DRAMPool (a distinct free
        path: the DRAM cache entry is dropped alongside the Arc<ObjectFile>)."""
        client = self.server.get_new_client()
        payload = b"D" * 4096
        assert client.execute_command("DBSIZE") == 0

        # Cold object: GET after DEL is nil; DEL drops the key from the keyspace.
        client.execute_command("BLOB.TCP_SET", "gk", payload)
        assert client.execute_command("DBSIZE") == 1
        assert client.execute_command("BLOB.TCP_GET", "gk") == payload
        client.execute_command("DEL", "gk")
        assert client.execute_command("DBSIZE") == 0
        assert client.execute_command("BLOB.TCP_GET", "gk") is None

        # Promoted object: DEL still resolves to nil and the NVMe file is unlinked.
        client.execute_command("BLOB.TCP_SET", "pk", payload)
        assert client.execute_command("BLOB.TCP_GET", "pk") == payload  # first GET promotes
        client.execute_command("DEL", "pk")
        assert client.execute_command("DBSIZE") == 0
        assert client.execute_command("BLOB.TCP_GET", "pk") is None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── Overwrite ────────────────────────────────────────────────────────

    def test_overwrite_semantics(self):
        """Overwriting a key commits a new object version and tears down the old
        one: GET returns the new payload and exactly one .dat remains per key.
        Covers the post-promotion overwrite (stale DRAM entry must be replaced)
        and repeated overwrite (steady state stays bounded -- no file leak)."""
        client = self.server.get_new_client()
        v1 = b"1" * 4096
        v2 = b"2" * 8192

        # Basic overwrite: the new value wins at commit; the old file is torn down.
        client.execute_command("BLOB.TCP_SET", "ok", v1)
        assert client.execute_command("DBSIZE") == 1
        wait_for_equal(self._dat_count, 1)
        client.execute_command("BLOB.TCP_SET", "ok", v2)
        assert client.execute_command("DBSIZE") == 1  # overwrite reuses the key
        assert client.execute_command("BLOB.TCP_GET", "ok") == v2
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)

        # Overwrite after promotion: the new GET must serve the new payload.
        client.execute_command("BLOB.TCP_SET", "opk", b"A" * 4096)
        assert client.execute_command("BLOB.TCP_GET", "opk") == b"A" * 4096  # promote v1
        client.execute_command("BLOB.TCP_SET", "opk", b"B" * 4096)
        assert client.execute_command("BLOB.TCP_GET", "opk") == b"B" * 4096
        assert client.execute_command("BLOB.TCP_GET", "opk") == b"B" * 4096

        # Repeated overwrite of one key never leaks files.
        for i in range(10):
            client.execute_command("BLOB.TCP_SET", "leakkey", bytes([65 + (i % 26)]) * 4096)
        assert client.execute_command("BLOB.TCP_GET", "leakkey") is not None
        self._wait_free_settled(client)
        # One live file per surviving key: ok, opk, leakkey.
        wait_for_equal(self._dat_count, 3)

    # ─── GET result outlives a concurrent DEL (honor rule) ────────────────

    def test_get_result_correct_across_delete_churn(self):
        """A GET that resolves the key returns its full data even under delete
        churn: the honor-rule pin keeps the file alive for the read's duration.
        We can't force a mid-flight race deterministically from the client, so we
        assert the observable invariant: interleaved GET/DEL never corrupts data."""
        client = self.server.get_new_client()
        payload = b"Z" * 8192
        for _ in range(20):
            client.execute_command("BLOB.TCP_SET", "churn", payload)
            assert client.execute_command("DBSIZE") == 1
            assert client.execute_command("BLOB.TCP_GET", "churn") == payload
            client.execute_command("DEL", "churn")
            assert client.execute_command("DBSIZE") == 0
            assert client.execute_command("BLOB.TCP_GET", "churn") is None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── Other free triggers: expiry & flush ──────────────────────────────

    def test_expiry_and_flushall_unlink_files(self):
        """Expiry (TTL) and FLUSHALL are free paths distinct from DEL/overwrite;
        both must unlink the .dat file(s)."""
        client = self.server.get_new_client()

        # Passive expiry: polling EXISTS drives expiry, then teardown unlinks.
        client.execute_command("BLOB.TCP_SET", "exk", b"E" * 4096)
        wait_for_equal(self._dat_count, 1)
        client.execute_command("PEXPIRE", "exk", 50)
        wait_for_equal(lambda: client.execute_command("EXISTS", "exk"), 0)
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

        # FLUSHALL unlinks every remaining file.
        for i in range(3):
            client.execute_command("BLOB.TCP_SET", f"fk{i}", b"F" * 4096)
        wait_for_equal(self._dat_count, 3)
        client.execute_command("FLUSHALL")
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── max-object-size tests ───────────────────────────────────────────

    def test_max_object_size_tiered(self):
        """max-object-size rejects oversized SETs in Tiered mode.
        No .dat file created for rejected SET. Reads unaffected after lowering limit."""
        client = self.server.get_new_client()
        limit = 4096
        client.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(limit))
        # Oversized SET is rejected with max-object-size error (not NVMe capacity).
        try:
            client.execute_command('BLOB.TCP_SET', 'bigkey', b'X' * (limit + 1))
            assert False, "Expected max object size rejection"
        except ResponseError as e:
            err = str(e).lower()
            assert 'max object size' in err, f"Unexpected error: {e}"
            assert 'used disk space' not in err, f"Should not hit disk OOM error: {e}"
        # Rejected SET must not leave a .dat file or phantom key.
        assert self._dat_count() == 0
        assert client.execute_command('DBSIZE') == 0
        # At-limit SET succeeds and creates a .dat file.
        assert client.execute_command('BLOB.TCP_SET', 'okkey', b'Y' * limit) == b'OK'
        wait_for_equal(self._dat_count, 1)
        # GET returns correct data (promotion path on first GET, DRAM on second).
        assert client.execute_command('BLOB.TCP_GET', 'okkey') == b'Y' * limit
        assert client.execute_command('BLOB.TCP_GET', 'okkey') == b'Y' * limit
        # Lowering limit below stored object size does not affect reads.
        client.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(limit // 2))
        assert client.execute_command('BLOB.TCP_GET', 'okkey') == b'Y' * limit

    def test_max_object_size_tiered_rejection(self):
        """CONFIG SET max-object-size > disk-maxmemory is rejected."""
        client = self.server.get_new_client()
        # Set disk-maxmemory to a small value so we can exceed it.
        nvme_limit = 1048576  # 1 MiB
        client.execute_command('CONFIG', 'SET', 'largeobj.disk-maxmemory', str(nvme_limit))
        obj_limit = 2 * 1048576  # 2 MiB
        try:
            client.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(obj_limit))
            assert False, "Expected CONFIG SET to be rejected"
        except ResponseError as e:
            assert 'max-object-size' in str(e).lower(), f"Unexpected error: {e}"
            assert 'disk-maxmemory' in str(e).lower(), f"Unexpected error: {e}"

    def test_num_objects(self):
        """INFO num_objects counts LargeObject keys through SET, overwrite, COPY
        and DEL, and ignores keys of standard (core) Valkey data types such as
        strings. Frees run async, so decrements are awaited."""
        client = self.server.get_new_client()
        num_objects = lambda: info_largeobj(client)['largeobj_num_objects']
        client.execute_command('BLOB.TCP_SET', 'blob', b'A' * 1024)
        client.execute_command('BLOB.TCP_SET', 'blob', b'B' * 1024)
        client.execute_command('COPY', 'blob', 'blob_copy')
        client.set('string_key', 'v')
        wait_for_true(lambda: num_objects() == 2)
        client.delete('blob', 'blob_copy')
        wait_for_true(lambda: num_objects() == 0)


class TestTieredDeleteDuringSet(ValkeyLargeObjTestCaseBase):
    """Deterministic delete-during-SET race via the test-pause hook.

    The hook is set at module load (immutable), so every SET pauses for 2s after
    writing chunks but before commit. This class runs in its own server instance."""

    def get_module_args(self, data_dir, direct_io):
        base = TestLargeObjTieredPromotion.get_module_args(self, data_dir, direct_io)
        return base + " test-pause-before-finalize-set-ms 2000"

    def test_tiered_delete_during_set(self):
        """The test-pause hook freezes the SET's tokio task after NVMe data
        writes complete but before set_finalize commits the key. While the SET
        is paused we fire a DEL from a second client, guaranteeing:
            Thread A (tokio):  write chunks → [PAUSE] → set_finalize (commits)
            Thread B (main) :                  DEL key (removes v1)
        The DEL removes the existing v1. The paused SET then resumes: set_finalize
        finds no existing key (DEL cleared it), so it commits payload2 as a fresh
        key. No crash, no corruption, and GET returns payload2."""
        client = self.server.get_new_client()
        del_client = self.server.get_new_client()
        payload = b'D' * 32768
        client.execute_command('BLOB.TCP_SET', 'delset_key', payload)
        assert client.execute_command('BLOB.TCP_GET', 'delset_key') == payload
        payload2 = b'E' * 32768
        set_result = [None]
        set_error = [None]
        def background_set():
            try:
                set_result[0] = client.execute_command(
                    'BLOB.TCP_SET', 'delset_key', payload2
                )
            except Exception as e:
                set_error[0] = e
        t = threading.Thread(target=background_set)
        t.start()
        # Wait long enough for the SET to begin its NVMe writes and enter the
        # pause window (chunk writes are fast for 32KB at 4KB chunks).
        time.sleep(0.5)
        # DEL fires while SET is paused — deterministically hits the race window.
        del_result = del_client.execute_command('DEL', 'delset_key')
        assert del_result == 1, f"Expected DEL to find key, got {del_result}"
        t.join(timeout=10)
        assert not t.is_alive(), "SET thread did not finish"
        assert set_error[0] is None, f"SET raised: {set_error[0]}"
        # The paused SET's set_finalize sees no existing key (DEL removed v1)
        # and commits payload2 as a fresh key.
        wait_for_equal(
            lambda: client.info('memory').get('lazyfree_pending_objects', 0), 0
        )
        assert client.execute_command('BLOB.TCP_GET', 'delset_key') == payload2

    def test_info_sets_in_flight(self):
        """Concurrent Tiered SETs each count in inflight_requests until they reply. The
        pause holds their commits long enough to observe them."""
        n = 3
        client = self.server.get_new_client()
        setters = [self.server.get_new_client() for _ in range(n)]
        results = [None] * n
        errors = [None] * n

        def inflight():
            # A failed SET would otherwise only show up as a wait timeout.
            assert errors == [None] * n, f"SET raised: {errors}"
            return client.info('largeobj_core_metrics')['largeobj_inflight_requests']

        def background_set(i):
            try:
                results[i] = setters[i].execute_command('BLOB.TCP_SET', f'key{i}', b'S' * 4096)
            except Exception as e:
                errors[i] = e

        assert inflight() == 0
        threads = [threading.Thread(target=background_set, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        try:
            wait_for_equal(inflight, n)
        finally:
            for t in threads:
                t.join(timeout=10)
        assert not any(t.is_alive() for t in threads), "SET threads did not finish"
        assert errors == [None] * n, f"SET raised: {errors}"
        assert results == [b'OK'] * n
        assert inflight() == 0

    def test_info_reports_disk_staging_utilization(self):
        """A Tiered SET holds its staging buffers until it replies, so staging utilization
        rises while the pause holds the SET and returns to 0 after. A one-chunk SET holds
        one buffer of the object's size."""
        client = self.server.get_new_client()
        setter = self.server.get_new_client()
        errors = []
        value_len = 4096

        def staging():
            assert not errors, f"SET raised: {errors}"
            return client.info('largeobj_disk_staging')

        def util():
            return staging()['largeobj_disk_staging_utilization_pct']

        def background_set():
            try:
                setter.execute_command('BLOB.TCP_SET', 'key', b'S' * value_len)
            except Exception as e:
                errors.append(e)

        info = staging()
        capacity = (info['largeobj_disk_staging_segments']
                    * info['largeobj_disk_staging_segment_size_bytes'])
        assert util() == 0
        t = threading.Thread(target=background_set)
        t.start()
        try:
            wait_for_true(lambda: util() > 0)
            assert util() == round(value_len * 100 / capacity, 2)
        finally:
            t.join(timeout=10)
        assert not t.is_alive(), "SET thread did not finish"
        assert util() == 0

    def test_commandstats_count_background_time(self):
        """An async SET's commandstats time includes its background work, not just the
        main-thread dispatch. The pause holds the SET 2 s before it commits."""
        client = self.server.get_new_client()
        assert client.execute_command('BLOB.TCP_SET', 'key', b'S' * 4096) == b'OK'
        stats = client.info('commandstats')['cmdstat_BLOB.TCP_SET']
        assert stats['calls'] == 1
        assert stats['usec'] >= 2_000_000, stats


class TestTieredCoalescing(ValkeyLargeObjTestCaseBase):
    """Deterministic coalescing test with test-pause-during-promotion-ms set at
    load time (immutable config). The 2s pause ensures concurrent GETs register
    as coalesced waiters on the Filling ObjectContext."""

    def get_module_args(self, data_dir, direct_io):
        base = TestLargeObjTieredPromotion.get_module_args(self, data_dir, direct_io)
        return base + " test-pause-during-promotion-ms 2000"

    def test_coalescing_deterministic(self):
        """Deterministic coalescing: pause the promotion leader so concurrent
        GETs are guaranteed to register as waiters on the Filling ObjectContext.
        Asserts the coalesced_reads metric incremented and all readers get
        correct data.

        Timeline:
            Leader GET:  insert Filling → [PAUSE 2s] → NVMe read → mark Ready
            Waiter GETs: arrive during pause → subscribe → wait → serve from DRAM
        """
        client = self.server.get_new_client()
        payload = os.urandom(32768)
        client.execute_command('BLOB.TCP_SET', 'det_coal_key', payload)
        waiters_before = int(
            client.info('largeobj').get('largeobj_coalesced_reads', 0)
        )
        num_readers = 4
        results = [None] * num_readers
        errors = [None] * num_readers
        def reader(idx):
            try:
                c = self.server.get_new_client()
                results[idx] = c.execute_command('BLOB.TCP_GET', 'det_coal_key')
            except Exception as e:
                errors[idx] = e
        # Fire all readers — the first becomes the leader, the rest arrive
        # during the 2s pause and deterministically become waiters.
        threads = []
        for i in range(num_readers):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
        for t in threads:
            t.start()
        # Wait for all to return (leader unblocks after 2s pause + read).
        for t in threads:
            t.join(timeout=10)
        for i in range(num_readers):
            assert errors[i] is None, f"Reader {i} got error: {errors[i]}"
            assert results[i] == payload, (
                f"Reader {i} data mismatch: got "
                f"{len(results[i]) if results[i] else 'None'} bytes"
            )
        # Exactly num_readers - 1 should have coalesced (one is the leader).
        waiters_after = int(
            client.info('largeobj').get('largeobj_coalesced_reads', 0)
        )
        assert waiters_after - waiters_before == num_readers - 1, (
            f"Expected {num_readers - 1} coalesced reads, "
            f"got {waiters_after - waiters_before} "
            f"(before={waiters_before} after={waiters_after})"
        )

    def test_delete_during_promotion(self):
        """DEL a key while a promotion leader and a coalesced waiter are both
        in-flight. The 2s pause holds the leader mid-promotion so the waiter
        registers as coalesced and the DEL lands while both are blocked.
        Timeline:
            t=0.0s  Leader GET starts promotion, enters 2s pause
            t=0.3s  Waiter GET arrives, subscribes as coalesced waiter
            t=0.6s  DEL arrives on a third client
            t=2.0s  Pause ends, leader + waiter complete or error
        No crash. Both GETs return either correct data or an error. The key
        must be gone afterwards. coalesced_reads must increment by 1."""
        client = self.server.get_new_client()
        payload = os.urandom(32768)
        client.execute_command('BLOB.TCP_SET', 'delprom_key', payload)
        coalesced_before = int(
            client.info('largeobj').get('largeobj_coalesced_reads', 0)
        )
        num_readers = 2
        results = [None] * num_readers
        errors = [None] * num_readers
        def background_get(idx):
            try:
                c = self.server.get_new_client()
                results[idx] = c.execute_command('BLOB.TCP_GET', 'delprom_key')
            except Exception as e:
                errors[idx] = e
        # Reader 0 becomes the promotion leader and enters the 2s pause.
        threads = [threading.Thread(target=background_get, args=(i,)) for i in range(num_readers)]
        threads[0].start()
        # Reader 1 arrives during the pause — becomes a coalesced waiter.
        time.sleep(0.3)
        threads[1].start()
        # DEL arrives while both readers are blocked on the paused promotion.
        time.sleep(0.3)
        del_client = self.server.get_new_client()
        del_result = del_client.execute_command('DEL', 'delprom_key')
        for t in threads:
            t.join(timeout=10)
        assert not any(t.is_alive() for t in threads), "GET threads did not finish"
        # Both GETs: either correct payload or error — no crash, no corruption.
        for i in range(num_readers):
            if errors[i] is None and isinstance(results[i], bytes):
                assert results[i] == payload, f"Reader {i} data mismatch"
        assert del_result in (0, 1)
        wait_for_equal(lambda: client.info('memory').get('lazyfree_pending_objects', 0), 0)
        # Key must be gone.
        assert client.execute_command('BLOB.TCP_GET', 'delprom_key') is None
        # Confirm the waiter actually coalesced (promotion path was taken).
        coalesced_after = int(
            client.info('largeobj').get('largeobj_coalesced_reads', 0)
        )
        assert coalesced_after - coalesced_before == 1, (
            f"Expected 1 coalesced read, got {coalesced_after - coalesced_before}"
        )


class TestLargeObjTieredNvmeOnly(ValkeyLargeObjTestCaseBase):
    """Tiered mode with max-promote-size=0 (no promotion, all reads from NVMe)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 0"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
        )

    def test_set_get_roundtrip_no_promotion(self):
        """With max-promote-size=0, GET always reads from NVMe (no DRAMPool caching)."""
        client = self.server.get_new_client()
        payload = b'N' * 8192
        client.execute_command('BLOB.TCP_SET', 'nvme_key', payload)
        result = client.execute_command('BLOB.TCP_GET', 'nvme_key')
        assert result == payload

    def test_multiple_gets_all_from_nvme(self):
        """Multiple GETs with max-promote-size=0 should all succeed."""
        client = self.server.get_new_client()
        payload = b'R' * 4096
        client.execute_command('BLOB.TCP_SET', 'repeat_key', payload)
        for _ in range(5):
            result = client.execute_command('BLOB.TCP_GET', 'repeat_key')
            assert result == payload
            assert client.execute_command('BLOB.INFO', 'repeat_key', 'TIER') == b'disk'

    def test_reject_invalid_buffer_configs(self):
        """CONFIG SET rejects min-buffers-per-op > max-buffers-per-op and
        vice-versa, exercising both validation callbacks."""
        client = self.server.get_new_client()
        # Direction 1: raise min above current max (default max=8).
        client.execute_command('CONFIG', 'SET', 'largeobj.max-buffers-per-op', '2')
        try:
            client.execute_command('CONFIG', 'SET', 'largeobj.min-buffers-per-op', '3')
            assert False, "Expected CONFIG SET rejection (min > max)"
        except ResponseError as e:
            assert 'min-buffers-per-op' in str(e).lower(), f"Unexpected error: {e}"
        # Direction 2: lower max below current min (default min=2).
        try:
            client.execute_command('CONFIG', 'SET', 'largeobj.max-buffers-per-op', '1')
            assert False, "Expected CONFIG SET rejection (max < min)"
        except ResponseError as e:
            assert 'max-buffers-per-op' in str(e).lower(), f"Unexpected error: {e}"

    # ─── MEMORY USAGE tests ───────────────────────────────────────────────

    def test_memory_usage_tiered_cold(self):
        """Without promotion, MEMORY USAGE reports only LoValue struct overhead.

        This test class sets max-promote-size=0, so objects are never promoted
        to DRAMPool. memory_usage reports only sizeof(LoValue) (24 bytes) — the
        payload lives on NVMe and does not consume DRAM.
        """
        client = self.server.get_new_client()
        payload_size = 4096
        payload = b'M' * payload_size
        client.execute_command('BLOB.TCP_SET', 'memkey', payload)
        # Even after a GET the object stays cold (max-promote-size=0).
        client.execute_command('BLOB.TCP_GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        lo_value_size = 24
        # Our callback returns only sizeof(LoValue) = 24. Valkey adds per-key
        # overhead (~72-120 bytes), so total is well below lo_value_size + payload_size.
        upper_bound = lo_value_size + payload_size
        assert mem < upper_bound, (
            f"Expected MEMORY USAGE < {upper_bound} (cold, not promoted), got {mem}"
        )


# ─── NVMe disk-usage accounting ──────────────────────────────────────────
# These Tiered-mode classes use small disk-maxmemory caps to exercise the
# capacity gate; each defines its own get_module_args.


class _NvmeAccountingBase(ValkeyLargeObjTestCaseBase):
    """Shared helpers for the NVMe disk-usage accounting tests.

    INFO reports the usage counter as a percentage of `disk-maxmemory`
    (`largeobj_disk_utilization_pct`). These tests also exercise its other
    externally-visible effect: the reserve-if-capacity gate
    (`try_reserve_nvme_disk_usage`) on the Tiered SET path.
    A SET that would push tracked usage past `disk-maxmemory` is rejected with
    "pool exhausted"; a SET that fits succeeds. By filling to the cap, freeing,
    and re-filling we prove the counter is incremented on create and -- critically --
    decremented at TRUE deletion (ObjectFile::Drop, after teardown), not merely at key-free.
    """

    # Must match storage::FILE_HEADER_SIZE and storage::IO_ALIGN in mod.rs.
    FILE_HEADER_SIZE = 4096
    IO_ALIGN = 4096

    def _align_up(self, n):
        return (n + self.IO_ALIGN - 1) & ~(self.IO_ALIGN - 1)

    def _dat_count(self):
        return len(glob.glob(os.path.join(self.data_dir, "*.dat")))

    def _wait_free_settled(self, client):
        """Wait for Valkey lazyfree to drain AND teardown to unlink the file."""
        wait_for_equal(
            lambda: client.info("memory").get("lazyfree_pending_objects", 0), 0
        )

    def _set_ok(self, client, key, payload):
        assert client.execute_command("BLOB.TCP_SET", key, payload) == b"OK"

    def _set_ok_eventually(self, client, key, payload, tries=100, delay=0.02):
        """Overwrite SET that tolerates a *transient* 'pool exhausted'.

        On overwrite the replaced object's bytes are released asynchronously in
        ObjectFile::Drop (teardown runs on the tokio blocking pool), so a rapid
        re-SET can momentarily observe the old reservation still outstanding and
        be rejected. `_wait_free_settled` can't help here: an overwrite frees the
        value synchronously on the main thread, so lazyfree never even increments.
        Retry within a bounded budget; if the SET never succeeds the capacity was
        genuinely not reclaimed -- a real leak -- and the assertion below fails.
        """
        last = None
        for _ in range(tries):
            try:
                assert client.execute_command("BLOB.TCP_SET", key, payload) == b"OK"
                return
            except ResponseError as e:
                if "used disk space" not in str(e).lower():
                    raise
                last = e
                time.sleep(delay)
        assert False, (
            f"SET '{key}' still OOM-disk after {tries} tries "
            f"({tries * delay:.1f}s) -- capacity not reclaimed on overwrite (leak): {last}"
        )

    def _set_rejected(self, client, key, payload):
        try:
            client.execute_command("BLOB.TCP_SET", key, payload)
            assert False, f"Expected '{key}' SET to be rejected (capacity exceeded)"
        except ResponseError as e:
            err = str(e).lower()
            assert "used disk space" in err, \
                f"Unexpected error: {e}"


class TestNvmeUsageFreedOnDelete(_NvmeAccountingBase):
    """Capacity is reclaimed only when the file is truly unlinked."""

    # 2 MiB cap. Each 256 KiB object has disk_len = 4096 (header) + 256 KiB = 266240.
    # Seven fit (1863680 < 2 MiB), eighth would exceed (2129920 > 2 MiB).
    CAP = 2 * 1024 * 1024
    OBJ = 256 * 1024

    def get_module_args(self, data_dir, direct_io):
        # max-promote-size must fit in segment after talc overhead.
        # seg=1M, chunk=OBJ=256K → max 1028096.
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-maxmemory {self.CAP}"
            f" max-object-size {self.CAP}"
            f" disk-staging-size {self.CAP}"
            f" segment-size 1048576"
            f" max-promote-size 1028096"
            f" chunk-size {self.OBJ}"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def test_capacity_reclaimed_on_true_free(self):
        """Capacity is reclaimed only when the file is truly unlinked -- via DEL
        (partial, per-object) and via FLUSHALL (all). The usage decrement lives in
        ObjectFile::Drop, so the waits below are load-bearing (freed at TRUE
        deletion, not merely at key-free)."""
        client = self.server.get_new_client()
        payload = b"X" * self.OBJ

        # Fill the cap (7 * 266240 = 1863680 < 2 MiB); the eighth must be rejected.
        for i in range(7):
            self._set_ok(client, f"k{i}", payload)
        wait_for_equal(self._dat_count, 7)
        assert client.execute_command("DBSIZE") == 7
        # A rejected SET must not leave a phantom key in the keyspace.
        self._set_rejected(client, "k7", payload)
        assert client.execute_command("DBSIZE") == 7

        # DEL one object: the slot frees only after teardown unlinks the file, so
        # the previously-rejected SET fits once (and only once) the file is gone.
        client.execute_command("DEL", "k0")
        assert client.execute_command("DBSIZE") == 6
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 6)
        self._set_ok(client, "k7", payload)
        assert client.execute_command("DBSIZE") == 7
        wait_for_equal(self._dat_count, 7)

        # FLUSHALL frees every object; the full cap becomes available again.
        client.execute_command("FLUSHALL")
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)
        for i in range(7):
            self._set_ok(client, f"g{i}", payload)
        assert client.execute_command("DBSIZE") == 7
        wait_for_equal(self._dat_count, 7)

    def test_info_reports_disk_usage(self):
        """INFO reports the usage the capacity gate enforces, as a percentage of disk-maxmemory."""
        client = self.server.get_new_client()

        def util():
            return client.info('largeobj_disk')['largeobj_disk_utilization_pct']

        for i in range(7):
            self._set_ok(client, f"k{i}", b"X" * self.OBJ)
        # Two decimals: 7 files of 266240 bytes = 1863680 / 2097152 = 88.867...%.
        assert util() == 88.87
        client.execute_command("DEL", "k0")
        self._wait_free_settled(client)
        # 6 * 266240 / 2097152 = 76.171875%.
        wait_for_equal(util, 76.17)
        client.execute_command("FLUSHALL")
        self._wait_free_settled(client)
        wait_for_equal(util, 0)

    def test_overwrite_does_not_leak_capacity(self):
        client = self.server.get_new_client()

        # Repeatedly overwrite one key far more times than the cap allows. If the
        # old object's bytes were not freed on overwrite, usage would climb past the
        # cap and a later overwrite would be wrongly rejected.
        for i in range(20):
            self._set_ok_eventually(client, "ow", bytes([65 + (i % 26)]) * self.OBJ)
        assert client.execute_command("DBSIZE") == 1  # one key throughout
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)


class TestNvmeUsageAccountsForPadding(_NvmeAccountingBase):
    """O_DIRECT pads writes up to IO_ALIGN (4096); accounting must count the padded
    on-disk size (FILE_HEADER_SIZE + align_up(logical_len)), not the logical length."""

    # Cap = FILE_HEADER_SIZE + 1 MiB = 1052672.
    # An aligned 1 MiB object fits exactly: disk_len = 4096 + 1048576 = 1052672 == cap.
    # A non-aligned object of 1048577 bytes does NOT fit:
    #   disk_len = 4096 + align_up(1048577) = 4096 + 1052672 = 1056768 > cap.
    #   The extra 4095 bytes of padding push it over — that IS the padding effect.
    CAP = 4096 + 1024 * 1024  # 1052672

    def get_module_args(self, data_dir, direct_io):
        # segment-size must exceed the largest staged object (object < segment):
        # this test stages 1 MiB and ~1 MiB+2KiB objects, so use 2 MiB segments.
        # disk-staging-size 4 MiB -> ceil(4MiB / 2MiB) = 2 NVMe staging segments.
        # max-promote-size must fit in segment after talc overhead (max 2084864).
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-maxmemory {self.CAP}"
            f" max-object-size {self.CAP}"
            f" disk-staging-size 4194304"
            f" segment-size 2097152"
            f" max-promote-size 2084864"
            f" chunk-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def test_padding_accounting(self):
        """Prove that alignment padding (not just the header) is accounted for.
        - Aligned 1 MiB object fits exactly: disk_len = 4096 + 1048576 = cap.
        - Non-aligned 1 MiB + 1 byte does NOT fit: align_up rounds up to the
          next 4 KiB boundary, pushing disk_len past the cap. The rejection is
          attributable to the padding bytes, not the header (both objects have
          the same header overhead).
        """
        client = self.server.get_new_client()
        aligned_payload = b"Q" * (1024 * 1024)       # 1048576 — already 4096-aligned
        unaligned_payload = b"P" * (1024 * 1024 + 1)  # 1048577 — needs padding
        # Verify the relationship between payload sizes and disk-maxmemory cap.
        assert self.FILE_HEADER_SIZE + len(aligned_payload) == self.CAP
        assert self.FILE_HEADER_SIZE + self._align_up(len(unaligned_payload)) > self.CAP
        # Aligned case succeeds: disk_len = 4096 + 1048576 = 1052672 == cap.
        self._set_ok(client, "fitkey", aligned_payload)
        assert client.execute_command("DBSIZE") == 1
        wait_for_equal(self._dat_count, 1)
        # Clean up so the unaligned case has full cap available.
        client.execute_command("DEL", "fitkey")
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)
        # Unaligned case rejected: disk_len = 4096 + 1052672 = 1056768 > cap.
        # Both objects differ by 1 byte logically, but 4095 bytes on disk (padding).
        self._set_rejected(client, "padkey", unaligned_payload)
        assert client.execute_command("DBSIZE") == 0


# ─── Corruption Detection ─────────────────────────────────────────────────
# Each test triggers a server panic by corrupting on-disk data, verified via
# expect_crash. Separate class so the crash doesn't affect other tests.

class TestTieredCorruptionCrcMismatch(ValkeyLargeObjTestCaseBase):
    """CRC mismatch: server must crash on corrupt data, not serve it."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 0"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def test_tiered_file_header_crc_mismatch(self):
        """Corrupt CRC in .dat header → GET triggers panic."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('BLOB.TCP_SET', 'crc_key', payload)
        # Corrupt the CRC field in the FileHeader (bytes 21-24 in packed layout).
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected .dat file after SET"
        with open(dat_files[-1], 'r+b') as f:
            f.seek(21)
            f.write(b'\xff\xff\xff\xff')
        # GET should trigger panic (CRC mismatch in read_and_verify_file_header).
        with self.server.expect_crash(self):
            try:
                client.execute_command('BLOB.TCP_GET', 'crc_key')
            except Exception:
                pass


class TestTieredCorruptionMagic(ValkeyLargeObjTestCaseBase):
    """FileHeader magic corruption: server must crash on corrupt data."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 0"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def test_tiered_file_header_magic_corruption(self):
        """Corrupt magic bytes → GET triggers panic."""
        client = self.server.get_new_client()
        payload = b'Y' * 4096
        client.execute_command('BLOB.TCP_SET', 'magic_key', payload)
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected .dat file after SET"
        with open(dat_files[-1], 'r+b') as f:
            f.seek(0)
            f.write(b'BAAD')
        with self.server.expect_crash(self):
            try:
                client.execute_command('BLOB.TCP_GET', 'magic_key')
            except Exception:
                pass

class TestLargeObjSmartlog(ValkeyLargeObjTestCaseBase):
    """SMART log INFO section in Tiered mode (default poll interval)."""

    def test_smartlog_sections_present(self):
        """Tiered mode: the aggregated smartlog sections appear"""
        client = self.server.get_new_client()
        wait_for_true(
            lambda: 'largeobj_snapshot_age_seconds' in client.info('largeobj_smartlog_usage')
        )
        usage = client.info('largeobj_smartlog_usage')
        warnings = client.info('largeobj_smartlog_critical_warnings')

        # Every field is present on every host, including ones where no
        # controller could be read (CI usually lacks /dev/nvme* access).
        assert usage['largeobj_devices'] >= usage['largeobj_devices_read_failed']
        for field in ('data_units_read', 'data_units_written',
                      'percentage_used_avg', 'available_spare_pct_avg',
                      'media_errors', 'unsafe_shutdowns'):
            assert f'largeobj_{field}' in usage
        for field in ('spare_below_threshold', 'temperature_warning',
                      'reliability_degraded', 'media_read_only',
                      'volatile_mem_backup_failed', 'persistent_mem_read_only'):
            assert warnings[f'largeobj_{field}'] in (0, 1)


class TestLargeObjSmartlogDisabled(ValkeyLargeObjTestCaseBase):
    """smartlog-poll-secs 0: no poller, no INFO section, even in Tiered mode."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 1048576"
            f" segment-size 1048576"
            f" max-promote-size 520192"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
            f" smartlog-poll-secs 0"
            f" fabric-provider Emulated"
        )

    def test_smartlog_absent_when_disabled(self):
        """The poller never starts at 0, so absence is immediate and permanent
        (nothing to wait out). Module load succeeding with the arg already
        proves the config is registered and accepts 0."""
        client = self.server.get_new_client()
        assert 'largeobj_snapshot_age_seconds' not in client.info('largeobj_smartlog_usage')


# ─── Second-touch admission ─────────────────────────────────────────────────


class TestLargeObjTieredAdmission(ValkeyLargeObjTestCaseBase):
    """Default admission: promote-min-hits 2."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
        )

    def test_second_touch_promotes(self):
        """SET moves no cache counter. The first GET is a miss that admission
        rejects; the second promotes; the third is a hit. Objects above
        max-promote-size are misses that never count as admission rejects."""
        client = self.server.get_new_client()
        dram = lambda: client.info('largeobj_dram')
        payload = b'T' * 4096
        base = dram()

        client.execute_command('BLOB.TCP_SET', 'basic_key', payload)
        assert dram() == base

        # GET 1: rejected by admission, served transiently.
        assert client.execute_command('BLOB.TCP_GET', 'basic_key') == payload
        assert client.execute_command('BLOB.INFO', 'basic_key', 'TIER') == b'disk'
        after1 = dram()
        assert after1['largeobj_cache_misses'] == 1
        assert after1['largeobj_cache_admission_rejects'] == 1
        assert after1['largeobj_cache_promotions'] == 0
        assert after1['largeobj_dram_objects'] == 0

        # GET 2: second touch, promoted. mark_ready runs before the reply.
        assert client.execute_command('BLOB.TCP_GET', 'basic_key') == payload
        assert client.execute_command('BLOB.INFO', 'basic_key', 'TIER') == b'dram'
        after2 = dram()
        assert after2['largeobj_cache_misses'] == 2
        assert after2['largeobj_cache_admission_rejects'] == 1
        assert after2['largeobj_cache_promotions'] == 1
        assert after2['largeobj_dram_objects'] == 1
        assert after2['largeobj_cache_hits'] == 0

        # GET 3: plain hit, nothing on the miss side moves.
        assert client.execute_command('BLOB.TCP_GET', 'basic_key') == payload
        after3 = dram()
        assert after3['largeobj_cache_hits'] == 1
        assert after3['largeobj_cache_misses'] == 2
        assert after3['largeobj_cache_admission_rejects'] == 1
        assert after3['largeobj_cache_promotions'] == 1

        # Oversize: three misses, no rejects, no promotion.
        client.execute_command('CONFIG', 'SET', 'largeobj.max-promote-size', '4096')
        big = b'O' * 8192
        client.execute_command('BLOB.TCP_SET', 'big_key', big)
        for _ in range(3):
            assert client.execute_command('BLOB.TCP_GET', 'big_key') == big
        after4 = dram()
        assert after4['largeobj_cache_misses'] == 5
        assert after4['largeobj_cache_admission_rejects'] == 1
        assert after4['largeobj_cache_promotions'] == 1

    def test_promote_min_hits_runtime(self):
        """promote-min-hits is runtime mutable: 1 restores first-GET promotion,
        3 requires three misses."""
        client = self.server.get_new_client()
        payload = b'R' * 4096

        client.execute_command('CONFIG', 'SET', 'largeobj.promote-min-hits', '1')
        client.execute_command('BLOB.TCP_SET', 'one_key', payload)
        rejects = client.info('largeobj_dram')['largeobj_cache_admission_rejects']
        assert client.execute_command('BLOB.TCP_GET', 'one_key') == payload
        assert client.execute_command('BLOB.INFO', 'one_key', 'TIER') == b'dram'
        assert client.info('largeobj_dram')['largeobj_cache_admission_rejects'] == rejects

        client.execute_command('CONFIG', 'SET', 'largeobj.promote-min-hits', '3')
        client.execute_command('BLOB.TCP_SET', 'three_key', payload)
        for _ in range(2):
            assert client.execute_command('BLOB.TCP_GET', 'three_key') == payload
            assert client.execute_command('BLOB.INFO', 'three_key', 'TIER') == b'disk'
        assert client.execute_command('BLOB.TCP_GET', 'three_key') == payload
        assert client.execute_command('BLOB.INFO', 'three_key', 'TIER') == b'dram'


# ─── Inline reclaim ────────────────────────────────────────────────────────


class TestLargeObjTieredReclaim(ValkeyLargeObjTestCaseBase):
    """One 1 MiB segment, no room to grow (server maxmemory leaves less than a
    segment of headroom, see _block_expansion), so a promotion into a full pool
    must reclaim. promote-min-hits 1 so every GET
    promotes; decay off so scores are stable across a minute boundary. The
    default victim-sample-size (5) exceeds the 3-entry map, so victim selection
    scans every entry and is exact. 256 KiB objects: three fill 768 KiB; a
    fourth needs the remaining 256 KiB exactly, which any allocator overhead
    denies."""

    OBJ = 256 * 1024
    KEYS = [f'ev_{i}' for i in range(3)]

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 262144"
            f" promote-min-hits 1"
            f" blob-decay-time 0"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def _block_expansion(self, client):
        """Set server maxmemory so a new segment crosses the expand ceiling
        (used + 1 MiB >= watermark * maxmemory) while used stays below the
        shrink trigger, leaving 512 KiB of slack."""
        watermark = int(client.config_get('largeobj.scaling-shrink-watermark')
                        ['largeobj.scaling-shrink-watermark']) / 100
        used = int(client.info('memory')['used_memory'])
        client.config_set('maxmemory-policy', 'noeviction')
        client.config_set('maxmemory', int((used + 512 * 1024) / watermark))

    def test_reclaims_lowest_score_object(self):
        """A fourth promotion into the full pool reclaims exactly one cold object and
        never the hot one."""
        client = self.server.get_new_client()
        self._block_expansion(client)
        # SET and promote three objects that fill the single segment.
        for i, k in enumerate(self.KEYS):
            payload = bytes([65 + i]) * self.OBJ
            client.execute_command('BLOB.TCP_SET', k, payload)
            assert client.execute_command('BLOB.TCP_GET', k) == payload
        info = client.info('largeobj_dram')
        assert info['largeobj_dram_objects'] == 3
        assert info['largeobj_cache_demotions'] == 0
        assert info['largeobj_dram_segments'] == 1

        hot = self.KEYS[0]
        for _ in range(10):
            assert client.execute_command('BLOB.TCP_GET', hot) == b'A' * self.OBJ

        payload = b'N' * self.OBJ
        client.execute_command('BLOB.TCP_SET', 'ev_new', payload)
        assert client.execute_command('BLOB.TCP_GET', 'ev_new') == payload

        info = client.info('largeobj_dram')
        assert info['largeobj_cache_demotions'] == 1
        assert info['largeobj_dram_objects'] == 3
        assert info['largeobj_dram_segments'] == 1
        assert info['largeobj_dram_scaling_expands'] == 0
        assert client.execute_command('BLOB.INFO', hot, 'TIER') == b'dram'
        assert client.execute_command('BLOB.INFO', 'ev_new', 'TIER') == b'dram'
        tiers = [client.execute_command('BLOB.INFO', k, 'TIER') for k in self.KEYS[1:]]
        assert tiers.count(b'disk') == 1, tiers

        # The reclaimed copy is still on NVMe and reads back intact.
        reclaimed = self.KEYS[1:][tiers.index(b'disk')]
        idx = self.KEYS.index(reclaimed)
        assert client.execute_command('BLOB.TCP_GET', reclaimed) == bytes([65 + idx]) * self.OBJ


class TestLargeObjTieredReclaimOneSegment(ValkeyLargeObjTestCaseBase):
    """Reclaim frees room in one segment, the least loaded, and allocates there.
    Two 1 MiB segments: three 256 KiB objects fill segment 0, and the pool
    expands once for the fourth. Segment 1 then gets the coldest objects, so a
    reclaim that looked across the whole pool would take one of those. The
    target is segment 0 (768 KiB vs 832 KiB) and its 3 entries are fewer than
    the default victim-sample-size (5), so the victim choice there is exact.
    The expand watermark is raised so the scaling cron never adds a third
    segment (2 MiB, at most 78% full)."""

    OBJ = 256 * 1024
    SMALL = 64 * 1024

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 262144"
            f" promote-min-hits 1"
            f" blob-decay-time 0"
            f" scaling-expand-watermark 95"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def _promote(self, client, key, payload):
        client.execute_command('BLOB.TCP_SET', key, payload)
        assert client.execute_command('BLOB.TCP_GET', key) == payload
        assert client.execute_command('BLOB.INFO', key, 'TIER') == b'dram'

    def test_reclaims_only_in_target_segment(self):
        client = self.server.get_new_client()
        # Segment 0: A, B, C (768 KiB). No maxmemory yet, so D expands the pool.
        for k in ('A', 'B', 'C', 'D'):
            self._promote(client, k, k.encode() * self.OBJ)
        info = client.info('largeobj_dram')
        assert info['largeobj_dram_segments'] == 2
        assert info['largeobj_dram_scaling_expands'] == 1
        assert info['largeobj_cache_demotions'] == 0

        # No more growth. Segment 1 (D, 256 KiB) is the less loaded one, so E,
        # H and F all land there: 256 + 256 + 64 + 256 = 832 KiB.
        TestLargeObjTieredReclaim._block_expansion(self, client)
        self._promote(client, 'E', b'E' * self.OBJ)
        self._promote(client, 'H', b'H' * self.SMALL)
        self._promote(client, 'F', b'F' * self.OBJ)
        info = client.info('largeobj_dram')
        assert info['largeobj_dram_objects'] == 7
        assert info['largeobj_dram_segments'] == 2

        # One hit takes A and B from the initial LFU counter 5 to 6 (the first
        # increment is certain). C and everything in segment 1 stay at 5.
        for k in ('A', 'B'):
            assert client.execute_command('BLOB.TCP_GET', k) == k.encode() * self.OBJ

        # G fits nowhere. The target is segment 0 (least loaded); its coldest
        # entry is C, so exactly C goes, even though segment 1 is just as cold.
        self._promote(client, 'G', b'G' * self.OBJ)
        info = client.info('largeobj_dram')
        assert info['largeobj_cache_demotions'] == 1, "no over-reclaim"
        assert info['largeobj_dram_objects'] == 7
        assert info['largeobj_dram_segments'] == 2
        assert info['largeobj_dram_scaling_expands'] == 1
        assert client.execute_command('BLOB.INFO', 'C', 'TIER') == b'disk'
        for k in ('A', 'B', 'D', 'E', 'H', 'F'):
            assert client.execute_command('BLOB.INFO', k, 'TIER') == b'dram', k
        # The reclaimed copy still reads back from NVMe.
        assert client.execute_command('BLOB.TCP_GET', 'C') == b'C' * self.OBJ


class TestLargeObjTieredFdCap(ValkeyLargeObjTestCaseBase):
    """max-cached-fds 2 with promotion effectively off (promote-min-hits 255), so
    every GET reads through the fd pool. Decay off so scores are stable across
    a minute boundary. The default victim-sample-size (5) exceeds the 2-entry
    map, so victim selection scans both entries and is exact."""

    OBJ = 64 * 1024
    KEYS = [f'fd_{i}' for i in range(5)]

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" promote-min-hits 255"
            f" blob-decay-time 0"
            f" max-cached-fds 2"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
            f" fabric-provider Emulated"
        )

    def _payload(self, i):
        return bytes([65 + i]) * self.OBJ

    def test_cap_respected_and_reads_succeed(self):
        """Reading more objects than the cap keeps fd_cache_entries at the cap, reclaims
        exactly the overflow, and every read still returns the right bytes."""
        client = self.server.get_new_client()
        for i, k in enumerate(self.KEYS):
            client.execute_command('BLOB.TCP_SET', k, self._payload(i))
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 0

        for i, k in enumerate(self.KEYS):
            assert client.execute_command('BLOB.TCP_GET', k) == self._payload(i)

        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 2
        assert info['largeobj_fd_cache_evictions'] == 3
        # Nothing was promoted, so these were all NVMe reads.
        dram = client.info('largeobj_dram')
        assert dram['largeobj_dram_objects'] == 0

        # A second pass reads everything back correctly through reopened fds.
        for i, k in enumerate(self.KEYS):
            assert client.execute_command('BLOB.TCP_GET', k) == self._payload(i)
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 2
        evictions = info['largeobj_fd_cache_evictions']

        # DEL drops the pool's fd through ObjectFile::Drop, so the next open
        # takes the free slot instead of reclaiming. KEYS[4] was read last, so
        # its fd is one of the two cached.
        client.execute_command('DEL', self.KEYS[4])
        wait_for_equal(
            lambda: client.info('largeobj_fd')['largeobj_fd_cache_entries'], 1)
        assert client.execute_command('BLOB.TCP_GET', self.KEYS[0]) == self._payload(0)
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 2
        assert info['largeobj_fd_cache_evictions'] == evictions

    def test_reclaims_coldest_keeps_hot(self):
        """A full pool reclaims the lowest-scoring fd. KEYS[0] is read three
        times and KEYS[1] once, so opening KEYS[2] must reclaim KEYS[1], and a
        later read of KEYS[0] is a cache hit that reclaims nothing."""
        client = self.server.get_new_client()
        for i, k in enumerate(self.KEYS[:3]):
            client.execute_command('BLOB.TCP_SET', k, self._payload(i))

        # The first read opens the fd; the next two touch its score. The first
        # touch on a fresh entry always increments, so KEYS[0] outscores KEYS[1].
        for _ in range(3):
            assert client.execute_command('BLOB.TCP_GET', self.KEYS[0]) == self._payload(0)
        assert client.execute_command('BLOB.TCP_GET', self.KEYS[1]) == self._payload(1)
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 2
        assert info['largeobj_fd_cache_evictions'] == 0

        assert client.execute_command('BLOB.TCP_GET', self.KEYS[2]) == self._payload(2)
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 2
        assert info['largeobj_fd_cache_evictions'] == 1

        # KEYS[0] survived, so reading it again needs no open and no reclaim.
        assert client.execute_command('BLOB.TCP_GET', self.KEYS[0]) == self._payload(0)
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 2
        assert info['largeobj_fd_cache_evictions'] == 1

    def test_lower_cap_at_runtime(self):
        """CONFIG SET to a smaller cap, down to 0, evicts the cache right away."""
        client = self.server.get_new_client()
        for i, k in enumerate(self.KEYS[:2]):
            client.execute_command('BLOB.TCP_SET', k, self._payload(i))
            assert client.execute_command('BLOB.TCP_GET', k) == self._payload(i)
        assert client.info('largeobj_fd')['largeobj_fd_cache_entries'] == 2

        client.execute_command('CONFIG', 'SET', 'largeobj.max-cached-fds', '1')
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 1
        assert info['largeobj_fd_cache_evictions'] == 1

        client.execute_command('BLOB.TCP_SET', self.KEYS[2], self._payload(2))
        assert client.execute_command('BLOB.TCP_GET', self.KEYS[2]) == self._payload(2)
        info = client.info('largeobj_fd')
        assert info['largeobj_fd_cache_entries'] == 1
        assert info['largeobj_fd_cache_evictions'] == 2

        # 0 is accepted: the cache empties and later GETs open, read and close.
        client.execute_command('CONFIG', 'SET', 'largeobj.max-cached-fds', '0')
        assert client.info('largeobj_fd')['largeobj_fd_cache_entries'] == 0
        assert client.execute_command('BLOB.TCP_GET', self.KEYS[2]) == self._payload(2)
        assert client.info('largeobj_fd')['largeobj_fd_cache_entries'] == 0
        wait_for_true(lambda: client.info('largeobj_fd')['largeobj_open_object_fds'] == 0)


# ─── Object File fd Placement ───


class TestTieredFdPlacement(ValkeyLargeObjTestCaseBase):
    """Object-file fds must not take the fd numbers Valkey serves clients on.

    Valkey's event loop only covers fds below maxclients + CONFIG_FDSET_INCR (128):
    the core range. Linux hands out the lowest free fd. With maxclients 100 and 300
    cached fds left low, every new client socket lands past the event loop: it is
    accepted and counted, but its commands are never read. The module moves its fds
    into the module range, which starts at the end of the core range."""

    OBJECTS = 300
    NEW_CLIENTS = 90

    def get_server_args(self):
        return {'maxclients': '100'}

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 1048576"
            f" segment-size 1048576"
            f" max-promote-size 520192"
            f" promote-min-hits 255"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
        )

    def object_fds(self, client):
        """fd numbers the server holds open on object files, from /proc."""
        fd_dir = f"/proc/{client.info('server')['process_id']}/fd"
        return [int(fd) for fd in os.listdir(fd_dir)
                if os.readlink(f"{fd_dir}/{fd}").startswith(self.data_dir)]

    PAYLOAD = b'x' * 4096

    def fd_stat(self, client, name):
        """An INFO largeobj_fd field, such as 'fd_cache_entries'."""
        return client.info('largeobj_fd')[f'largeobj_{name}']

    def set_objects(self, client, count):
        for i in range(count):
            client.execute_command('BLOB.TCP_SET', f'obj_{i}', self.PAYLOAD)

    def get_objects(self, client, count):
        for i in range(count):
            assert client.execute_command('BLOB.TCP_GET', f'obj_{i}') == self.PAYLOAD

    def _start_with_nofile_limit(self, name, nofile, module_extra='', connect=True):
        """Start a server under `ulimit -n <nofile>` (soft = hard, like systemd's
        LimitNOFILE=<nofile>) with maxclients 100. Returns (server, client). With
        connect=False it does not wait for the server, for a load that must fail."""
        testdir = os.path.abspath(os.path.join(self.testdir, name))
        os.makedirs(testdir, exist_ok=True)
        wrapper = os.path.join(testdir, 'valkey-server')
        with open(wrapper, 'w') as f:
            f.write(f'#!/bin/sh\nulimit -n {nofile}\nexec {self.server_path} "$@"\n')
        os.chmod(wrapper, 0o755)
        module_args = self.get_module_args(os.path.join(testdir, 'nvme'), 'no')
        server, client = self.create_server(
            testdir=testdir,
            server_path=wrapper,
            args={'maxclients': '100',
                  'loadmodule': f"{os.environ['MODULE_PATH']} {module_args} {module_extra}"},
            connect_client=connect,
        )
        if connect:
            with open(f"/proc/{client.info('server')['process_id']}/limits") as f:
                line = next(line for line in f if line.startswith('Max open files'))
            assert line.split()[3:5] == [str(nofile), str(nofile)]  # soft, hard
        return server, client

    def test_new_clients_served_with_full_fd_cache(self):
        client = self.server.get_new_client()
        self.set_objects(client, self.OBJECTS)
        self.get_objects(client, self.OBJECTS)
        assert self.fd_stat(client, 'fd_cache_entries') == self.OBJECTS
        wait_for_true(
            lambda: self.fd_stat(client, 'open_object_fds') == self.OBJECTS)

        # A timeout so a client whose commands are never read fails the test
        # instead of hanging it.
        clients = [Valkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=2)
                   for _ in range(self.NEW_CLIENTS)]
        try:
            for c in clients:
                assert c.ping()
        finally:
            for c in clients:
                c.close()

    def test_config_set_maxclients_evicts_cached_fds_below_the_new_module_start(self):
        """CONFIG SET maxclients grows the event loop over the fds placed at load (228+).
        Growing 100 to 250 moves the module start to 378: only the cached fds in 228..377
        are evicted, those at 378+ stay. Growing to 500 moves it to 628: the rest are
        evicted, none stays in the core range, and the next GETs reopen them at 628+.
        Lowering maxclients again keeps the module start: clients above the lower value
        stay connected, so nothing is evicted and new opens still land at 628+."""
        client = self.server.get_new_client()
        self.set_objects(client, self.OBJECTS)
        self.get_objects(client, self.OBJECTS)
        before = self.object_fds(client)
        kept = sorted(fd for fd in before if fd >= 378)
        assert len(before) == self.OBJECTS
        assert min(before) >= 228
        assert kept and len(kept) < self.OBJECTS

        client.execute_command('CONFIG', 'SET', 'maxclients', '250')
        assert self.fd_stat(client, 'fd_cache_entries') == len(kept)
        assert sorted(self.object_fds(client)) == kept

        client.execute_command('CONFIG', 'SET', 'maxclients', '500')
        assert self.fd_stat(client, 'fd_cache_entries') == 0
        self.get_objects(client, self.OBJECTS)
        assert self.fd_stat(client, 'fd_cache_entries') == self.OBJECTS
        object_fds = self.object_fds(client)
        assert len(object_fds) == self.OBJECTS
        assert min(object_fds) >= 628

        client.execute_command('CONFIG', 'SET', 'maxclients', '100')
        assert self.fd_stat(client, 'fd_cache_entries') == self.OBJECTS
        client.execute_command('BLOB.TCP_SET', 'obj_new', self.PAYLOAD)
        assert client.execute_command('BLOB.TCP_GET', 'obj_new') == self.PAYLOAD
        assert self.fd_stat(client, 'fd_cache_entries') == self.OBJECTS + 1
        assert min(self.object_fds(client)) >= 628

    def test_max_cached_fds_above_what_the_fd_limit_allows_is_clamped(self):
        """Limit 1000 leaves 772 fds in the module range (from 228), and 100 of them are
        the command reserve (maxclients 100), so the fd cache can hold 672. A
        max-cached-fds above that is accepted and CONFIG GET shows it, but fd cache
        max entries stops at 672 and the log says why."""
        server, client = self._start_with_nofile_limit('max-entries', 1000)
        assert self.fd_stat(client, 'fd_cache_max_entries') == 672

        client.execute_command('CONFIG', 'SET', 'largeobj.max-cached-fds', '100')
        assert self.fd_stat(client, 'fd_cache_max_entries') == 100

        client.execute_command('CONFIG', 'SET', 'largeobj.max-cached-fds', '700')
        assert client.config_get('largeobj.max-cached-fds') == {
            'largeobj.max-cached-fds': '700'}
        assert self.fd_stat(client, 'fd_cache_max_entries') == 672
        assert server.verify_string_in_logfile('is too low for max-cached-fds 700')

    def test_config_set_maxclients_without_room_turns_placement_off_and_keeps_serving(self):
        """Limit 400 holds a cache of 72 fds in the module range (from 228). Raising
        maxclients to 200 moves the module start to 328 and the command reserve to 200,
        so the limit no longer covers the reserve (528). Valkey has already applied the
        change and a module cannot veto it, so fd-reserve Lenient turns placement off
        and empties the cache. The server keeps running and commands still work."""
        _, client = self._start_with_nofile_limit('maxclients-no-room', 400)
        self.set_objects(client, 10)
        self.get_objects(client, 10)
        assert self.fd_stat(client, 'fd_cache_entries') == 10
        assert self.fd_stat(client, 'fd_cache_max_entries') == 72

        client.execute_command('CONFIG', 'SET', 'maxclients', '200')

        assert client.ping()
        assert self.fd_stat(client, 'fd_placement_enabled') == 0
        assert self.fd_stat(client, 'fd_cache_entries') == 0
        assert self.fd_stat(client, 'fd_cache_max_entries') == 0
        self.get_objects(client, 10)

    def test_limit_lowered_after_load_fails_the_command_not_the_server(self):
        """An fd limit lowered after load (here below the module start) leaves no
        number to move an fd to. The command that needs it gets an error reply, the failure is
        counted, and the server keeps running."""
        client = self.server.get_new_client()
        pid = client.info('server')['process_id']
        soft, hard = resource.prlimit(pid, resource.RLIMIT_NOFILE)
        resource.prlimit(pid, resource.RLIMIT_NOFILE, (200, hard))  # module start is 228
        try:
            with pytest.raises(ResponseError):
                client.execute_command('BLOB.TCP_SET', 'no_room', self.PAYLOAD)
        finally:
            resource.prlimit(pid, resource.RLIMIT_NOFILE, (soft, hard))
        assert client.ping()
        assert self.fd_stat(client, 'fd_placement_failures') >= 1
        wait_for_true(lambda: self.fd_stat(client, 'open_object_fds') == 0)
        client.execute_command('BLOB.TCP_SET', 'has_room', self.PAYLOAD)
        assert client.execute_command('BLOB.TCP_GET', 'has_room') == self.PAYLOAD

    @pytest.mark.parametrize('nofile', [132, 300])
    def test_lenient_loads_with_placement_and_cache_off_below_command_reserve(self, nofile):
        """fd-reserve Lenient loads when the fd limit is below module start 228 + maxclients
        100 = 328, with placement and the fd cache off: a cached fd would hold a client's
        fd number. 132 is Valkey's stock limit (maxclients + 32), inside the core range, so
        no fd number past it exists. 300 is past the module start but short of the reserve.
        Commands still work."""
        server, client = self._start_with_nofile_limit(f'lenient-off-{nofile}', nofile)
        assert server.verify_string_in_logfile('fd placement and the fd cache are off')
        assert server.verify_string_in_logfile('set LimitNOFILE to at least 328')
        assert self.fd_stat(client, 'fd_placement_enabled') == 0

        self.set_objects(client, 10)
        self.get_objects(client, 10)
        assert self.fd_stat(client, 'fd_cache_entries') == 0
        assert self.fd_stat(client, 'fd_cache_max_entries') == 0

    # A refused load makes Valkey drop its reference to the config data the valkey-module
    # crate boxed for each config, and nothing frees it, so LeakSanitizer reports it at exit.
    # That is not a leak in this module's code, so the ASAN job skips this test.
    @pytest.mark.skip_for_asan
    @pytest.mark.parametrize('nofile,max_cached_fds,needed', [
        (300, 0, 328),    # the command reserve alone does not fit: 228 + 100
        (400, 100, 428),  # the reserve fits, the cache does not: 228 + 100 + 100
    ])
    def test_strict_refuses_to_load_when_the_reserve_or_cache_does_not_fit(
            self, nofile, max_cached_fds, needed):
        """fd-reserve Strict needs module start 228 + maxclients 100 + max-cached-fds. A
        limit below that does not load, and the log names the limit to set."""
        server, _ = self._start_with_nofile_limit(
            f'strict-refuses-{nofile}', nofile,
            f'fd-reserve Strict max-cached-fds {max_cached_fds}', connect=False)
        wait_for_true(lambda: server.server.poll() is not None)
        assert server.server.returncode != 0
        assert server.verify_string_in_logfile(f'set LimitNOFILE to at least {needed}')

    # The abort runs Valkey's crash-report memtest, which overflows its own stack array
    # under ASAN. That is a Valkey bug, not this module's, so the ASAN job skips this test.
    @pytest.mark.skip_for_asan
    @pytest.mark.parametrize('nofile,max_cached_fds,new_maxclients,needed', [
        (400, 0, 200, 528),    # the reserve breaks: module start 328 + 200
        (500, 100, 140, 508),  # the reserve (408) fits, the cache does not: 268 + 140 + 100
    ])
    def test_strict_aborts_when_maxclients_breaks_the_reserve_or_cache(
            self, nofile, max_cached_fds, new_maxclients, needed):
        """Under fd-reserve Strict the server loads, says how far maxclients may rise
        (136 for both), and serves. A CONFIG SET maxclients Valkey accepts but the fd
        limit cannot hold beside the reserve and the cache aborts the server."""
        server, client = self._start_with_nofile_limit(
            f'strict-aborts-{nofile}', nofile,
            f'fd-reserve Strict max-cached-fds {max_cached_fds}')
        assert server.verify_string_in_logfile('above 136 aborts the server')
        self.set_objects(client, 1)
        self.get_objects(client, 1)

        with server.expect_crash(self):
            client.execute_command('CONFIG', 'SET', 'maxclients', str(new_maxclients))
        assert server.verify_string_in_logfile(f'set LimitNOFILE to at least {needed}')

    def test_strict_rejects_max_cached_fds_that_does_not_fit(self):
        """Limit 500 fits max-cached-fds 172 (228 + 100 + 172), not 173. Under
        fd-reserve Strict CONFIG SET refuses 173, names the limit that would fit, and
        keeps the old value."""
        _, client = self._start_with_nofile_limit(
            'strict-rejects', 500, 'fd-reserve Strict max-cached-fds 100')
        with pytest.raises(ResponseError, match='set LimitNOFILE to at least 501'):
            client.execute_command('CONFIG', 'SET', 'largeobj.max-cached-fds', '173')
        assert client.config_get('largeobj.max-cached-fds') == {
            'largeobj.max-cached-fds': '100'}

        client.execute_command('CONFIG', 'SET', 'largeobj.max-cached-fds', '172')
        assert self.fd_stat(client, 'fd_cache_max_entries') == 172

    def test_loads_with_max_cached_fds_larger_than_the_fd_limit_allows(self):
        """Limit 400 leaves 172 fds in the module range (from 228), and 100 of them are
        the command reserve (maxclients 100), so the fd cache can hold 72. The default
        max-cached-fds (16384) asks for more: the server still loads, logs why, and
        caches 72."""
        server, client = self._start_with_nofile_limit('small-limit', 400)
        assert server.verify_string_in_logfile('is too low for max-cached-fds')
        assert self.fd_stat(client, 'fd_cache_max_entries') == 72
        assert self.fd_stat(client, 'fd_placement_enabled') == 1

        self.set_objects(client, 80)
        self.get_objects(client, 80)
        assert self.fd_stat(client, 'fd_cache_entries') == 72


# ─── NVMe Staging Queue ──────────────────────────────────────────────────────

# Payload size for staging queue tests: 512 KiB uses 2 × 256 KiB staging buffers.
STAGING_PAYLOAD_LEN = 524288
STAGING_PATTERN = bytes(i % 256 for i in range(STAGING_PAYLOAD_LEN))


class TestStagingQueueRdma(ValkeyLargeObjTestCaseBase):
    """NVMe staging queue via RDMA: queueing, depth metrics, and QueueFull rejection.

    RDMA/EFA requests use async alloc_window_cancellable which queues when staging
    is full. TCP uses sync alloc_window which fails immediately (tested separately).

    Config: 1 MiB staging pool (1 segment), chunk-size 256 KiB, max/min-buffers 2.
    Each SET of 512 KiB holds 2 × 256 KiB = 512 KiB of staging, so 2 SETs fill the pool.
    test-pause-before-finalize-set-ms 3000 holds staging buffers for 3s after
    the NVMe write completes but before commit, creating a deterministic window
    where additional RDMA requests must queue for buffers.
    disk-staging-queue-capacity 2: the 3rd and 4th RDMA requests queue; the 5th is
    rejected with ERR too many pending disk requests."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 1048576"
            f" segment-size 1048576"
            f" max-promote-size 0"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 262144"
            f" max-buffers-per-op 2"
            f" min-buffers-per-op 2"
            f" disk-staging-queue-capacity 2"
            f" test-pause-before-finalize-set-ms 3000"
            f" fabric-provider Emulated"
            f" fabric-interfaces lo"
        )

    def start_target(self, *flags, split=None):
        """Launch the passive peer and return it with the regions it advertised."""
        target = os.path.join(os.path.dirname(os.environ['MODULE_PATH']), 'fabric_target')
        command = [target, '127.0.0.1', '--read', *flags]
        if split is not None:
            total_size = sum(split)
            command.append(f'--size={total_size}')
            command.append('--split=' + ','.join(str(size) for size in split))
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        regions = []
        for _ in range(1 if split is None else len(split)):
            line = process.stdout.readline()
            assert line.startswith('advertisement: '), line
            address, rkey, remote_addr, length = line.split()[1:]
            regions.append(Region(address, int(rkey), int(remote_addr), int(length)))
        return process, regions

    def _background_rdma_set(self, client, key, regions):
        """Launch an RDMA SET in a background thread, return (thread, result, error)."""
        result = [None]
        error = [None]

        def run():
            try:
                result[0] = client.execute_command(
                    'BLOB.RDMA_SET', key, STAGING_PAYLOAD_LEN, *address_args(regions))
            except Exception as e:
                error[0] = e

        t = threading.Thread(target=run)
        t.start()
        return t, result, error

    def test_rdma_staging_queue_depth_under_pressure(self):
        """Two RDMA SETs fill the staging pool; a third queues for buffers.
        Assert queue depth == 1 while all three are in flight, then drains to 0."""
        # Start 3 fabric targets (one per RDMA SET).
        targets = []
        all_regions = []
        for _ in range(3):
            process, regions = self.start_target(split=[STAGING_PAYLOAD_LEN])
            targets.append(process)
            all_regions.append(regions)

        try:
            # Each SET needs its own client with its own RDMA session.
            clients = [self.server.get_new_client() for _ in range(3)]
            observer = self.server.get_new_client()

            # Establish RDMA sessions.
            for i, (client, regions) in enumerate(zip(clients, all_regions)):
                client.execute_command('BLOB.RDMA_HELLO', regions[0].address)

            # Launch 2 RDMA SETs to fill the staging pool.
            threads, results, errors = [], [], []
            for i in range(2):
                t, r, e = self._background_rdma_set(clients[i], f'q{i}', all_regions[i])
                threads.append(t)
                results.append(r)
                errors.append(e)
            # Wait for both to reach the pause window (buffers held).
            time.sleep(1.0)

            # Launch a 3rd RDMA SET — staging is full, so it queues.
            t3, r3, e3 = self._background_rdma_set(clients[2], 'q2', all_regions[2])
            threads.append(t3)
            results.append(r3)
            errors.append(e3)
            time.sleep(0.5)

            # While the first two hold buffers and the third is queued:
            info = observer.info('largeobj_disk_staging')
            assert info['largeobj_disk_staging_queue_depth'] == 1

            # Wait for all to complete (pause expires, buffers freed, queued request proceeds).
            for t in threads:
                t.join(timeout=15)
            assert not any(t.is_alive() for t in threads), "RDMA SET threads did not finish"
            for i, (r, e) in enumerate(zip(results, errors)):
                assert e[0] is None, f"RDMA SET q{i} raised: {e[0]}"
                assert r[0] == b'OK', f"RDMA SET q{i} returned {r[0]}"

            # Queue should have drained.
            wait_for_equal(
                lambda: observer.info('largeobj_disk_staging')['largeobj_disk_staging_queue_depth'], 0
            )

            # Waiting is not an error — the queue_full counter must not have incremented.
            assert observer.info('largeobj_error_metrics')['largeobj_disk_staging_queue_full'] == 0

            # All objects readable.
            for i in range(3):
                assert observer.execute_command('BLOB.TCP_GET', f'q{i}') == STAGING_PATTERN
        finally:
            for p in targets:
                p.kill()

    def test_rdma_staging_queue_full_rejection(self):
        """With queue capacity 2, the 5th concurrent RDMA SET is rejected.
        Two SETs hold staging buffers (paused), two queue, the fifth overflows."""
        # Start 5 fabric targets.
        targets = []
        all_regions = []
        for _ in range(5):
            process, regions = self.start_target(split=[STAGING_PAYLOAD_LEN])
            targets.append(process)
            all_regions.append(regions)

        try:
            # Each SET needs its own client with its own RDMA session.
            clients = [self.server.get_new_client() for _ in range(5)]
            observer = self.server.get_new_client()

            # Establish RDMA sessions.
            for client, regions in zip(clients, all_regions):
                client.execute_command('BLOB.RDMA_HELLO', regions[0].address)

            # Fill staging pool (2 RDMA SETs holding buffers) + fill queue (2 more waiting).
            threads, results, errors = [], [], []
            for i in range(2):
                t, r, e = self._background_rdma_set(clients[i], f'fill{i}', all_regions[i])
                threads.append(t)
                results.append(r)
                errors.append(e)
            time.sleep(1.0)
            for i in range(2, 4):
                t, r, e = self._background_rdma_set(clients[i], f'fill{i}', all_regions[i])
                threads.append(t)
                results.append(r)
                errors.append(e)
            time.sleep(0.5)

            # The 5th RDMA SET should be rejected — queue is at capacity.
            with pytest.raises(ResponseError, match='too many pending disk requests'):
                clients[4].execute_command(
                    'BLOB.RDMA_SET', 'overflow', STAGING_PAYLOAD_LEN, *address_args(all_regions[4]))

            # Verify the error counter incremented.
            info = observer.info('largeobj_error_metrics')
            assert info['largeobj_disk_staging_queue_full'] == 1

            # Let everything drain.
            for t in threads:
                t.join(timeout=15)
            assert not any(t.is_alive() for t in threads), "RDMA SET threads did not finish"
            for i, (r, e) in enumerate(zip(results, errors)):
                assert e[0] is None, f"RDMA SET fill{i} raised: {e[0]}"
                assert r[0] == b'OK', f"RDMA SET fill{i} returned {r[0]}"
            # The first 4 should succeed (2 immediate + 2 queued).
        finally:
            for p in targets:
                p.kill()


class TestStagingDisconnectCancels(ValkeyLargeObjTestCaseBase):
    """GATE TEST: a client disconnect cancels its queued staging waiter. Valkey
    fires the module client-disconnect event even for a client blocked inside the
    module, so the disconnect handler cancels the queued request. The request
    leaves the queue without allocating, never wedging the FIFO behind it.

    Config: 1 MiB staging (1 segment), 512 KiB objects hold the full pool.
    test-pause-before-finalize-set-ms 3000 keeps the holder's buffers locked.
    The pause is immutable so ALL SETs pause, but the contender is disconnected
    while still queued (before it gets buffers), so it never reaches the pause."""

    OBJ = 524288  # 512 KiB

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 1048576"
            f" segment-size 1048576"
            f" max-promote-size 0"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 262144"
            f" max-buffers-per-op 2"
            f" min-buffers-per-op 2"
            f" disk-staging-queue-capacity 1024"
            f" test-pause-before-finalize-set-ms 3000"
            f" fabric-provider Emulated"
        )

    def test_disconnect_cancels_queued_waiter(self):
        """A hard-disconnect while a SET is queued for staging buffers must
        cancel the queued request, leave the FIFO unwedged, and never commit
        the cancelled key."""
        client = self.server.get_new_client()
        contender = self.server.get_new_client()

        # Fill the staging pool with two SETs that pause holding buffers.
        holders = []
        for i in range(2):
            h = self.server.get_new_client()
            t = threading.Thread(
                target=h.execute_command,
                args=('BLOB.TCP_SET', f'holder{i}', b'H' * self.OBJ)
            )
            t.start()
            holders.append((h, t))
        time.sleep(1.0)  # both holders have their windows and are paused

        # The contender's SET queues behind the full pool.
        err = [None]
        def contender_set():
            try:
                contender.execute_command('BLOB.TCP_SET', 'contender', b'C' * self.OBJ)
            except Exception as e:
                err[0] = e
        ct = threading.Thread(target=contender_set)
        ct.start()
        time.sleep(0.5)  # contender is now queued

        # Hard-disconnect the contender while it is blocked in the module.
        contender.connection_pool.disconnect()
        ct.join(timeout=10)
        assert not ct.is_alive(), "contender thread did not finish after disconnect"

        # The holders' SETs still complete normally (they pause 3s then commit).
        for _, t in holders:
            t.join(timeout=10)
            assert not t.is_alive(), "holder SET did not finish"
        for i in range(2):
            assert client.execute_command('BLOB.TCP_GET', f'holder{i}') == b'H' * self.OBJ

        # The cancelled waiter never committed its key.
        assert client.execute_command('EXISTS', 'contender') == 0

        # The FIFO is not wedged: a fresh SET on a healthy client succeeds.
        # It will also pause (immutable config), so allow time for it.
        fresh = self.server.get_new_client()
        t_fresh = threading.Thread(
            target=lambda: fresh.execute_command('BLOB.TCP_SET', 'after', b'A' * self.OBJ)
        )
        t_fresh.start()
        t_fresh.join(timeout=10)
        assert not t_fresh.is_alive(), "fresh SET did not finish"
        assert client.execute_command('BLOB.TCP_GET', 'after') == b'A' * self.OBJ


class TestTieredPromotionFailureRace(ValkeyLargeObjTestCaseBase):
    """Exercises the race where a GET arrives between set_promotion_failure and
    remove_object.  Without the else-if guard in serve_filling_or_became_ready,
    the late GET would try to serve half-filled DRAM buffers.

    Hooks:
        test-fail-promotion-read          — inject NvmeRead error on first promotion
        test-pause-after-promotion-failure-ms — widen the window between
            set_promotion_failure and remove_object so the second GET lands
            in the race window deterministically
        test-pause-during-promotion-ms    — hold the leader long enough for the
            second GET to find the Filling entry (before the failure fires)
    """

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 4194304"
            f" segment-size 4194304"
            f" promote-min-hits 1"
            f" max-promote-size 2093056"
            f" max-object-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
            f" test-fail-promotion-read yes"
            f" test-pause-during-promotion-ms 1000"
            f" test-pause-after-promotion-failure-ms 2000"
        )

    def test_late_get_during_promotion_failure_gets_error(self):
        """A GET that arrives after set_promotion_failure but before
        remove_object must receive an error, not corrupt data.

        Timeline:
            GET-A (leader): insert Filling → [PAUSE 1s] → NVMe read fails →
                set_promotion_failure → [PAUSE 2s] → remove_object
            GET-B (late):   arrives during the 2s pause → finds Filling entry →
                subscribe() returns None → else-if returns disk read error
        """
        client = self.server.get_new_client()
        payload = os.urandom(32768)
        client.execute_command('BLOB.TCP_SET', 'race_key', payload)

        errors_before = int(
            client.info('largeobj_error_metrics').get('largeobj_disk_read_errors', 0)
        )

        results = [None, None]
        caught = [None, None]

        def get_a():
            """Leader GET — triggers promotion, hits injected failure."""
            try:
                c = self.server.get_new_client()
                results[0] = c.execute_command('BLOB.TCP_GET', 'race_key')
            except ResponseError as e:
                caught[0] = e

        def get_b():
            """Late GET — arrives during the pause after promotion failure.
            Wait long enough for the leader to enter the failure pause window
            (1s promotion pause + a small margin), but before remove_object
            runs (failure pause is 2s)."""
            time.sleep(1.5)
            try:
                c = self.server.get_new_client()
                results[1] = c.execute_command('BLOB.TCP_GET', 'race_key')
            except ResponseError as e:
                caught[1] = e

        ta = threading.Thread(target=get_a)
        tb = threading.Thread(target=get_b)
        ta.start()
        tb.start()
        ta.join(timeout=10)
        tb.join(timeout=10)

        # GET-A (leader) must get an error — the promotion read was injected to fail.
        assert caught[0] is not None, (
            f"GET-A should have received a disk read error, got: {results[0]}"
        )
        assert 'disk read failed' in str(caught[0]).lower(), (
            f"GET-A unexpected error: {caught[0]}"
        )

        # GET-B (late subscriber) must also get an error — not corrupt DRAM data.
        assert caught[1] is not None, (
            f"GET-B should have received a disk read error, got: {results[1]}"
        )
        assert 'disk read failed' in str(caught[1]).lower(), (
            f"GET-B unexpected error: {caught[1]}"
        )

        # disk_read_errors metric should have incremented for both GETs.
        errors_after = int(
            client.info('largeobj_error_metrics').get('largeobj_disk_read_errors', 0)
        )
        assert errors_after >= errors_before + 2, (
            f"Expected disk_read_errors to increment by at least 2, "
            f"got {errors_after - errors_before} "
            f"(before={errors_before} after={errors_after})"
        )
