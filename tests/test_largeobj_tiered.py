import os
import glob
import time
import threading
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal, wait_for_true


class TestLargeObjTieredPromotion(ValkeyLargeObjTestCaseBase):
    """Tiered mode with DRAMPool promotion enabled (default max-promote-size).
    promote-min-hits 1 so the first GET promotes and these tests observe promotion
    directly; second-touch admission has its own classes below."""

    def get_module_args(self, data_dir, direct_io):
        # max-promote-size must fit in one segment after talc overhead.
        # With seg=4M and chunk=4K the max is 2093056 (~2044 KiB).
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 4194304"
            f" promote-min-hits 1"
            f" max-promote-size 2093056"
            f" max-object-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
        )

    def test_set_creates_nvme_file(self):
        """In Tiered mode, BLOB.SET persists to NVMe."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('BLOB.SET', 'tiered_key', payload)
        dat_files = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files) >= 1, "Tiered SET should create an NVMe .dat file"

    def test_keyspace_events(self):
        """Async NVMe commit publishes largeobj.create then largeobj.update."""
        client = self.server.get_new_client()
        pubsub = self.subscribe_keyspace_events(client)
        client.execute_command('BLOB.SET', 'eventkey', b'A' * 4096)
        client.execute_command('BLOB.SET', 'eventkey', b'B' * 8192)
        assert self.read_keyspace_events(pubsub, 2) == [
            ('largeobj.create', 'eventkey'),
            ('largeobj.update', 'eventkey'),
        ]

    def _dat_count(self):
        return len(glob.glob(os.path.join(self.data_dir, "*.dat")))

    def _wait_free_settled(self, client):
        """Wait for Valkey lazyfree to drain AND teardown to unlink the file."""
        wait_for_equal(
            lambda: client.info("stats").get("lazyfree_pending_objects", 0), 0
        )

    def test_get_after_set_roundtrip(self):
        """Tiered mode: SET then GET returns correct data, including multi-chunk objects.
        Validates FileHeader presence and data offset in the .dat file."""
        client = self.server.get_new_client()
        # chunk-size is 4096 in this class.
        # Single chunk: 4096 bytes.
        payload_single = b'A' * 4096
        client.execute_command('BLOB.SET', 'rt_single', payload_single)
        assert client.execute_command('BLOB.GET', 'rt_single') == payload_single
        # Multi-chunk with partial last chunk: 8192 + 1 = 8193 → 3 chunks.
        payload_multi = b'M' * 8193
        client.execute_command('BLOB.SET', 'rt_multi', payload_multi)
        # GET via serve-and-discard (first GET triggers promotion, verify data).
        result = client.execute_command('BLOB.GET', 'rt_multi')
        assert result == payload_multi
        # Second GET from DRAM after promotion.
        assert client.execute_command('BLOB.GET', 'rt_multi') == payload_multi
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
        client.execute_command('BLOB.SET', 'promo_key', payload)

        # Freshly SET: on NVMe only, nothing cached yet.
        assert client.execute_command('BLOB.INFO', 'promo_key', 'TIER') == b'nvme'

        # First GET: DRAMPool miss -> NVMe read -> promote to DRAMPool.
        result1 = client.execute_command('BLOB.GET', 'promo_key')
        assert result1 == payload
        assert client.execute_command('BLOB.INFO', 'promo_key', 'TIER') == b'dram'

        # Second GET: served from DRAMPool (promotion happened).
        result2 = client.execute_command('BLOB.GET', 'promo_key')
        assert result2 == payload
        assert client.execute_command('BLOB.INFO', 'promo_key', 'TIER') == b'dram'

    def test_delete_removes_nvme_file(self):
        """DEL removes the NVMe file."""
        client = self.server.get_new_client()
        payload = b'D' * 4096
        client.execute_command('BLOB.SET', 'del_key', payload)
        dat_files_before = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_before) >= 1
        client.execute_command('DEL', 'del_key')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        dat_files_after = glob.glob(os.path.join(self.data_dir, '*.dat'))
        assert len(dat_files_after) < len(dat_files_before)

    # ─── COPY callback tests ─────────────────────────────────────────────

    def test_copy(self):
        """COPY in Tiered mode: independent NVMe file, digest differs, delete independence."""
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('BLOB.SET', 'srckey', payload)
        # COPY creates an independent object with its own NVMe file
        result = client.execute_command('COPY', 'srckey', 'dstkey')
        assert result == 1 or result is True
        assert client.execute_command('BLOB.GET', 'srckey') == payload
        assert client.execute_command('BLOB.GET', 'dstkey') == payload
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
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('BLOB.GET', 'dstkey') == payload
        # Deleting copy does not affect the source
        client.execute_command('BLOB.SET', 'srckey2', payload)
        client.execute_command('COPY', 'srckey2', 'dstkey2')
        client.execute_command('DEL', 'dstkey2')
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        assert client.execute_command('BLOB.GET', 'srckey2') == payload
        #  The copy starts un-promoted even if the source is cached
        client.execute_command('BLOB.GET', 'srckey2')  # promote source
        assert client.execute_command('BLOB.INFO', 'srckey2', 'TIER') == b'dram'
        client.execute_command('COPY', 'srckey2', 'dstkey3')
        assert client.execute_command('BLOB.INFO', 'dstkey3', 'TIER') == b'nvme'

    # ─── MEMORY USAGE callback tests ──────────────────────────────────────

    def test_memory_usage(self):
        """MEMORY USAGE after promotion returns a non-zero value for the key."""
        client = self.server.get_new_client()
        payload_size = 4096
        client.execute_command('BLOB.SET', 'memkey', b'M' * payload_size)
        # Single GET promotes into DRAMPool (promote-on-first-GET policy).
        client.execute_command('BLOB.GET', 'memkey')
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        assert mem > 0, f"Expected non-zero MEMORY USAGE after promotion, got {mem}"

    # ─── DEBUG DIGEST callback tests ──────────────────────────────────────

    def test_debug_digest(self):
        """DEBUG DIGEST-VALUE is deterministic; nonexistent key returns nil digest."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.SET', 'digkey', b'G' * 4096)
        d1 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        d2 = client.execute_command('DEBUG', 'DIGEST-VALUE', 'digkey')
        assert d1 == d2
        # Nonexistent key returns nil digest
        nil_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'noexist')
        assert nil_digest == [b'0' * 40]

    # ─── Streaming tests ─────────────────────────────────────────────────
    # CRC integrity, delete-during-SET, nvme-maxmemory.
    # These use the same config as the promotion tests above.

    def test_tiered_delete_during_set(self):
        """Deterministic delete-during-SET: the test-pause hook freezes the
        SET's tokio task after NVMe data writes complete but before set_finalize
        commits the key. While the SET is paused we fire a DEL from a second
        client, guaranteeing the interleaving:
            Thread A (tokio):  write chunks → [PAUSE] → set_finalize (commits)
            Thread B (main) :                  DEL key (removes v1)
        The DEL removes the existing v1. The paused SET then resumes: set_finalize
        finds no existing key (DEL cleared it), so it commits payload2 as a fresh
        key. No crash, no corruption, and GET returns payload2."""
        client = self.server.get_new_client()
        del_client = self.server.get_new_client()
        payload = b'D' * 32768
        client.execute_command('BLOB.SET', 'delset_key', payload)
        assert client.execute_command('BLOB.GET', 'delset_key') == payload
        # Enable the test hook: pause tiered SET for 2s after writing chunks.
        client.execute_command(
            'CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '2000'
        )
        payload2 = b'E' * 32768
        set_result = [None]
        set_error = [None]
        def background_set():
            try:
                # This SET blocks for ~2s (paused after NVMe write, before commit).
                set_result[0] = client.execute_command(
                    'BLOB.SET', 'delset_key', payload2
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
        # Disable the hook.
        client.execute_command(
            'CONFIG', 'SET', 'largeobj.test-pause-before-finalize-set-ms', '0'
        )
        # The paused SET's set_finalize sees no existing key (DEL removed v1)
        # and commits payload2 as a fresh key.
        wait_for_equal(
            lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0
        )
        assert client.execute_command('BLOB.GET', 'delset_key') == payload2

    def test_zero_length_object_rejected(self):
        """BLOB.SET with zero-length payload is rejected."""
        client = self.server.get_new_client()
        try:
            client.execute_command('BLOB.SET', 'empty_key', b'')
            assert False, "Expected error for zero-length object"
        except ResponseError as e:
            assert 'object length must be > 0' in str(e).lower(), f"Unexpected: {e}"

    def test_nvme_maxmemory_exhaustion(self):
        """SET that would exceed nvme-maxmemory is rejected."""
        client = self.server.get_new_client()
        # nvme-maxmemory minimum is 1MB. Set to 1MB.
        client.execute_command('CONFIG', 'SET', 'largeobj.nvme-maxmemory', '1048576')
        # Each 256KB object has disk_len = 4096 (header) + 256KB = 266240 bytes.
        # Three fit (798720 < 1MB), fourth exceeds (1064960 > 1MB).
        payload = b'A' * (256 * 1024)
        for i in range(3):
            result = client.execute_command('BLOB.SET', f'nvme_cap_{i}', payload)
            assert result == b'OK'
        # Fourth SET should fail (would exceed 1MB).
        try:
            client.execute_command('BLOB.SET', 'nvme_cap_3', payload)
            assert False, "Expected capacity exceeded error from nvme-maxmemory"
        except ResponseError as e:
            assert 'nvme disk capacity exceeded' in str(e).lower(), f"Unexpected error: {e}"

    # ─── DEL semantics ────────────────────────────────────────────────────

    def test_delete_semantics(self):
        """DEL resolves a subsequent GET to nil and unlinks the .dat file -- both
        for a cold object and one already promoted into DRAMPool (a distinct free
        path: the DRAM cache entry is dropped alongside the Arc<ObjectFile>)."""
        client = self.server.get_new_client()
        payload = b"D" * 4096
        assert client.execute_command("DBSIZE") == 0

        # Cold object: GET after DEL is nil; DEL drops the key from the keyspace.
        client.execute_command("BLOB.SET", "gk", payload)
        assert client.execute_command("DBSIZE") == 1
        assert client.execute_command("BLOB.GET", "gk") == payload
        client.execute_command("DEL", "gk")
        assert client.execute_command("DBSIZE") == 0
        assert client.execute_command("BLOB.GET", "gk") is None

        # Promoted object: DEL still resolves to nil and the NVMe file is unlinked.
        client.execute_command("BLOB.SET", "pk", payload)
        assert client.execute_command("BLOB.GET", "pk") == payload  # first GET promotes
        client.execute_command("DEL", "pk")
        assert client.execute_command("DBSIZE") == 0
        assert client.execute_command("BLOB.GET", "pk") is None
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
        client.execute_command("BLOB.SET", "ok", v1)
        assert client.execute_command("DBSIZE") == 1
        wait_for_equal(self._dat_count, 1)
        client.execute_command("BLOB.SET", "ok", v2)
        assert client.execute_command("DBSIZE") == 1  # overwrite reuses the key
        assert client.execute_command("BLOB.GET", "ok") == v2
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 1)

        # Overwrite after promotion: the new GET must serve the new payload.
        client.execute_command("BLOB.SET", "opk", b"A" * 4096)
        assert client.execute_command("BLOB.GET", "opk") == b"A" * 4096  # promote v1
        client.execute_command("BLOB.SET", "opk", b"B" * 4096)
        assert client.execute_command("BLOB.GET", "opk") == b"B" * 4096
        assert client.execute_command("BLOB.GET", "opk") == b"B" * 4096

        # Repeated overwrite of one key never leaks files.
        for i in range(10):
            client.execute_command("BLOB.SET", "leakkey", bytes([65 + (i % 26)]) * 4096)
        assert client.execute_command("BLOB.GET", "leakkey") is not None
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
            client.execute_command("BLOB.SET", "churn", payload)
            assert client.execute_command("DBSIZE") == 1
            assert client.execute_command("BLOB.GET", "churn") == payload
            client.execute_command("DEL", "churn")
            assert client.execute_command("DBSIZE") == 0
            assert client.execute_command("BLOB.GET", "churn") is None
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

    # ─── Other free triggers: expiry & flush ──────────────────────────────

    def test_expiry_and_flushall_unlink_files(self):
        """Expiry (TTL) and FLUSHALL are free paths distinct from DEL/overwrite;
        both must unlink the .dat file(s)."""
        client = self.server.get_new_client()

        # Passive expiry: polling EXISTS drives expiry, then teardown unlinks.
        client.execute_command("BLOB.SET", "exk", b"E" * 4096)
        wait_for_equal(self._dat_count, 1)
        client.execute_command("PEXPIRE", "exk", 50)
        wait_for_equal(lambda: client.execute_command("EXISTS", "exk"), 0)
        assert client.execute_command("DBSIZE") == 0
        self._wait_free_settled(client)
        wait_for_equal(self._dat_count, 0)

        # FLUSHALL unlinks every remaining file.
        for i in range(3):
            client.execute_command("BLOB.SET", f"fk{i}", b"F" * 4096)
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
            client.execute_command('BLOB.SET', 'bigkey', b'X' * (limit + 1))
            assert False, "Expected max object size rejection"
        except ResponseError as e:
            err = str(e).lower()
            assert 'max object size' in err, f"Unexpected error: {e}"
            assert 'nvme' not in err, f"Should not hit NVMe error: {e}"
        # Rejected SET must not leave a .dat file or phantom key.
        assert self._dat_count() == 0
        assert client.execute_command('DBSIZE') == 0
        # At-limit SET succeeds and creates a .dat file.
        assert client.execute_command('BLOB.SET', 'okkey', b'Y' * limit) == b'OK'
        wait_for_equal(self._dat_count, 1)
        # GET returns correct data (promotion path on first GET, DRAM on second).
        assert client.execute_command('BLOB.GET', 'okkey') == b'Y' * limit
        assert client.execute_command('BLOB.GET', 'okkey') == b'Y' * limit
        # Lowering limit below stored object size does not affect reads.
        client.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(limit // 2))
        assert client.execute_command('BLOB.GET', 'okkey') == b'Y' * limit

    def test_max_object_size_tiered_rejection(self):
        """CONFIG SET max-object-size > nvme-maxmemory is rejected."""
        client = self.server.get_new_client()
        # Set nvme-maxmemory to a small value so we can exceed it.
        nvme_limit = 1048576  # 1 MiB
        client.execute_command('CONFIG', 'SET', 'largeobj.nvme-maxmemory', str(nvme_limit))
        obj_limit = 2 * 1048576  # 2 MiB
        try:
            client.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(obj_limit))
            assert False, "Expected CONFIG SET to be rejected"
        except ResponseError as e:
            assert 'max-object-size' in str(e).lower(), f"Unexpected error: {e}"
            assert 'nvme-maxmemory' in str(e).lower(), f"Unexpected error: {e}"


class TestLargeObjTieredNvmeOnly(ValkeyLargeObjTestCaseBase):
    """Tiered mode with max-promote-size=0 (no promotion, all reads from NVMe)."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 0"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
        )

    def test_set_get_roundtrip_no_promotion(self):
        """With max-promote-size=0, GET always reads from NVMe (no DRAMPool caching)."""
        client = self.server.get_new_client()
        payload = b'N' * 8192
        client.execute_command('BLOB.SET', 'nvme_key', payload)
        result = client.execute_command('BLOB.GET', 'nvme_key')
        assert result == payload

    def test_multiple_gets_all_from_nvme(self):
        """Multiple GETs with max-promote-size=0 should all succeed."""
        client = self.server.get_new_client()
        payload = b'R' * 4096
        client.execute_command('BLOB.SET', 'repeat_key', payload)
        for _ in range(5):
            result = client.execute_command('BLOB.GET', 'repeat_key')
            assert result == payload
            assert client.execute_command('BLOB.INFO', 'repeat_key', 'TIER') == b'nvme'

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
        client.execute_command('BLOB.SET', 'memkey', payload)
        # Even after a GET the object stays cold (max-promote-size=0).
        client.execute_command('BLOB.GET', 'memkey')
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
# These Tiered-mode classes use small nvme-maxmemory caps to exercise the
# capacity gate; each defines its own get_module_args.


class _NvmeAccountingBase(ValkeyLargeObjTestCaseBase):
    """Shared helpers for the NVMe disk-usage accounting tests.

    The usage counter is not observable directly (no INFO section / command), so
    these tests exercise it through its only externally-visible effect: the
    reserve-if-capacity gate (`try_reserve_nvme_disk_usage`) on the Tiered SET path.
    A SET that would push tracked usage past `nvme-maxmemory` is rejected with
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
            lambda: client.info("stats").get("lazyfree_pending_objects", 0), 0
        )

    def _set_ok(self, client, key, payload):
        assert client.execute_command("BLOB.SET", key, payload) == b"OK"

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
                assert client.execute_command("BLOB.SET", key, payload) == b"OK"
                return
            except ResponseError as e:
                if "pool exhausted" not in str(e).lower():
                    raise
                last = e
                time.sleep(delay)
        assert False, (
            f"SET '{key}' still pool-exhausted after {tries} tries "
            f"({tries * delay:.1f}s) -- capacity not reclaimed on overwrite (leak): {last}"
        )

    def _set_rejected(self, client, key, payload):
        try:
            client.execute_command("BLOB.SET", key, payload)
            assert False, f"Expected '{key}' SET to be rejected (capacity exceeded)"
        except ResponseError as e:
            err = str(e).lower()
            assert "pool exhausted" in err or "capacity exceeded" in err, \
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
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" max-object-size {self.CAP}"
            f" nvme-staging-size {self.CAP}"
            f" segment-size 1048576"
            f" max-promote-size 1028096"
            f" chunk-size {self.OBJ}"
            f" bench-mode no"
            f" direct-io no"
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
        # nvme-staging-size 4 MiB -> ceil(4MiB / 2MiB) = 2 NVMe staging segments.
        # max-promote-size must fit in segment after talc overhead (max 2084864).
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-maxmemory {self.CAP}"
            f" max-object-size {self.CAP}"
            f" nvme-staging-size 4194304"
            f" segment-size 2097152"
            f" max-promote-size 2084864"
            f" chunk-size 1048576"
            f" bench-mode no"
            f" direct-io no"
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
        # Verify the relationship between payload sizes and nvme-maxmemory cap.
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
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 0"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_file_header_crc_mismatch(self):
        """Corrupt CRC in .dat header → GET triggers panic."""
        client = self.server.get_new_client()
        payload = b'X' * 4096
        client.execute_command('BLOB.SET', 'crc_key', payload)
        # Corrupt the CRC field in the FileHeader (bytes 21-24 in packed layout).
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected .dat file after SET"
        with open(dat_files[-1], 'r+b') as f:
            f.seek(21)
            f.write(b'\xff\xff\xff\xff')
        # GET should trigger panic (CRC mismatch in read_and_verify_file_header).
        with self.server.expect_crash(self):
            try:
                client.execute_command('BLOB.GET', 'crc_key')
            except Exception:
                pass


class TestTieredCorruptionMagic(ValkeyLargeObjTestCaseBase):
    """FileHeader magic corruption: server must crash on corrupt data."""

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 0"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_tiered_file_header_magic_corruption(self):
        """Corrupt magic bytes → GET triggers panic."""
        client = self.server.get_new_client()
        payload = b'Y' * 4096
        client.execute_command('BLOB.SET', 'magic_key', payload)
        dat_files = sorted(glob.glob(os.path.join(self.data_dir, '*.dat')))
        assert len(dat_files) >= 1, "Expected .dat file after SET"
        with open(dat_files[-1], 'r+b') as f:
            f.seek(0)
            f.write(b'BAAD')
        with self.server.expect_crash(self):
            try:
                client.execute_command('BLOB.GET', 'magic_key')
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
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 1048576"
            f" segment-size 1048576"
            f" max-promote-size 520192"
            f" chunk-size 4096"
            f" bench-mode no"
            f" direct-io no"
            f" smartlog-poll-secs 0"
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
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 4194304"
            f" max-promote-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
        )

    def test_second_touch_promotes(self):
        """SET moves no cache counter. The first GET is a miss that admission
        rejects; the second promotes; the third is a hit. Objects above
        max-promote-size are misses that never count as admission rejects."""
        client = self.server.get_new_client()
        dram = lambda: client.info('largeobj_dram')
        payload = b'T' * 4096
        base = dram()

        client.execute_command('LO.SET', 'basic_key', payload)
        assert dram() == base

        # GET 1: rejected by admission, served transiently.
        assert client.execute_command('LO.GET', 'basic_key') == payload
        assert client.execute_command('LO.INFO', 'basic_key', 'TIER') == b'nvme'
        after1 = dram()
        assert after1['largeobj_cache_misses_total'] == 1
        assert after1['largeobj_admission_rejects_total'] == 1
        assert after1['largeobj_promotions_total'] == 0
        assert after1['largeobj_cached_objects'] == 0

        # GET 2: second touch, promoted. mark_ready runs before the reply.
        assert client.execute_command('LO.GET', 'basic_key') == payload
        assert client.execute_command('LO.INFO', 'basic_key', 'TIER') == b'dram'
        after2 = dram()
        assert after2['largeobj_cache_misses_total'] == 2
        assert after2['largeobj_admission_rejects_total'] == 1
        assert after2['largeobj_promotions_total'] == 1
        assert after2['largeobj_cached_objects'] == 1
        assert after2['largeobj_cache_hits_total'] == 0

        # GET 3: plain hit, nothing on the miss side moves.
        assert client.execute_command('LO.GET', 'basic_key') == payload
        after3 = dram()
        assert after3['largeobj_cache_hits_total'] == 1
        assert after3['largeobj_cache_misses_total'] == 2
        assert after3['largeobj_admission_rejects_total'] == 1
        assert after3['largeobj_promotions_total'] == 1

        # Oversize: three misses, no rejects, no promotion.
        client.execute_command('CONFIG', 'SET', 'largeobj.max-promote-size', '4096')
        big = b'O' * 8192
        client.execute_command('LO.SET', 'big_key', big)
        for _ in range(3):
            assert client.execute_command('LO.GET', 'big_key') == big
        after4 = dram()
        assert after4['largeobj_cache_misses_total'] == 5
        assert after4['largeobj_admission_rejects_total'] == 1
        assert after4['largeobj_promotions_total'] == 1

    def test_promote_min_hits_runtime(self):
        """promote-min-hits is runtime mutable: 1 restores first-GET promotion,
        3 requires three misses."""
        client = self.server.get_new_client()
        payload = b'R' * 4096

        client.execute_command('CONFIG', 'SET', 'largeobj.promote-min-hits', '1')
        client.execute_command('LO.SET', 'one_key', payload)
        rejects = client.info('largeobj_dram')['largeobj_admission_rejects_total']
        assert client.execute_command('LO.GET', 'one_key') == payload
        assert client.execute_command('LO.INFO', 'one_key', 'TIER') == b'dram'
        assert client.info('largeobj_dram')['largeobj_admission_rejects_total'] == rejects

        client.execute_command('CONFIG', 'SET', 'largeobj.promote-min-hits', '3')
        client.execute_command('LO.SET', 'three_key', payload)
        for _ in range(2):
            assert client.execute_command('LO.GET', 'three_key') == payload
            assert client.execute_command('LO.INFO', 'three_key', 'TIER') == b'nvme'
        assert client.execute_command('LO.GET', 'three_key') == payload
        assert client.execute_command('LO.INFO', 'three_key', 'TIER') == b'dram'


# ─── Inline demotion ────────────────────────────────────────────────────────


class TestLargeObjTieredDemotion(ValkeyLargeObjTestCaseBase):
    """One 1 MiB segment, no room to grow (server maxmemory leaves less than a
    segment of headroom, see _block_expansion), so a promotion into a full pool
    must demote. promote-min-hits 1 so every GET
    promotes; decay off so scores are stable across a minute boundary. The
    default demote-sample-size (5) exceeds the 3-entry map, so victim selection
    scans every entry and is exact. 256 KiB objects: three fill 768 KiB; a
    fourth needs the remaining 256 KiB exactly, which any allocator overhead
    denies."""

    OBJ = 256 * 1024
    KEYS = [f'ev_{i}' for i in range(3)]

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 262144"
            f" promote-min-hits 1"
            f" tiered-decay-time 0"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
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

    def test_demotes_lowest_score_object(self):
        """A fourth promotion into the full pool demotes exactly one cold object and
        never the hot one."""
        client = self.server.get_new_client()
        self._block_expansion(client)
        # SET and promote three objects that fill the single segment.
        for i, k in enumerate(self.KEYS):
            payload = bytes([65 + i]) * self.OBJ
            client.execute_command('LO.SET', k, payload)
            assert client.execute_command('LO.GET', k) == payload
        info = client.info('largeobj_dram')
        assert info['largeobj_cached_objects'] == 3
        assert info['largeobj_reclaims_total'] == 0
        assert info['largeobj_dram_live_segments'] == 1

        hot = self.KEYS[0]
        for _ in range(10):
            assert client.execute_command('LO.GET', hot) == b'A' * self.OBJ

        payload = b'N' * self.OBJ
        client.execute_command('LO.SET', 'ev_new', payload)
        assert client.execute_command('LO.GET', 'ev_new') == payload

        info = client.info('largeobj_dram')
        assert info['largeobj_reclaims_total'] == 1
        assert info['largeobj_cached_objects'] == 3
        assert info['largeobj_dram_live_segments'] == 1
        assert info['largeobj_scaling_expand_total'] == 0
        assert client.execute_command('LO.INFO', hot, 'TIER') == b'dram'
        assert client.execute_command('LO.INFO', 'ev_new', 'TIER') == b'dram'
        tiers = [client.execute_command('LO.INFO', k, 'TIER') for k in self.KEYS[1:]]
        assert tiers.count(b'nvme') == 1, tiers

        # The demoted copy is still on NVMe and reads back intact.
        demoted = self.KEYS[1:][tiers.index(b'nvme')]
        idx = self.KEYS.index(demoted)
        assert client.execute_command('LO.GET', demoted) == bytes([65 + idx]) * self.OBJ


class TestLargeObjTieredReclaimOneSegment(ValkeyLargeObjTestCaseBase):
    """Reclaim frees room in one segment, the least loaded, and allocates there.
    Two 1 MiB segments: three 256 KiB objects fill segment 0, and the pool
    expands once for the fourth. Segment 1 then gets the coldest objects, so a
    reclaim that looked across the whole pool would take one of those. The
    target is segment 0 (768 KiB vs 832 KiB) and its 3 entries are fewer than
    the default demote-sample-size (5), so the victim choice there is exact.
    The expand watermark is raised so the scaling cron never adds a third
    segment (2 MiB, at most 78% full)."""

    OBJ = 256 * 1024
    SMALL = 64 * 1024

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 262144"
            f" promote-min-hits 1"
            f" tiered-decay-time 0"
            f" scaling-expand-watermark 95"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def _promote(self, client, key, payload):
        client.execute_command('LO.SET', key, payload)
        assert client.execute_command('LO.GET', key) == payload
        assert client.execute_command('LO.INFO', key, 'TIER') == b'dram'

    def test_reclaims_only_in_target_segment(self):
        client = self.server.get_new_client()
        # Segment 0: A, B, C (768 KiB). No maxmemory yet, so D expands the pool.
        for k in ('A', 'B', 'C', 'D'):
            self._promote(client, k, k.encode() * self.OBJ)
        info = client.info('largeobj_dram')
        assert info['largeobj_dram_live_segments'] == 2
        assert info['largeobj_scaling_expand_total'] == 1
        assert info['largeobj_reclaims_total'] == 0

        # No more growth. Segment 1 (D, 256 KiB) is the less loaded one, so E,
        # H and F all land there: 256 + 256 + 64 + 256 = 832 KiB.
        TestLargeObjTieredDemotion._block_expansion(self, client)
        self._promote(client, 'E', b'E' * self.OBJ)
        self._promote(client, 'H', b'H' * self.SMALL)
        self._promote(client, 'F', b'F' * self.OBJ)
        info = client.info('largeobj_dram')
        assert info['largeobj_cached_objects'] == 7
        assert info['largeobj_dram_live_segments'] == 2

        # One hit takes A and B from the initial LFU counter 5 to 6 (the first
        # increment is certain). C and everything in segment 1 stay at 5.
        for k in ('A', 'B'):
            assert client.execute_command('LO.GET', k) == k.encode() * self.OBJ

        # G fits nowhere. The target is segment 0 (least loaded); its coldest
        # entry is C, so exactly C goes, even though segment 1 is just as cold.
        self._promote(client, 'G', b'G' * self.OBJ)
        info = client.info('largeobj_dram')
        assert info['largeobj_reclaims_total'] == 1, "no over-reclaim"
        assert info['largeobj_cached_objects'] == 7
        assert info['largeobj_dram_live_segments'] == 2
        assert info['largeobj_scaling_expand_total'] == 1
        assert client.execute_command('LO.INFO', 'C', 'TIER') == b'nvme'
        for k in ('A', 'B', 'D', 'E', 'H', 'F'):
            assert client.execute_command('LO.INFO', k, 'TIER') == b'dram', k
        # The reclaimed copy still reads back from NVMe.
        assert client.execute_command('LO.GET', 'C') == b'C' * self.OBJ


class TestLargeObjTieredFdCap(ValkeyLargeObjTestCaseBase):
    """max-open-fds 2 with promotion effectively off (promote-min-hits 255), so
    every GET reads through the fd pool. Decay off so scores are stable across
    a minute boundary. The default demote-sample-size (5) exceeds the 2-entry
    map, so victim selection scans both entries and is exact."""

    OBJ = 64 * 1024
    KEYS = [f'fd_{i}' for i in range(5)]

    def get_module_args(self, data_dir, direct_io):
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 4194304"
            f" segment-size 1048576"
            f" max-promote-size 983040"
            f" promote-min-hits 255"
            f" tiered-decay-time 0"
            f" max-open-fds 2"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def _payload(self, i):
        return bytes([65 + i]) * self.OBJ

    def test_cap_respected_and_reads_succeed(self):
        """Reading more objects than the cap keeps open_fds at the cap, demotes
        exactly the overflow, and every read still returns the right bytes."""
        client = self.server.get_new_client()
        for i, k in enumerate(self.KEYS):
            client.execute_command('LO.SET', k, self._payload(i))
        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 0

        for i, k in enumerate(self.KEYS):
            assert client.execute_command('LO.GET', k) == self._payload(i)

        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 2
        assert info['largeobj_fd_demotions_total'] == 3
        # Nothing was promoted, so these were all NVMe reads.
        dram = client.info('largeobj_dram')
        assert dram['largeobj_cached_objects'] == 0

        # A second pass reads everything back correctly through reopened fds.
        for i, k in enumerate(self.KEYS):
            assert client.execute_command('LO.GET', k) == self._payload(i)
        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 2
        demotions = info['largeobj_fd_demotions_total']

        # DEL drops the pool's fd through ObjectFile::Drop, so the next open
        # takes the free slot instead of demoting. KEYS[4] was read last, so
        # its fd is one of the two cached.
        client.execute_command('DEL', self.KEYS[4])
        wait_for_equal(
            lambda: client.info('largeobj_fd')['largeobj_open_fds'], 1)
        assert client.execute_command('LO.GET', self.KEYS[0]) == self._payload(0)
        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 2
        assert info['largeobj_fd_demotions_total'] == demotions

    def test_demotes_coldest_keeps_hot(self):
        """A full pool demotes the lowest-scoring fd. KEYS[0] is read three
        times and KEYS[1] once, so opening KEYS[2] must demote KEYS[1], and a
        later read of KEYS[0] is a cache hit that demotes nothing."""
        client = self.server.get_new_client()
        for i, k in enumerate(self.KEYS[:3]):
            client.execute_command('LO.SET', k, self._payload(i))

        # The first read opens the fd; the next two touch its score. The first
        # touch on a fresh entry always increments, so KEYS[0] outscores KEYS[1].
        for _ in range(3):
            assert client.execute_command('LO.GET', self.KEYS[0]) == self._payload(0)
        assert client.execute_command('LO.GET', self.KEYS[1]) == self._payload(1)
        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 2
        assert info['largeobj_fd_demotions_total'] == 0

        assert client.execute_command('LO.GET', self.KEYS[2]) == self._payload(2)
        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 2
        assert info['largeobj_fd_demotions_total'] == 1

        # KEYS[0] survived, so reading it again needs no open and no demotion.
        assert client.execute_command('LO.GET', self.KEYS[0]) == self._payload(0)
        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 2
        assert info['largeobj_fd_demotions_total'] == 1

    def test_lower_cap_at_runtime(self):
        """CONFIG SET to a smaller cap takes effect on the next open."""
        client = self.server.get_new_client()
        for i, k in enumerate(self.KEYS[:2]):
            client.execute_command('LO.SET', k, self._payload(i))
            assert client.execute_command('LO.GET', k) == self._payload(i)
        assert client.info('largeobj_fd')['largeobj_open_fds'] == 2

        client.execute_command('CONFIG', 'SET', 'largeobj.max-open-fds', '1')
        client.execute_command('LO.SET', self.KEYS[2], self._payload(2))
        assert client.execute_command('LO.GET', self.KEYS[2]) == self._payload(2)
        info = client.info('largeobj_fd')
        assert info['largeobj_open_fds'] == 1
        assert info['largeobj_fd_demotions_total'] == 2
