import os
from valkey import ResponseError
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase


class TestLargeObjDram(ValkeyLargeObjTestCaseBase):
    """Dram-only mode: all objects live in DRAMPool, no NVMe."""

    def get_module_args(self, data_dir, direct_io):
        # max-object-size must fit in one segment after talc per-chunk overhead.
        # With seg=2M and chunk=4K the maximum is 1044480 (~1020 KiB).
        return (
            f"operating-mode Dram"
            f" segment-size 2097152"
            f" max-object-size 1044480"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
        )

    def test_set_get_roundtrip(self):
        """Basic SET + GET in Dram mode, including multi-chunk objects."""
        client = self.server.get_new_client()
        # Single-chunk: 4096 bytes with chunk-size=4096 → 1 chunk.
        payload = b'A' * 4096
        result = client.execute_command('BLOB.SET', 'dramkey', payload)
        assert result == b'OK'
        data = client.execute_command('BLOB.GET', 'dramkey')
        assert data == payload
        # Partial last chunk: 4096 + 1 = 4097 → 2 chunks (second chunk is 1 byte).
        payload_partial = b'B' * 4097
        client.execute_command('BLOB.SET', 'partial_key', payload_partial)
        assert client.execute_command('BLOB.GET', 'partial_key') == payload_partial
        # Exact multiple: 8192 = 2 * 4096 → 2 full chunks.
        payload_exact = b'C' * 8192
        client.execute_command('BLOB.SET', 'exact_key', payload_exact)
        assert client.execute_command('BLOB.GET', 'exact_key') == payload_exact
        # Many chunks: 20000 bytes → 5 chunks (last chunk is 20000 % 4096 = 3616 bytes).
        payload_many = b'D' * 20000
        client.execute_command('BLOB.SET', 'many_key', payload_many)
        assert client.execute_command('BLOB.GET', 'many_key') == payload_many

    def test_get_nonexistent_key(self):
        """GET on nonexistent key returns nil in Dram mode."""
        client = self.server.get_new_client()
        result = client.execute_command('BLOB.GET', 'nokey')
        assert result is None

    def test_overwrite_key(self):
        """SET same key twice returns OK both times."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.SET', 'overkey', b'X' * 4096)
        client.execute_command('BLOB.SET', 'overkey', b'Y' * 4096)
        data = client.execute_command('BLOB.GET', 'overkey')
        assert data == b'Y' * 4096

    def test_delete_key(self):
        """DEL removes the key, subsequent GET returns nil."""
        client = self.server.get_new_client()
        client.execute_command('BLOB.SET', 'delkey', b'Z' * 4096)
        client.execute_command('DEL', 'delkey')
        result = client.execute_command('BLOB.GET', 'delkey')
        assert result is None

    def test_object_larger_than_segment_rejected(self):
        """An object larger than max-object-size is rejected.
        Compare with test_max_object_size where max-object-size is lowered at
        runtime via CONFIG SET.
        """
        client = self.server.get_new_client()
        # max-object-size is 1044480 (~1020 KiB). A 4MB object exceeds it.
        obj_size = 4 * 1024 * 1024
        payload = b'D' * obj_size
        try:
            client.execute_command('BLOB.SET', 'toobig', payload)
            assert False, "Expected max-object-size rejection"
        except ResponseError as e:
            assert 'max object size' in str(e).lower(), f"Unexpected error: {e}"

    def test_multiple_objects(self):
        """Multiple small objects can coexist in DRAMPool."""
        client = self.server.get_new_client()
        for i in range(10):
            payload = bytes([i % 256]) * 4096
            client.execute_command('BLOB.SET', f'multi{i}', payload)
        for i in range(10):
            data = client.execute_command('BLOB.GET', f'multi{i}')
            expected = bytes([i % 256]) * 4096
            assert data == expected, f"Key multi{i} mismatch"

    # ─── Keyspace event tests ────────────────────────────────────────────

    def test_keyspace_events(self):
        """BLOB.SET publishes largeobj.create on a new key and largeobj.update on overwrite."""
        client = self.server.get_new_client()
        pubsub = self.subscribe_keyspace_events(client)
        client.execute_command('BLOB.SET', 'eventkey', b'A' * 4096)
        client.execute_command('BLOB.SET', 'eventkey', b'B' * 8192)
        assert self.read_keyspace_events(pubsub, 2) == [
            ('largeobj.create', 'eventkey'),
            ('largeobj.update', 'eventkey'),
        ]
        # A failed SET publishes nothing. max-object-size (1044480) rejects a 4MB
        # object in the command handler before set_value, so no event fires.
        try:
            client.execute_command('BLOB.SET', 'toobig', b'D' * (4 * 1024 * 1024))
            assert False, "Expected max-object-size rejection"
        except ResponseError:
            pass
        assert self.read_keyspace_events(pubsub, 1) == []

    # ─── COPY callback tests ─────────────────────────────────────────────

    def test_copy(self):
        """COPY in DRAM-only mode: independent object, digest differs, delete independence."""
        client = self.server.get_new_client()
        payload = b'C' * 4096
        client.execute_command('BLOB.SET', 'srckey', payload)
        # COPY creates an independent object
        result = client.execute_command('COPY', 'srckey', 'dstkey')
        assert result == 1 or result is True
        assert client.execute_command('BLOB.GET', 'srckey') == payload
        assert client.execute_command('BLOB.GET', 'dstkey') == payload
        # COPY gets a new OID so digests differ
        src_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'srckey')
        dst_digest = client.execute_command('DEBUG', 'DIGEST-VALUE', 'dstkey')
        assert src_digest != dst_digest
        # Deleting source does not affect the copy
        client.execute_command('DEL', 'srckey')
        assert client.execute_command('BLOB.GET', 'dstkey') == payload
        # Deleting copy does not affect the source
        client.execute_command('BLOB.SET', 'srckey2', payload)
        client.execute_command('COPY', 'srckey2', 'dstkey2')
        client.execute_command('DEL', 'dstkey2')
        assert client.execute_command('BLOB.GET', 'srckey2') == payload

    # ─── MEMORY USAGE callback tests ──────────────────────────────────────

    def test_memory_usage(self):
        """MEMORY USAGE in DRAM-only mode includes LoValue struct + payload."""
        client = self.server.get_new_client()
        payload_size = 4096
        client.execute_command('BLOB.SET', 'memkey', b'M' * payload_size)
        mem = client.execute_command('MEMORY', 'USAGE', 'memkey')
        assert mem is not None
        lo_value_size = 24
        assert mem >= lo_value_size + payload_size, (
            f"Expected MEMORY USAGE >= {lo_value_size + payload_size}, got {mem}"
        )

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

    # ─── max-object-size tests ───────────────────────────────────────────

    def test_max_object_size(self):
        """max-object-size rejects oversized SETs, allows at-limit SETs,
        and does not affect reads of already-stored objects."""
        client = self.server.get_new_client()
        limit = 8192
        client.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(limit))
        # Oversized SET is rejected.
        try:
            client.execute_command('BLOB.SET', 'bigkey', b'X' * (limit + 1))
            assert False, "Expected max object size rejection"
        except ResponseError as e:
            assert 'max object size' in str(e).lower(), f"Unexpected error: {e}"
        # Rejected SET must not leave a phantom key.
        assert client.execute_command('DBSIZE') == 0
        assert client.execute_command('BLOB.GET', 'bigkey') is None
        # At-limit SET succeeds.
        assert client.execute_command('BLOB.SET', 'okkey', b'Y' * limit) == b'OK'
        assert client.execute_command('BLOB.GET', 'okkey') == b'Y' * limit
        # Lowering limit below stored object size does not affect reads.
        client.execute_command('CONFIG', 'SET', 'largeobj.max-object-size', str(limit // 2))
        assert client.execute_command('BLOB.GET', 'okkey') == b'Y' * limit

    # ─── SMART LOG tests ───────────────────────────────────────────────────

    def test_smartlog_section_absent(self):
        """Dram mode never starts the SMART log poller"""
        client = self.server.get_new_client()
        assert 'largeobj_snapshot_age_seconds' not in client.info('largeobj_smartlog_usage')

    # ─── BLOB.INFO tests ───────────────────────────────────────────────────

    def test_info(self):
        client = self.server.get_new_client()
        payload = bytes(range(256)) * 16  # 4096 bytes, non-uniform so CRC is meaningful
        client.execute_command('BLOB.SET', 'infokey', payload)
        # Check the specfic fields for info
        assert client.execute_command('BLOB.INFO', 'infokey', 'LEN') == len(payload)
        # CRC is a u32: in range, stable across calls, and identical for identical payloads.
        crc = client.execute_command('BLOB.INFO', 'infokey', 'CRC')
        assert 0 <= crc <= 0xFFFFFFFF
        assert client.execute_command('BLOB.INFO', 'infokey', 'CRC') == crc
        client.execute_command('BLOB.SET', 'infokey2', payload)
        assert client.execute_command('BLOB.INFO', 'infokey2', 'CRC') == crc
        assert client.execute_command('BLOB.INFO', 'infokey', 'TIER') == b'dram'
        # Check full info call
        result = client.execute_command('BLOB.INFO', 'infokey')
        assert result == [
            b'len', len(payload),
            b'crc', crc,
            b'tier', b'dram',
        ], f"Unexpected BLOB.INFO reply: {result!r}"

    def test_info_crc_changes_on_overwrite(self):
        """Overwriting a key updates LEN and CRC."""
        client = self.server.get_new_client()
        first = b'A' * 4096
        second = b'B' * 8192
        client.execute_command('BLOB.SET', 'owkey', first)
        first_crc = client.execute_command('BLOB.INFO', 'owkey', 'CRC')
        client.execute_command('BLOB.SET', 'owkey', second)
        assert client.execute_command('BLOB.INFO', 'owkey', 'LEN') == 8192
        assert client.execute_command('BLOB.INFO', 'owkey', 'CRC') != first_crc

    def test_info_errors(self):
        """BLOB.INFO errors are correct"""
        client = self.server.get_new_client()
        # Nonexistant key
        self.verify_error_response(client, 'BLOB.INFO nokey', 'not found')
        self.verify_error_response(client, 'BLOB.INFO nokey LEN', 'not found')
        # Wrong type error
        client.execute_command('SET', 'strkey', 'plain')
        try:
            client.execute_command('BLOB.INFO', 'strkey')
            assert False, "Expected WRONGTYPE error"
        except ResponseError as e:
            assert 'existing key has wrong valkey type' in str(e).lower(), f"Unexpected error: {e}"
        # Wrong number of arguments error
        client.execute_command('BLOB.SET', 'badkey', b'x' * 4096)
        try:
            client.execute_command('BLOB.INFO', 'badkey', 'LEN', 'CRC')
            assert False, "Expected arity error"
        except ResponseError as e:
            assert 'wrong number of arguments' in str(e).lower(), f"Unexpected error: {e}"
        # Bad field error
        try:
            client.execute_command('BLOB.INFO', 'badkey', 'NOTREAL')
            assert False, "Expected wrong information field error"
        except ResponseError as e:
            assert 'invalid information value' in str(e).lower(), f"Unexpected error: {e}"


class TestLargeObjDramCopyExhaustion(ValkeyLargeObjTestCaseBase):
    """COPY fails when the pool cannot fit the duplicate and cannot expand.

    Own class with a coarse chunk-size (64KB) so a large object co-locates in one
    segment with negligible talc overhead (4KB chunks waste ~50% and would not
    fit). Server maxmemory is the only expansion ceiling now, so we cap it just
    above current usage and COPY's expansion crosses the watermark and fails.
    """

    def get_module_args(self, data_dir, direct_io):
        # max-object-size must fit in one segment after talc overhead.
        # With seg=2M and chunk=64K the max is 1966080 (~1920 KiB).
        return (
            f"operating-mode Dram"
            f" segment-size 2097152"
            f" max-object-size 1966080"
            f" chunk-size 65536"
            f" bench-mode no"
            f" direct-io no"
        )

    def test_copy_pool_exhausted(self):
        client = self.server.get_new_client()
        client.execute_command('FLUSHALL')
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'noeviction')
        payload = b'F' * (1500 * 1024)
        assert client.execute_command('BLOB.SET', 'bigkey', payload) == b'OK'
        # Cap server maxmemory just above current used — no room for a 2nd segment.
        used = int(client.info('memory')['used_memory'])
        client.execute_command('CONFIG', 'SET', 'maxmemory', str(used + 256 * 1024))
        try:
            client.execute_command('COPY', 'bigkey', 'bigcopy')
            assert False, "Expected COPY to fail — expansion would cross maxmemory"
        except ResponseError:
            pass  # Expected — cannot fit a second object without crossing the watermark
        finally:
            client.execute_command('CONFIG', 'SET', 'maxmemory', '0')
        # Source intact.
        assert client.execute_command('BLOB.GET', 'bigkey') == payload
