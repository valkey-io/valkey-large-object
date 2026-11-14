import os
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, position_pattern
from valkeytestframework.util.waiters import wait_for_equal, wait_for_true


def wait_previous_run_deleted(client, data_dir, timeout=10):
    """Wait until INFO reports nothing left of the previous run and its dirs are gone."""
    remaining = lambda: client.info('largeobj_disk')['largeobj_disk_previous_run_remaining_bytes']
    wait_for_equal(remaining, 0, timeout=timeout)
    wait_for_true(lambda: not [d for d in os.listdir(data_dir) if d.startswith('previous-run-')],
                  timeout=timeout)


class TestLargeObjCleanup(ValkeyLargeObjTestCaseBase):
    """File-system cleanup across server lifetimes (Tiered mode).

    NVMe object files live on the module's dedicated ``disk-dir`` and must not
    accumulate across server lifetimes. Deleting them takes time proportional to the data, so it
    never blocks startup or shutdown:

    - graceful shutdown leaves object files in place;
    - startup moves everything in ``disk-dir`` into a ``previous-run-*`` directory,
      one rename per entry, charges what it holds against ``disk-maxmemory``, and
      deletes it in the background — or inline, before any eviction, when a SET needs
      the room.

    Cleanup never touches the parent ``testdir`` (the server's logfile, rdb), nor this
    run's shards.
    """

    def test_shutdown_leaves_files_for_next_startup(self):
        """Shutdown deletes nothing; the next startup deletes it all in the background."""
        client = self.server.get_new_client()
        client.execute_command("BLOB.TCP_SET", "a", position_pattern(4096, "A"))
        client.execute_command("BLOB.TCP_SET", "b", position_pattern(4096, "B"))
        client.execute_command("BLOB.TCP_SET", "c", position_pattern(4096, "C"))
        # DEL frees asynchronously (BIO thread); wait for completion.
        client.execute_command("DEL", "b")
        wait_for_equal(lambda: client.info('memory').get('lazyfree_pending_objects', 0), 0)
        assert len(self._object_files()) == 2

        # exit() issues SHUTDOWN NOSAVE, which fires the Shutdown server event.
        # cleanup=False so the framework does not touch testdir.
        self.server.exit(cleanup=False)
        assert len(self._object_files()) == 2, "shutdown must not delete object files"

        self.server.start()
        client = self.server.get_new_client()
        wait_previous_run_deleted(client, self.data_dir)
        assert self._object_files() == [], "previous run's files not deleted"

    def test_startup_deletes_orphaned_files(self):
        """Startup deletes everything a crashed run left behind; files outside disk-dir survive.

        Plant files while the server is down: in a shard, in a ``previous-run-*`` directory
        from a run that crashed mid-cleanup, and loose in disk-dir (one oddly named, to show
        it is name-agnostic), plus an unrelated file in testdir itself (outside disk-dir).
        """
        self.server.exit(cleanup=False)
        assert self._object_files() == [], "no object files expected before planting"

        planted = [
            os.path.join(self.data_dir, "0001", "0000000000000001.dat"),
            os.path.join(self.data_dir, "previous-run-1", "0002", "0000000000000002.dat"),
            os.path.join(self.data_dir, "000000000000dead.dat"),
            os.path.join(self.data_dir, "000000000000beef.dat.tmp"),
            os.path.join(self.data_dir, "not-a-normal-name"),
        ]
        foreign = os.path.join(self.testdir, "keepme.txt")
        for path in planted:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(b"stale")
        with open(foreign, "wb") as f:
            f.write(b"do not delete")

        self.server.start()
        client = self.server.get_new_client()
        # The OID-1 path is free for this run straight away: the old shard was moved out.
        assert client.execute_command("BLOB.TCP_SET", "k", position_pattern(4096, "K")) == b"OK"
        wait_previous_run_deleted(client, self.data_dir)

        # planted[0] is OID 1's path: it now holds this run's object, not the orphan.
        with open(planted[0], "rb") as f:
            assert f.read() != b"stale", "new object must not land on the orphan"
        for path in planted[1:]:
            assert not os.path.exists(path), f"orphan not deleted: {path}"
        assert os.path.exists(foreign), "file outside disk-dir must be preserved"
        assert len(self._object_files()) == 1, "only this run's object remains"
        assert client.execute_command("BLOB.TCP_GET", "k") == position_pattern(4096, "K")

    def test_restart_leaves_no_object_files(self):
        """End-to-end: data written before a restart does not linger."""
        client = self.server.get_new_client()
        for i in range(3):
            client.execute_command("BLOB.TCP_SET", f"rk{i}", position_pattern(4096, "Z"))
        assert len(self._object_files()) == 3

        self.server.restart()

        client = self.server.get_new_client()
        wait_previous_run_deleted(client, self.data_dir)
        assert self._object_files() == [], "object files lingered across restart"
        # A fresh server can still serve new writes.
        assert client.execute_command("BLOB.TCP_SET", "after", position_pattern(4096, "Q")) == b"OK"
        assert len(self._object_files()) == 1


class TestLargeObjCleanupUnderPressure(ValkeyLargeObjTestCaseBase):
    """A full disk left by the previous run: SETs right after startup delete it
    inline instead of evicting live objects or failing."""

    PREVIOUS_RUN_FILES = 20000
    VALUE = 4096

    def get_module_args(self, data_dir, direct_io):
        # disk-maxmemory must be >= max-object-size; keep both small.
        return super().get_module_args(data_dir, direct_io) + " max-object-size 1048576"

    def test_writes_after_restart_delete_previous_run_not_live_objects(self):
        self.server.exit(cleanup=False)

        # Many small files in an old shard, so the background cleanup takes a while. The
        # startup charge (statvfs: the whole test filesystem) is far above
        # disk-maxmemory, so until they are all deleted every SET must delete inline.
        shard = os.path.join(self.data_dir, "0003")
        os.makedirs(shard, exist_ok=True)
        payload = b"g" * 8192
        for i in range(self.PREVIOUS_RUN_FILES):
            with open(os.path.join(shard, f"{i:016x}.dat"), "wb") as f:
                f.write(payload)

        self.server.start()
        client = self.server.get_new_client()
        client.execute_command('CONFIG', 'SET', 'maxmemory-policy', 'allkeys-lru')
        # Room for ~64 live objects; disk-maxmemory minimum is 1 MiB.
        client.execute_command('CONFIG', 'SET', 'largeobj.disk-maxmemory', str(1 << 20))
        for i in range(32):
            assert client.execute_command(
                "BLOB.TCP_SET", f"k{i}", position_pattern(self.VALUE, i)) == b"OK"

        wait_previous_run_deleted(client, self.data_dir, timeout=60)
        assert client.info('largeobj')['largeobj_evictions'] == 0, \
            "a live object was evicted to make room for the previous run's files"
        for i in range(32):
            assert client.execute_command("BLOB.TCP_GET", f"k{i}") == position_pattern(self.VALUE, i)
        assert len(self._object_files()) == 32
