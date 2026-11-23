import glob
import os
import shutil
import pytest
from valkey_largeobj_test_case import DISK_DIR_MARKER, ValkeyLargeObjTestCaseBase, position_pattern
from valkeytestframework.util.waiters import wait_for_equal, wait_for_true


def wait_previous_run_deleted(client, timeout=10):
    """Wait until the cleanup is done: INFO reaches 0 after its dirs are removed."""
    remaining = lambda: client.info('largeobj_disk')['largeobj_disk_previous_run_remaining_bytes']
    wait_for_equal(remaining, 0, timeout=timeout)


class TestLargeObjCleanup(ValkeyLargeObjTestCaseBase):
    """File-system cleanup across server lifetimes (Tiered mode).

    NVMe object files live on the module's dedicated ``disk-dir`` and must not
    accumulate across server lifetimes. Deleting them takes time proportional to the data, so it
    never blocks startup or shutdown:

    - graceful shutdown leaves object files in place;
    - startup moves each shard dir in ``disk-dir`` into ``disk-dir/previous-run/``
      under the next free number (one rename per shard; what earlier runs' unfinished
      cleanups left there stays), charges what it holds against ``disk-maxmemory``, and
      deletes it in the background — or inline, before any eviction, when a SET needs
      the room.

    Cleanup never touches entries of ``disk-dir`` that are not shard dirs, files in shards
    that are not object files, the parent ``testdir`` (the server's logfile, rdb), nor this
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
        wait_previous_run_deleted(client)
        assert self._object_files() == [], "previous run's files not deleted"

    def test_startup_deletes_orphaned_files(self):
        """Startup deletes what crashed runs left in their shards; other files survive.

        Plant files while the server is down: in a shard, in ``previous-run/`` from two
        earlier runs that crashed mid-cleanup, and ones that are not the module's (kept):
        loose in disk-dir, in a dir named like a shard, in a shard, and in testdir.
        """
        self.server.exit(cleanup=False)
        assert self._object_files() == [], "no object files expected before planting"

        planted = [
            os.path.join(self.data_dir, "0001", "0000000000000001.dat"),
            os.path.join(self.data_dir, "previous-run", "0", "0000000000000002.dat"),
            os.path.join(self.data_dir, "previous-run", "1", "0000000000000003.dat"),
        ]
        kept = [
            os.path.join(self.data_dir, "000000000000dead.dat"),
            os.path.join(self.data_dir, "not-a-normal-name"),
            os.path.join(self.data_dir, "deadbeef", "0000000000000004.dat"),
            os.path.join(self.testdir, "keepme.txt"),
        ]
        in_shard = os.path.join(self.data_dir, "0002", "keep.txt")
        for path in planted + kept + [in_shard]:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(b"stale")

        self.server.start()
        client = self.server.get_new_client()
        # The OID-1 path is free for this run straight away: the old shard was moved out.
        assert client.execute_command("BLOB.TCP_SET", "k", position_pattern(4096, "K")) == b"OK"
        wait_previous_run_deleted(client)

        # planted[0] is OID 1's path: it now holds this run's object, not the orphan.
        with open(planted[0], "rb") as f:
            assert f.read() != b"stale", "new object must not land on the orphan"
        for path in planted[1:]:
            assert not os.path.exists(path), f"orphan not deleted: {path}"
        for path in kept:
            assert os.path.exists(path), f"file the module did not create was deleted: {path}"
            os.remove(path)  # disk-dir is shared with the tests that follow
        # The shard holding it was moved, and is all that is left of previous-run/.
        assert len(glob.glob(os.path.join(self.data_dir, "previous-run", "*", "keep.txt"))) == 1
        shutil.rmtree(os.path.join(self.data_dir, "previous-run"))
        os.rmdir(os.path.join(self.data_dir, "deadbeef"))
        assert self._object_files() == [planted[0]], "only this run's object remains"
        assert client.execute_command("BLOB.TCP_GET", "k") == position_pattern(4096, "K")

    def test_restart_leaves_no_object_files(self):
        """End-to-end: data written before a restart does not linger."""
        client = self.server.get_new_client()
        for i in range(3):
            client.execute_command("BLOB.TCP_SET", f"rk{i}", position_pattern(4096, "Z"))
        assert len(self._object_files()) == 3

        self.server.restart()

        client = self.server.get_new_client()
        wait_previous_run_deleted(client)
        assert self._object_files() == [], "object files lingered across restart"
        assert os.path.exists(os.path.join(self.data_dir, DISK_DIR_MARKER))
        # A fresh server can still serve new writes.
        assert client.execute_command("BLOB.TCP_SET", "after", position_pattern(4096, "Q")) == b"OK"
        assert len(self._object_files()) == 1

    # A refused load leaks (see test_unaligned_chunk_size_refuses_to_load in
    # test_largeobj_tiered.py).
    @pytest.mark.skip_for_asan
    def test_refuses_a_disk_dir_it_did_not_create(self):
        """A non-empty disk-dir without the module's marker is not used, and nothing in it
        is touched, even files named like the module's."""
        testdir = os.path.abspath(os.path.join(self.testdir, 'unmarked'))
        disk_dir = os.path.join(testdir, 'nvme')
        user_file = os.path.join(disk_dir, '0000', '0000000000000001.dat')
        os.makedirs(os.path.dirname(user_file), exist_ok=True)
        with open(user_file, 'wb') as f:
            f.write(b'user data')
        server, _ = self.create_server(
            testdir=testdir,
            server_path=self.server.valkey_path,
            args={'loadmodule': f"{os.environ['MODULE_PATH']} {self.get_module_args(disk_dir, 'no')}"},
            connect_client=False)
        wait_for_true(lambda: server.server.poll() is not None)
        assert server.server.returncode != 0
        assert server.verify_string_in_logfile(f'has no {DISK_DIR_MARKER} file')
        assert os.listdir(disk_dir) == ['0000']
        with open(user_file, 'rb') as f:
            assert f.read() == b'user data'


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

        wait_previous_run_deleted(client, timeout=60)
        assert client.info('largeobj')['largeobj_evictions'] == 0, \
            "a live object was evicted to make room for the previous run's files"
        for i in range(32):
            assert client.execute_command("BLOB.TCP_GET", f"k{i}") == position_pattern(self.VALUE, i)
        assert len(self._object_files()) == 32
