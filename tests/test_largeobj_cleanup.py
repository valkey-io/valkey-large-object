import os
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal


class TestLargeObjCleanup(ValkeyLargeObjTestCaseBase):
    """File-system cleanup on teardown and startup (Tiered mode).

    NVMe object files live on the module's dedicated ``disk-dir`` and must not
    accumulate across server lifetimes. These tests exercise the two cleanup
    paths:

    - graceful shutdown (SHUTDOWN command / SIGTERM / SIGINT) removes every
      object file this instance created;
    - startup purges files orphaned by a previous run that could not clean up
      in-process (the crash path).

    The purge wipes ``disk-dir`` wholesale (name-agnostically) but never touches
    its parent ``testdir`` — where the server keeps its own files (logfile, rdb).
    So anything the fixture or the server places outside ``disk-dir`` must
    survive a purge. The base fixture points ``self.data_dir`` at that dedicated
    ``disk-dir``.
    """

    def test_shutdown_clean_after_set_and_delete(self):
        """A graceful SHUTDOWN deletes every object file from disk-dir, whether
        it was left by a SET or partially cleared by an async DEL."""
        client = self.server.get_new_client()
        client.execute_command("BLOB.TCP_SET", "a", b"A" * 4096)
        client.execute_command("BLOB.TCP_SET", "b", b"B" * 4096)
        client.execute_command("BLOB.TCP_SET", "c", b"C" * 4096)
        # DEL frees asynchronously (BIO thread); wait for completion.
        client.execute_command("DEL", "b")
        wait_for_equal(lambda: client.info('stats').get('lazyfree_pending_objects', 0), 0)
        assert len(self._object_files()) == 2

        # exit() issues SHUTDOWN NOSAVE, which fires the Shutdown server event
        # our handler subscribes to. cleanup=False so the framework does not
        # touch testdir — the module is solely responsible for the .dat files.
        self.server.exit(cleanup=False)
        assert self._object_files() == [], "shutdown did not clean remaining files"

    def test_startup_purges_orphaned_files(self):
        """Startup wipes crash-orphaned files in disk-dir; files outside it survive.

        Simulate a hard crash (no in-process cleanup) by planting files in
        disk-dir while the server is down — including an oddly-named one to show
        the purge is name-agnostic — plus an unrelated file in testdir itself
        (outside disk-dir). On restart, initialize() must clear disk-dir but
        never touch anything in its parent.
        """
        # Stop the server without letting the shutdown handler clean up first,
        # so we control exactly what is on disk before the next start.
        self.server.exit(cleanup=False)
        assert self._object_files() == [], "no object files expected before planting"

        os.makedirs(self.data_dir, exist_ok=True)
        orphan_dat = os.path.join(self.data_dir, "000000000000dead.dat")
        orphan_tmp = os.path.join(self.data_dir, "000000000000beef.dat.tmp")
        orphan_misc = os.path.join(self.data_dir, "not-a-normal-name")  # name-agnostic
        # Foreign file OUTSIDE disk-dir (in testdir) — must survive the purge.
        foreign = os.path.join(self.testdir, "keepme.txt")
        for path in (orphan_dat, orphan_tmp, orphan_misc):
            with open(path, "wb") as f:
                f.write(b"stale")
        with open(foreign, "wb") as f:
            f.write(b"do not delete")

        # Restart: the module's startup purge runs during initialize().
        self.server.start()

        assert not os.path.exists(orphan_dat), "orphaned .dat not purged at startup"
        assert not os.path.exists(orphan_tmp), "orphaned .dat.tmp not purged at startup"
        assert not os.path.exists(orphan_misc), "arbitrarily-named object not purged"
        assert os.path.exists(foreign), "file outside disk-dir must be preserved"

    def test_restart_leaves_no_object_files(self):
        """End-to-end: data written before a restart does not linger."""
        client = self.server.get_new_client()
        for i in range(3):
            client.execute_command("BLOB.TCP_SET", f"rk{i}", b"Z" * 4096)
        assert len(self._object_files()) == 3

        # restart() = graceful exit (handler purges) + fresh start (startup purge).
        self.server.restart()

        assert self._object_files() == [], "object files lingered across restart"
        # A fresh server can still serve new writes.
        client = self.server.get_new_client()
        assert client.execute_command("BLOB.TCP_SET", "after", b"Q" * 4096) == b"OK"
        assert len(self._object_files()) == 1
