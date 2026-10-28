import os
import shutil
import pytest
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase
from valkeytestframework.util.waiters import wait_for_equal


def _rejects_o_direct(path):
    """tmpfs refuses O_DIRECT with EINVAL, which makes it a stand-in for a filesystem
    that cannot serve the write path. Confirm it rather than assume it."""
    probe = os.path.join(path, "o-direct-check")
    try:
        fd = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_DIRECT, 0o644)
    except OSError:
        return True
    os.close(fd)
    os.unlink(probe)
    return False


NO_TMPFS = not os.path.isdir("/dev/shm") or not _rejects_o_direct("/dev/shm")


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
        wait_for_equal(lambda: client.info('memory').get('lazyfree_pending_objects', 0), 0)
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
        # The startup probe writes a real file to prove disk-dir can serve a write, so it
        # has to clean up after itself too: disk-dir is empty by the time clients arrive.
        assert self._object_files() == [], "startup left a file in disk-dir"

    @pytest.mark.skipif(NO_TMPFS, reason="needs a filesystem that rejects O_DIRECT")
    def test_a_disk_dir_that_cannot_serve_the_configured_io_refuses_the_load(self):
        """The startup probe performs the operations an object write needs. Point disk-dir
        at tmpfs with direct-io on and its O_DIRECT open fails, so the module refuses to
        load rather than starting a node whose every write would fail."""
        self.server.exit(cleanup=False)
        bad_dir = f"/dev/shm/largeobj-probe-{os.getpid()}"
        os.makedirs(bad_dir, exist_ok=True)
        good_args = self.server.args["loadmodule"]
        self.server.args["loadmodule"] = good_args.replace(
            f"disk-dir {self.data_dir}", f"disk-dir {bad_dir}"
        ).replace("direct-io no", "direct-io yes")
        try:
            with pytest.raises(RuntimeError):
                self.server.start()
            assert self.server.verify_string_in_logfile("not usable with direct-io=true")
        finally:
            shutil.rmtree(bad_dir, ignore_errors=True)
            self.server.args["loadmodule"] = good_args

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
