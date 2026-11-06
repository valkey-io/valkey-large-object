import os
import pytest
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, position_pattern
from valkeytestframework.util.waiters import wait_for_equal, wait_for_true


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
        client.execute_command("BLOB.TCP_SET", "a", position_pattern(4096, "A"))
        client.execute_command("BLOB.TCP_SET", "b", position_pattern(4096, "B"))
        client.execute_command("BLOB.TCP_SET", "c", position_pattern(4096, "C"))
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

    def test_restart_leaves_no_object_files(self):
        """End-to-end: data written before a restart does not linger."""
        client = self.server.get_new_client()
        for i in range(3):
            client.execute_command("BLOB.TCP_SET", f"rk{i}", position_pattern(4096, "Z"))
        assert len(self._object_files()) == 3

        # restart() = graceful exit (handler purges) + fresh start (startup purge).
        self.server.restart()

        assert self._object_files() == [], "object files lingered across restart"
        # A fresh server can still serve new writes.
        client = self.server.get_new_client()
        assert client.execute_command("BLOB.TCP_SET", "after", position_pattern(4096, "Q")) == b"OK"
        assert len(self._object_files()) == 1

    # A refused load makes Valkey drop its reference to the config data the valkey-module
    # crate boxed for each config, and nothing frees it, so LeakSanitizer reports it at exit.
    # That is not a leak in this module's code, so the ASAN job skips this test.
    @pytest.mark.skip_for_asan
    @pytest.mark.parametrize('case', ['missing', 'unwritable'])
    def test_refuses_to_load_when_disk_dir_cannot_be_reset(self, case):
        """Tiered mode refuses to load when the startup purge cannot run, rather than
        start on a dirty disk-dir. Two reasons: no disk-dir given, or one that cannot be
        reset (its parent is read-only, so its entries cannot be unlinked). The log names
        the reason, and the module args are otherwise identical to the ones that load."""
        self.server.exit(cleanup=False)
        # Absolute, as the base fixture does: the server resolves a relative disk-dir
        # against its own cwd, which is not this directory.
        testdir = os.path.abspath(os.path.join(self.testdir, f'no-reset-{case}'))
        os.makedirs(testdir, exist_ok=True)
        if case == 'missing':
            disk_dir = ''
            reason = 'failed to reset disk-dir : disk-dir is required in Tiered operating mode'
        else:
            parent = os.path.join(testdir, 'ro')
            disk_dir = os.path.join(parent, 'nvme')
            if os.path.exists(parent):  # a previous run's leftover; it may still be read-only
                os.chmod(parent, 0o755)
            os.makedirs(disk_dir, exist_ok=True)
            with open(os.path.join(disk_dir, 'stale.dat'), 'wb') as f:
                f.write(b'stale')
            os.chmod(parent, 0o555)
            # remove_dir_all cannot unlink stale.dat: the message names the dir and EACCES.
            reason = f'failed to reset disk-dir {disk_dir}: Permission denied'
        try:
            module_args = self.get_module_args(disk_dir, 'no')
            if not disk_dir:
                # Leave disk-dir out entirely; an empty value would swallow the next token.
                module_args = module_args.replace(' disk-dir ', ' ', 1)
            server, _ = self.create_server(
                testdir=testdir,
                server_path=self.server_path,
                args={'loadmodule': f"{os.environ['MODULE_PATH']} {module_args}"},
                connect_client=False,
            )
            wait_for_true(lambda: server.server.poll() is not None)
            assert server.server.returncode != 0
            assert server.verify_string_in_logfile('startup failed to reset disk-dir')
            assert server.verify_string_in_logfile(reason)
        finally:
            if case == 'unwritable':
                os.chmod(parent, 0o755)
