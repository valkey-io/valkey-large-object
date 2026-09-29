import os
import glob
import pytest
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import logging


def info_largeobj(client):
    """Return the largeobj INFO section as a dict with integer values decoded."""
    raw = client.execute_command('INFO', 'largeobj')
    result = {}
    for k, v in raw.items():
        key = k.decode() if isinstance(k, bytes) else k
        val = v.decode() if isinstance(v, bytes) else str(v)
        result[key] = int(val) if val.lstrip('-').isdigit() else val
    return result


class ValkeyLargeObjTestCaseBase(ValkeyTestCase):
    """Base test class for valkey-largeobj module integration tests.

    Uses valkey-test-framework (same pattern as valkey-bloom).
    Spawns a valkey-server with the module loaded per test.

    Subclasses can override get_module_args() to customize config.

    Env vars:
        MODULE_PATH: path to libvalkey_largeobj.so
        SERVER_VERSION: valkey-server version directory name
    """

    def get_module_args(self, data_dir, direct_io):
        """Override in subclasses to customize module load args.
        Default: Tiered mode with 1MB pools (small, suitable for basic tests).
        Note: direct-io forced to 'no' because test dirs may be on tmpfs.
        """
        return (
            f"operating-mode Tiered"
            f" nvme-dir {data_dir}"
            f" nvme-staging-size 1048576"
            f" segment-size 1048576"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
        )

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        module_path = os.getenv('MODULE_PATH')
        # Give the module a DEDICATED nvme-dir under testdir (not testdir itself).
        # The module owns this directory outright and wipes it wholesale on startup
        # and teardown, so it must not be shared with the server's own files
        # (logfile, rdb) which live directly in testdir. Absolute path so file
        # assertions work regardless of cwd.
        data_dir = os.path.abspath(os.path.join(self.testdir, "nvme"))
        # Disable O_DIRECT in ASAN builds — ASAN tests focus on memory safety,
        # not I/O bypass correctness. Avoids EINVAL from O_DIRECT alignment edge cases.
        direct_io = "no" if os.environ.get("ASAN_BUILD") else "yes"
        module_args = self.get_module_args(data_dir, direct_io)
        args = {
            'enable-debug-command': 'yes',
            'loadmodule': f"{module_path} {module_args}",
        }
        server_path = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{os.environ['SERVER_VERSION']}/valkey-server"
        self.server, self.client = self.create_server(
            testdir=self.testdir,
            server_path=server_path,
            args=args,
        )
        self.data_dir = data_dir
        logging.info("startup args are: %s", args)

    def _object_files(self):
        """Every file currently in nvme-dir (name-agnostic)."""
        return sorted(glob.glob(os.path.join(self.data_dir, "*")))

    def subscribe_keyspace_events(self, client):
        """Enable module keyspace events and return a pubsub on the keyevent channels."""
        client.execute_command('CONFIG', 'SET', 'notify-keyspace-events', 'KEA')
        pubsub = client.pubsub()
        pubsub.psubscribe('__keyevent@*__:largeobj.*')
        assert pubsub.get_message(timeout=1)['type'] == 'psubscribe'
        return pubsub

    def read_keyspace_events(self, pubsub, count):
        """Read up to `count` events as (event, key) tuples."""
        events = []
        for _ in range(count):
            msg = pubsub.get_message()
            if msg is None:
                break
            event = msg['channel'].decode().split(':', 1)[1]
            events.append((event, msg['data'].decode()))
        return events

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False, f"Expected error but command succeeded"
        except ResponseError as e:
            assert str(e) == expected_err_reply, (
                f"Actual error '{str(e)}' != expected '{expected_err_reply}'"
            )
            return str(e)
