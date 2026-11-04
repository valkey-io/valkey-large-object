import array
import os
import errno
import functools
import glob
import sys
import tempfile
import pytest
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkey import ResponseError
import logging


@functools.lru_cache(maxsize=None)
def direct_io_supported(directory):
    """Whether files in `directory` can be opened with O_DIRECT. Filesystems that reject
    it (tmpfs before Linux 6.6) fail the open with EINVAL."""
    fd, path = tempfile.mkstemp(dir=directory)
    os.close(fd)
    try:
        os.close(os.open(path, os.O_RDONLY | os.O_DIRECT))
        return True
    except OSError as e:
        if e.errno != errno.EINVAL:
            raise
        return False
    finally:
        os.unlink(path)


_TAG_STRIDE = 0x9E3779B1  # odd, so distinct tags give distinct offsets mod 2**32


def position_pattern(length, tag=0):
    """`length` bytes of 4-byte little-endian words, word i = i + tag * _TAG_STRIDE (mod 2**32).

    Every position differs, so bytes returned at the wrong offset cannot compare equal; and
    two different tags differ at every word, so objects written with different tags (an
    overwrite, one key per tag) cannot be mistaken for each other. `tag` is an int, or a
    one-character str standing for its code point ('A' == 65). Tag 0 matches
    `generate_pattern` in tests/harness/fabric_target.rs."""
    if isinstance(tag, str):
        tag = ord(tag)
    words = (length + 3) // 4
    start = tag * _TAG_STRIDE % 2**32
    end = start + words
    pattern = array.array('I', range(start, min(end, 2**32)))
    pattern.extend(range(0, max(end - 2**32, 0)))
    if sys.byteorder == 'big':
        pattern.byteswap()
    return pattern.tobytes()[:length]


def assert_bytes_equal(actual, expected, chunk_size):
    """Fail at the first `chunk_size` piece of `actual` that differs from `expected`.

    For an untagged `position_pattern` payload the message names the chunk whose bytes are
    there instead. Not `assert actual == expected`: pytest's diff of two large byte strings
    does not finish."""
    if len(actual) != len(expected):
        pytest.fail(f"got {len(actual)} bytes, expected {len(expected)}")
    for offset in range(0, len(expected), chunk_size):
        piece = actual[offset:offset + chunk_size]
        if piece != expected[offset:offset + chunk_size]:
            found = int.from_bytes(piece[:4], 'little') * 4 // chunk_size
            pytest.fail(f"chunk {offset // chunk_size} holds the bytes of chunk {found}")


MIB = 1024 * 1024
# Chunk-order tests: an object of many chunks read from disk, where reads complete out of
# order. 65 chunks, the last one partial.
ORDER_CHUNK_SIZE = 64 * 1024
ORDER_OBJECT_LEN = 64 * ORDER_CHUNK_SIZE + 1
# One segment holds every object a chunk-order test writes (10 of ~4 MiB): a promotion
# that finds the DRAM pool full is skipped while the pool grows in the background.
ORDER_SEGMENT_SIZE = 64 * MIB


def chunk_order_module_args(data_dir, direct_io, max_promote_size):
    """Tiered module args for chunk-order tests; `max_promote_size` 0 disables promotion.

    Skips without O_DIRECT: page-cache reads complete in submit order, so the test could
    not catch a reply assembled in completion order."""
    if direct_io != "yes":
        pytest.skip("needs O_DIRECT on the test filesystem")
    return (
        f"operating-mode Tiered"
        f" disk-dir {data_dir}"
        f" disk-staging-size {ORDER_SEGMENT_SIZE}"
        f" segment-size {ORDER_SEGMENT_SIZE}"
        f" promote-min-hits 1"
        f" max-promote-size {max_promote_size}"
        f" max-object-size {8 * MIB}"
        f" bench-mode no"
        f" direct-io yes"
        f" chunk-size {ORDER_CHUNK_SIZE}"
        f" fabric-provider Emulated"
        f" fabric-interfaces lo"
    )


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
        MODULE_PATH: path to libvalkey_large_object.so
        SERVER_VERSION: valkey-server version directory name
    """

    def get_module_args(self, data_dir, direct_io):
        """Override in subclasses to customize module load args.
        Default: Tiered mode with 1MB pools (small, suitable for basic tests).
        Note: direct-io forced to 'no' because test dirs may be on tmpfs.
        """
        return (
            f"operating-mode Tiered"
            f" disk-dir {data_dir}"
            f" disk-staging-size 1048576"
            f" segment-size 1048576"
            f" max-promote-size 520192"
            f" bench-mode no"
            f" direct-io no"
            f" chunk-size 4096"
            f" fabric-provider Emulated"
        )

    def get_server_args(self):
        """Override in subclasses to add valkey-server args (e.g. maxclients)."""
        return {}

    @pytest.fixture(autouse=True)
    def setup_test(self, setup):
        module_path = os.getenv('MODULE_PATH')
        # Give the module a DEDICATED disk-dir under testdir (not testdir itself).
        # The module owns this directory outright and wipes it wholesale on startup
        # and teardown, so it must not be shared with the server's own files
        # (logfile, rdb) which live directly in testdir. Absolute path so file
        # assertions work regardless of cwd.
        data_dir = os.path.abspath(os.path.join(self.testdir, "nvme"))
        # Disable O_DIRECT in ASAN builds — ASAN tests focus on memory safety,
        # not I/O bypass correctness. Avoids EINVAL from O_DIRECT alignment edge cases.
        # Also off where the test filesystem rejects it (tmpfs).
        direct_io = (
            "yes"
            if not os.environ.get("ASAN_BUILD") and direct_io_supported(self.testdir)
            else "no"
        )
        module_args = self.get_module_args(data_dir, direct_io)
        args = {
            'enable-debug-command': 'yes',
            'loadmodule': f"{module_path} {module_args}",
            **self.get_server_args(),
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
        """Every file currently in disk-dir (name-agnostic)."""
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
