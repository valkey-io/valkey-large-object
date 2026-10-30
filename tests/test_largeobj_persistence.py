"""Persistence is not supported: rdb_save and aof_rewrite write nothing, and loading an RDB
yields placeholders that read as missing until the scaling cron deletes them.

Every command that reaches these callbacks must neither crash the server nor leave state a
later command could trip on. After each scenario, `assert_consistent` checks the server is
alive and logged no crash, frees and evictions drained, `num_objects` matches the blob keys
in every database, and (Tiered) the object files match the objects.
"""

import os
import pytest
from valkey import ResponseError
from valkeytestframework.util.waiters import wait_for_true
from valkey_largeobj_test_case import ValkeyLargeObjTestCaseBase, info_largeobj

BLOB_TYPE = 'largeob-k'
BLOBS = 4

DRAM_ARGS = (
    "operating-mode Dram"
    " segment-size 2097152"
    " max-object-size 1044480"
    " bench-mode no"
    " direct-io no"
    " chunk-size 4096"
    " fabric-provider Emulated"
)

# No scaling cron tick for a minute, so placeholders outlive the test.
SLOW_CRON = " scaling-poll-ms 60000 reaper-poll-ms 60000"


def blob(n):
    """A distinct payload spanning several 4KB chunks."""
    return bytes([n % 250 + 1]) * 3000 + bytes(range(256)) * 30


class PersistenceHelpers:

    @property
    def tiered(self):
        return 'Tiered' in self.get_module_args(self.data_dir, 'no')

    def populate(self, client):
        """BLOBS blob keys and as many plain keys. Returns the blobs by key name."""
        blobs = {}
        for i in range(BLOBS):
            key = f'blob:{i}'
            assert client.execute_command('BLOB.TCP_SET', key, blob(i)) == b'OK'
            blobs[key] = blob(i)
            client.set(f'plain:{i}', f'value{i}')
        return blobs

    def blob_keys(self, client):
        """Keys of the module's type across every database."""
        keys = []
        dbs = int(client.config_get('databases')['databases'])
        for db in range(dbs):
            client.execute_command('SELECT', db)
            cursor = 0
            while True:
                cursor, batch = client.execute_command('SCAN', cursor, 'COUNT', 1000, 'TYPE', BLOB_TYPE)
                keys.extend((db, k) for k in batch)
                if int(cursor) == 0:
                    break
        client.execute_command('SELECT', 0)
        return keys

    def pending_evictions(self, client):
        return info_largeobj(client)['largeobj_pending_evictions']

    def assert_no_crash(self, server):
        log = os.path.join(self.testdir, server.args['logfile'])
        with open(log, errors='replace') as f:
            text = f.read()
        assert 'crashed by signal' not in text, f'server crashed, see {log}'
        assert 'panicked' not in text, f'module panicked, see {log}'

    def assert_consistent(self, server, client, data_dir=None):
        assert client.ping()
        self.assert_no_crash(server)
        wait_for_true(lambda: self.pending_evictions(client) == 0, timeout=20)
        wait_for_true(lambda: client.info('memory').get('lazyfree_pending_objects', 0) == 0)
        info = info_largeobj(client)
        assert info['largeobj_num_objects'] == len(self.blob_keys(client))
        if self.tiered:
            files = os.listdir(data_dir or self.data_dir)
            assert len(files) == info['largeobj_num_objects']

    def assert_intact(self, client, blobs):
        for key, value in blobs.items():
            assert client.execute_command('BLOB.TCP_GET', key) == value
            assert client.execute_command('BLOB.INFO', key, 'LEN') == len(value)

    def assert_plain_intact(self, client):
        for i in range(BLOBS):
            assert client.get(f'plain:{i}') == f'value{i}'.encode()

    def assert_blobs_gone(self, client):
        """Loaded placeholders are deleted by the scaling cron; nothing of them remains."""
        wait_for_true(lambda: self.blob_keys(client) == [], timeout=20)
        for i in range(BLOBS):
            assert client.execute_command('BLOB.TCP_GET', f'blob:{i}') is None

    def wait_for_bgsave(self, client):
        wait_for_true(lambda: client.info('persistence')['rdb_bgsave_in_progress'] == 0)
        assert client.info('persistence')['rdb_last_bgsave_status'] == 'ok'

    def wait_for_aof_rewrite(self, client):
        wait_for_true(lambda: client.info('persistence')['aof_rewrite_in_progress'] == 0
                      and client.info('persistence')['aof_rewrite_scheduled'] == 0)

    def start_second_server(self, name):
        """Another server with the module loaded, its own disk-dir. Returns (server, client, data_dir)."""
        data_dir = os.path.abspath(os.path.join(self.testdir, f'nvme-{name}'))
        args = {
            'enable-debug-command': 'yes',
            'loadmodule': f"{os.getenv('MODULE_PATH')} {self.get_module_args(data_dir, 'no')}",
        }
        server_path = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{os.environ['SERVER_VERSION']}/valkey-server"
        server, client = self.create_server(testdir=self.testdir, server_path=server_path, args=args)
        return server, client, data_dir


class TestPersistenceTiered(ValkeyLargeObjTestCaseBase, PersistenceHelpers):
    """Commands that serialize blobs. Tiered here, Dram in the subclass."""

    def test_dump_returns_and_changes_nothing(self):
        client = self.server.get_new_client()
        blobs = self.populate(client)
        for key in blobs:
            assert client.execute_command('DUMP', key)
        self.assert_intact(client, blobs)
        self.assert_consistent(self.server, client)

    def test_restore_is_refused_and_changes_nothing(self):
        client = self.server.get_new_client()
        blobs = self.populate(client)
        payload = client.execute_command('DUMP', 'blob:0')

        with pytest.raises(ResponseError, match='Bad data format'):
            client.execute_command('RESTORE', 'restored', 0, payload)
        with pytest.raises(ResponseError, match='Bad data format'):
            client.execute_command('RESTORE', 'blob:1', 0, payload, 'REPLACE')
        assert client.exists('restored') == 0
        # Any client can send RESTORE, so it must not flood the log.
        log = os.path.join(self.testdir, self.server.args['logfile'])
        with open(log, errors='replace') as f:
            assert f.read().count('cannot RESTORE a blob') == 1
        self.assert_intact(client, blobs)
        self.assert_consistent(self.server, client)

    @pytest.mark.parametrize('options', [[], ['COPY'], ['REPLACE'], ['COPY', 'REPLACE']])
    def test_migrate_is_refused_and_keeps_the_source(self, options):
        client = self.server.get_new_client()
        blobs = self.populate(client)
        target, target_client, target_dir = self.start_second_server('migrate')
        host, port = target.bind_ip, target.port

        with pytest.raises(ResponseError, match='Bad data format'):
            client.execute_command('MIGRATE', host, port, 'blob:0', 0, 5000, *options)
        # Mixed with a plain key: the plain key moves, the blob stays put.
        with pytest.raises(ResponseError, match='Bad data format'):
            client.execute_command('MIGRATE', host, port, '', 0, 5000, *options, 'KEYS', 'blob:1', 'plain:1')
        assert target_client.get('plain:1') == b'value1'

        self.assert_intact(client, blobs)
        self.assert_consistent(self.server, client)
        assert self.blob_keys(target_client) == []
        self.assert_consistent(target, target_client, target_dir)

    def test_save_and_bgsave_succeed_and_change_nothing(self):
        client = self.server.get_new_client()
        blobs = self.populate(client)
        assert client.execute_command('SAVE')
        client.execute_command('BGSAVE')
        self.wait_for_bgsave(client)
        for key in blobs:
            assert b'serializedlength' in client.execute_command('DEBUG', 'OBJECT', key)
        self.assert_intact(client, blobs)
        self.assert_consistent(self.server, client)

    @pytest.mark.parametrize('preamble', ['yes', 'no'])
    def test_aof_rewrite_succeeds_and_changes_nothing(self, preamble):
        client = self.server.get_new_client()
        client.config_set('aof-use-rdb-preamble', preamble)
        client.config_set('appendonly', 'yes')
        self.wait_for_aof_rewrite(client)
        blobs = self.populate(client)
        client.execute_command('BGREWRITEAOF')
        self.wait_for_aof_rewrite(client)
        assert client.info('persistence')['aof_last_bgrewrite_status'] == 'ok'
        self.assert_intact(client, blobs)
        self.assert_consistent(self.server, client)

    def test_restart_from_an_rdb_with_blobs_starts_and_drops_them(self):
        client = self.server.get_new_client()
        self.populate(client)
        assert client.execute_command('BLOB.TCP_SET', 'expiring', blob(9)) == b'OK'
        client.pexpire('expiring', 100)
        assert client.execute_command('BLOB.TCP_SET', 'ttl', blob(8)) == b'OK'
        client.expire('ttl', 3600)
        client.execute_command('SAVE')
        wait_for_true(lambda: client.exists('expiring') == 0)

        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        self.assert_plain_intact(client)
        self.assert_blobs_gone(client)
        assert client.exists('ttl') == 0
        self.assert_consistent(self.server, client)

        # The names are free for new blobs.
        assert client.execute_command('BLOB.TCP_SET', 'blob:0', blob(5)) == b'OK'
        assert client.execute_command('BLOB.TCP_GET', 'blob:0') == blob(5)
        self.assert_consistent(self.server, client)

    def test_debug_reload_drops_blobs_and_keeps_the_rest(self):
        client = self.server.get_new_client()
        self.populate(client)
        assert client.execute_command('DEBUG', 'RELOAD')
        self.assert_plain_intact(client)
        self.assert_blobs_gone(client)
        self.assert_consistent(self.server, client)
        assert client.execute_command('BLOB.TCP_SET', 'blob:0', blob(5)) == b'OK'
        assert client.execute_command('BLOB.TCP_GET', 'blob:0') == blob(5)

    @pytest.mark.parametrize('preamble', ['yes', 'no'])
    def test_aof_load_drops_blobs_and_replays_commands_on_them(self, preamble):
        client = self.server.get_new_client()
        client.config_set('aof-use-rdb-preamble', preamble)
        client.config_set('appendonly', 'yes')
        self.wait_for_aof_rewrite(client)
        self.populate(client)
        client.execute_command('BGREWRITEAOF')
        self.wait_for_aof_rewrite(client)
        # Logged to the AOF after the rewrite, so replayed against the loaded placeholders
        # (preamble) or against missing keys (no preamble).
        client.delete('blob:0')
        client.rename('blob:1', 'renamed')
        client.expire('blob:2', 3600)
        assert client.execute_command('COPY', 'blob:3', 'copied') == 1
        client.execute_command('SELECT', 0)
        client.move('plain:0', 1)
        client.execute_command('SELECT', 1)
        client.move('plain:0', 0)
        client.execute_command('SELECT', 0)

        assert client.execute_command('DEBUG', 'LOADAOF')
        self.assert_plain_intact(client)
        self.assert_blobs_gone(client)
        assert client.exists('renamed', 'copied') == 0
        self.assert_consistent(self.server, client)

    @pytest.mark.parametrize('diskless_sync,diskless_load,dual_channel', [
        ('no', 'disabled', 'no'), ('yes', 'disabled', 'no'), ('yes', 'swapdb', 'no'),
        ('yes', 'on-empty-db', 'no'), ('yes', 'disabled', 'yes'), ('yes', 'swapdb', 'yes'),
    ])
    def test_replica_full_sync_drops_blobs_and_keeps_the_primary(self, diskless_sync, diskless_load, dual_channel):
        client = self.server.get_new_client()
        blobs = self.populate(client)
        client.config_set('repl-diskless-sync', diskless_sync)
        client.config_set('repl-diskless-sync-delay', 0)
        client.config_set('dual-channel-replication-enabled', dual_channel)
        replica, replica_client, replica_dir = self.start_second_server('replica')
        replica_client.config_set('repl-diskless-load', diskless_load)
        replica_client.config_set('dual-channel-replication-enabled', dual_channel)

        replica_client.execute_command('REPLICAOF', self.server.bind_ip, self.server.port)
        wait_for_true(lambda: replica_client.info('replication')['master_link_status'] == 'up', timeout=30)
        self.assert_plain_intact(replica_client)
        self.assert_blobs_gone(replica_client)
        self.assert_consistent(replica, replica_client, replica_dir)

        self.assert_intact(client, blobs)
        self.assert_consistent(self.server, client)
        replica_client.execute_command('REPLICAOF', 'NO', 'ONE')


class TestPersistenceDram(TestPersistenceTiered):

    def get_module_args(self, data_dir, direct_io):
        return DRAM_ARGS


class TestLoadedPlaceholdersTiered(ValkeyLargeObjTestCaseBase, PersistenceHelpers):
    """With the scaling cron held off, placeholders loaded from an RDB stay in the keyspace:
    every command must treat them as missing, and whatever frees them must not strand their
    oids on the reclaim list."""

    def get_module_args(self, data_dir, direct_io):
        return super().get_module_args(data_dir, direct_io) + SLOW_CRON

    def restart_with_placeholders(self):
        client = self.server.get_new_client()
        self.populate(client)
        assert client.execute_command('BLOB.TCP_SET', 'expiring', blob(9)) == b'OK'
        client.pexpire('expiring', 100)
        client.execute_command('SAVE')
        wait_for_true(lambda: client.exists('expiring') == 0)
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        # The expired blob was dropped while loading, freeing its placeholder at once.
        assert self.pending_evictions(client) == BLOBS
        assert len(self.blob_keys(client)) == BLOBS
        return client

    def test_placeholders_read_as_missing(self):
        client = self.restart_with_placeholders()
        for i in range(BLOBS):
            key = f'blob:{i}'
            assert client.exists(key) == 1
            assert client.type(key) == BLOB_TYPE.encode()
            assert client.execute_command('BLOB.TCP_GET', key) is None
            assert client.execute_command('BLOB.INFO', key) is None
            assert client.execute_command('BLOB.INFO', key, 'TIER') is None
            with pytest.raises(ResponseError):
                client.execute_command('COPY', key, f'copy:{i}')
            assert client.exists(f'copy:{i}') == 0
            assert client.execute_command('DUMP', key)
            with pytest.raises(ResponseError, match='Bad data format'):
                client.execute_command('RESTORE', f'restored:{i}', 0, client.execute_command('DUMP', key))
            assert client.memory_usage(key) > 0
            assert b'serializedlength' in client.execute_command('DEBUG', 'OBJECT', key)
            assert client.execute_command('DEBUG', 'DIGEST-VALUE', key)
            client.expire(key, 3600)
            client.persist(key)
        assert client.ping()
        self.assert_plain_intact(client)
        self.assert_no_crash(self.server)

    def test_freeing_placeholders_takes_them_off_the_reclaim_list(self):
        client = self.restart_with_placeholders()

        client.delete('blob:0')
        wait_for_true(lambda: self.pending_evictions(client) == BLOBS - 1)

        assert client.execute_command('BLOB.TCP_SET', 'blob:1', blob(7)) == b'OK'
        assert client.execute_command('BLOB.TCP_GET', 'blob:1') == blob(7)
        wait_for_true(lambda: self.pending_evictions(client) == BLOBS - 2)

        client.rename('blob:2', 'renamed')
        client.move('renamed', 1)
        client.execute_command('SELECT', 1)
        client.execute_command('FLUSHDB', 'ASYNC')
        client.execute_command('SELECT', 0)
        wait_for_true(lambda: self.pending_evictions(client) == BLOBS - 3)

        client.pexpire('blob:3', 1)
        wait_for_true(lambda: self.pending_evictions(client) == 0)
        self.assert_consistent(self.server, client)
        assert self.blob_keys(client) == [(0, b'blob:1')]

    def test_persisting_placeholders_succeeds(self):
        client = self.restart_with_placeholders()
        assert client.execute_command('SAVE')
        client.execute_command('BGSAVE')
        self.wait_for_bgsave(client)
        client.config_set('appendonly', 'yes')
        self.wait_for_aof_rewrite(client)
        assert client.info('persistence')['aof_last_bgrewrite_status'] == 'ok'
        assert client.execute_command('DEBUG', 'RELOAD')
        assert self.pending_evictions(client) == BLOBS
        self.assert_plain_intact(client)

        client.flushall()
        self.assert_consistent(self.server, client)


class TestLoadedPlaceholdersDram(TestLoadedPlaceholdersTiered):

    def get_module_args(self, data_dir, direct_io):
        return DRAM_ARGS + SLOW_CRON
