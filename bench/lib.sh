# shellcheck shell=bash
# Workload plugin support for bench.sh: loading bench/workloads/<name>.sh, and the shared
# checks workloads use. Sourced by bench.sh; not run on its own. See bench/workloads/README.md.
#
# Functions run against the bench server on $PORT and use bench.sh's globals (VALKEY_CLI,
# VALKEY_BENCH, BENCH_MODE, DISK_DIR, ...).

BENCH_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Print the workload names: get (built in) and every plugin.
available_workloads() {
    echo "get $(ls "$BENCH_LIB_DIR/workloads/" | sed -n 's/\.sh$//p' | xargs)"
}

# Source the plugin for $WORKLOAD. get is built into bench.sh and has no plugin.
source_workload() {
    [ "$WORKLOAD" = "get" ] && return 0
    local file="$BENCH_LIB_DIR/workloads/$WORKLOAD.sh"
    if [[ ! "$WORKLOAD" =~ ^[a-z0-9_-]+$ ]] || [ ! -f "$file" ]; then
        echo "ERROR: Unknown workload: $WORKLOAD (available: $(available_workloads))"
        exit 1
    fi
    # shellcheck source=/dev/null
    source "$file"
}

# For --help: print the options of the workload chosen with --workload, if any.
print_workload_usage() {
    if [ "$WORKLOAD" = "get" ]; then
        echo ""
        echo "Workloads: $(available_workloads)"
        return 0
    fi
    source_workload
    echo ""
    echo "Options for --workload $WORKLOAD:"
    workload_usage
}

# Load the workload and hand it the flags bench.sh didn't recognise. Args: those flags.
load_workload() {
    SERVER_LOG=/tmp/bench-server-$PORT.log
    SERVER_PIDFILE=/tmp/bench-server-$PORT.pid
    KEY_FORMAT="k:{i:012d}"    # get: matches valkey-benchmark's k:__rand_int__
    if [ "$WORKLOAD" = "get" ]; then
        if [ $# -gt 0 ]; then
            echo "Unknown option: $1"
            exit 1
        fi
        return 0
    fi
    source_workload
    KEY_FORMAT="${WORKLOAD_KEY_FORMAT:-$KEY_FORMAT}"
    # The fio baseline is for the get benchmark only.
    MODES_STR="${MODES_STR//fio/}"
    # shellcheck disable=SC2034  # read by bench.sh
    RUN_FIO=0
    workload_init "$@"
}

print_workload_header() {
    [ "$WORKLOAD" = "get" ] && return 0
    echo "Workload:       $WORKLOAD"
    if declare -F workload_header > /dev/null; then
        workload_header
    fi
}

# Run one pass of the workload on the started, populated server.
run_workload_pass() {
    # The server log is appended to across servers: checks read only this pass's part.
    PASS_LOG_START=$(stat -c %s "$SERVER_LOG" 2>/dev/null || echo 0)
    workload_run
}

# Shut the server down and wait for it to exit. With 32GB+ allocated, exit can take seconds.
stop_server() {
    $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
    for _ in $(seq 1 15); do
        if ! $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    pkill -9 -f "valkey-server.*port $PORT" 2>/dev/null || true
    sleep 2
}

# Print a FATAL message, stop the server, and exit the run. Args: message lines.
fail() {
    local line
    for line in "$@"; do
        echo "  FATAL: $line"
    done
    stop_server
    exit 1
}

# Print one INFO field. Args: section, field.
info_field() {
    $VALKEY_CLI -p $PORT INFO "$1" 2>/dev/null | tr -d '\r' | grep "^$2:" | cut -d: -f2
}

# Print the name of every key of the module's type in one database. Arg: db.
scan_blob_keys() {
    local cursor=0 reply
    while :; do
        reply=$($VALKEY_CLI -p $PORT -n "$1" SCAN $cursor COUNT 1000 TYPE largeob-k)
        cursor=$(echo "$reply" | head -1)
        echo "$reply" | tail -n +2 | grep . || true
        if [ "$cursor" = "0" ]; then
            break
        fi
    done
}

# Print the part of the server log written during this pass.
pass_log() {
    tail -c +$(( ${PASS_LOG_START:-0} + 1 )) "$SERVER_LOG" 2>/dev/null
}

# Print the useful parts of a server crash report: the signal, the main thread's stack,
# the command being executed, and any Rust panic.
print_crash() {
    local log
    log=$(pass_log)
    echo "$log" | grep -A1 "crashed by signal" | sed 's/^/     /' || true
    echo "$log" | awk '/ valkey-server \*$/,/^$/' | head -20 | sed 's/^/     /' || true
    echo "$log" | awk '/CURRENT CLIENT INFO/,/^$/' | grep -E "^argc|^argv" | sed 's/^/     /' || true
    echo "$log" | grep -A5 "panicked" | head -20 | sed 's/^/     /' || true
}

# Fail the run if the server crashed, the module panicked, or the server stopped answering.
assert_server_healthy() {
    if pass_log | grep -q -e "crashed by signal" -e "panicked" \
        || ! kill -0 "$(cat "$SERVER_PIDFILE" 2>/dev/null)" 2>/dev/null; then
        echo "  FATAL: Server crashed. From $SERVER_LOG:"
        print_crash
        exit 1
    fi
    if ! timeout 10 $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
        fail "Server is running but did not answer PING within 10s. Server hung."
    fi
}

# Fail the run if the module's accounting no longer matches the keyspace: every live blob
# is a key of the module's type in some database, and in Tiered/NVMe each owns a file in
# disk-dir. Workloads may have moved blobs between databases or set TTLs on them.
assert_accounting() {
    # SCAN deletes the already-expired keys it visits; PERSIST the rest so keys don't
    # expire while they are counted. Under import-mode or a paused expire, SCAN hides
    # expired keys without deleting them, so lift both first.
    $VALKEY_CLI -p $PORT CONFIG SET import-mode no > /dev/null 2>&1 || true
    $VALKEY_CLI -p $PORT CLIENT UNPAUSE > /dev/null 2>&1 || true
    local dbs db
    dbs=$($VALKEY_CLI -p $PORT CONFIG GET databases | tail -1)
    for db in $(seq 0 $(( dbs - 1 ))); do
        scan_blob_keys $db | sed 's/^/PERSIST /' | $VALKEY_CLI -p $PORT -n $db > /dev/null
    done

    # Deletes finish asynchronously: wait for evictions and lazy frees to drain.
    local pending lazy
    for _ in $(seq 1 30); do
        pending=$(info_field largeobj largeobj_pending_evictions)
        lazy=$(info_field memory lazyfree_pending_objects)
        if [ "${pending:-0}" = "0" ] && [ "${lazy:-0}" = "0" ]; then
            break
        fi
        sleep 1
    done
    # Undrained, a deleted key still counts as both a blob key and an object, so the counts
    # below could match and hide it.
    if [ "${pending:-0}" != "0" ] || [ "${lazy:-0}" != "0" ]; then
        fail "Deletes didn't drain within 30s: pending_evictions=${pending:-?}, lazyfree_pending_objects=${lazy:-?}."
    fi

    local objects blobs=0
    for db in $(seq 0 $(( dbs - 1 ))); do
        blobs=$(( blobs + $(scan_blob_keys $db | grep -c . || true) ))
    done
    objects=$(info_field largeobj largeobj_num_objects)
    echo "  Blob keys: $blobs, largeobj_num_objects: ${objects:-<missing>}, pending evictions: ${pending:-?}, lazyfree pending: ${lazy:-?}"
    if [ "$blobs" != "${objects:-}" ]; then
        fail "largeobj_num_objects ($objects) != blob keys across all databases ($blobs)."
    fi
    if [ "$BENCH_MODE" != "Dram" ]; then
        local files
        files=$(find "$DISK_DIR" -name "*.dat" | wc -l)
        echo "  Object files: $files"
        if [ "$files" -ne "${objects:-0}" ]; then
            fail "$files object files in $DISK_DIR != $objects objects."
        fi
    fi
}
