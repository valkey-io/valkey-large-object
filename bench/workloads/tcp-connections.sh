# shellcheck shell=bash
# Workload: tcp-connections. Sourced by bench.sh for --workload tcp-connections; see
# bench/workloads/README.md.
#
# A high rate of new connections against blob reads and writes over the TCP commands:
# valkey-benchmark with -k 0 opens a fresh connection for every request, so clients keep
# disconnecting while their BLOB.TCP_GET reply is still being prepared off the main
# thread, or while their BLOB.TCP_SET is still committing. Then asserts the server
# survived, every blob is still there, and the module's accounting still holds.
#
# TCP only: it doesn't touch the EFA commands (BLOB.RDMA_*) or their per-connection DMA
# sessions. Those need a libfabric peer holding the client's memory and a HELLO on every
# connection, which valkey-benchmark can't do; they get their own workload.

# Real GET replies, so disconnects race a full bulk reply instead of the size only.
# shellcheck disable=SC2034  # read by bench.sh
WORKLOAD_BENCH_MODE="no"

TCP_CONNECTIONS_DURATION=60        # seconds of reconnect churn per pass
TCP_CONNECTIONS_COMMANDS="get,set"  # BLOB.TCP_GET and/or BLOB.TCP_SET
TCP_CONNECTIONS_SET_BYTES=65536     # SET value size cap: the value goes on the command line

workload_usage() {
    echo "  --tcp-connections-duration <SEC>  Seconds of reconnect churn per pass (default: 60)"
    echo "  --tcp-connections-commands <LIST> get, set, or get,set (default: get,set); --clients is split across them"
}

workload_init() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --tcp-connections-duration) TCP_CONNECTIONS_DURATION="$2"; shift 2 ;;
            --tcp-connections-commands) TCP_CONNECTIONS_COMMANDS="$2"; shift 2 ;;
            *) echo "Unknown option for --workload tcp-connections: $1"; exit 1 ;;
        esac
    done
    local cmd cmds
    IFS=, read -r -a cmds <<< "$TCP_CONNECTIONS_COMMANDS"
    for cmd in "${cmds[@]}"; do
        case "$cmd" in
            get|set) ;;
            *) echo "ERROR: --tcp-connections-commands takes get and/or set, got: $cmd"; exit 1 ;;
        esac
    done
}

workload_header() {
    echo "TCP connections: reconnect per request, ${TCP_CONNECTIONS_DURATION}s per pass, commands=$TCP_CONNECTIONS_COMMANDS"
}

workload_run() {
    local cmds
    IFS=, read -r -a cmds <<< "$TCP_CONNECTIONS_COMMANDS"
    local clients=$(( EFFECTIVE_CLIENTS / ${#cmds[@]} ))
    [ $clients -lt 1 ] && clients=1
    local set_bytes=$(( BYTES < TCP_CONNECTIONS_SET_BYTES ? BYTES : TCP_CONNECTIONS_SET_BYTES ))
    local value
    value=$(head -c "$set_bytes" /dev/zero | tr '\0' 'v')

    # One valkey-benchmark per command, run side by side. -r matches the populated keys
    # (k:<12-digit index>), so SETs overwrite existing blobs and the count stays fixed.
    local cmd pids=() outs=()
    for cmd in "${cmds[@]}"; do
        local out
        out=$(mktemp /tmp/bench-connections-XXXXXX)
        outs+=("$out")
        local blob_cmd=(BLOB.TCP_GET "k:__rand_int__")
        [ "$cmd" = "set" ] && blob_cmd=(BLOB.TCP_SET "k:__rand_int__" "$value")
        timeout $(( TCP_CONNECTIONS_DURATION + 30 )) \
            taskset -c $BENCH_CPUS \
            $VALKEY_BENCH -p $PORT -k 0 --duration "$TCP_CONNECTIONS_DURATION" -c $clients -r $EFFECTIVE_KEYS -q \
            -- "${blob_cmd[@]}" > "$out" 2>&1 &
        pids+=($!)
    done

    local i rc
    for i in "${!pids[@]}"; do
        rc=0
        wait "${pids[$i]}" || rc=$?
        echo "  ┌─ ${cmds[$i]}: reconnect per request, $clients clients, ${TCP_CONNECTIONS_DURATION}s (exit $rc)"
        # valkey-benchmark echoes the whole command, SET value included: keep the numbers.
        tr '\r' '\n' < "${outs[$i]}" | grep -o -e '[0-9.]* requests per second.*' -e 'Error.*' \
            | tail -2 | cut -c1-160 | sed 's/^/  │ /' || true
        rm -f "${outs[$i]}"
        if [ $rc -eq 124 ]; then
            fail "${cmds[$i]} benchmark did not finish within $(( TCP_CONNECTIONS_DURATION + 30 ))s."
        fi
    done

    assert_server_healthy
    assert_accounting
    local objects
    objects=$(info_field largeobj largeobj_num_objects)
    if [ "${objects:-}" != "$EFFECTIVE_KEYS" ]; then
        fail "Expected all $EFFECTIVE_KEYS populated blobs to survive, found ${objects:-<missing>}."
    fi
}
