# shellcheck shell=bash
# Workload: fuzz. Sourced by bench.sh for --workload fuzz; see bench/workloads/README.md.
#
# Runs valkey-benchmark --fuzz (Valkey 9.1+) against keys holding blobs, so the fuzzer's
# generic commands (DEL, COPY, RENAME, MOVE, SWAPDB, EXPIRE, OBJECT, MEMORY USAGE, Lua,
# MULTI/EXEC, wrong-type commands) land on blobs. Then asserts the server survived and the
# module's accounting still holds.

# The fuzzer's generic commands pick key names key:<n> for n < -r.
# shellcheck disable=SC2034  # read by bench.sh
WORKLOAD_KEY_FORMAT="key:{i}"
# Real GET replies; bench-mode would reply with the size only.
# shellcheck disable=SC2034  # read by bench.sh
WORKLOAD_BENCH_MODE="no"

FUZZ_REQUESTS=100000    # total fuzzer commands, split across --clients
FUZZ_MODE=""            # empty = valid commands only
FUZZ_DURATION=300       # seconds per pass; the fuzzer is stopped at this limit

workload_usage() {
    echo "  --fuzz-requests <N>        Total commands across all clients (default: 100000)"
    echo "  --fuzz-mode <modes>        valkey-benchmark --fuzz-mode, e.g. malformed-commands,config-commands"
    echo "  --fuzz-duration <SEC>      Stop each pass after this long (default: 300)"
}

workload_init() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --fuzz-requests) FUZZ_REQUESTS="$2"; shift 2 ;;
            --fuzz-mode)     FUZZ_MODE="$2"; shift 2 ;;
            --fuzz-duration) FUZZ_DURATION="$2"; shift 2 ;;
            *) echo "Unknown option for --workload fuzz: $1"; exit 1 ;;
        esac
    done
    if ! $VALKEY_BENCH --help 2>&1 | grep -q -- "--fuzz"; then
        echo "ERROR: $VALKEY_BENCH has no --fuzz (needs valkey-benchmark 9.1+)"
        exit 1
    fi
}

workload_header() {
    echo "Fuzz requests:  $FUZZ_REQUESTS"
    echo "Fuzz mode:      ${FUZZ_MODE:-valid-commands}"
    echo "Fuzz duration:  up to ${FUZZ_DURATION}s per pass"
}

workload_run() {
    local out
    out=$(mktemp /tmp/bench-fuzz-XXXXXX)
    local fuzz_args=(--fuzz --fuzz-loglevel error -n "$FUZZ_REQUESTS" -c "$EFFECTIVE_CLIENTS" -r "$EFFECTIVE_KEYS")
    if [ -n "$FUZZ_MODE" ]; then
        fuzz_args+=(--fuzz-mode "$FUZZ_MODE")
    fi
    local fuzz_exit=0
    local start=$SECONDS
    timeout $FUZZ_DURATION \
        taskset -c $BENCH_CPUS \
        $VALKEY_BENCH -p $PORT "${fuzz_args[@]}" > "$out" 2>&1 || fuzz_exit=$?
    local ran="$((SECONDS - start))s"
    # Hitting the time limit is fine (malformed-commands spends most of its time
    # reconnecting); the server only has to still be up and responsive.
    if [ $fuzz_exit -eq 124 ]; then
        ran="stopped at the ${FUZZ_DURATION}s limit"
        fuzz_exit=0
    fi
    echo "  ┌─ fuzz: up to $FUZZ_REQUESTS commands, $EFFECTIVE_CLIENTS clients, mode=${FUZZ_MODE:-valid-commands} ($ran, exit $fuzz_exit)"
    tr '\r' '\n' < "$out" | grep -v -e '^Progress' -e '^[[:space:]]*$' | tail -5 | sed 's/^/  │ /' || true

    assert_server_healthy
    if grep -q "FUZZING ABORTED" "$out"; then
        echo "  FATAL: Fuzzer aborted on a malformed reply from the server."
        tr '\r' '\n' < "$out" | grep -E "Error:|Command that caused" | head -4 | sed 's/^/     /' || true
        awk '/MALFORMED REPLY DATA BEGIN/,/MALFORMED REPLY DATA END/' "$out" | head -20 | sed 's/^/     /' || true
        rm -f "$out"
        fail "See the malformed reply above."
    fi
    # A fuzzer client aborts when it can't reconnect after a disconnect the fuzzer provoked
    # (QUIT, CLIENT KILL, ...), or when a blocking command it generated outlasts its own 15s
    # timeout. Both happen against a stock server too; the checks cover the server.
    if [ $fuzz_exit -ne 0 ]; then
        echo "  WARNING: $fuzz_exit fuzzer clients aborted; the server stayed up:"
        tr '\r' '\n' < "$out" | grep "ERROR" | head -5 | sed 's/^/     /' || true
    fi
    rm -f "$out"

    assert_accounting
}
