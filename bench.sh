#!/bin/bash
# ValkeyLargeObj Benchmark Script
#
# Cycles through operating modes and object sizes, measuring LO.GET throughput.
#
# Three modes:
#   Dram    — all objects in DRAMPool (no NVMe)
#   Tiered  — NVMe persistence + DRAM read cache with promotion
#   NVMe    — NVMe persistence, no DRAM cache (max-promote-size 0)
#
# bench-mode: LO.GET does full storage path but replies with integer size only
# (no TCP bulk copy). Isolates storage throughput from network bandwidth.
#
# Prerequisites:
#   - valkey-server, valkey-cli, valkey-benchmark in PATH (or set env vars)
#   - Module built: cargo build --release
#   - Python 3 (no pip packages)
#   - For Tiered/NVMe modes: O_DIRECT capable mount (XFS/ext4 on NVMe)
#   - For fio baselines: fio installed

set -e

# ─── Defaults ─────────────────────────────────────────────────────────────────

PORT=""
NVME_DIR=""
RUN_FIO=0
CLIENTS=200
DURATION=10
NUM_KEYS=500
MAXMEMORY="34359738368"            # 32GB — server maxmemory; DRAM pool scales up toward this
SEGMENT_SIZE="67108864"      # 64MB
MAX_OBJECT_SIZE="58720256"   # 56MB — must be >= largest SIZES entry (50MB) and fit segment-size (64MB, minus talc overhead)
NVME_MAXMEMORY="107374182400"    # 100GB
NVME_STAGING_SIZE="67108864"      # 64MB
WORKER_THREADS=2
IO_THREADS=8                      # Valkey io-threads (network read/write offload)
SERVER_CPUS="0-15"                # taskset for valkey-server
BENCH_CPUS="16-63"                # taskset for valkey-benchmark
MODES_STR=""                      # empty = auto (Dram if no nvme-dir; all 3 if nvme-dir)
SIZES_STR="4KB 1MB 50MB"

# ─── Parse args ───────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --port)          PORT="$2"; shift 2 ;;
        --nvme-dir)      NVME_DIR="$2"; shift 2 ;;
        --modes)         MODES_STR="$2"; shift 2 ;;
        --sizes)         SIZES_STR="$2"; shift 2 ;;
        --clients)       CLIENTS="$2"; shift 2 ;;
        --duration)      DURATION="$2"; shift 2 ;;
        --keys)          NUM_KEYS="$2"; shift 2 ;;
        --maxmemory)         MAXMEMORY="$2"; shift 2 ;;
        --segment-size)      SEGMENT_SIZE="$2"; shift 2 ;;
        --nvme-maxmemory)    NVME_MAXMEMORY="$2"; shift 2 ;;
        --worker-threads)    WORKER_THREADS="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: $0 --port <PORT> [options]"
            echo ""
            echo "Options:"
            echo "  --port <PORT>              Valkey server port (required)"
            echo "  --nvme-dir <DIR>           NVMe directory (required for Tiered/NVMe modes)"
            echo "  --modes <\"fio Dram ...\"> Modes to run (default: Dram; with nvme-dir: fio Dram Tiered NVMe)"
            echo "  --sizes <\"4KB 1MB ...\">   Object sizes (default: \"4KB 1MB 50MB\")"
            echo "  --clients <N>              Benchmark clients (default: 200)"
            echo "  --duration <SEC>           Duration per size (default: 10)"
            echo "  --keys <N>                 Number of keys to populate (default: 500)"
            echo "  --maxmemory <BYTES>        Server maxmemory; DRAM pool scales toward it (default: 34359738368 = 32GB)"
            echo "  --segment-size <BYTES>     Segment size in bytes (default: 67108864 = 64MB)"
            echo "  --nvme-maxmemory <BYTES>   NVMe budget in bytes (default: 107374182400 = 100GB)"
            echo "  --worker-threads <N>       Tokio threads (default: 2)"
            echo ""
            echo "Modes:"
            echo "  Dram    All objects in DRAMPool. No NVMe."
            echo "  Tiered  NVMe persistence + DRAM read cache with promotion."
            echo "  NVMe    NVMe persistence only. No DRAM cache."
            echo ""
            echo "Examples:"
            echo "  $0 --port 7380                                    # Dram only (no nvme-dir)"
            echo "  $0 --port 7380 --nvme-dir /mnt/nvme               # All 3 modes"
            echo "  $0 --port 7380 --nvme-dir /mnt/nvme --modes Tiered  # Tiered only"
            echo "  $0 --port 7380 --modes Dram --sizes \"4KB 1MB\"      # Dram, specific sizes"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$PORT" ]; then
    echo "ERROR: --port is required"
    exit 1
fi

# Auto-detect modes if not specified
if [ -z "$MODES_STR" ]; then
    if [ -n "$NVME_DIR" ]; then
        MODES_STR="fio Dram Tiered NVMe"
    else
        MODES_STR="Dram"
    fi
fi

# fio runs if it's in MODES_STR
RUN_FIO=0
for m in $MODES_STR; do
    if [ "$m" = "fio" ]; then
        RUN_FIO=1
    fi
done

# Validate: Tiered/NVMe modes need nvme-dir
for m in $MODES_STR; do
    if [ "$m" = "Tiered" ] || [ "$m" = "NVMe" ]; then
        if [ -z "$NVME_DIR" ]; then
            echo "ERROR: --nvme-dir is required for $m mode"
            exit 1
        fi
    fi
done

MODULE_SO="${MODULE_SO:-$(dirname "$0")/target/release/libvalkey_largeobj.so}"

# Verify nvme-dir is NOT on the root disk (common mistake: /data falls through to /)
if [ -n "$NVME_DIR" ]; then
    mkdir -p "$NVME_DIR"
    NVME_DEV=$(df "$NVME_DIR" 2>/dev/null | tail -1 | awk '{print $1}')
    ROOT_DEV=$(df / 2>/dev/null | tail -1 | awk '{print $1}')
    if [ "$NVME_DEV" = "$ROOT_DEV" ]; then
        echo "WARNING: --nvme-dir ($NVME_DIR) is on the ROOT disk ($ROOT_DEV)!"
        echo "         NVMe benchmark results will be wrong. Use a path on the NVMe stripe."
        echo "         Example: --nvme-dir /mnt/bigobj-data/bench-test"
        echo ""
        read -p "Continue anyway? [y/N] " -r
        if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        echo "NVMe device:    $NVME_DEV ($(df -h "$NVME_DIR" | tail -1 | awk '{print $2}'))"
    fi
fi
VALKEY_SERVER="${VALKEY_SERVER:-valkey-server}"
VALKEY_CLI="${VALKEY_CLI:-valkey-cli}"
VALKEY_BENCH="${VALKEY_BENCH:-valkey-benchmark}"

# Check binaries exist
for bin in "$VALKEY_SERVER" "$VALKEY_CLI" "$VALKEY_BENCH"; do
    if ! command -v "$bin" &>/dev/null; then
        echo "ERROR: $bin not found in PATH"
        exit 1
    fi
done

if [ $RUN_FIO -eq 1 ]; then
    if ! command -v fio &>/dev/null; then
        echo "ERROR: fio not found in PATH (required for fio mode)"
        exit 1
    fi
fi

if [ ! -f "$MODULE_SO" ]; then
    echo "ERROR: Module not found at $MODULE_SO. Build with: cargo build --release"
    exit 1
fi

# Parse sizes
declare -a SIZES_LABEL
declare -a SIZES_BYTES
for s in $SIZES_STR; do
    SIZES_LABEL+=("$s")
    case "$s" in
        4KB)   SIZES_BYTES+=(4096) ;;
        16KB)  SIZES_BYTES+=(16384) ;;
        50KB)  SIZES_BYTES+=(51200) ;;
        256KB) SIZES_BYTES+=(262144) ;;
        1MB)   SIZES_BYTES+=(1048576) ;;
        4MB)   SIZES_BYTES+=(4194304) ;;
        16MB)  SIZES_BYTES+=(16777216) ;;
        50MB)  SIZES_BYTES+=(52428800) ;;
        *)     echo "Unknown size: $s (use 4KB, 16KB, 50KB, 256KB, 1MB, 4MB, 16MB, 50MB)"; exit 1 ;;
    esac
done

# ─── Helpers ──────────────────────────────────────────────────────────────────

# Print a one-line DRAM scaling snapshot from INFO largeobj. Arg: label.
print_scaling() {
    local label="$1"
    local info
    info=$($VALKEY_CLI -p $PORT INFO largeobj 2>/dev/null | tr -d '\r')
    local live nvme_live util expands shrinks dram_uring nvme_uring efa
    live=$(echo       "$info" | grep -E "dram_live_segments"              | awk -F: '{print $2}' | tr -d '[:space:]')
    nvme_live=$(echo  "$info" | grep -E "nvme_live_segments"              | awk -F: '{print $2}' | tr -d '[:space:]')
    util=$(echo       "$info" | grep -E "utilization_pct"                 | awk -F: '{print $2}' | tr -d '[:space:]')
    expands=$(echo    "$info" | grep -E "scaling_expand_total"            | awk -F: '{print $2}' | tr -d '[:space:]')
    shrinks=$(echo    "$info" | grep -E "scaling_shrink_total"            | awk -F: '{print $2}' | tr -d '[:space:]')
    dram_uring=$(echo "$info" | grep -E "dram_uring_registered_segments"  | awk -F: '{print $2}' | tr -d '[:space:]')
    nvme_uring=$(echo "$info" | grep -E "nvme_uring_registered_segments"  | awk -F: '{print $2}' | tr -d '[:space:]')
    efa=$(echo        "$info" | grep -E "efa_registered_segments"         | awk -F: '{print $2}' | tr -d '[:space:]')
    local total_live=$(( ${live:-0} + ${nvme_live:-0} ))
    echo "     [scaling: $label] dram_live=${live:-?} nvme_live=${nvme_live:-0} total_live=${total_live} util_pct=${util:-?} expand=${expands:-?} shrink=${shrinks:-?}"
    # io_uring registration coverage, per pool: each should equal that pool's live
    # count once the post-expand re-register has completed. Dram mode shows dram=0 (no
    # io_uring engine). EFA is fabric-global (one MR per segment, not per-pool), so
    # it is reported once against total_live.
    echo "     [registered: $label] io_uring dram=${dram_uring:-0}/${live:-?} nvme=${nvme_uring:-0}/${nvme_live:-0}  efa=${efa:-?}/${total_live}"
}

# Run one LO.GET benchmark pass and print throughput + latency. Arg: label.
# Leaves output in $BENCH_TMPFILE and exit code in $BENCH_EXIT for the caller
# to assert on / clean up. Needs BENCH_TIMEOUT / EFFECTIVE_* set by the caller.
run_bench_pass() {
    local label="$1"
    BENCH_TMPFILE=$(mktemp /tmp/bench-output-XXXXXX)
    BENCH_EXIT=0
    timeout $BENCH_TIMEOUT \
        taskset -c $BENCH_CPUS \
        $VALKEY_BENCH -p $PORT --duration $DURATION -c $EFFECTIVE_CLIENTS -r $EFFECTIVE_KEYS \
        -- LO.GET "k:__rand_int__" > "$BENCH_TMPFILE" 2>&1 || BENCH_EXIT=$?
    echo "  ┌─ $label"
    tr '\r' '\n' < "$BENCH_TMPFILE" | grep -A3 "throughput summary" | sed 's/^/  │ /' || true
}

# ─── Header ───────────────────────────────────────────────────────────────────

echo "=============================================="
echo "ValkeyLargeObj Benchmark"
echo "=============================================="
echo "Port:           $PORT"
echo "Modes:          $MODES_STR"
echo "Sizes:          ${SIZES_LABEL[*]}"
echo "Clients:        $CLIENTS"
echo "Duration:       ${DURATION}s"
echo "Keys:           $NUM_KEYS"
echo "Module:         $MODULE_SO"
[ -n "$NVME_DIR" ] && echo "NVMe dir:       $NVME_DIR"
echo "Server maxmem:  $MAXMEMORY ($((MAXMEMORY / 1048576))MB) — DRAM pool scales toward this"
echo "Segment size:   $SEGMENT_SIZE ($((SEGMENT_SIZE / 1048576))MB)"
echo "Worker threads: $WORKER_THREADS"
echo "IO threads:     $IO_THREADS"
echo "Server CPUs:    $SERVER_CPUS"
echo "Bench CPUs:     $BENCH_CPUS"
echo "=============================================="
echo ""

# ─── fio baseline ─────────────────────────────────────────────────────────────

if [ $RUN_FIO -eq 1 ] && [ -n "$NVME_DIR" ]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Mode: fio baseline (O_DIRECT random read, io_uring)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Detect block device for disk read assertion
    FIO_BLOCK_DEV=$(basename "$(readlink -f "$(df "$NVME_DIR" | tail -1 | awk '{print $1}')")" 2>/dev/null)
    FIO_STAT_FILE="/sys/block/${FIO_BLOCK_DEV}/stat"

    FIO_FILE="$NVME_DIR/fio_testfile"
    for i in "${!SIZES_LABEL[@]}"; do
        LABEL="${SIZES_LABEL[$i]}"
        BYTES="${SIZES_BYTES[$i]}"
        echo ""
        echo "  ── fio: $LABEL random read ──"

        FIO_READS_BEFORE=""
        if [ -f "$FIO_STAT_FILE" ]; then
            FIO_READS_BEFORE=$(awk '{print $1}' "$FIO_STAT_FILE")
        fi

        fio --name="randread_${LABEL}" \
            --filename="$FIO_FILE" \
            --size=4294967296 \
            --bs=$BYTES \
            --rw=randread \
            --ioengine=io_uring \
            --direct=1 \
            --iodepth=128 \
            --numjobs=16 \
            --runtime=10 \
            --time_based \
            --group_reporting \
            --output-format=terse \
            2>/dev/null | awk -F';' '{printf "  IOPS: %s  BW: %s KB/s  lat_avg: %s us\n", $8, $7, $16}'

        if [ -n "$FIO_READS_BEFORE" ] && [ -f "$FIO_STAT_FILE" ]; then
            FIO_READS_AFTER=$(awk '{print $1}' "$FIO_STAT_FILE")
            FIO_READS_DELTA=$(( FIO_READS_AFTER - FIO_READS_BEFORE ))
            echo "  Disk reads: $FIO_READS_DELTA"
            if [ $FIO_READS_DELTA -eq 0 ]; then
                echo "  FATAL: fio produced zero disk reads — nvme-dir ($NVME_DIR) is likely on the wrong device."
                echo "         Verify with: df $NVME_DIR"
                rm -f "$FIO_FILE"
                exit 1
            fi
        fi
    done
    rm -f "$FIO_FILE"
    echo ""
fi

# ─── Benchmark loop: modes × sizes ───────────────────────────────────────────

for BENCH_MODE in $MODES_STR; do
    # fio runs separately above, skip it in the module loop
    if [ "$BENCH_MODE" = "fio" ]; then
        continue
    fi
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Mode: $BENCH_MODE"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    for i in "${!SIZES_LABEL[@]}"; do
        LABEL="${SIZES_LABEL[$i]}"
        BYTES="${SIZES_BYTES[$i]}"

        # Scale key count for large objects to keep populate time reasonable.
        # 50MB × 500 = 25GB over TCP is unrealistic. Target ~10s populate time.
        EFFECTIVE_KEYS=$NUM_KEYS
        if [ $BYTES -ge 52428800 ]; then
            EFFECTIVE_KEYS=20   # 50MB: 20 keys = 1GB, ~10s populate
        elif [ $BYTES -ge 16777216 ]; then
            EFFECTIVE_KEYS=50   # 16MB: 50 keys
        elif [ $BYTES -ge 4194304 ]; then
            EFFECTIVE_KEYS=100  # 4MB: 100 keys
        fi

        # NVMe staging must hold all concurrent in-flight reads: each client holds
        # one contiguous obj_size buffer for the full GET (NVMe read + serve). A
        # buffer cannot span two segments, so every 64MB segment wastes its tail
        # (~obj_size - talc_overhead per segment). A flat 20% aggregate reserve is
        # not enough once the pool spans several segments — the per-segment loss
        # scales with segment count. Size to 2x the concurrent buffer bytes so the
        # pool survives per-segment tail waste. Cap at 1GB (kernel per-buffer limit,
        # IORING_REGISTER_BUFFERS); reduce effective clients if 2x would exceed it.
        STAGING_CAP=1073741824  # 1GB
        USABLE_CAP=$(( STAGING_CAP * 80 / 100 ))  # 80% usable after talc overhead
        STAGING_NEEDED=$(( CLIENTS * BYTES * 2 ))
        EFFECTIVE_CLIENTS=$CLIENTS
        if [ $STAGING_NEEDED -gt $USABLE_CAP ]; then
            EFFECTIVE_CLIENTS=$(( USABLE_CAP / (BYTES * 2) ))
            if [ $EFFECTIVE_CLIENTS -lt 1 ]; then
                EFFECTIVE_CLIENTS=1
            fi
        fi
        STAGING_NEEDED=$(( EFFECTIVE_CLIENTS * BYTES * 2 ))
        if [ $STAGING_NEEDED -gt $STAGING_CAP ]; then
            STAGING_NEEDED=$STAGING_CAP
        fi
        if [ $STAGING_NEEDED -lt 67108864 ]; then
            STAGING_NEEDED=67108864  # minimum 64MB
        fi

        echo ""
        echo "  ── $LABEL ($BYTES bytes) ── [keys=$EFFECTIVE_KEYS, clients=$EFFECTIVE_CLIENTS]"
        echo "     mode=$BENCH_MODE maxmemory=$((MAXMEMORY / 1048576))MB segment-size=$((SEGMENT_SIZE / 1048576))MB nvme-staging-size=$((STAGING_NEEDED / 1048576))MB worker-threads=$WORKER_THREADS io-threads=$IO_THREADS"

        # Build module args based on mode.
        case "$BENCH_MODE" in
            Dram)
                MODULE_ARGS="operating-mode Dram"
                MODULE_ARGS="$MODULE_ARGS segment-size $SEGMENT_SIZE"
                MODULE_ARGS="$MODULE_ARGS max-object-size $MAX_OBJECT_SIZE"
                ;;
            Tiered)
                MODULE_ARGS="operating-mode Tiered"
                MODULE_ARGS="$MODULE_ARGS nvme-dir $NVME_DIR"
                MODULE_ARGS="$MODULE_ARGS segment-size $SEGMENT_SIZE"
                MODULE_ARGS="$MODULE_ARGS nvme-maxmemory $NVME_MAXMEMORY"
                MODULE_ARGS="$MODULE_ARGS nvme-staging-size $STAGING_NEEDED"
                MODULE_ARGS="$MODULE_ARGS max-object-size $MAX_OBJECT_SIZE"
                MODULE_ARGS="$MODULE_ARGS max-promote-size $MAX_OBJECT_SIZE"
                ;;
            NVMe)
                MODULE_ARGS="operating-mode Tiered"
                MODULE_ARGS="$MODULE_ARGS nvme-dir $NVME_DIR"
                MODULE_ARGS="$MODULE_ARGS segment-size $SEGMENT_SIZE"
                MODULE_ARGS="$MODULE_ARGS nvme-maxmemory $NVME_MAXMEMORY"
                MODULE_ARGS="$MODULE_ARGS nvme-staging-size $STAGING_NEEDED"
                MODULE_ARGS="$MODULE_ARGS max-object-size $MAX_OBJECT_SIZE"
                MODULE_ARGS="$MODULE_ARGS max-promote-size 0"
                ;;
            *)
                echo "  ERROR: Unknown mode $BENCH_MODE"; continue ;;
        esac
        MODULE_ARGS="$MODULE_ARGS worker-threads $WORKER_THREADS"
        MODULE_ARGS="$MODULE_ARGS bench-mode yes"

        # Clean nvme-dir for Tiered/NVMe modes
        if [ "$BENCH_MODE" = "Tiered" ] || [ "$BENCH_MODE" = "NVMe" ]; then
            mkdir -p "$NVME_DIR"
            find "$NVME_DIR" -name "*.dat" -delete 2>/dev/null || true
        fi

        # Kill any stale server on this port from a previous run
        $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
        sleep 1
        # Force kill if graceful shutdown didn't work
        pkill -f "valkey-server.*port $PORT" 2>/dev/null || true
        sleep 1

        # Start server
        # Start server (pinned to SERVER_CPUS, with io-threads for network offload)
        taskset -c $SERVER_CPUS \
        $VALKEY_SERVER --port $PORT --daemonize yes \
            --logfile /tmp/bench-server-$PORT.log \
            --pidfile /tmp/bench-server-$PORT.pid \
            --loadmodule "$MODULE_SO" $MODULE_ARGS \
            --maxmemory $MAXMEMORY \
            --maxmemory-policy noeviction \
            --save "" \
            --appendonly no \
            --io-threads $IO_THREADS
        sleep 1

        # Wait for server to be ready (Tiered mode allocates 32GB+ on startup)
        READY=0
        for attempt in $(seq 1 30); do
            if $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
                READY=1
                break
            fi
            sleep 1
        done

        if [ $READY -eq 0 ]; then
            echo "  ERROR: Server failed to start. Check /tmp/bench-server-$PORT.log"
            tail -5 /tmp/bench-server-$PORT.log 2>/dev/null
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi

        # Verify this is a fresh server, not a stale one from a previous run.
        # Server uptime should be < 60 seconds (we just started it).
        UPTIME_SEC=$($VALKEY_CLI -p $PORT INFO server 2>/dev/null | grep uptime_in_seconds | awk -F: '{print $2}' | tr -d '[:space:]')
        if [ -z "$UPTIME_SEC" ] || ! echo "$UPTIME_SEC" | grep -qE '^[0-9]+$'; then
            echo "  FATAL: Could not read server uptime (got '$UPTIME_SEC'). Server may not be responding."
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi
        if [ "$UPTIME_SEC" -gt 60 ]; then
            echo "  FATAL: Stale server detected (uptime=${UPTIME_SEC}s). Expected fresh server with uptime < 60s."
            echo "         A previous server is still running on port $PORT. Kill it and retry."
            exit 1
        fi

        # Populate keys via raw RESP
        python3 -c "
import socket, os, time
def resp(*args):
    parts = [f'*{len(args)}\r\n']
    for a in args:
        if isinstance(a, bytes):
            parts.append(f'\${len(a)}\r\n')
            return ''.join(parts).encode() + a + b'\r\n'
        s = str(a)
        parts.append(f'\${len(s)}\r\n{s}\r\n')
    return ''.join(parts).encode()
s = socket.socket(); s.connect(('127.0.0.1', $PORT))
s.setsockopt(6, 1, 1)
payload = os.urandom($BYTES)
start = time.monotonic()
for i in range($EFFECTIVE_KEYS):
    s.sendall(resp('LO.SET', f'k:{i:012d}', payload))
    r = s.recv(1024)
elapsed = time.monotonic() - start
s.close()
print(f'  Populated $EFFECTIVE_KEYS keys ($LABEL) in {elapsed:.1f}s ({$EFFECTIVE_KEYS/elapsed:.0f} keys/s)')
"

        DBSIZE=$($VALKEY_CLI -p $PORT DBSIZE 2>/dev/null | awk '{print $NF}')
        if [ -z "$DBSIZE" ] || ! echo "$DBSIZE" | grep -qE '^[0-9]+$'; then
            echo "  FATAL: Could not read DBSIZE (got '$DBSIZE'). Server may not be responding."
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi
        echo "  DBSIZE: $DBSIZE"
        if [ "$DBSIZE" != "$EFFECTIVE_KEYS" ]; then
            echo "  FATAL: DBSIZE mismatch — expected $EFFECTIVE_KEYS, got $DBSIZE."
            echo "         LO.SET populate failed or keys were not stored correctly."
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi

        # ── Two-phase benchmark ──────────────────────────────────────────────
        # Phase 1 (warm-up) runs on a COLD pool: in Tiered/NVMe the first GET of
        # each key promotes it NVMe→DRAM, filling DRAM and triggering reactive
        # scaling (expand); in Dram the pool already expanded during populate.
        # Phase 2 (steady-state) reruns the same benchmark on the now-scaled,
        # warm pool. Printing both, with the scaling counters between them, shows
        # how scaling/promotion affects latency.
        BENCH_TIMEOUT=$(( DURATION + 30 ))

        # Reset stats before the warm-up pass. Disk reads are captured across the
        # warm-up pass only — that is the promotion pass; steady-state is cache hits.
        $VALKEY_CLI -p $PORT CONFIG RESETSTAT > /dev/null 2>&1

        # Detect the block device for the disk-read assertion (e.g. dm-0 for LVM).
        DISK_READS_BEFORE=""
        if [ -n "$NVME_DIR" ] && [ "$BENCH_MODE" != "Dram" ]; then
            BLOCK_DEV=$(df "$NVME_DIR" 2>/dev/null | tail -1 | awk '{print $1}' | sed 's|/dev/||; s|/|-|g')
            STAT_FILE="/sys/block/${BLOCK_DEV}/stat"
            if [ ! -f "$STAT_FILE" ]; then
                # LVM: /dev/mapper/vg-lv -> dm-X
                BLOCK_DEV=$(basename "$(readlink -f "$(df "$NVME_DIR" | tail -1 | awk '{print $1}')")" 2>/dev/null)
                STAT_FILE="/sys/block/${BLOCK_DEV}/stat"
            fi
            if [ -f "$STAT_FILE" ]; then
                DISK_READS_BEFORE=$(awk '{print $1}' "$STAT_FILE")
            else
                echo "  FATAL: Cannot find block device stat file for $NVME_DIR (tried $STAT_FILE)."
                echo "         Cannot verify NVMe I/O. Check mount point with: df $NVME_DIR"
                $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
                exit 1
            fi
        fi

        print_scaling "before warm-up (cold)"

        # Phase 1: warm-up — drives promotion + scaling.
        run_bench_pass "PHASE 1 — warm-up (cold pool: promotion + scaling)"
        WARMUP_EXIT=$BENCH_EXIT
        WARMUP_TMPFILE=$BENCH_TMPFILE

        # Disk-read assertion on the warm-up pass (the promotion pass).
        if [ -n "$DISK_READS_BEFORE" ] && [ -f "$STAT_FILE" ]; then
            DISK_READS_AFTER=$(awk '{print $1}' "$STAT_FILE")
            DISK_READS_DELTA=$(( DISK_READS_AFTER - DISK_READS_BEFORE ))
            echo "  Disk reads (warm-up): $DISK_READS_DELTA"
            # Tiered: >= one promotion read per key. NVMe-only: every GET reads disk (>> keys).
            if [ $DISK_READS_DELTA -lt $EFFECTIVE_KEYS ]; then
                echo "  FATAL: $BENCH_MODE mode warm-up disk reads ($DISK_READS_DELTA) < keys ($EFFECTIVE_KEYS)."
                echo "         NVMe reads are not happening. Check: wrong mount point, key format mismatch, or promotion-path bug."
                rm -f "$WARMUP_TMPFILE"
                $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
                exit 1
            fi
        fi

        print_scaling "after warm-up (scaled)"

        # Phase 2: steady-state — warm, scaled pool.
        run_bench_pass "PHASE 2 — steady-state (warm, scaled pool)"
        REAL_EXIT=$BENCH_EXIT
        REAL_TMPFILE=$BENCH_TMPFILE

        print_scaling "after steady-state"

        # Assert both passes completed and produced results.
        for pass in "warm-up|$WARMUP_EXIT|$WARMUP_TMPFILE" "steady-state|$REAL_EXIT|$REAL_TMPFILE"; do
            P_NAME="${pass%%|*}"; rest="${pass#*|}"; P_EXIT="${rest%%|*}"; P_FILE="${rest##*|}"
            if [ "$P_EXIT" -eq 124 ]; then
                echo "  FATAL: $P_NAME benchmark timed out after ${BENCH_TIMEOUT}s. Server hung or staging pool exhausted."
                rm -f "$WARMUP_TMPFILE" "$REAL_TMPFILE"
                $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
                exit 1
            fi
            if ! tr '\r' '\n' < "$P_FILE" | grep -q "throughput summary"; then
                echo "  FATAL: $P_NAME benchmark produced no throughput results."
                echo "         Raw output:"
                tr '\r' '\n' < "$P_FILE" | tail -10
                rm -f "$WARMUP_TMPFILE" "$REAL_TMPFILE"
                $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
                exit 1
            fi
        done
        rm -f "$WARMUP_TMPFILE" "$REAL_TMPFILE"

        # Cache hit/miss assertion (cumulative over both passes: hits > 0, misses == 0).
        CACHE_HITS=$($VALKEY_CLI -p $PORT INFO stats 2>/dev/null | grep keyspace_hits | awk -F: '{print $2}' | tr -d '[:space:]')
        CACHE_MISSES=$($VALKEY_CLI -p $PORT INFO stats 2>/dev/null | grep keyspace_misses | awk -F: '{print $2}' | tr -d '[:space:]')
        echo "  Cache hits: ${CACHE_HITS:-<missing>}, misses: ${CACHE_MISSES:-<missing>}"

        # Validate stats are present and numeric
        if [ -z "$CACHE_HITS" ] || ! echo "$CACHE_HITS" | grep -qE '^[0-9]+$'; then
            echo "  FATAL: keyspace_hits is missing or non-numeric ('$CACHE_HITS'). Server stats unavailable."
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi
        if [ -z "$CACHE_MISSES" ] || ! echo "$CACHE_MISSES" | grep -qE '^[0-9]+$'; then
            echo "  FATAL: keyspace_misses is missing or non-numeric ('$CACHE_MISSES'). Server stats unavailable."
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi
        # Hits must be > 0 (proves LO.GET actually ran and found keys)
        if [ "$CACHE_HITS" -eq 0 ]; then
            echo "  FATAL: 0 cache hits — benchmark did not perform any valid key lookups."
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi
        # Any miss = keys don't match
        if [ "$CACHE_MISSES" -gt 0 ]; then
            echo "  FATAL: $CACHE_MISSES cache misses — benchmark is querying non-existent keys."
            echo "         Key format mismatch between populate (k:{i:012d}) and valkey-benchmark (k:__rand_int__)."
            $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
            exit 1
        fi

        # Shutdown — wait for process to fully exit before next iteration.
        # With 32GB+ allocated, process exit can take several seconds.
        $VALKEY_CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null || true
        for attempt in $(seq 1 15); do
            if ! $VALKEY_CLI -p $PORT PING > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        # Force kill if graceful shutdown didn't work
        pkill -9 -f "valkey-server.*port $PORT" 2>/dev/null || true
        sleep 2
    done
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Done."
