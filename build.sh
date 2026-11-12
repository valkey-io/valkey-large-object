#!/usr/bin/env sh

# Build valkey-large-object module and run tests.
#
# Usage:
#   ./build.sh                # fmt + build + unit tests + integration tests
#   ./build.sh build          # fmt + build + unit tests (no valkey-server needed)
#   ./build.sh unit-test      # unit tests only (assumes already built)
#   ./build.sh integ-test     # integration tests only (assumes already built)
#   ./build.sh test           # unit tests + integration tests (assumes already built)
#   ./build.sh clean          # remove build artifacts
#
# Environment variables:
#   SERVER_VERSION        valkey branch, tag, or PR (like pull/4050/head) to build (default: unstable)
#   VALKEY_SERVER_PATH    path to pre-built valkey-server binary (skips building valkey)
#   TEST_PATTERN          pytest -k filter pattern
#   ASAN_BUILD            if set, builds valkey with SANITIZER=address
#   COVERAGE              if set, instruments the module and prints integration-test
#                         coverage after the run (needs: rustup component add llvm-tools-preview)
#   COVERAGE_MIN          with COVERAGE, fail if unit + integration line coverage is below this percent

set -e

SCRIPT_DIR=$(pwd)
REPO_URL="https://github.com/valkey-io/valkey.git"

MODULE_EXT=".so"
if [ "$(uname)" = "Darwin" ]; then
    MODULE_EXT=".dylib"
fi
export MODULE_PATH="$SCRIPT_DIR/target/release/libvalkey_large_object$MODULE_EXT"

# ─── Coverage ─────────────────────────────────────────────────────────────────

if [ -n "$COVERAGE" ]; then
    LLVM_BIN=$(dirname "$(find "$(rustc --print sysroot)" -name llvm-profdata | head -1)")
    if [ ! -x "$LLVM_BIN/llvm-profdata" ]; then
        echo "ERROR: llvm-profdata not found. Run: rustup component add llvm-tools-preview"
        exit 1
    fi
    export RUSTFLAGS="-C instrument-coverage"
    COV_DIR="$SCRIPT_DIR/target/coverage"
    rm -rf "$COV_DIR"
    mkdir -p "$COV_DIR"
    # Each valkey-server the tests spawn inherits this and writes a profile on clean shutdown.
    export LLVM_PROFILE_FILE="$COV_DIR/lo-%p-%m.profraw"
fi

# Run the unit tests. Under COVERAGE, also record the test binary's path so its counters can
# be mapped back to source alongside the module's.
run_unit_tests() {
    if [ -n "$COVERAGE" ]; then
        cargo test --features enable-system-alloc --no-run --message-format=json \
            | grep '"executable":"[^"]*valkey_large_object-' \
            | sed 's/.*"executable":"\([^"]*\)".*/\1/' | head -1 > "$COV_DIR/unit_bin"
    fi
    cargo test --features enable-system-alloc
}

# ─── Clean ────────────────────────────────────────────────────────────────────

if [ "$1" = "clean" ]; then
    echo "Cleaning build artifacts..."
    rm -rf target/
    rm -rf tests/build/
    echo "Done."
    exit 0
fi

# ─── Unit Tests Only ──────────────────────────────────────────────────────────

if [ "$1" = "unit-test" ]; then
    echo "Running unit tests..."
    run_unit_tests
    exit 0
fi

# ─── Build Module ─────────────────────────────────────────────────────────────

if [ "$1" != "test" ] && [ "$1" != "integ-test" ]; then
    if ! pkg-config --exists libfabric; then
        echo "ERROR: libfabric not found via pkg-config."
        echo "Install it (libfabric-devel or brew libfabric) or point PKG_CONFIG_PATH at its .pc file."
        exit 1
    fi

    echo "Running cargo fmt check..."
    cargo fmt --check
    echo ""

    echo "Running cargo clippy..."
    cargo clippy --profile release --all-targets --features test-harness -- -D warnings
    echo ""

    echo "Running cargo build release..."
    cargo build --release
    echo "Module built: $MODULE_PATH"
    echo ""

    echo "Running unit tests..."
    run_unit_tests
    echo ""
fi

if [ "$1" = "build" ]; then
    exit 0
fi

# ─── Run unit tests if "test" command ─────────────────────────────────────────

if [ "$1" = "test" ]; then
    echo "Running unit tests..."
    run_unit_tests
    echo ""
fi

# ─── Valkey Server Binary ─────────────────────────────────────────────────────

if [ -z "$SERVER_VERSION" ]; then
    echo "SERVER_VERSION not set. Defaulting to 'unstable'."
    export SERVER_VERSION="unstable"
fi

# Use separate binary dir for ASAN so normal and ASAN builds don't collide.
# GIT_VERSION is used for git checkout, SERVER_VERSION is exported for tests.
GIT_VERSION="$SERVER_VERSION"
# SERVER_VERSION is also a directory name, and the tests rebuild the same path from the exported
# value — so flatten refs that contain slashes (a PR ref like pull/4050/head) before either uses it.
SERVER_VERSION=$(printf '%s' "$SERVER_VERSION" | tr '/' '-')
if [ ! -z "${ASAN_BUILD}" ]; then
    SERVER_VERSION="${SERVER_VERSION}-asan"
fi
export SERVER_VERSION
BINARY_DIR="tests/build/binaries/${SERVER_VERSION}"

BINARY_PATH="$BINARY_DIR/valkey-server"
CACHED_VALKEY_PATH="tests/build/valkey"

# Optional: use an externally built valkey-server binary.
if [ -n "$VALKEY_SERVER_PATH" ]; then
    if [ ! -f "$VALKEY_SERVER_PATH" ] || [ ! -x "$VALKEY_SERVER_PATH" ]; then
        echo "ERROR: VALKEY_SERVER_PATH is set but file does not exist or is not executable: $VALKEY_SERVER_PATH"
        exit 1
    fi
    echo "Using external valkey-server binary: $VALKEY_SERVER_PATH"
    mkdir -p "$BINARY_DIR"
    cp "$VALKEY_SERVER_PATH" "$BINARY_PATH"
fi

if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
else
    echo "valkey-server binary '$BINARY_PATH' not found. Building from source..."
    mkdir -p "$BINARY_DIR"
    rm -rf $CACHED_VALKEY_PATH
    cd tests/build
    git clone "$REPO_URL"
    cd valkey
    # A clone fetches branches and tags only, so a GitHub PR ref has to be fetched
    # before it can be checked out. For versions like pull/4050/head
    case "$GIT_VERSION" in
        pull/*|refs/pull/*)
            git fetch origin "$GIT_VERSION"
            git checkout FETCH_HEAD
            ;;
        *)
            git checkout "$GIT_VERSION"
            ;;
    esac
    make distclean
    if [ ! -z "${ASAN_BUILD}" ]; then
        make -j SANITIZER=address
    else
        make -j
    fi
    cp src/valkey-server "$SCRIPT_DIR/$BINARY_DIR/"
    cd $SCRIPT_DIR
    rm -rf $CACHED_VALKEY_PATH
fi

# ─── Test Framework ──────────────────────────────────────────────────────────

TEST_FRAMEWORK_REPO="https://github.com/valkey-io/valkey-test-framework"
TEST_FRAMEWORK_DIR="tests/build/valkeytestframework"

if [ -d "$TEST_FRAMEWORK_DIR" ]; then
    echo "valkeytestframework found."
else
    echo "Cloning valkey-test-framework..."
    git clone "$TEST_FRAMEWORK_REPO"
    mkdir -p "$TEST_FRAMEWORK_DIR"
    mv "valkey-test-framework/src"/* "$TEST_FRAMEWORK_DIR/"
    rm -rf valkey-test-framework
fi

# ─── Python Dependencies ─────────────────────────────────────────────────────

REQUIREMENTS_FILE="requirements.txt"
if [ -f "$REQUIREMENTS_FILE" ]; then
    if command -v pip3 > /dev/null 2>&1; then
        pip3 install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
    elif command -v pip > /dev/null 2>&1; then
        pip install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
    elif python3 -m pip --version > /dev/null 2>&1; then
        python3 -m pip install -r "$SCRIPT_DIR/$REQUIREMENTS_FILE"
    else
        echo "WARNING: No pip available. Ensure 'valkey' and 'pytest' are installed."
    fi
fi

# ─── Run Integration Tests ───────────────────────────────────────────────────

# The passive fabric peer for fabric tests.
cargo build --release --features test-harness --bin fabric_target

echo ""
echo "Running integration tests..."
echo "  MODULE_PATH=$MODULE_PATH"
echo "  SERVER_VERSION=$SERVER_VERSION"
echo ""

if [ -n "$TEST_PATTERN" ]; then
    python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/" -k "$TEST_PATTERN"
else
    python3 -m pytest --cache-clear -v "$SCRIPT_DIR/tests/"
fi

echo ""
echo "All tests passed."

# ─── Coverage Report ─────────────────────────────────────────────────────────

if [ -n "$COVERAGE" ]; then
    # Exclude dependencies, generated FFI bindings, std and tests so the number is the module's.
    COV_IGNORE='(/\.cargo/|/registry/|cargo-home|/rustc/|rustlib/src/rust|ofi-libfabric-sys|/out/|target/(release|debug)/build|vendor|/tests?/|build\.rs)'
    "$LLVM_BIN/llvm-profdata" merge -sparse "$COV_DIR"/*.profraw -o "$COV_DIR/lo.profdata"
    echo ""
    echo "Integration test coverage (module src/ only):"
    "$LLVM_BIN/llvm-cov" report "$MODULE_PATH" -instr-profile="$COV_DIR/lo.profdata" \
        -ignore-filename-regex="$COV_IGNORE" | tail -1
    # The unit tests are a separate binary; adding it maps their counters back to source too.
    UNIT_BIN=$(cat "$COV_DIR/unit_bin" 2>/dev/null)
    if [ -n "$UNIT_BIN" ] && [ -x "$UNIT_BIN" ]; then
        echo ""
        echo "Combined unit + integration test coverage (module src/ only):"
        "$LLVM_BIN/llvm-cov" report "$MODULE_PATH" -object "$UNIT_BIN" \
            -instr-profile="$COV_DIR/lo.profdata" -ignore-filename-regex="$COV_IGNORE"
    fi
    if [ -n "$COVERAGE_MIN" ]; then
        if [ -z "$UNIT_BIN" ] || [ ! -x "$UNIT_BIN" ]; then
            echo "ERROR: COVERAGE_MIN needs the unit tests to run in the same build (run ./build.sh with no arguments)."
            exit 1
        fi
        LINE_COV=$("$LLVM_BIN/llvm-cov" export "$MODULE_PATH" -object "$UNIT_BIN" \
            -instr-profile="$COV_DIR/lo.profdata" -ignore-filename-regex="$COV_IGNORE" -summary-only \
            | python3 -c 'import json, sys; print(json.load(sys.stdin)["data"][0]["totals"]["lines"]["percent"])')
        echo ""
        if awk -v c="$LINE_COV" -v m="$COVERAGE_MIN" 'BEGIN { exit !(c >= m) }'; then
            echo "Unit + integration line coverage ${LINE_COV}% meets the ${COVERAGE_MIN}% minimum."
        else
            echo "ERROR: Unit + integration line coverage ${LINE_COV}% is below the ${COVERAGE_MIN}% minimum."
            exit 1
        fi
    fi
fi
