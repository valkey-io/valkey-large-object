# bench.sh workloads

`bench.sh --workload <name>` sources `bench/workloads/<name>.sh`. bench.sh owns the run: it parses the core flags, starts a fresh server for every mode × size, populates it with blobs, and calls the workload once per pass. Any flag bench.sh doesn't recognise is handed to the workload. The built-in `get` workload (BLOB.TCP_GET throughput, the default) lives in bench.sh itself.

To add a workload, add `<name>.sh` here. bench.sh needs no changes.

## Contract

| Name | Kind | Required | Purpose |
|---|---|---|---|
| `WORKLOAD_KEY_FORMAT` | variable | no | Python `str.format` pattern for populated key names, with `{i}` the key index. Default `k:{i:012d}`. |
| `WORKLOAD_BENCH_MODE` | variable | no | Module `bench-mode` (`yes`: GET replies with the size only). Default `yes`. |
| `workload_usage` | function | yes | Print the workload's own options, one per line, for `--help`. |
| `workload_init "$@"` | function | yes | Parse the workload's options (exit on unknown ones) and check prerequisites. Runs once, before any server starts. |
| `workload_header` | function | no | Print extra lines for the run header. |
| `workload_run` | function | yes | Run one pass against the populated server, and fail the run on any problem. |

`workload_run` can use these globals: `PORT`, `BENCH_MODE` (`Dram`, `Tiered` or `NVMe`), `LABEL` and `BYTES` (object size), `EFFECTIVE_KEYS`, `EFFECTIVE_CLIENTS`, `DISK_DIR`, `VALKEY_CLI`, `VALKEY_BENCH`, `BENCH_CPUS`, `SERVER_LOG`, `SERVER_PIDFILE`. bench.sh stops the server after the pass.

bench.sh itself only has a few hooks into `bench/lib.sh` (`load_workload`, `print_workload_header`, `run_workload_pass`, `print_workload_usage`); all plugin logic lives in `bench/`.

Shared checks from `bench/lib.sh`:

- `assert_server_healthy`: fails on a crash, a Rust panic, or a server that doesn't answer PING within 10s.
- `assert_accounting`: fails if `largeobj_num_objects` differs from the blob keys across all databases, or (Tiered/NVMe) if the object files differ from objects plus orphans.
- `fail "<message>"`: prints the message, stops the server, and exits the run.
- `pass_log`: prints the part of the server log written during this pass.

Use the workload's name as a prefix for its options (`--fuzz-mode`) and globals (`FUZZ_MODE`), so workloads can't collide with bench.sh or each other.
