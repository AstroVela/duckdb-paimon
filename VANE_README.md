# Paimon Extension for Vane

[Project overview](README.md) · [DuckDB guide](DUCKDB_README.md)

Use Apache Paimon from Vane's SQL and Python Relation APIs. Supported data
queries and writes execute with the default Ray runner: leave `VANE_RUNNER`
unset and do not call a runner-selection API.

## Install a provider package

Install `vane-extension-paimon` from PyPI together with the exact `vane-ai`
version required by its package metadata. Use the same matching wheels on the
application, Ray coordinator, and all workers. Provider versions include an
artifact identity; their version numbers differ from the base runtime.
See the [release guide](docs/VANE_RELEASE.md) for the development and production
channels.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip

python -m pip install vane-extension-paimon
python -m pip check
```

Pin exact versions (for example `"vane-extension-paimon==<version>"`) for
reproducible deployments. Published Linux wheels
must match your interpreter and platform. Load the installed provider:

```python
import vane
from vane import col

connection = vane.connect()
vane.load_installed_extension("paimon", connection=connection)
```

No `set_runner_ray()`, local runner, or `ray.init()` call is needed in the
application. Vane starts Ray when execution requires it. To use an existing
cluster, supply `RAY_ADDRESS` before starting Python; this selects its address
without selecting a different Vane runner.

## Usage

Run the following blocks in order in the same Python process and working
directory. They use a new local warehouse and need no cloud credentials,
Flink, Spark, or downloaded sample data. Use a fresh warehouse path when
repeating the example: CTAS does not overwrite an existing table.

### Prepare the connection

```python
from pathlib import Path

warehouse = str(Path("paimon_demo").resolve())
warehouse_sql = "'" + warehouse.replace("'", "''") + "'"
connection.execute(f"ATTACH {warehouse_sql} AS pm (TYPE paimon)")
connection.execute("CREATE SCHEMA pm.demo")
```

`ATTACH` registers the warehouse as `pm`; `pm.demo.events` then refers to its
`demo` database and `events` table. Connection/catalog setup executes on the
client connection. The data operations below use Ray. The warehouse is
created by these operations; a separate Python `mkdir` is unnecessary. Use an
absolute warehouse path because Ray actors can have a different working
directory from the calling process.

### 1. Create a table with CTAS

CTAS means **CREATE TABLE AS SELECT**: the query supplies the new table's
schema and initial rows. `.create()` is its Relation API form:

```python
connection.sql("""
    SELECT i::BIGINT AS id,
           (i % 4)::INTEGER AS bucket,
           ('value-' || i::VARCHAR)::VARCHAR AS payload
    FROM range(1000) AS source(i)
""").create("pm.demo.events", partition_by=["bucket"])
```

This creates a partitioned append-only Paimon table with 1,000 rows. Its path
is `paimon_demo/demo.db/events`. The coordinator prepares the empty table,
workers write data, and the coordinator commits the selected results.

| Operation | Purpose |
| --- | --- |
| Schema-only `CREATE TABLE` | Define an empty table. |
| `relation.create(...)` | Create the table and write the query's rows. |
| `relation.insert_into(...)` | Append query rows to an existing table. |

### 2. Insert rows

```python
connection.sql("""
    SELECT i::BIGINT AS id,
           (i % 4)::INTEGER AS bucket,
           ('value-' || i::VARCHAR)::VARCHAR AS payload
    FROM range(1000, 1100) AS source(i)
""").insert_into("pm.demo.events")
```

The table now has 1,100 rows. Paimon's distributed write support here is
append-only: UPDATE, DELETE, MERGE, and primary-key upserts are outside this
contract. They are not substituted with local execution in this guide.

### 3. Query the results

Use `.show()` for a display and `.fetchall()` when application code needs
Python rows. Both execute supported relations through Ray.

```python
connection.sql("""
    SELECT count(*) AS rows, sum(id)::BIGINT AS id_sum
    FROM pm.demo.events
""").show()
# rows = 1100, id_sum = 604450

connection.sql("""
    SELECT id, bucket, payload
    FROM pm.demo.events
    WHERE id < 5
""").show()
```

Read the same table directly with `paimon_scan`:

```python
table_path = str(Path(warehouse) / "demo.db" / "events")
connection.sql(
    "SELECT count(*) AS rows FROM paimon_scan(?)", params=[table_path]
).show()
# rows = 1100
```

A direct path scan does not require `ATTACH`. The function also accepts a
warehouse, database, and table as three arguments.

### Use the Relation API

```python
events = connection.table("pm.demo.events")
filtered = events.filter(col("id") >= 1000).select(col("id"), col("payload"))
filtered.limit(5).show()
filtered.aggregate("count(*) AS rows, sum(id)::BIGINT AS id_sum").show()
# rows = 100, id_sum = 104950

(
    events.aggregate(
        "bucket, count(*) AS rows, sum(id)::BIGINT AS id_sum",
        group_expr="bucket",
    )
    .show()
)
# Each bucket contains 275 rows; result order is unspecified.
```

These operations construct lazy relations. The preview is a separate relation,
so its limit does not change the aggregate input. Cast sums to `BIGINT` for a
standard Arrow integer result. Filtered relations can also be passed to
`.create()` or `.insert_into()`.

### 4. Inspect snapshots and query history

The successful CTAS and append create two snapshots. Inspect them on Ray:

```python
connection.sql("""
    SELECT snapshot_id, commit_kind, total_record_count
    FROM paimon_snapshots(?)
""", params=[table_path]).show()
# Snapshot 1 has 1000 records; snapshot 2 has 1100 records.
```

Read the first snapshot using either the catalog or a direct table scan:

```python
connection.sql("""
    SELECT count(*) AS rows, sum(id)::BIGINT AS id_sum
    FROM pm.demo.events AT (VERSION => 1)
""").show()
# rows = 1000, id_sum = 499500

connection.sql("""
    SELECT count(*) AS rows
    FROM paimon_scan(?, snapshot_from_id=1)
""", params=[table_path]).show()
# rows = 1000
```

Snapshot inspection and historical reads use Ray. For timestamp-based SQL
syntax, see [time travel](DUCKDB_README.md#time-travel-queries). Keep the selected
snapshot and its files available while a query is running.

## Execution and storage boundaries

The walkthrough leaves the runner unset throughout. The pinned Vane runtime
also dispatches supported SQL SELECT, CTAS, and INSERT through `execute()` and
`sql()`; the Relation API is used above to compose queries and writes.

| Operation | Execution |
| --- | --- |
| Provider loading, ATTACH, schema setup | Client connection initialization |
| Table scans and historical reads | Ray tasks over selected Paimon splits |
| Snapshot inspection | Ray metadata scan |
| Append-only CTAS and INSERT | Ray writers, followed by coordinator commit |
| UPDATE, DELETE, MERGE, primary-key writes | Outside the distributed write contract |

Distributed writes require auto-commit mode and a writable catalog that can
resolve the target location before writers start. Workers must be able to
access the same table paths. For multiple hosts, use a filesystem mounted at
the same absolute path on each node. Remote filesystem and REST catalog
configuration is documented in the [DuckDB guide](DUCKDB_README.md#query-remote-paimon-tables),
but its client-side Secret examples are not a claim of distributed credential
transport or remote-write support. The self-contained walkthrough uses local
storage only.

Paimon splits determine available parallelism; choosing Ray does not imply
every small scan or metadata query uses multiple workers. Workers return
attempt-scoped commit messages, and the coordinator commits selected messages
once. A known failure before finalization best-effort aborts messages received
by the coordinator. Failed CTAS retains its prepared table. After finalization
starts, a failed response can mean an unknown commit outcome, so artifacts are
retained. Paimon orphan-file garbage collection handles unreported files.
Clean up a retained CTAS target explicitly before retrying.

## Build from source

The default Make targets build for upstream DuckDB. Vane targets use the exact
revision in [vane-extension.toml](vane-extension.toml) and enable
`PAIMON_VANE_DISTRIBUTED` via [extension_config_vane.cmake](extension_config_vane.cmake).

```bash
git clone --branch main_vane --recurse-submodules \
  https://github.com/AstroVela/duckdb-paimon.git
cd duckdb-paimon
make vane_validate
make vane_ci VANE_BUILD_JOBS=8
make vane_wheel VANE_BUILD_JOBS=8
```

These targets require the native toolchain and vcpkg dependencies described by
the [shared build tools](https://github.com/AstroVela/vane-extension-ci-tools).
`vane_ci` runs the selected native test; it does not execute the full Ray suite.
`vane_wheel` produces a custom static Vane wheel, a separate installation path
from the provider recipe above. See [provider releases](docs/VANE_RELEASE.md)
for packaging and qualification. Always use non-editable installations.

The repository's [Ray integration tests](test/vane/test_vane_wheel_ray_paimon.py)
cover scans, snapshots, CTAS, append, partitioning, conflicts, and failure cleanup.

## Tested examples

A temporary local test executed all nine Python blocks above sequentially on
2026-09-17 with Python 3.12 and `vane-ai==0.2.0.dev663`, built from the Vane
`main` head selected for this run: `d1460a580455f01485e2e508e05d0049cb18a105`. The Paimon provider
was rebuilt against that exact revision and installed as a non-editable wheel.
The runtime's native SourceID was verified as `d8a9d61d59`.

The test left `VANE_RUNNER` unset, verified the default Ray runner, and used
an owned cluster with two CPU execution nodes on one physical host. All blocks
passed in 38.08 seconds, with 13 Ray read dispatches and two Ray writes including
additional assertions. Checks compared all 1,100 rows, per-bucket totals,
both snapshots, and the initial snapshot totals.

This validates the local provider walkthrough, not cloud storage or a multi-host
cluster. Matching local wheels were built and installed; the TestPyPI placeholders
must be replaced with published versions. The alternative static-wheel recipe
was not run by this walkthrough test.
