# Paimon Extension for Vane and DuckDB

Query [Apache Paimon](https://paimon.apache.org/) tables with SQL and Python,
create append-only tables, append rows, inspect snapshots, and read historical
data. This extension uses [paimon-cpp](https://github.com/apache/paimon-cpp)
without requiring a JVM for native reads and writes.

## Choose your runtime

| Runtime | Execution | Guide |
| --- | --- | --- |
| Vane | Default Ray runner for supported scans, CTAS, INSERT, and snapshot inspection | [VANE_README.md](VANE_README.md) |
| DuckDB | Native DuckDB extension and SQL interface | [DUCKDB_README.md](DUCKDB_README.md) |

Install the artifact built for your runtime. Vane provider wheels and official
DuckDB extension binaries are not interchangeable.

## Start here

- [Install the Vane provider](VANE_README.md#install-a-provider-package).
- [Create a table, insert rows, and query](VANE_README.md#usage) using default Ray.
- [Use the Relation API](VANE_README.md#use-the-relation-api).
- [Inspect snapshots and query history](VANE_README.md#4-inspect-snapshots-and-query-history).
- [Use native DuckDB SQL](DUCKDB_README.md#usage), including filesystem and REST catalogs.

Vane's distributed write contract currently supports append-only CTAS and
INSERT. UPDATE, DELETE, and MERGE are not supported distributed mutations.
Catalog and schema initialization are separate from data execution. See the
[Vane execution limits](VANE_README.md#execution-and-storage-boundaries).

## Development

The default Make targets build against the upstream DuckDB submodule. The
Vane targets use the exact runtime selected in
[vane-extension.toml](vane-extension.toml).

- [DuckDB development guide](DUCKDB_README.md#development-guide)
- [Vane source build](VANE_README.md#build-from-source)
- [Test guide](test/README.md)
- [Provider release process](docs/VANE_RELEASE.md)
- [Related projects and community](DUCKDB_README.md#related-projects)
