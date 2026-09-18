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
