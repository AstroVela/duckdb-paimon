#!/usr/bin/env python3
# Copyright (c) 2026, Alibaba Group Holding Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the statically linked Paimon extension in a packaged Vane wheel."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TABLE_PATH = REPOSITORY_ROOT / "data/testdb.db/testtbl"


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def verify_extension_is_wheel_linked(connection: object) -> None:
    extension = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() " "WHERE extension_name = 'paimon'"
    ).fetchone()
    if extension is None:
        raise AssertionError("the packaged Vane wheel does not contain paimon")
    require_equal(extension[1], "STATICALLY_LINKED", "Paimon install mode before LOAD")

    connection.execute("LOAD paimon")
    loaded = connection.execute(
        "SELECT loaded, install_mode FROM duckdb_extensions() " "WHERE extension_name = 'paimon'"
    ).fetchone()
    require_equal(loaded, (True, "STATICALLY_LINKED"), "Paimon after LOAD")


def exercise_local_fast_insert(connection: object) -> None:
    with tempfile.TemporaryDirectory(prefix="vane-paimon-local-insert-") as warehouse_text:
        warehouse = Path(warehouse_text).resolve()
        uuidless_target = warehouse / "legacy.db/uuidless_target"
        shutil.copytree(TABLE_PATH, uuidless_target)
        connection.execute(f"ATTACH {sql_string(warehouse)} AS local_pm (TYPE paimon)")
        connection.execute("CREATE SCHEMA local_pm.local_insert")
        connection.execute(
            "CREATE TABLE local_pm.local_insert.target "
            "(id INTEGER, part INTEGER, payload VARCHAR) PARTITIONED BY (part)"
        )
        source = connection.sql(
            "SELECT i::INTEGER AS id, (i % 3)::INTEGER AS part, "
            "('local-' || i::VARCHAR)::VARCHAR AS payload FROM range(0, 12) source(i)"
        )
        source.insert_into("local_pm.local_insert.target")
        connection.execute("INSERT INTO local_pm.local_insert.target (id, payload) " "VALUES (12, 'local-partial')")
        source.create(
            "local_pm.local_insert.ctas_target",
            properties={"partition.default-name": "local-null"},
            partition_by=["part"],
        )
        require_equal(
            connection.execute(
                "SELECT count(*)::BIGINT, sum(id)::BIGINT, count(DISTINCT part)::BIGINT "
                "FROM local_pm.local_insert.target"
            ).fetchone(),
            (13, 78, 3),
            "local-fast native Paimon INSERT",
        )
        require_equal(
            connection.execute(
                "SELECT count(*)::BIGINT, sum(id)::BIGINT, count(DISTINCT part)::BIGINT "
                "FROM local_pm.local_insert.ctas_target"
            ).fetchone(),
            (12, 66, 3),
            "local-fast native Paimon CTAS",
        )
        require_equal(
            connection.execute("SELECT id, part, payload FROM local_pm.local_insert.target WHERE id = 12").fetchone(),
            (12, None, "local-partial"),
            "local-fast native partial-column Paimon INSERT",
        )
        connection.execute("INSERT INTO local_pm.legacy.uuidless_target VALUES ('local-fast', 9, 90, 99.5)")
        require_equal(
            connection.execute(
                "SELECT count(*)::BIGINT, max(f1), max(f2), max(f3) " "FROM local_pm.legacy.uuidless_target"
            ).fetchone(),
            (10, 9, 90, 99.5),
            "local-fast native INSERT into a UUID-less Paimon table",
        )


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "local-fast":
        raise RuntimeError("the wheel integration test requires VANE_RUNNER=local-fast")

    import vane

    connection = vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        verify_extension_is_wheel_linked(connection)
        scan = f"paimon_scan({sql_string(TABLE_PATH)})"
        require_equal(
            connection.execute(f"SELECT count(*)::BIGINT, min(f1), max(f1), round(sum(f3), 1) FROM {scan}").fetchone(),
            (9, 1, 3, 198.9),
            "local-fast packaged Paimon scan",
        )
        require_equal(
            connection.execute(f"SELECT f0, f3 FROM {scan} WHERE f1 = 2 ORDER BY f2").fetchall(),
            [("David", 21.0), ("Eve", 22.1), ("Frank", 23.2)],
            "local-fast projection and residual filter",
        )
        exercise_local_fast_insert(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
