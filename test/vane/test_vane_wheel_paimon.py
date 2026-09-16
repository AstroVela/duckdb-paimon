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

"""Exercise provider-backed Paimon from separately packaged Vane wheels."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TABLE_PATH = REPOSITORY_ROOT / "data/testdb.db/testtbl"
TEST_DIRECTORY = Path(__file__).resolve().parent
# Isolated mode omits the script directory from sys.path; expose only this
# sibling helper for the duration of its import.
sys.path.insert(0, str(TEST_DIRECTORY))
try:
    from packaged_dynamic_extension import load_packaged_dynamic_paimon
    from test_vane_wheel_ray_paimon import (
        RayPaimonHarness,
        create_two_worker_cluster,
        require_error,
    )
finally:
    sys.path.pop(0)


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def sql_string(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def exercise_default_ray_insert(connection: object) -> None:
    with tempfile.TemporaryDirectory(prefix="vane-paimon-smoke-insert-") as warehouse_text:
        warehouse = Path(warehouse_text).resolve()
        uuidless_target = warehouse / "legacy.db/uuidless_target"
        shutil.copytree(TABLE_PATH, uuidless_target)
        connection.execute(f"ATTACH {sql_string(warehouse)} AS pm (TYPE paimon)")
        connection.execute("CREATE SCHEMA pm.smoke")
        connection.execute(
            "CREATE TABLE pm.smoke.target " "(id INTEGER, part INTEGER, payload VARCHAR) PARTITIONED BY (part)"
        )
        source = connection.sql(
            "SELECT i::INTEGER AS id, (i % 3)::INTEGER AS part, "
            "('smoke-' || i::VARCHAR)::VARCHAR AS payload FROM range(0, 12) source(i)"
        )
        source.insert_into("pm.smoke.target")
        connection.execute("INSERT INTO pm.smoke.target (id, payload) " "VALUES (12, 'smoke-partial')")
        source.create(
            "pm.smoke.ctas_target",
            properties={"partition.default-name": "smoke-null"},
            partition_by=["part"],
        )
        require_equal(
            connection.execute(
                "SELECT count(*)::BIGINT, sum(id)::BIGINT, count(DISTINCT part)::BIGINT " "FROM pm.smoke.target"
            ).fetchone(),
            (13, 78, 3),
            "default Ray Paimon INSERT",
        )
        require_equal(
            connection.execute(
                "SELECT count(*)::BIGINT, sum(id)::BIGINT, count(DISTINCT part)::BIGINT " "FROM pm.smoke.ctas_target"
            ).fetchone(),
            (12, 66, 3),
            "default Ray Paimon CTAS",
        )
        require_equal(
            connection.execute("SELECT id, part, payload FROM pm.smoke.target WHERE id = 12").fetchone(),
            (12, None, "smoke-partial"),
            "default Ray partial-column Paimon INSERT",
        )
        require_error(
            "UUID-less Paimon target is rejected before a distributed commit",
            lambda: connection.execute("INSERT INTO pm.legacy.uuidless_target VALUES ('rejected', 9, 90, 99.5)"),
            "empty table UUID",
        )
        require_equal(
            connection.execute(
                "SELECT count(*)::BIGINT, max(f1), max(f2), max(f3) " "FROM pm.legacy.uuidless_target"
            ).fetchone(),
            (9, 3, 2, 33.2),
            "UUID-less Paimon table is unchanged",
        )


def main() -> None:
    if "VANE_RUNNER" in os.environ:
        raise RuntimeError("leave VANE_RUNNER unset to qualify the default Ray runner")

    import ray
    import vane
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("the wheel smoke test must own its Ray cluster")
    cluster = create_two_worker_cluster(ray)
    connection = None
    try:
        runner = runners.get_or_create_runner()
        require_equal(getattr(runner, "name", None), "ray", "default Vane runner")
        connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        load_packaged_dynamic_paimon(connection)
        scan = f"paimon_scan({sql_string(TABLE_PATH)})"
        require_equal(
            connection.execute(f"SELECT count(*)::BIGINT, min(f1), max(f1), round(sum(f3), 1) FROM {scan}").fetchone(),
            (9, 1, 3, 198.9),
            "default Ray packaged Paimon scan",
        )
        require_equal(
            connection.execute(f"SELECT f0, f3 FROM {scan} WHERE f1 = 2 ORDER BY f2").fetchall(),
            [("David", 21.0), ("Eve", 22.1), ("Frank", 23.2)],
            "default Ray projection and residual filter",
        )
        harness = RayPaimonHarness(vane, connection, runner)
        exercise_default_ray_insert(connection)
        if harness.write_dispatch_count < 3 or harness.read_dispatch_count < 4:
            raise AssertionError("Paimon smoke reads and writes did not reach Ray")
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                ray.shutdown()
                cluster.shutdown()


if __name__ == "__main__":
    main()
