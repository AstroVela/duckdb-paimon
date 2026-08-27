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
    finally:
        connection.close()


if __name__ == "__main__":
    main()
