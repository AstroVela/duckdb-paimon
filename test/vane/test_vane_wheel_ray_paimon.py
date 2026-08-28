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

"""Exercise Paimon scans through a packaged two-worker Vane Ray runtime."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCALAR_INDEX_PATH = REPOSITORY_ROOT / "data/scalar_index.db/t1"
WORKER_COUNT = 2
WORKER_PLAN_CAPTURE_SENTINEL = "intentional worker-plan capture stop"
WORKER_PLAN_CAPTURE_TIMEOUT_SECONDS = 30
INSERT_TARGET = "pm.vane_ray.insert_target"
PARTITIONED_INSERT_TARGET = "pm.vane_ray.partitioned_insert_target"
EMPTY_INSERT_TARGET = "pm.vane_ray.empty_insert_target"
FAILURE_INSERT_TARGET = "pm.vane_ray.failure_insert_target"
CONFLICT_INSERT_TARGET = "pm.vane_ray.conflict_insert_target"
SCHEMA_CONFLICT_INSERT_TARGET = "pm.vane_ray.schema_conflict_insert_target"
ATTEMPT_METADATA_INSERT_TARGET = "pm.vane_ray.attempt_metadata_insert_target"
UNSELECTED_ATTEMPT_INSERT_TARGET = "pm.vane_ray.unselected_attempt_insert_target"


def require_equal(actual: object, expected: object, description: str) -> None:
    if actual != expected:
        raise AssertionError(f"{description}: expected {expected!r}, got {actual!r}")


def require_true(value: bool, description: str) -> None:
    if not value:
        raise AssertionError(description)


def error_chain_contains(error: BaseException, expected_message: str) -> bool:
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        if expected_message in str(current):
            return True
        seen.add(id(current))
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return False


def require_error(
    description: str,
    operation: Callable[[], object],
    expected_message: str | None = None,
) -> None:
    try:
        operation()
    except Exception as error:
        if expected_message is not None and not error_chain_contains(error, expected_message):
            raise AssertionError(
                f"{description}: expected error containing {expected_message!r}, got {error!r}"
            ) from error
    else:
        raise AssertionError(f"{description}: operation unexpectedly succeeded")


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


def create_two_worker_cluster(ray: object) -> object:
    from ray.cluster_utils import Cluster

    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=100 * 1024 * 1024,
        )
        for _ in range(WORKER_COUNT):
            cluster.add_node(
                include_dashboard=False,
                num_cpus=1,
                num_gpus=0,
                object_store_memory=100 * 1024 * 1024,
            )
        ray.init(address=cluster.address, ignore_reinit_error=False, log_to_driver=True)
        return cluster
    except BaseException:
        cluster.shutdown()
        raise


def execution_node_ids(ray: object) -> set[str]:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        node_ids = {
            str(node["NodeID"])
            for node in ray.nodes()
            if node.get("Alive") and float((node.get("Resources") or {}).get("CPU", 0)) >= 1
        }
        if len(node_ids) == WORKER_COUNT:
            return node_ids
        time.sleep(0.25)
    raise AssertionError(f"expected {WORKER_COUNT} live Ray execution nodes")


def assert_vane_worker_topology(ray: object, runner: object) -> None:
    client = runner.query_driver_client
    if client is None:
        raise AssertionError("the Ray runner did not create a query driver client")
    stats = ray.get(client.runner.fragment_stats.remote())
    workers = stats.get("workers") if isinstance(stats, dict) else None
    if not isinstance(workers, dict):
        raise AssertionError(f"Vane fragment statistics do not expose workers: {stats!r}")
    require_equal(len(workers), WORKER_COUNT, "Vane Ray worker count")


class AnnotateWorkerNode:
    """Record which Ray node consumed each Paimon-backed batch."""

    def __call__(self, table: object) -> object:
        import pyarrow as pa
        import ray

        time.sleep(0.05)
        node_id = str(ray.get_runtime_context().get_node_id())
        return pa.table(
            {
                "id": table.column("id"),
                "worker_node_id": [node_id] * table.num_rows,
            }
        )


class FailSelectedPaimonWorker:
    """Fail one source partition after other worker tasks can finish writing."""

    def __call__(self, table: object) -> object:
        import pyarrow.compute as pc

        if table.num_rows and bool(pc.any(pc.equal(table.column("part"), 7)).as_py()):
            time.sleep(0.75)
            raise RuntimeError("intentional distributed Paimon worker failure")
        return table


class WaitForPaimonTargetConflict:
    """Hold every worker batch until the coordinator target baseline changes."""

    started_path = ""
    release_path = ""

    def __call__(self, table: object) -> object:
        if not self.started_path or not self.release_path:
            raise RuntimeError("Paimon target conflict UDF has no coordination paths")
        Path(self.started_path).touch()
        deadline = time.monotonic() + 90
        while not Path(self.release_path).exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("timed out waiting for the Paimon target conflict")
            time.sleep(0.05)
        return table


class WorkerPlanCaptureBackend:
    """Capture the worker-template scan plan produced by Vane's translator."""

    def __init__(self, scan_node_id: str):
        self.scan_node_id = scan_node_id
        self.worker_plan = None

    def register_query_owner(self, query_id: str, owner_query_id: str) -> None:
        require_equal(query_id, owner_query_id, "worker-plan capture query owner")

    def worker_snapshots(self) -> list[dict[str, object]]:
        return [
            {
                "worker_id": "worker-plan-capture",
                "num_cpus": 1.0,
                "num_gpus": 0.0,
                "total_memory_bytes": 1 << 30,
            }
        ]

    def submit_tasks(self, tasks: object) -> list[object]:
        for task in tasks:
            split_input = task.Inputs().get(self.scan_node_id)
            if split_input is None or split_input.get("kind") != "scan_split_batch":
                continue
            self.worker_plan = task.plan()
            break
        if self.worker_plan is None:
            raise AssertionError(f"no worker task contained Paimon scan node {self.scan_node_id}")
        raise RuntimeError(WORKER_PLAN_CAPTURE_SENTINEL)

    def drop_query(self, _query_id: str) -> None:
        return None

    def fte_prepare_drop_query(self, _query_id: str) -> dict[str, int]:
        return {"tasks_removed": 0, "tasks_canceled": 0, "fragments_removed": 0}

    def fte_cleanup_query(self, _query_id: str) -> dict[str, object]:
        return {}

    def fte_drop_query(self, _query_id: str) -> dict[str, int]:
        return {"tasks_removed": 0, "tasks_canceled": 0, "fragments_removed": 0}

    def shutdown(self) -> None:
        return None


class RayPaimonHarness:
    def __init__(self, vane: object, connection: object, runner: object):
        self.vane = vane
        self.connection = connection
        self.runner = runner
        self.read_dispatch_count = 0
        self.write_dispatch_count = 0
        self.plan_counter = 0
        self.last_write_result: dict[str, object] | None = None
        self._original_run_iter_tables = runner.run_iter_tables
        self._original_run_write = runner.run_write

        def record_distributed_read(*args: object, **kwargs: object) -> object:
            self.read_dispatch_count += 1
            return self._original_run_iter_tables(*args, **kwargs)

        def record_distributed_write(*args: object, **kwargs: object) -> object:
            self.write_dispatch_count += 1
            result = self._original_run_write(*args, **kwargs)
            self.last_write_result = result
            return result

        self._record_distributed_write = record_distributed_write
        runner.run_iter_tables = record_distributed_read
        runner.run_write = record_distributed_write

    def capture_write_plan(self, operation: Callable[[], object]) -> object:
        captured: list[object] = []

        def capture(relation: object) -> dict[str, object]:
            captured.append(
                self.vane.ray_cxx.PyLogicalPlan.from_duckdb_write_relation(
                    relation,
                    f"vane-wheel-ray-paimon-write-{uuid.uuid4().hex}",
                )
            )
            return {}

        self.runner.run_write = capture
        try:
            operation()
        finally:
            self.runner.run_write = self._record_distributed_write
        require_equal(len(captured), 1, "captured distributed Paimon write plan count")
        return captured[0]

    def require_query(
        self,
        query: str,
        expected: list[tuple[object, ...]],
        description: str,
    ) -> None:
        previous_count = self.read_dispatch_count
        actual = self.connection.sql(query).fetchall()
        require_equal(self.read_dispatch_count, previous_count + 1, f"{description} Ray dispatch")
        require_equal(actual, expected, description)

    def physical_plan(self, query: str) -> object:
        self.plan_counter += 1
        relation = self.connection.sql(query)
        return self.vane.ray_cxx.PyLogicalPlan.from_duckdb_relation(
            relation,
            f"vane-wheel-ray-paimon-plan-{self.plan_counter}-{uuid.uuid4().hex}",
        ).to_physical_plan(self.connection)

    def scan_split_count(self, query: str) -> int:
        return sum(len(batches) for batches in self.physical_plan(query).scan_split_batch_map().values())

    def require_write(
        self,
        description: str,
        operation: Callable[[], object],
        expected_rows: int,
        minimum_task_results: int,
    ) -> dict[str, object]:
        previous_count = self.write_dispatch_count
        self.last_write_result = None
        operation()
        require_equal(self.write_dispatch_count, previous_count + 1, f"{description} Ray dispatch")
        if self.last_write_result is None:
            raise AssertionError(f"{description}: Vane returned no distributed write result")
        result = self.last_write_result
        require_equal(result.get("extension_write"), True, f"{description} extension write marker")
        require_equal(
            result.get("extension_write_name"),
            "insert",
            f"{description} extension write name",
        )
        require_equal(
            result.get("extension_write_mode"),
            "callback",
            f"{description} extension write mode",
        )
        require_equal(
            result.get("extension_catalog_committed"),
            True,
            f"{description} catalog commit",
        )
        require_equal(result.get("rows_copied"), expected_rows, f"{description} affected rows")
        task_results = int(result.get("extension_task_result_count", 0))
        require_true(
            task_results >= minimum_task_results,
            f"{description} did not select enough worker tasks",
        )
        expected_fragments = 0 if expected_rows == 0 else task_results
        require_equal(
            result.get("extension_fragment_count"),
            expected_fragments,
            f"{description} fragments",
        )
        require_equal(
            result.get("extension_artifact_count"),
            expected_fragments,
            f"{description} attempt-manifest artifacts",
        )
        return result


def capture_worker_scan_plans(
    harness: RayPaimonHarness,
    coordinator_plan: object,
    scan_node_id: str,
    worker_connection: object,
    clone_count: int,
) -> list[object]:
    backend = WorkerPlanCaptureBackend(scan_node_id)
    runner = harness.vane.ray_cxx.DistributedPhysicalPlanRunner(backend)
    stream = runner.run_plan(coordinator_plan, harness.connection)
    try:
        deadline = time.monotonic() + WORKER_PLAN_CAPTURE_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                item = stream.next_nowait()
            except Exception as error:
                if not error_chain_contains(error, WORKER_PLAN_CAPTURE_SENTINEL):
                    raise AssertionError(f"worker-plan capture failed: {error!r}") from error
                break
            if item is not None:
                raise AssertionError(f"worker-plan capture unexpectedly produced a result: {item!r}")
            time.sleep(0.01)
        else:
            raise AssertionError("timed out waiting for Vane to produce a worker scan plan")

        if backend.worker_plan is None:
            raise AssertionError("Vane stopped worker-plan capture without producing a plan")
        return [backend.worker_plan.clone(worker_connection) for _ in range(clone_count)]
    finally:
        runner.drop_query_fragments(coordinator_plan.idx())


def create_paimon_fixture(connection: object, warehouse: Path) -> tuple[Path, Path, int, int, dict[str, Path]]:
    connection.execute(f"ATTACH {sql_string(warehouse)} AS pm (TYPE paimon)")
    connection.execute("CREATE SCHEMA pm.vane_ray")
    connection.execute(
        "CREATE TABLE pm.vane_ray.multi_split " "(id INTEGER, part INTEGER, payload VARCHAR) PARTITIONED BY (part)"
    )
    connection.execute(
        "INSERT INTO pm.vane_ray.multi_split "
        "SELECT i::INTEGER, (i % 4)::INTEGER, ('value-' || i::VARCHAR)::VARCHAR "
        "FROM range(0, 40) source(i)"
    )
    connection.execute(
        "INSERT INTO pm.vane_ray.multi_split "
        "SELECT i::INTEGER, (4 + i % 4)::INTEGER, ('value-' || i::VARCHAR)::VARCHAR "
        "FROM range(40, 80) source(i)"
    )
    connection.execute("CREATE TABLE pm.vane_ray.empty_table (id INTEGER, payload VARCHAR)")
    connection.execute(f"CREATE TABLE {INSERT_TARGET} (id INTEGER, part INTEGER, payload VARCHAR)")
    connection.execute(
        f"CREATE TABLE {PARTITIONED_INSERT_TARGET} " "(id INTEGER, part INTEGER, payload VARCHAR) PARTITIONED BY (part)"
    )
    connection.execute(f"CREATE TABLE {EMPTY_INSERT_TARGET} (id INTEGER, part INTEGER, payload VARCHAR)")
    connection.execute(f"CREATE TABLE {FAILURE_INSERT_TARGET} (id INTEGER, part INTEGER, payload VARCHAR)")
    connection.execute(f"CREATE TABLE {CONFLICT_INSERT_TARGET} (id INTEGER, part INTEGER, payload VARCHAR)")
    connection.execute(f"CREATE TABLE {SCHEMA_CONFLICT_INSERT_TARGET} (id INTEGER, part INTEGER, payload VARCHAR)")
    connection.execute(f"CREATE TABLE {ATTEMPT_METADATA_INSERT_TARGET} (id INTEGER, part INTEGER, payload VARCHAR)")
    connection.execute(f"CREATE TABLE {UNSELECTED_ATTEMPT_INSERT_TARGET} (id INTEGER, part INTEGER, payload VARCHAR)")

    multi_path = warehouse / "vane_ray.db/multi_split"
    empty_path = warehouse / "vane_ray.db/empty_table"
    snapshots = connection.execute(
        f"SELECT snapshot_id FROM paimon_snapshots({sql_string(multi_path)}) ORDER BY snapshot_id"
    ).fetchall()
    require_equal(len(snapshots), 2, "Paimon fixture snapshot count")
    target_paths = {
        INSERT_TARGET: warehouse / "vane_ray.db/insert_target",
        PARTITIONED_INSERT_TARGET: warehouse / "vane_ray.db/partitioned_insert_target",
        EMPTY_INSERT_TARGET: warehouse / "vane_ray.db/empty_insert_target",
        FAILURE_INSERT_TARGET: warehouse / "vane_ray.db/failure_insert_target",
        CONFLICT_INSERT_TARGET: warehouse / "vane_ray.db/conflict_insert_target",
        SCHEMA_CONFLICT_INSERT_TARGET: warehouse / "vane_ray.db/schema_conflict_insert_target",
        ATTEMPT_METADATA_INSERT_TARGET: warehouse / "vane_ray.db/attempt_metadata_insert_target",
        UNSELECTED_ATTEMPT_INSERT_TARGET: warehouse / "vane_ray.db/unselected_attempt_insert_target",
    }
    return (
        multi_path,
        empty_path,
        int(snapshots[0][0]),
        int(snapshots[1][0]),
        target_paths,
    )


def exercise_reads(
    harness: RayPaimonHarness,
    multi_path: Path,
    empty_path: Path,
    first_snapshot: int,
    second_snapshot: int,
) -> None:
    scan = f"paimon_scan({sql_string(multi_path)})"
    require_equal(harness.scan_split_count(f"SELECT id FROM {scan}"), 8, "Paimon split count")
    harness.require_query(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {scan}",
        [(80, 3160)],
        "multi-split aggregate",
    )

    partition_query = f"SELECT count(*)::BIGINT, min(id), max(id), sum(id)::BIGINT FROM {scan} WHERE part = 3"
    require_equal(harness.scan_split_count(partition_query), 1, "partition-pruned split count")
    harness.require_query(partition_query, [(10, 3, 39, 210)], "partition pruning")
    harness.require_query(
        f"SELECT id, payload FROM {scan} WHERE id BETWEEN 8 AND 12 ORDER BY id",
        [(value, f"value-{value}") for value in range(8, 13)],
        "projection and residual filter",
    )

    first_scan = f"paimon_scan({sql_string(multi_path)}, snapshot_from_id={first_snapshot})"
    second_scan = f"paimon_scan({sql_string(multi_path)}, snapshot_from_id={second_snapshot})"
    require_equal(
        harness.scan_split_count(f"SELECT id FROM {first_scan}"),
        4,
        "snapshot-one splits",
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT, max(id) FROM {first_scan}",
        [(40, 39)],
        "first frozen snapshot",
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT, max(id) FROM {second_scan}",
        [(80, 79)],
        "second frozen snapshot",
    )

    empty_scan = f"paimon_scan({sql_string(empty_path)})"
    require_equal(
        harness.scan_split_count(f"SELECT id FROM {empty_scan}"),
        1,
        "empty sentinel split",
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT FROM {empty_scan}",
        [(0,)],
        "empty Paimon table",
    )
    zero_match_query = f"SELECT count(*)::BIGINT FROM {scan} WHERE id = 100000"
    require_equal(harness.scan_split_count(zero_match_query), 1, "zero-match sentinel split")
    harness.require_query(zero_match_query, [(0,)], "zero-match Paimon scan")

    indexed_scan = f"paimon_scan({sql_string(SCALAR_INDEX_PATH)})"
    indexed_query = f"SELECT idx, part, payload FROM {indexed_scan} WHERE idx = 40"
    require_equal(harness.scan_split_count(indexed_query), 1, "indexed Paimon split count")
    harness.require_query(indexed_query, [(40, 0, "hit_40")], "serialized indexed split")


def exercise_worker_topology(
    harness: RayPaimonHarness,
    ray: object,
    expected_nodes: set[str],
    multi_path: Path,
) -> None:
    previous_count = harness.read_dispatch_count
    rows = (
        harness.connection.sql(f"SELECT id FROM paimon_scan({sql_string(multi_path)})")
        .map_batches(
            AnnotateWorkerNode,
            schema={
                "id": harness.vane.sqltype("INTEGER"),
                "worker_node_id": harness.vane.sqltype("VARCHAR"),
            },
            batch_size=8,
            cpus=1.0,
            execution_backend="ray_actor",
            actor_number=WORKER_COUNT,
            target_max_batch_bytes=4096,
        )
        .fetchall()
    )
    require_equal(harness.read_dispatch_count, previous_count + 1, "topology query Ray dispatch")
    require_equal(len(rows), 80, "topology query row count")
    require_equal(
        {str(row[1]) for row in rows},
        expected_nodes,
        "Ray nodes consuming Paimon splits",
    )
    assert_vane_worker_topology(ray, harness.runner)


def snapshot_count(connection: object, table_path: Path) -> int:
    return int(
        connection.execute(f"SELECT count(*)::BIGINT FROM paimon_snapshots({sql_string(table_path)})").fetchone()[0]
    )


def vane_attempt_artifacts(table_path: Path) -> list[Path]:
    return sorted(path for path in table_path.rglob("vane_*") if path.is_file())


def vane_attempt_manifests(table_path: Path) -> list[Path]:
    manifest_root = table_path / ".vane/paimon"
    if not manifest_root.exists():
        return []
    return sorted(path for path in manifest_root.rglob("*.commit") if path.is_file())


def vane_open_operations(table_path: Path) -> list[Path]:
    operation_root = table_path / ".vane/paimon/operations"
    if not operation_root.exists():
        return []
    return sorted(path for path in operation_root.rglob("*.open") if path.is_file())


def vane_recovery_records(table_path: Path) -> list[Path]:
    recovery_root = table_path / ".vane/paimon/recovery"
    if not recovery_root.exists():
        return []
    return sorted(path for path in recovery_root.rglob("*") if path.is_file())


def table_file_inventory(table_path: Path) -> list[Path]:
    return sorted(path.relative_to(table_path) for path in table_path.rglob("*") if path.is_file())


def source_insert_relation(harness: RayPaimonHarness, multi_path: Path) -> object:
    return harness.connection.sql(f"SELECT id, part, payload FROM paimon_scan({sql_string(multi_path)})")


def exercise_distributed_inserts(
    harness: RayPaimonHarness,
    multi_path: Path,
    target_paths: dict[str, Path],
) -> None:
    connection = harness.connection
    harness.require_write(
        "unpartitioned distributed Paimon INSERT",
        lambda: source_insert_relation(harness, multi_path).insert_into(INSERT_TARGET),
        expected_rows=80,
        minimum_task_results=2,
    )
    require_equal(
        snapshot_count(connection, target_paths[INSERT_TARGET]),
        1,
        "unpartitioned INSERT snapshots",
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {INSERT_TARGET}",
        [(80, 3160)],
        "unpartitioned distributed INSERT result",
    )

    harness.require_write(
        "partitioned distributed Paimon INSERT",
        lambda: source_insert_relation(harness, multi_path).insert_into(PARTITIONED_INSERT_TARGET),
        expected_rows=80,
        minimum_task_results=2,
    )
    require_equal(
        snapshot_count(connection, target_paths[PARTITIONED_INSERT_TARGET]),
        1,
        "partitioned INSERT snapshots",
    )
    partition_query = (
        f"SELECT count(*)::BIGINT, min(id), max(id), sum(id)::BIGINT "
        f"FROM {PARTITIONED_INSERT_TARGET} WHERE part = 6"
    )
    harness.require_query(partition_query, [(10, 42, 78, 600)], "partitioned distributed INSERT result")

    empty_source = harness.connection.sql(
        f"SELECT id, part, payload FROM paimon_scan({sql_string(multi_path)}) WHERE id < 0"
    )
    harness.require_write(
        "empty distributed Paimon INSERT",
        lambda: empty_source.insert_into(EMPTY_INSERT_TARGET),
        expected_rows=0,
        minimum_task_results=1,
    )
    require_equal(
        snapshot_count(connection, target_paths[EMPTY_INSERT_TARGET]),
        0,
        "empty INSERT snapshots",
    )
    require_equal(
        connection.execute(f"SELECT count(*)::BIGINT FROM {EMPTY_INSERT_TARGET}").fetchone(),
        (0,),
        "empty distributed INSERT result",
    )


def exercise_duplicate_retry_attempt_metadata(
    harness: RayPaimonHarness,
    multi_path: Path,
    target_path: Path,
) -> None:
    from vane.runners.fte import FteTaskAttemptId
    from vane.runners.fte.backends.native import NativeFteWorkerManagerBackend
    from vane.runners.local.runner import _InProcessFragmentExecutor

    baseline_files = table_file_inventory(target_path)
    logical_plan = harness.capture_write_plan(
        lambda: source_insert_relation(harness, multi_path).insert_into(ATTEMPT_METADATA_INSERT_TARGET)
    )
    physical_plan = logical_plan.to_physical_plan(harness.connection)
    executor = _InProcessFragmentExecutor()
    backend = NativeFteWorkerManagerBackend(
        execute_fn=executor,
        num_workers=WORKER_COUNT,
        max_running_tasks=WORKER_COUNT,
    )
    original_submit_tasks = backend.submit_tasks
    retried_attempt_ids: list[str] = []

    def submit_with_retried_attempt(tasks: object) -> list[object]:
        task_list = list(tasks)
        handles = list(original_submit_tasks(task_list))
        if task_list and handles and not retried_attempt_ids:
            retry_request = backend._request_from_task(task_list[0])
            selected_id = FteTaskAttemptId.coerce(retry_request["task_id"])
            retry_id = FteTaskAttemptId(selected_id.task_id, selected_id.attempt_id + 1)
            retried_attempt_ids.append(str(retry_id))
            retry_request["task_id"] = retry_id.to_dict()
            retry_context = dict(retry_request.get("context") or {})
            retry_context["attempt_id"] = str(retry_id.attempt_id)
            retry_request["context"] = retry_context
            # Execute a genuine retry of the same logical task. Vane's generic
            # envelope parser sees two unique attempt IDs; Paimon must reject
            # selecting both attempts for one logical task before committing.
            handles.extend(original_submit_tasks([retry_request]))
        return handles

    backend.submit_tasks = submit_with_retried_attempt
    plan_runner = harness.vane.ray_cxx.DistributedPhysicalPlanRunner(backend)
    try:
        result = plan_runner.run_copy_plan(physical_plan, harness.connection)
        require_equal(
            result.get("extension_write"),
            True,
            "attempt metadata extension write marker",
        )
        require_equal(
            result.get("extension_catalog_committed"),
            False,
            "attempt metadata catalog commit marker",
        )
        require_equal(
            result.get("copy_output_outcome_unknown"),
            True,
            "attempt metadata runner outcome",
        )
        require_true(
            "selected multiple attempts for Vane logical task" in str(result.get("copy_output_outcome_error") or ""),
            "attempt metadata rejection did not reach the Paimon coordinator",
        )
        require_true(
            int(result.get("extension_task_result_count") or 0) > WORKER_COUNT,
            "attempt metadata rejection did not select the retried task result",
        )
        require_true(
            bool(retried_attempt_ids),
            "duplicate/retried attempt injection did not reach a write task",
        )
        require_equal(
            snapshot_count(harness.connection, target_path),
            0,
            "attempt metadata failure snapshots",
        )
        require_equal(
            vane_attempt_artifacts(target_path),
            [],
            "attempt metadata failure artifact cleanup",
        )
        require_equal(
            table_file_inventory(target_path),
            baseline_files,
            "attempt metadata failure file cleanup",
        )
    finally:
        try:
            backend.shutdown()
        finally:
            executor.close()

    harness.require_write(
        "distributed Paimon INSERT retry after attempt metadata rejection",
        lambda: source_insert_relation(harness, multi_path).insert_into(ATTEMPT_METADATA_INSERT_TARGET),
        expected_rows=80,
        minimum_task_results=2,
    )
    require_equal(
        snapshot_count(harness.connection, target_path),
        1,
        "attempt metadata retry snapshots",
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {ATTEMPT_METADATA_INSERT_TARGET}",
        [(80, 3160)],
        "attempt metadata retry result",
    )


def exercise_unselected_attempt_artifact_cleanup(
    harness: RayPaimonHarness,
    multi_path: Path,
    target_path: Path,
) -> None:
    from vane.runners.fte import FteTaskAttemptId
    from vane.runners.fte.backends.native import NativeFteWorkerManagerBackend
    from vane.runners.local.runner import _InProcessFragmentExecutor

    baseline_open_operations = set(vane_open_operations(target_path))
    logical_plan = harness.capture_write_plan(
        lambda: source_insert_relation(harness, multi_path).insert_into(UNSELECTED_ATTEMPT_INSERT_TARGET)
    )
    physical_plan = logical_plan.to_physical_plan(harness.connection)
    executor = _InProcessFragmentExecutor()
    backend = NativeFteWorkerManagerBackend(
        execute_fn=executor,
        num_workers=WORKER_COUNT,
        max_running_tasks=WORKER_COUNT,
    )
    retry_executor = _InProcessFragmentExecutor()
    retry_started = threading.Event()
    retry_release = threading.Event()

    def execute_late_retry(request: object) -> object:
        retry_started.set()
        if not retry_release.wait(timeout=120):
            raise TimeoutError("timed out waiting to release the late unselected retry")
        return retry_executor(request)

    retry_backend = NativeFteWorkerManagerBackend(
        execute_fn=execute_late_retry,
        num_workers=1,
        max_running_tasks=1,
    )
    original_submit_tasks = backend.submit_tasks
    loser_attempt_digests: list[str] = []
    loser_handles: list[object] = []
    orphan_operation_marker: tuple[Path, bytes] | None = None
    retry_scheduled = False

    def submit_with_unselected_attempt_artifact(tasks: object) -> list[object]:
        nonlocal retry_scheduled
        task_list = list(tasks)
        handles = list(original_submit_tasks(task_list))
        if task_list and handles and not retry_scheduled:
            retry_scheduled = True
            retry_request = backend._request_from_task(task_list[0])
            selected_id = FteTaskAttemptId.coerce(retry_request["task_id"])
            retry_id = FteTaskAttemptId(selected_id.task_id, selected_id.attempt_id + 1)
            retry_request["task_id"] = retry_id.to_dict()
            retry_context = dict(retry_request.get("context") or {})
            retry_context["attempt_id"] = str(retry_id.attempt_id)
            retry_request["context"] = retry_context
            digest = hashlib.md5()
            digest.update(retry_id.query_id.encode())
            digest.update(b"\0")
            digest.update(str(retry_id).encode())
            loser_attempt_digests.append(digest.hexdigest())
            original_get_result = handles[0].get_result_sync

            def get_result_with_unselected_retry() -> object:
                nonlocal orphan_operation_marker
                selected_result = original_get_result()
                submitted_handles = list(retry_backend.submit_tasks([retry_request]))
                require_equal(len(submitted_handles), 1, "unselected retry handle count")
                loser_handles.extend(submitted_handles)
                source_node_ids = list(retry_request.get("source_node_ids") or [])
                if source_node_ids:
                    retry_backend.task_input_stream_exhausted(retry_id.query_id, source_node_ids)
                # Register a genuine retry before the coordinator sees the selected result, but keep its execution
                # blocked. Releasing it only after run_copy_plan returns forces it to finish after the coordinator's
                # manifest scan and catalog commit.
                if not retry_started.wait(timeout=WORKER_PLAN_CAPTURE_TIMEOUT_SECONDS):
                    raise TimeoutError("timed out waiting for the late unselected retry to start")
                new_open_operations = set(vane_open_operations(target_path)) - baseline_open_operations
                require_equal(len(new_open_operations), 1, "active INSERT operation marker count")
                marker_path = next(iter(new_open_operations))
                orphan_operation_marker = (marker_path, marker_path.read_bytes())
                return selected_result

            handles[0].get_result_sync = get_result_with_unselected_retry
        return handles

    backend.submit_tasks = submit_with_unselected_attempt_artifact
    plan_runner = harness.vane.ray_cxx.DistributedPhysicalPlanRunner(backend)
    blocked_directory = target_path / "unrelated/blocked"
    blocked_directory.mkdir(parents=True)
    blocked_directory.chmod(0)
    try:
        result = plan_runner.run_copy_plan(physical_plan, harness.connection)
        require_equal(
            result.get("extension_write"),
            True,
            "unselected attempt extension write marker",
        )
        require_equal(
            result.get("extension_catalog_committed"),
            True,
            "unselected attempt catalog commit",
        )
        require_equal(result.get("rows_copied"), 80, "unselected attempt affected rows")
        require_true(
            int(result.get("extension_task_result_count") or 0) >= WORKER_COUNT,
            "unselected attempt cleanup did not select enough worker tasks",
        )
        require_equal(
            set(vane_open_operations(target_path)),
            baseline_open_operations,
            "committed INSERT operation-marker cleanup",
        )
        retry_release.set()
        require_equal(len(loser_handles), 1, "late unselected retry handle count")
        loser_handle = loser_handles[0]
        deadline = time.monotonic() + WORKER_PLAN_CAPTURE_TIMEOUT_SECONDS
        while not loser_handle.done():
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for the late unselected retry attempt")
            time.sleep(0.01)
        late_retry_error: BaseException | None = None
        try:
            loser_handle.get_result_sync()
        except BaseException as error:
            late_retry_error = error
        finally:
            try:
                loser_handle.ack()
            finally:
                loser_handle.release_result_payload()
        require_true(late_retry_error is not None, "late unselected retry unexpectedly succeeded")
        if not error_chain_contains(late_retry_error, "operation is closed"):
            raise AssertionError(
                f"late unselected retry returned the wrong error: {late_retry_error!r}"
            ) from late_retry_error
    finally:
        retry_release.set()
        try:
            blocked_directory.chmod(0o755)
        finally:
            try:
                backend.shutdown()
            finally:
                try:
                    retry_backend.shutdown()
                finally:
                    try:
                        executor.close()
                    finally:
                        retry_executor.close()

    require_true(bool(loser_attempt_digests), "unselected retry attempt did not run")
    remaining_loser_artifacts = [
        path
        for path in vane_attempt_artifacts(target_path)
        if any(digest in path.name for digest in loser_attempt_digests)
    ]
    require_equal(
        remaining_loser_artifacts,
        [],
        "unselected retry attempt data/index cleanup",
    )
    require_equal(
        vane_attempt_manifests(target_path),
        [],
        "successful INSERT attempt-manifest cleanup",
    )
    require_equal(
        set(vane_open_operations(target_path)),
        baseline_open_operations,
        "successful INSERT retained operation markers",
    )
    require_true(orphan_operation_marker is not None, "active INSERT operation marker was not captured")
    marker_path, marker_payload = orphan_operation_marker
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_bytes(marker_payload)
    require_equal(
        set(vane_open_operations(target_path)),
        baseline_open_operations | {marker_path},
        "crashed coordinator operation marker setup",
    )
    empty_source = harness.connection.sql(
        f"SELECT id, part, payload FROM paimon_scan({sql_string(multi_path)}) WHERE id < 0"
    )
    harness.require_write(
        "orphan operation marker recovery",
        lambda: empty_source.insert_into(UNSELECTED_ATTEMPT_INSERT_TARGET),
        expected_rows=0,
        minimum_task_results=1,
    )
    require_equal(
        set(vane_open_operations(target_path)),
        baseline_open_operations,
        "orphan operation-marker cleanup",
    )
    require_equal(vane_recovery_records(target_path), [], "orphan operation recovery-record cleanup")
    require_equal(
        snapshot_count(harness.connection, target_path),
        1,
        "unselected attempt snapshot count",
    )
    harness.require_query(
        f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {UNSELECTED_ATTEMPT_INSERT_TARGET}",
        [(80, 3160)],
        "unselected attempt committed result",
    )


def exercise_worker_failure_and_retry(
    harness: RayPaimonHarness,
    multi_path: Path,
    target_path: Path,
) -> None:
    vane = harness.vane
    baseline_files = table_file_inventory(target_path)
    baseline_open_operations = set(vane_open_operations(target_path))
    relation = source_insert_relation(harness, multi_path).map_batches(
        FailSelectedPaimonWorker,
        schema={
            "id": vane.sqltype("INTEGER"),
            "part": vane.sqltype("INTEGER"),
            "payload": vane.sqltype("VARCHAR"),
        },
        batch_size=8,
        cpus=1.0,
        execution_backend="ray_actor",
        actor_number=WORKER_COUNT,
        target_max_batch_bytes=4096,
    )
    previous_count = harness.write_dispatch_count
    require_error(
        "distributed Paimon worker failure",
        lambda: relation.insert_into(FAILURE_INSERT_TARGET),
        "intentional distributed Paimon worker failure",
    )
    require_equal(harness.write_dispatch_count, previous_count + 1, "worker failure Ray dispatch")
    require_equal(
        snapshot_count(harness.connection, target_path),
        0,
        "worker failure snapshot cleanup",
    )
    require_equal(vane_attempt_artifacts(target_path), [], "worker failure artifact cleanup")
    require_equal(table_file_inventory(target_path), baseline_files, "worker failure file cleanup")
    require_equal(
        set(vane_open_operations(target_path)),
        baseline_open_operations,
        "failed INSERT operation-marker cleanup",
    )

    harness.require_write(
        "distributed Paimon INSERT retry after worker failure",
        lambda: source_insert_relation(harness, multi_path).insert_into(FAILURE_INSERT_TARGET),
        expected_rows=80,
        minimum_task_results=2,
    )
    require_equal(
        snapshot_count(harness.connection, target_path),
        1,
        "worker retry snapshot count",
    )
    require_equal(
        harness.connection.execute(f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {FAILURE_INSERT_TARGET}").fetchone(),
        (80, 3160),
        "worker retry result",
    )
    require_equal(
        set(vane_open_operations(target_path)),
        baseline_open_operations,
        "retried INSERT operation-marker cleanup",
    )


def run_blocked_distributed_insert(
    harness: RayPaimonHarness,
    multi_path: Path,
    target_path: Path,
    target: str,
    description: str,
    mutation: Callable[[], None],
) -> list[BaseException]:
    marker = uuid.uuid4().hex
    started_path = target_path.parent.parent / f".vane-paimon-{marker}-started"
    release_path = target_path.parent.parent / f".vane-paimon-{marker}-release"

    class ConfiguredWaitForPaimonTargetConflict(WaitForPaimonTargetConflict):
        pass

    ConfiguredWaitForPaimonTargetConflict.started_path = str(started_path)
    ConfiguredWaitForPaimonTargetConflict.release_path = str(release_path)
    relation = source_insert_relation(harness, multi_path).map_batches(
        ConfiguredWaitForPaimonTargetConflict,
        schema={
            "id": harness.vane.sqltype("INTEGER"),
            "part": harness.vane.sqltype("INTEGER"),
            "payload": harness.vane.sqltype("VARCHAR"),
        },
        batch_size=8,
        cpus=1.0,
        execution_backend="ray_actor",
        actor_number=WORKER_COUNT,
        target_max_batch_bytes=4096,
    )
    errors: list[BaseException] = []
    previous_count = harness.write_dispatch_count

    def execute_distributed_insert() -> None:
        try:
            relation.insert_into(target)
        except BaseException as error:
            errors.append(error)

    write_thread = threading.Thread(
        target=execute_distributed_insert,
        name=f"vane-paimon-{description}",
        daemon=True,
    )
    write_thread.start()
    coordination_error: BaseException | None = None
    try:
        deadline = time.monotonic() + 90
        while not started_path.exists():
            if not write_thread.is_alive():
                if errors:
                    raise AssertionError(f"{description} write failed too early: {errors[0]!r}") from errors[0]
                raise AssertionError(f"{description} write stopped before worker execution")
            if time.monotonic() >= deadline:
                raise AssertionError(f"timed out waiting for {description} worker execution")
            time.sleep(0.05)
        mutation()
    except BaseException as error:
        coordination_error = error
    finally:
        release_path.touch()

    write_thread.join(timeout=120)
    started_path.unlink(missing_ok=True)
    release_path.unlink(missing_ok=True)
    if write_thread.is_alive():
        raise AssertionError(f"distributed Paimon {description} write did not stop")
    if coordination_error is not None:
        raise coordination_error
    require_equal(harness.write_dispatch_count, previous_count + 1, f"{description} Ray dispatch")
    return errors


def exercise_target_conflict_and_retry(
    harness: RayPaimonHarness,
    multi_path: Path,
    target_path: Path,
) -> None:
    def commit_conflicting_snapshot() -> None:
        mutation_connection = harness.vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        try:
            verify_extension_is_wheel_linked(mutation_connection)
            warehouse = target_path.parent.parent
            # Use a lexically different spelling of the coordinator warehouse so this conflict also exercises the
            # canonical cross-ATTACH mutation-lock identity.
            warehouse_alias = warehouse / "vane_ray.db/.."
            mutation_connection.execute(f"ATTACH {sql_string(warehouse_alias)} AS conflict_pm (TYPE paimon)")
            mutation_connection.execute(
                "INSERT INTO conflict_pm.vane_ray.conflict_insert_target VALUES (-1, 99, 'conflict')"
            )
        finally:
            mutation_connection.close()

    errors = run_blocked_distributed_insert(
        harness,
        multi_path,
        target_path,
        CONFLICT_INSERT_TARGET,
        "target conflict",
        commit_conflicting_snapshot,
    )
    require_equal(len(errors), 1, "target conflict failure count")
    if not error_chain_contains(errors[0], "snapshot changed after the distributed INSERT was planned"):
        raise AssertionError(f"target conflict returned the wrong error: {errors[0]!r}") from errors[0]
    require_equal(
        snapshot_count(harness.connection, target_path),
        1,
        "target conflict snapshot count",
    )
    require_equal(vane_attempt_artifacts(target_path), [], "target conflict artifact cleanup")
    require_equal(
        harness.connection.execute(f"SELECT id, part, payload FROM {CONFLICT_INSERT_TARGET} ORDER BY id").fetchall(),
        [(-1, 99, "conflict")],
        "target conflict retained only the concurrent commit",
    )

    harness.require_write(
        "distributed Paimon INSERT retry after target conflict",
        lambda: source_insert_relation(harness, multi_path).insert_into(CONFLICT_INSERT_TARGET),
        expected_rows=80,
        minimum_task_results=2,
    )
    require_equal(
        snapshot_count(harness.connection, target_path),
        2,
        "target conflict retry snapshots",
    )
    require_equal(
        harness.connection.execute(
            f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {CONFLICT_INSERT_TARGET}"
        ).fetchone(),
        (81, 3159),
        "target conflict retry result",
    )


def exercise_schema_conflict_and_retry(
    harness: RayPaimonHarness,
    multi_path: Path,
    target_path: Path,
) -> None:
    def publish_schema_revision() -> None:
        schema_directory = target_path / "schema"
        source_path = schema_directory / "schema-0"
        schema = json.loads(source_path.read_text(encoding="utf-8"))
        schema["id"] = 1
        schema["timeMillis"] = int(schema["timeMillis"]) + 1
        temporary_path = schema_directory / f".schema-1-{uuid.uuid4().hex}"
        temporary_path.write_text(
            json.dumps(schema, indent=4) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(schema_directory / "schema-1")

    errors = run_blocked_distributed_insert(
        harness,
        multi_path,
        target_path,
        SCHEMA_CONFLICT_INSERT_TARGET,
        "schema conflict",
        publish_schema_revision,
    )
    require_equal(len(errors), 1, "schema conflict failure count")
    if not error_chain_contains(errors[0], "schema changed after the distributed INSERT was planned"):
        raise AssertionError(f"schema conflict returned the wrong error: {errors[0]!r}") from errors[0]
    require_equal(
        snapshot_count(harness.connection, target_path),
        0,
        "schema conflict snapshot count",
    )
    require_equal(vane_attempt_artifacts(target_path), [], "schema conflict artifact cleanup")

    harness.require_write(
        "distributed Paimon INSERT retry after schema conflict",
        lambda: source_insert_relation(harness, multi_path).insert_into(SCHEMA_CONFLICT_INSERT_TARGET),
        expected_rows=80,
        minimum_task_results=2,
    )
    require_equal(
        snapshot_count(harness.connection, target_path),
        1,
        "schema conflict retry snapshots",
    )
    require_equal(
        harness.connection.execute(
            f"SELECT count(*)::BIGINT, sum(id)::BIGINT FROM {SCHEMA_CONFLICT_INSERT_TARGET}"
        ).fetchone(),
        (80, 3160),
        "schema conflict retry result",
    )


def exercise_fail_closed_payloads(harness: RayPaimonHarness, multi_path: Path) -> None:
    query = f"SELECT id FROM paimon_scan({sql_string(multi_path)})"
    source_plan = harness.physical_plan(query)
    target_plan = harness.physical_plan(query)
    source_batches = source_plan.scan_split_batch_map()
    target_batches = target_plan.scan_split_batch_map()
    require_equal(len(source_batches), 1, "source Paimon scan-node count")
    require_equal(len(target_batches), 1, "target Paimon scan-node count")
    source_node_id, source_node_batches = next(iter(source_batches.items()))
    target_node_id, target_node_batches = next(iter(target_batches.items()))
    require_equal(source_node_id, target_node_id, "cross-plan Paimon scan-node id")
    source_batch = bytes(source_node_batches[0])
    target_batch = bytes(target_node_batches[0])

    worker_connection = harness.vane.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    try:
        verify_extension_is_wheel_linked(worker_connection)
        coordinator_worker = target_plan.clone(worker_connection)
        cross_plan_worker, malformed_worker = capture_worker_scan_plans(
            harness,
            target_plan,
            str(target_node_id),
            worker_connection,
            clone_count=2,
        )
        require_error(
            "coordinator Paimon split assignment",
            lambda: harness.vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                worker_connection,
                coordinator_worker,
                scan_split_batch={str(target_node_id): target_batch},
            ),
            "require an unassigned worker bind",
        )
        require_error(
            "cross-plan Paimon split",
            lambda: harness.vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                worker_connection,
                cross_plan_worker,
                scan_split_batch={str(target_node_id): source_batch},
            ),
            "does not match its worker bind identity",
        )

        require_error(
            "malformed Paimon split batch",
            lambda: harness.vane.ray_cxx.DistributedPhysicalPlanRunner().execute_native(
                worker_connection,
                malformed_worker,
                scan_split_batch={str(target_node_id): target_batch[:-1]},
            ),
        )
        require_error(
            "duplicate Paimon split id",
            lambda: harness.vane.ray_cxx.merge_scan_split_batches([target_batch, target_batch]),
        )
    finally:
        worker_connection.close()


def main() -> None:
    if os.environ.get("VANE_RUNNER") != "ray":
        raise RuntimeError("the distributed wheel integration test requires VANE_RUNNER=ray")
    os.environ["VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION"] = "1"

    import ray
    import vane
    from vane import runners

    if ray.is_initialized():
        raise RuntimeError("the Ray wheel integration test must own its Ray cluster")

    cluster = create_two_worker_cluster(ray)
    connection = None
    try:
        expected_nodes = execution_node_ids(ray)
        vane.set_runner_ray(noop_if_initialized=True)
        runner = runners.get_or_create_runner()
        require_equal(getattr(runner, "name", None), "ray", "configured Vane runner")

        connection = vane.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": "false",
                "autoload_known_extensions": "false",
            },
        )
        verify_extension_is_wheel_linked(connection)
        with tempfile.TemporaryDirectory(prefix="vane-paimon-ray-") as warehouse_text:
            warehouse = Path(warehouse_text).resolve()
            multi_path, empty_path, first_snapshot, second_snapshot, target_paths = create_paimon_fixture(
                connection, warehouse
            )
            harness = RayPaimonHarness(vane, connection, runner)
            exercise_reads(
                harness,
                multi_path,
                empty_path,
                first_snapshot,
                second_snapshot,
            )
            exercise_worker_topology(harness, ray, expected_nodes, multi_path)
            exercise_fail_closed_payloads(harness, multi_path)
            exercise_distributed_inserts(harness, multi_path, target_paths)
            exercise_duplicate_retry_attempt_metadata(
                harness,
                multi_path,
                target_paths[ATTEMPT_METADATA_INSERT_TARGET],
            )
            exercise_unselected_attempt_artifact_cleanup(
                harness,
                multi_path,
                target_paths[UNSELECTED_ATTEMPT_INSERT_TARGET],
            )
            exercise_worker_failure_and_retry(
                harness,
                multi_path,
                target_paths[FAILURE_INSERT_TARGET],
            )
            exercise_target_conflict_and_retry(
                harness,
                multi_path,
                target_paths[CONFLICT_INSERT_TARGET],
            )
            exercise_schema_conflict_and_retry(
                harness,
                multi_path,
                target_paths[SCHEMA_CONFLICT_INSERT_TARGET],
            )
            for target, target_path in target_paths.items():
                require_equal(vane_open_operations(target_path), [], f"{target} retained operation markers")
                require_equal(vane_attempt_manifests(target_path), [], f"{target} retained attempt manifests")
                require_equal(vane_recovery_records(target_path), [], f"{target} retained recovery records")
            require_true(
                harness.read_dispatch_count >= 9,
                "Ray suite did not exercise enough reads",
            )
            require_true(
                harness.write_dispatch_count >= 9,
                "Ray suite did not exercise enough writes",
            )
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                vane.teardown_runner()
            finally:
                if ray.is_initialized():
                    ray.shutdown()
                cluster.shutdown()


if __name__ == "__main__":
    main()
