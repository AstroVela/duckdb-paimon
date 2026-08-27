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

import os
import tempfile
import time
import uuid
from collections.abc import Callable
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCALAR_INDEX_PATH = REPOSITORY_ROOT / "data/scalar_index.db/t1"
WORKER_COUNT = 2
WORKER_PLAN_CAPTURE_SENTINEL = "intentional worker-plan capture stop"
WORKER_PLAN_CAPTURE_TIMEOUT_SECONDS = 30


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
        self.plan_counter = 0
        self._original_run_iter_tables = runner.run_iter_tables

        def record_distributed_read(*args: object, **kwargs: object) -> object:
            self.read_dispatch_count += 1
            return self._original_run_iter_tables(*args, **kwargs)

        runner.run_iter_tables = record_distributed_read

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


def create_paimon_fixture(connection: object, warehouse: Path) -> tuple[Path, Path, int, int]:
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

    multi_path = warehouse / "vane_ray.db/multi_split"
    empty_path = warehouse / "vane_ray.db/empty_table"
    snapshots = connection.execute(
        f"SELECT snapshot_id FROM paimon_snapshots({sql_string(multi_path)}) ORDER BY snapshot_id"
    ).fetchall()
    require_equal(len(snapshots), 2, "Paimon fixture snapshot count")
    return multi_path, empty_path, int(snapshots[0][0]), int(snapshots[1][0])


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
    require_equal(harness.scan_split_count(f"SELECT id FROM {first_scan}"), 4, "snapshot-one splits")
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
    require_equal(harness.scan_split_count(f"SELECT id FROM {empty_scan}"), 1, "empty sentinel split")
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
    require_equal({str(row[1]) for row in rows}, expected_nodes, "Ray nodes consuming Paimon splits")
    assert_vane_worker_topology(ray, harness.runner)


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
            multi_path, empty_path, first_snapshot, second_snapshot = create_paimon_fixture(connection, warehouse)
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
            require_true(harness.read_dispatch_count >= 9, "Ray suite did not exercise enough reads")
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
