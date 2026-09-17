# SPDX-FileCopyrightText: 2026 lance-duckdb contributors
# SPDX-FileCopyrightText: 2026 AstroVela contributors
# SPDX-License-Identifier: Apache-2.0

"""Execute the Vane README provider walkthrough on an owned default-Ray cluster."""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def ray_cluster():
    import ray
    import vane
    from ray.cluster_utils import Cluster

    if "VANE_RUNNER" in os.environ:
        raise RuntimeError("leave VANE_RUNNER unset to qualify the default Ray runner")
    if ray.is_initialized():
        raise RuntimeError("the integration suite must own its Ray cluster")
    environment = pytest.MonkeyPatch()
    environment.setenv("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")
    environment.setenv("RAY_task_events_report_interval_ms", "100")
    cluster = Cluster(shutdown_at_exit=False)
    try:
        cluster.add_node(
            include_dashboard=False,
            num_cpus=0,
            num_gpus=0,
            object_store_memory=128 * 1024 * 1024,
        )
        for _ in range(2):
            cluster.add_node(
                include_dashboard=False,
                num_cpus=1,
                num_gpus=0,
                object_store_memory=128 * 1024 * 1024,
            )
        ray.init(address=cluster.address, ignore_reinit_error=False, log_to_driver=True)
        deadline = time.monotonic() + 30
        while True:
            nodes = frozenset(
                str(node["NodeID"])
                for node in ray.nodes()
                if node.get("Alive") and (node.get("Resources") or {}).get("CPU", 0) >= 1
            )
            if len(nodes) == 2:
                break
            if time.monotonic() >= deadline:
                raise AssertionError("expected two Ray execution nodes")
            time.sleep(0.1)
        yield nodes
    finally:
        try:
            vane.teardown_runner()
        finally:
            try:
                ray.shutdown()
            finally:
                try:
                    cluster.shutdown()
                finally:
                    environment.undo()


@pytest.fixture
def default_ray_runtime(ray_cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    import vane
    from vane import runners

    assert "VANE_RUNNER" not in os.environ
    assert len(ray_cluster) == 2
    monkeypatch.setenv("VANE_DISTRIBUTED_NODE_COUNT", "2")
    monkeypatch.setenv("VANE_DISTRIBUTED_WORKER_SLOTS", "2")
    monkeypatch.setenv("VANE_RAY_SCAN_SPLIT_MIN_COUNT", "4")
    monkeypatch.setenv("VANE_FTE_DYNAMIC_SCAN_MAX_SPLITS_PER_PARTITION", "1")
    monkeypatch.setenv("VANE_SHUFFLE_LOCAL_DIRS", str(tmp_path / "shuffle"))
    vane.teardown_runner()
    runner = runners.get_or_create_runner()
    assert runner.name == "ray"
    try:
        yield runner
    finally:
        vane.teardown_runner()


pytestmark = [
    pytest.mark.real_ray,
    pytest.mark.ray_cluster_owner,
    pytest.mark.usefixtures("default_ray_runtime"),
]


def test_readme(tmp_path, monkeypatch):
    import vane
    from vane import runners

    readme = Path(__file__).resolve().parents[2] / "VANE_README.md"
    markdown = readme.read_text()
    monkeypatch.chdir(tmp_path)
    namespace = {}
    runner = runners.get_or_create_runner()
    dispatch = {"reads": 0, "writes": 0}
    original_read = runner.run_iter_tables
    original_write = runner.run_write

    def read(*a, **kw):
        dispatch["reads"] += 1
        return original_read(*a, **kw)

    def write(*a, **kw):
        dispatch["writes"] += 1
        return original_write(*a, **kw)

    monkeypatch.setattr(runner, "run_iter_tables", read)
    monkeypatch.setattr(runner, "run_write", write)
    try:
        for index, block in enumerate(re.finditer(r"^```python\n(.*?)^```", markdown, re.M | re.S), 1):
            print("README block", index, flush=True)
            exec(compile(block.group(1), str(readme), "exec"), namespace)
            assert "VANE_RUNNER" not in os.environ
            assert runner.name == "ray"
        c = namespace["connection"]
        assert sorted(c.sql("SELECT id,bucket,payload FROM pm.demo.events").fetchall()) == [
            (i, i % 4, f"value-{i}") for i in range(1100)
        ]
        assert sorted(
            c.sql(
                "SELECT snapshot_id,total_record_count FROM paimon_snapshots(?)",
                params=[namespace["table_path"]],
            ).fetchall()
        ) == [(1, 1000), (2, 1100)]
        assert c.sql("SELECT count(*),sum(id)::BIGINT FROM pm.demo.events AT (VERSION => 1)").fetchall() == [
            (1000, 499500)
        ]
        assert sorted(
            c.sql("SELECT bucket,count(*),sum(id)::BIGINT FROM pm.demo.events GROUP BY bucket").fetchall()
        ) == [(b, 275, sum(range(b, 1100, 4))) for b in range(4)]
        assert dispatch["writes"] == 2, dispatch
        assert dispatch["reads"] >= 9, dispatch
        print("PASS blocks", index, "dispatch", dispatch, flush=True)
    finally:
        if "connection" in namespace:
            namespace["connection"].close()
