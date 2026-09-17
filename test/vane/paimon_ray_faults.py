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

"""Scoped fault injection inside the default Ray driver and real workers."""

from __future__ import annotations

import sys
import threading
import time
from contextlib import contextmanager


def _install_worker_duplicate(actor: object, scheduled_id: str, first_id: str) -> None:
    from vane.runners.fte import FteTaskAttemptId

    manager = actor._get_fte_task_manager()
    original = manager.execute_fn
    first = FteTaskAttemptId.coerce(first_id)
    retry = FteTaskAttemptId(first.task_id, first.attempt_id + 1)
    state = {"original": original, "attempts": [], "worker_pid": None}
    actor._paimon_duplicate_fault = state

    async def execute_with_duplicate_identity(request: dict[str, object]) -> object:
        import os

        if str(FteTaskAttemptId.coerce(request["task_id"])) != scheduled_id:
            return await original(request)
        manager.execute_fn = original
        # Corrupt only this worker's native attempt identity. The scheduler
        # still owns its original task, inputs, lease and result. Two real
        # tasks therefore publish individually valid native envelopes for
        # distinct attempts of one logical task, which Paimon must reject.
        native_request = dict(request)
        native_request["task_id"] = retry.to_dict()
        native_request["context"] = {**dict(request.get("context") or {}), "attempt_id": str(retry.attempt_id)}
        result = await original(native_request)
        state["attempts"] = [str(first), str(retry)]
        state["worker_pid"] = os.getpid()
        return result

    state["wrapper"] = execute_with_duplicate_identity
    manager.execute_fn = execute_with_duplicate_identity


def _restore_worker_duplicate(actor: object) -> dict[str, object]:
    state = vars(actor).pop("_paimon_duplicate_fault")
    manager = actor._get_fte_task_manager()
    if manager.execute_fn is state["wrapper"]:
        manager.execute_fn = state["original"]
    return {"attempts": state["attempts"], "worker_pid": state["worker_pid"]}


def _install_driver_fault(actor: object, query_id: str, mode: str) -> None:
    import ray
    from vane.runners.ray.fragment_worker_client import RayWorkerActorHandle

    if hasattr(actor, "_paimon_ray_fault"):
        raise RuntimeError("Paimon Ray fault already installed")
    state = {"submissions": 0, "workers": [], "prepared": 0, "native_result": None, "lock": threading.Lock()}
    state["first_attempt"] = None
    state["prepared_event"] = threading.Event()
    state["release_event"] = threading.Event()
    actor._paimon_ray_fault = state
    original_prepare = actor._prepare_copy_plan_sync
    original_run = actor._run_copy_plan_sync_with_lifecycle
    original_submit = RayWorkerActorHandle.submit_tasks
    original_create = RayWorkerActorHandle.fte_create_task
    state["instance_methods"] = {
        name: (name in vars(actor), vars(actor).get(name))
        for name in ("_prepare_copy_plan_sync", "_run_copy_plan_sync_with_lifecycle")
    }
    state["original_submit"] = original_submit
    state["original_create"] = original_create

    def prepare(*args: object) -> object:
        plan, connection = original_prepare(*args)
        if str(plan.idx()) == query_id:
            state["prepared"] += 1
            if mode == "ctas-race":
                # Freeze the driver plan while the target is absent. The
                # client owns the catalog and publishes the competing table
                # through public SQL before releasing native preparation.
                state["prepared_event"].set()
                if not state["release_event"].wait(60):
                    raise TimeoutError("waiting for competing Paimon CTAS table")
        return plan, connection

    def run(plan_runner: object, plan: object, connection: object, lifecycle: object) -> object:
        result = original_run(plan_runner, plan, connection, lifecycle)
        if str(plan.idx()) == query_id:
            state["native_result"] = dict(result)
        return result

    def submit(handle: object, tasks: list[object]) -> object:
        matching = sum(str(task.context().get("resource_query_id")) == query_id for task in tasks)
        with state["lock"]:
            state["submissions"] += matching
        return original_submit(handle, tasks)

    def create(handle: object, request: dict[str, object]) -> object:
        from vane.runners.fte import FteTaskAttemptId

        install = False
        # Extension writes execute a child query under the logical plan's
        # resource owner. Match ownership, not the child's execution query ID.
        owner = str(dict(request.get("context") or {}).get("resource_query_id"))
        if mode == "duplicate-attempt" and owner == query_id:
            attempt = FteTaskAttemptId.coerce(request["task_id"])
            with state["lock"]:
                first = state["first_attempt"]
                if first is None:
                    state["first_attempt"] = attempt
                elif not state["workers"] and attempt.task_id != first.task_id:
                    if attempt.task_id.fragment_execution_id != first.task_id.fragment_execution_id:
                        raise AssertionError("duplicate identity must belong to the same write fragment")
                    state["workers"].append(handle.actor_handle)
                    install = True
            if install:
                ray.get(
                    handle.actor_handle.__ray_call__.remote(_install_worker_duplicate, str(attempt), str(first)),
                    timeout=60,
                )
        return original_create(handle, request)

    actor._prepare_copy_plan_sync = prepare
    actor._run_copy_plan_sync_with_lifecycle = run
    RayWorkerActorHandle.submit_tasks = submit
    RayWorkerActorHandle.fte_create_task = create


def _restore_driver_fault(actor: object) -> dict[str, object]:
    import ray
    from vane.runners.ray.fragment_worker_client import RayWorkerActorHandle

    state = vars(actor).pop("_paimon_ray_fault")
    state["release_event"].set()
    RayWorkerActorHandle.submit_tasks = state["original_submit"]
    RayWorkerActorHandle.fte_create_task = state["original_create"]
    for name, (existed, previous) in state["instance_methods"].items():
        if existed:
            setattr(actor, name, previous)
        else:
            delattr(actor, name)
    workers = [
        ray.get(worker.__ray_call__.remote(_restore_worker_duplicate), timeout=60) for worker in state["workers"]
    ]
    return {**{key: state[key] for key in ("prepared", "submissions", "native_result")}, "workers": workers}


def _preparation_gate(actor: object, release: bool) -> bool:
    state = actor._paimon_ray_fault
    if release:
        state["release_event"].set()
    return state["prepared_event"].is_set()


def wait_for_write_preparation(runner: object) -> None:
    import ray

    driver = runner.query_driver_client.runner
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if ray.get(driver.__ray_call__.remote(_preparation_gate, False), timeout=10):
            return
        time.sleep(0.05)
    raise TimeoutError("waiting for Paimon Ray physical planning")


def release_write_preparation(runner: object) -> None:
    import ray

    ray.get(runner.query_driver_client.runner.__ray_call__.remote(_preparation_gate, True), timeout=10)


@contextmanager
def ray_write_fault(runner: object, query_id: str, mode: str):
    import ray
    import ray.cloudpickle

    if mode not in {"ctas-race", "duplicate-attempt"}:
        raise ValueError(mode)
    driver = runner.query_driver_client.runner
    module = sys.modules[__name__]
    ray.cloudpickle.register_pickle_by_value(module)
    observations: dict[str, object] = {}
    try:
        ray.get(driver.__ray_call__.remote(_install_driver_fault, query_id, mode), timeout=60)
        try:
            try:
                yield observations
            finally:
                observations.update(ray.get(driver.__ray_call__.remote(_restore_driver_fault), timeout=120))
        except Exception as error:
            error.add_note(f"Paimon Ray fault observations: {observations!r}")
            raise
    finally:
        ray.cloudpickle.unregister_pickle_by_value(module)
