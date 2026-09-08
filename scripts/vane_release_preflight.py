#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Gate Paimon publishing before entering an environment containing a signing key."""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

from packaging.utils import parse_wheel_filename

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_KEY_REVISION = "033b549afcb498633fd6669b26c054c00363004e"


def release_tools() -> ModuleType:
    path = REPOSITORY_ROOT / "vane-extension-ci-tools/scripts/vane_provider_release.py"
    spec = importlib.util.spec_from_file_location("_paimon_release_tools", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load pinned shared release tooling")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def require_context(operation: str, environment: dict[str, str]) -> None:
    if operation not in {"testpypi-dev", "release"}:
        raise ValueError("publishing requires testpypi-dev or release")
    expected = {
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REPOSITORY": "AstroVela/duckdb-paimon",
        "GITHUB_REF": "refs/heads/main_vane",
        "GITHUB_REF_PROTECTED": "true",
    }
    if any(environment.get(name) != value for name, value in expected.items()):
        raise ValueError(
            "publishing requires a dispatch on the protected AstroVela/duckdb-paimon main_vane branch"
        )


def source_version(source: Path) -> str:
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("SETUPTOOLS_SCM_PRETEND_VERSION")
        and name not in {"VANE_VERSION_BRANCH", "GITHUB_REF_NAME", "GITHUB_BASE_REF"}
    }
    return subprocess.check_output(
        [sys.executable, "-I", "-m", "setuptools_scm"],
        cwd=source,
        env=environment,
        text=True,
    ).strip()


def require_production_ancestry(source: Path) -> None:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", PRODUCTION_KEY_REVISION, "HEAD"],
        cwd=source,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise ValueError(
            "production Vane source must include the reviewed production public key"
        )


def require_indexed_runtime(release, version: str, channel: str, config: Path) -> None:
    index = "pypi" if channel == "release" else "testpypi"
    status, document = release._request_json(
        f"{release.INDEX_JSON_BASES[index]}/vane-ai/{version}/json"
    )
    if status != 200 or not isinstance(document, dict):
        raise ValueError(f"exact Vane runtime {version} is not published on {index}")
    files = document.get("urls")
    if not isinstance(files, list):
        raise ValueError("runtime index returned an invalid file list")
    expected = release.load_config(config)
    found = set()
    for record in files:
        if not isinstance(record, dict):
            raise ValueError("runtime index returned an invalid file record")
        if record.get("packagetype") != "bdist_wheel":
            continue
        distribution, wheel_version, _, tags = parse_wheel_filename(record["filename"])
        if distribution != "vane-ai" or str(wheel_version) != version:
            raise ValueError("runtime index returned an unexpected wheel identity")
        if record.get("yanked", False) is not False:
            continue
        digest = record.get("digests", {}).get("sha256", "")
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ValueError("runtime index returned an invalid wheel digest")
        found.update(
            (tag.interpreter, tag.platform)
            for tag in tags
            if tag.abi == tag.interpreter
        )
    required = {
        (interpreter, platform)
        for interpreter in expected.interpreters
        for platform in expected.platforms
    }
    if not required <= found:
        raise ValueError(
            f"exact Vane runtime is missing indexed wheels: {required - found}"
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operation", required=True, choices=("testpypi-dev", "release")
    )
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--ci-tools-version", required=True)
    parser.add_argument("--github-output", required=True, type=Path)
    arguments = parser.parse_args(argv)
    require_context(arguments.operation, dict(os.environ))
    manifest = (
        "vane-extension-release.toml"
        if arguments.operation == "release"
        else "vane-extension.toml"
    )
    tools = release_tools()
    tools.verify_sources(
        REPOSITORY_ROOT / manifest,
        REPOSITORY_ROOT,
        arguments.vane_source,
        arguments.ci_tools_version,
    )
    if arguments.operation == "release":
        require_production_ancestry(arguments.vane_source)
    version = source_version(arguments.vane_source)
    tools.validate_vane_version(version, arguments.operation)
    require_indexed_runtime(
        tools,
        version,
        arguments.operation,
        REPOSITORY_ROOT / "vane-provider-release.toml",
    )
    with arguments.github_output.open("a", encoding="utf-8") as output:
        output.write(f"vane_version={version}\n")
    print(f"Validated {arguments.operation} Vane source version: {version}")


if __name__ == "__main__":
    main()
