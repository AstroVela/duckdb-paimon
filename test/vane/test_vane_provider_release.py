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

"""Exercise the immutable Paimon release validator and key consumption."""

from __future__ import annotations

import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType
from unittest import mock

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VANE_VERSION = "0.2.0.dev612"
PAIMON_VERSION = "0.2.0.0.612.1"
INTERPRETERS = ("cp310", "cp311", "cp312", "cp313", "cp314")
PLATFORM = "manylinux_2_28_x86_64"


def load_script(name: str, relative_path: str) -> ModuleType:
    path = REPOSITORY_ROOT / relative_path
    specification = importlib.util.spec_from_file_location(name, path)
    if specification is None or specification.loader is None:
        raise AssertionError(f"could not load {path}")
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


def require_error(error_type: type[BaseException], operation: Callable[[], object]) -> BaseException:
    try:
        operation()
    except error_type as error:
        return error
    raise AssertionError(f"operation did not raise {error_type.__name__}")


def write_wheel(
    directory: Path,
    interpreter: str,
    *,
    requirement: str = f"vane-ai==={VANE_VERSION}",
    metadata_name: str = "vane-extension-paimon",
) -> Path:
    distribution = "vane_extension_paimon"
    filename = f"{distribution}-{PAIMON_VERSION}-{interpreter}-none-{PLATFORM}.whl"
    metadata_directory = f"{distribution}-{PAIMON_VERSION}.dist-info"
    metadata = (
        "Metadata-Version: 2.4\n"
        f"Name: {metadata_name}\n"
        f"Version: {PAIMON_VERSION}\n"
        f"Requires-Dist: {requirement}\n"
        "\n"
    )
    path = directory / filename
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr(f"{metadata_directory}/METADATA", metadata)
    return path


def write_release(directory: Path) -> tuple[Path, ...]:
    return tuple(write_wheel(directory, interpreter) for interpreter in INTERPRETERS)


def exercise_release_validator(validator: ModuleType) -> None:
    config_path = REPOSITORY_ROOT / "vane-provider-release.toml"
    config = validator.load_config(config_path)
    if config.interpreters != INTERPRETERS or config.platforms != (PLATFORM,):
        raise AssertionError("release config differs from the built wheel matrix")
    if len(config.providers) != 1 or config.providers[0].name != "paimon":
        raise AssertionError("Paimon must publish exactly its own provider")
    if config.providers[0].distribution != "vane-extension-paimon" or config.providers[0].dependencies:
        raise AssertionError("Paimon must depend only on vane-ai")

    # Generic matrix/index cases live in vane-extension-ci-tools. Keep this a
    # consumer smoke test of the real config, CLI and GitHub job output names.
    with tempfile.TemporaryDirectory(prefix="vane-paimon-release-validator-") as value:
        directory = Path(value)
        write_release(directory)
        versions = validator.validate_release(directory, VANE_VERSION, config)
        if versions != {"paimon": PAIMON_VERSION}:
            raise AssertionError(f"unexpected provider versions: {versions}")
        outputs = directory / "github-output"
        command = [
            "validate",
            "--manifest",
            str(REPOSITORY_ROOT / "vane-extension.toml"),
            "--extension-root",
            str(REPOSITORY_ROOT),
            "--vane-source",
            str(directory / "vane"),
            "--ci-tools-version",
            "a" * 40,
            "--config",
            str(config_path),
            "--directory",
            str(directory),
            "--vane-version",
            VANE_VERSION,
            "--github-output",
            str(outputs),
        ]
        output = io.StringIO()
        with mock.patch.object(validator, "verify_sources") as verify, redirect_stdout(output):
            if validator.main(command) != 0:
                raise AssertionError("shared CLI rejected the configured Paimon matrix")
        verify.assert_called_once_with(
            REPOSITORY_ROOT / "vane-extension.toml", REPOSITORY_ROOT, directory / "vane", "a" * 40
        )
        expected = {"vane_version": VANE_VERSION, "paimon_version": PAIMON_VERSION}
        if json.loads(output.getvalue()) != expected:
            raise AssertionError("shared CLI returned different workflow output names")
        if dict(line.split("=", 1) for line in outputs.read_text().splitlines()) != expected:
            raise AssertionError("shared CLI did not write the expected GitHub outputs")
        write_wheel(directory, "cp314", requirement="vane-ai>=0.2")
        with mock.patch.object(validator, "verify_sources"), redirect_stderr(io.StringIO()):
            if validator.main(command) != 2:
                raise AssertionError("shared CLI accepted an inexact Vane dependency")


def exercise_integration_pins() -> None:
    with (REPOSITORY_ROOT / "vane-extension.toml").open("rb") as source:
        manifest = tomllib.load(source)
    vcpkg = json.loads((REPOSITORY_ROOT / "vcpkg.json").read_text())
    if manifest["schema_version"] != 2 or manifest["vcpkg"]["repository"] != "microsoft/vcpkg":
        raise AssertionError("integration must use the current explicit-vcpkg contract")
    if manifest["vcpkg"]["revision"] != vcpkg["builtin-baseline"]:
        raise AssertionError("native and dynamic Paimon lanes must use the same reviewed vcpkg revision")
    tools = REPOSITORY_ROOT / "vane-extension-ci-tools"
    actual = subprocess.check_output(["git", "-C", str(tools), "rev-parse", "HEAD"], text=True).strip()
    workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
    if workflow.count(actual) != 4:
        raise AssertionError("all four workflow CI-tools pins must match the shared checkout")
    if "scripts/validate_vane_provider_release.py" in workflow:
        raise AssertionError("workflow still invokes a private release validator")


def exercise_private_key_consumption(builder: ModuleType) -> None:
    with tempfile.TemporaryDirectory(prefix="vane-paimon-signing-key-") as value:
        directory = Path(value)
        private_key = directory / "candidate.pem"
        private_key.write_bytes(b"candidate-private-key")
        private_key.chmod(0o600)
        contents = builder._read_signing_private_key(private_key, consume=True)
        if bytes(contents) != b"candidate-private-key" or private_key.exists():
            raise AssertionError("private key was not read and consumed exactly once")
        contents[:] = b"\0" * len(contents)
        contents.clear()

        public_mode = directory / "public-mode.pem"
        public_mode.write_bytes(b"candidate-private-key")
        public_mode.chmod(0o644)
        require_error(
            builder.QualificationError,
            lambda: builder._read_signing_private_key(public_mode, consume=True),
        )
        if not public_mode.exists():
            raise AssertionError("an invalid private-key input was unexpectedly consumed")

        target = directory / "target.pem"
        target.write_bytes(b"candidate-private-key")
        target.chmod(0o600)
        symbolic = directory / "symbolic.pem"
        symbolic.symlink_to(target)
        require_error(
            builder.QualificationError,
            lambda: builder._read_signing_private_key(symbolic, consume=True),
        )
        if not target.exists():
            raise AssertionError("a symbolic private-key target was unexpectedly consumed")


def main() -> None:
    validator = load_script(
        "vane_paimon_release_validator",
        "vane-extension-ci-tools/scripts/vane_provider_release.py",
    )
    builder = load_script(
        "vane_paimon_dynamic_wheel_builder",
        "scripts/build_vane_dynamic_wheel.py",
    )
    try:
        exercise_release_validator(validator)
        exercise_integration_pins()
        exercise_private_key_consumption(builder)
    finally:
        sys.modules.pop(validator.__name__, None)
        sys.modules.pop(builder.__name__, None)


if __name__ == "__main__":
    main()
