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

import hashlib
import importlib.util
import sys
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
VANE_VERSION = "0.2.0.dev603"
PAIMON_VERSION = "0.2.0.0.603.1"
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


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def exercise_release_validator(validator: ModuleType) -> None:
    with tempfile.TemporaryDirectory(prefix="vane-paimon-release-validator-") as value:
        directory = Path(value)
        wheels = write_release(directory)
        actual_version = validator.validate_release(directory, VANE_VERSION)
        if actual_version != PAIMON_VERSION:
            raise AssertionError(f"expected Paimon version {PAIMON_VERSION}, got {actual_version}")

        expected_hashes = {wheel.name: sha256(wheel) for wheel in wheels}
        original_request = validator._request_json
        try:
            validator._request_json = lambda _url: (404, None)
            validator.require_index_publishable(directory, PAIMON_VERSION)

            indexed_subset = {
                "urls": [
                    {
                        "digests": {"sha256": sha256(wheels[0])},
                        "filename": wheels[0].name,
                        "packagetype": "bdist_wheel",
                    }
                ]
            }
            validator._request_json = lambda _url: (200, indexed_subset)
            validator.require_index_publishable(directory, PAIMON_VERSION)

            indexed_complete = {
                "urls": [
                    {
                        "digests": {"sha256": digest},
                        "filename": filename,
                        "packagetype": "bdist_wheel",
                    }
                    for filename, digest in expected_hashes.items()
                ]
            }
            validator._request_json = lambda _url: (200, indexed_complete)
            validator.require_index_match(
                directory,
                PAIMON_VERSION,
                attempts=1,
                delay_seconds=0,
            )

            conflict = {
                "urls": [
                    {
                        "digests": {"sha256": "0" * 64},
                        "filename": wheels[0].name,
                        "packagetype": "bdist_wheel",
                    }
                ]
            }
            validator._request_json = lambda _url: (200, conflict)
            require_error(
                validator.ReleaseValidationError,
                lambda: validator.require_index_publishable(directory, PAIMON_VERSION),
            )
        finally:
            validator._request_json = original_request

        wheels[-1].unlink()
        require_error(
            validator.ReleaseValidationError,
            lambda: validator.validate_release(directory, VANE_VERSION),
        )

    with tempfile.TemporaryDirectory(prefix="vane-paimon-release-dependency-") as value:
        directory = Path(value)
        write_release(directory)
        invalid = directory / (f"vane_extension_paimon-{PAIMON_VERSION}-cp314-none-{PLATFORM}.whl")
        invalid.unlink()
        write_wheel(directory, "cp314", requirement="vane-ai>=0.2")
        require_error(
            validator.ReleaseValidationError,
            lambda: validator.validate_release(directory, VANE_VERSION),
        )

    with tempfile.TemporaryDirectory(prefix="vane-paimon-release-index-tags-") as value:
        directory = Path(value)
        wheels = write_release(directory)
        wheels[-1].unlink()
        write_wheel(directory, "cp39")
        original_request = validator._request_json
        try:
            validator._request_json = lambda _url: (404, None)
            require_error(
                validator.ReleaseValidationError,
                lambda: validator.require_index_publishable(directory, PAIMON_VERSION),
            )
        finally:
            validator._request_json = original_request

    with tempfile.TemporaryDirectory(prefix="vane-paimon-release-index-metadata-") as value:
        directory = Path(value)
        wheels = write_release(directory)
        wheels[-1].unlink()
        write_wheel(directory, "cp314", metadata_name="vane-extension-not-paimon")
        indexed_complete = {
            "urls": [
                {
                    "digests": {"sha256": sha256(wheel)},
                    "filename": wheel.name,
                    "packagetype": "bdist_wheel",
                }
                for wheel in sorted(directory.glob("*.whl"))
            ]
        }
        original_request = validator._request_json
        try:
            validator._request_json = lambda _url: (200, indexed_complete)
            require_error(
                validator.ReleaseValidationError,
                lambda: validator.require_index_match(
                    directory,
                    PAIMON_VERSION,
                    attempts=1,
                    delay_seconds=0,
                ),
            )
        finally:
            validator._request_json = original_request


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
        "scripts/validate_vane_provider_release.py",
    )
    builder = load_script(
        "vane_paimon_dynamic_wheel_builder",
        "scripts/build_vane_dynamic_wheel.py",
    )
    try:
        exercise_release_validator(validator)
        exercise_private_key_consumption(builder)
    finally:
        sys.modules.pop(validator.__name__, None)
        sys.modules.pop(builder.__name__, None)


if __name__ == "__main__":
    main()
