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
import io
import json
import os
import shlex
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

import yaml

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
        versions = validator.validate_release(directory, VANE_VERSION, config, channel="testpypi-dev")
        if versions != {"paimon": PAIMON_VERSION}:
            raise AssertionError(f"unexpected provider versions: {versions}")
        outputs = directory / "github-output"
        command = [
            "validate",
            "--channel",
            "testpypi-dev",
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
    if manifest["vane"]["revision"] != "472df75ab51fd3eac2642f6646545075549e5921":
        raise AssertionError("the existing development runtime pin must remain unchanged")
    with (REPOSITORY_ROOT / "vane-extension-release.toml").open("rb") as source:
        production_manifest = tomllib.load(source)
    if production_manifest["vane"]["revision"] != "033b549afcb498633fd6669b26c054c00363004e":
        raise AssertionError("production preparation must use the reviewed public-key pin")
    production_manifest["vane"]["revision"] = manifest["vane"]["revision"]
    if production_manifest != manifest:
        raise AssertionError("production and development must retain the same native dependency configuration")
    tools = REPOSITORY_ROOT / "vane-extension-ci-tools"
    actual = subprocess.check_output(["git", "-C", str(tools), "rev-parse", "HEAD"], text=True).strip()
    workflow = (REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text()
    if workflow.count(actual) != 4:
        raise AssertionError("all four workflow CI-tools pins must match the shared checkout")
    if "scripts/validate_vane_provider_release.py" in workflow:
        raise AssertionError("workflow still invokes a private release validator")


def exercise_promotion_cli(validator: ModuleType) -> None:
    with tempfile.TemporaryDirectory(prefix="vane-paimon-promotion-") as value:
        directory = Path(value)
        wheels = [write_wheel(directory, interpreter, requirement="vane-ai===0.2.0") for interpreter in INTERPRETERS]
        document = {
            "urls": [
                {
                    "filename": wheel.name,
                    "packagetype": "bdist_wheel",
                    "digests": {"sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()},
                }
                for wheel in wheels
            ]
        }
        source_arguments = [
            "--manifest",
            str(REPOSITORY_ROOT / "vane-extension-release.toml"),
            "--extension-root",
            str(REPOSITORY_ROOT),
            "--vane-source",
            str(directory / "vane"),
            "--ci-tools-version",
            "a" * 40,
            "--config",
            str(REPOSITORY_ROOT / "vane-provider-release.toml"),
            "--directory",
            str(directory),
        ]
        command = ["verify-promotion", *source_arguments, "--vane-version", "0.2.0", "--attempts", "1"]
        with (
            mock.patch.object(validator, "verify_sources"),
            mock.patch.object(validator, "_request_json", side_effect=[(200, document), (404, {})]) as request,
            redirect_stdout(io.StringIO()),
        ):
            if validator.main(command) != 0:
                raise AssertionError("the complete byte-identical Paimon stage must be promotable")
            urls = [call.args[0] for call in request.call_args_list]
            if urls != [
                f"https://test.pypi.org/pypi/vane-extension-paimon/{PAIMON_VERSION}/json",
                f"https://pypi.org/pypi/vane-extension-paimon/{PAIMON_VERSION}/json",
            ]:
                raise AssertionError("promotion must verify fixed TestPyPI and PyPI indexes in order")
        with (
            mock.patch.object(validator, "verify_sources"),
            mock.patch.object(validator, "_request_json", return_value=(404, {})),
            redirect_stderr(io.StringIO()),
        ):
            if validator.main(command) != 2:
                raise AssertionError("a missing staged release must not be promotable")
        for index in ("testpypi", "pypi"):
            with (
                mock.patch.object(validator, "verify_sources"),
                mock.patch.object(validator, "_request_json", return_value=(200, document)),
                redirect_stdout(io.StringIO()),
            ):
                if (
                    validator.main(
                        [
                            "verify-index",
                            *source_arguments,
                            "--provider",
                            "paimon",
                            "--version",
                            PAIMON_VERSION,
                            "--index",
                            index,
                            "--attempts",
                            "1",
                        ]
                    )
                    != 0
                ):
                    raise AssertionError("the configured indexed Paimon matrix must match exactly")


def exercise_workflow_contract() -> None:
    workflow = yaml.load((REPOSITORY_ROOT / ".github/workflows/VaneExtension.yml").read_text(), Loader=yaml.BaseLoader)
    operation = workflow["on"]["workflow_dispatch"]["inputs"]["operation"]
    if operation["default"] != "build-only" or operation["options"] != ["build-only", "testpypi-dev", "release"]:
        raise AssertionError("publishing must remain explicit and build-only must stay the default")
    jobs = workflow["jobs"]
    preflight = jobs["vane-release-preflight"]
    if "environment" in preflight or preflight["permissions"] != {"contents": "read"}:
        raise AssertionError("preflight must not have publishing authority or a signing environment")
    build = jobs["vane-testpypi-wheels"]
    if build["needs"] != "vane-release-preflight":
        raise AssertionError("the signing job must wait for the secret-free source/version gate")
    download_index = next(i for i, step in enumerate(build["steps"]) if step.get("id") == "vane_candidate")
    native_index = next(
        i for i, step in enumerate(build["steps"]) if step["name"] == "Install native and wheel tooling"
    )
    if download_index >= native_index:
        raise AssertionError("missing indexed Vane runtimes must fail before native preparation")
    if "'production-signing'" not in build["environment"]["name"]:
        raise AssertionError("production signing needs its separate protected environment")
    signing = next(
        step for step in build["steps"] if step["name"] == "Build, sign and verify the Paimon provider wheel matrix"
    )
    if signing["env"]["VANE_SELECTED_SIGNING_PRIVATE_KEY"] != (
        "${{ secrets[inputs.operation == 'release' && 'VANE_EXTENSION_SIGNING_PRIVATE_KEY' || "
        "'VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY'] }}"
    ):
        raise AssertionError("a missing production secret must never select the development secret")
    for job in jobs.values():
        for step in job.get("steps", []):
            if step.get("uses", "").startswith("actions/download-artifact@"):
                if step["uses"] != "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c":
                    raise AssertionError("candidate downloads must use the reviewed v8 artifact implementation")
                if step["with"].get("digest-mismatch") != "error":
                    raise AssertionError("an artifact digest mismatch must fail closed")
    smoke_jobs = {"testpypi-local-paimon-integration", "testpypi-ray-paimon-integration"}
    for job_name in smoke_jobs:
        smoke = jobs[job_name]
        scripts = "\n".join(step.get("run", "") for step in smoke["steps"])
        if 'cmp "${expected_paimon[0]}" "${paimon_wheels[0]}"' not in scripts:
            raise AssertionError("both indexed smoke jobs must execute the exact candidate bytes")
        if "'https://pypi.org/simple/'" not in smoke["env"]["RUNTIME_INDEX_URL"]:
            raise AssertionError("production smoke runtimes must come from PyPI")
    promotion = jobs["publish-pypi-paimon"]
    if set(promotion["needs"]) != smoke_jobs | {"assemble-testpypi-paimon"}:
        raise AssertionError("PyPI promotion requires both successful smoke jobs and the same candidate set")
    if promotion["if"] != "inputs.operation == 'release'" or promotion["environment"]["name"] != "pypi":
        raise AssertionError("only release dispatches may enter the production approval environment")
    steps = promotion["steps"]
    verify = next(i for i, step in enumerate(steps) if "verify-promotion" in step.get("run", ""))
    upload = next(i for i, step in enumerate(steps) if step.get("uses", "").startswith("pypa/gh-action-pypi-publish@"))
    if verify + 1 != upload or steps[upload]["with"]["packages-dir"] != "dist":
        raise AssertionError("fresh promotion verification must immediately precede uploading the same directory")
    if "--index pypi" not in steps[upload + 1]["run"]:
        raise AssertionError("the complete production index must be verified after publishing")


def exercise_preflight(preflight: ModuleType, validator: ModuleType) -> None:
    environment = {
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REPOSITORY": "AstroVela/duckdb-paimon",
        "GITHUB_REF": "refs/heads/main_vane",
        "GITHUB_REF_PROTECTED": "true",
    }
    for operation in ("testpypi-dev", "release"):
        preflight.require_context(operation, environment)
        for field in environment:
            require_error(ValueError, lambda: preflight.require_context(operation, {**environment, field: "wrong"}))
    require_error(ValueError, lambda: preflight.require_context("build-only", environment))
    with tempfile.TemporaryDirectory(prefix="vane-paimon-preflight-") as value:
        directory = Path(value)
        output = directory / "github-output"
        command = [
            "--operation",
            "release",
            "--vane-source",
            str(directory / "vane"),
            "--ci-tools-version",
            "a" * 40,
            "--github-output",
            str(output),
        ]
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(preflight, "release_tools", return_value=validator),
            mock.patch.object(validator, "verify_sources") as verify,
            mock.patch.object(preflight, "require_production_ancestry") as ancestry,
            mock.patch.object(preflight, "source_version", return_value="0.2.0.dev641"),
        ):
            require_error(validator.ReleaseValidationError, lambda: preflight.main(command))
            verify.assert_called_once_with(
                REPOSITORY_ROOT / "vane-extension-release.toml", REPOSITORY_ROOT, directory / "vane", "a" * 40
            )
            ancestry.assert_called_once_with(directory / "vane")
        if output.exists():
            raise AssertionError("a preparation-only source pin must not emit a publishable version")
        with (
            mock.patch.dict(os.environ, environment, clear=True),
            mock.patch.object(preflight, "release_tools", return_value=validator),
            mock.patch.object(validator, "verify_sources"),
            mock.patch.object(preflight, "require_production_ancestry"),
            mock.patch.object(preflight, "source_version", return_value="0.2.0"),
            redirect_stdout(io.StringIO()),
        ):
            preflight.main(command)
        if output.read_text() != "vane_version=0.2.0\n":
            raise AssertionError("a valid formal source version must remain exact")
    with (
        mock.patch.dict(
            os.environ, {"SETUPTOOLS_SCM_PRETEND_VERSION_FOR_VANE_AI": "0.2.0", "VANE_VERSION_BRANCH": "release"}
        ),
        mock.patch.object(preflight.subprocess, "check_output", return_value="0.2.0.dev641\n") as capture,
    ):
        if preflight.source_version(Path("vane")) != "0.2.0.dev641":
            raise AssertionError("source version must be derived without overrides")
        passed_environment = capture.call_args.kwargs["env"]
        if (
            any(name.startswith("SETUPTOOLS_SCM_PRETEND_VERSION") for name in passed_environment)
            or "VANE_VERSION_BRANCH" in passed_environment
        ):
            raise AssertionError("source version derivation must strip version overrides")
    with mock.patch.object(preflight.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as ancestry:
        preflight.require_production_ancestry(Path("vane"))
        if ancestry.call_args.args[0] != [
            "git",
            "merge-base",
            "--is-ancestor",
            "033b549afcb498633fd6669b26c054c00363004e",
            "HEAD",
        ]:
            raise AssertionError("production source ancestry must include the exact reviewed key commit")
    for return_code in (1, 128):
        with mock.patch.object(preflight.subprocess, "run", return_value=subprocess.CompletedProcess([], return_code)):
            require_error(ValueError, lambda: preflight.require_production_ancestry(Path("vane")))


def exercise_production_signing(builder: ModuleType) -> None:
    if builder.SIGNING_PROFILES["production"] != ("astrovela/vane", None):
        raise AssertionError("production must use the default native trust store")
    builder._require_signing_mode("production", consume=True, local_runtime=False)
    for consume, local in ((False, False), (True, True), (False, True)):
        require_error(
            builder.QualificationError,
            lambda: builder._require_signing_mode("production", consume=consume, local_runtime=local),
        )
    # Generate an isolated throwaway key, not the developer's production key.
    private = bytearray(
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        ).stdout
    )
    try:
        public = subprocess.run(
            ["openssl", "pkey", "-pubout", "-outform", "DER"],
            input=private,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        ).stdout
        require_error(builder.QualificationError, lambda: builder._require_production_key(private))
        with mock.patch.object(builder, "PRODUCTION_PUBLIC_KEY_SHA256", hashlib.sha256(public).hexdigest()):
            builder._require_production_key(private)
        require_error(
            builder.QualificationError, lambda: builder._require_production_key(bytearray(b"invalid private key"))
        )
    finally:
        private[:] = b"\0" * len(private)
        private.clear()
    with tempfile.TemporaryDirectory(prefix="vane-paimon-production-inputs-") as value:
        directory = Path(value)
        for version in ("0.2.0", "0.2.0rc1", "0.2.0.dev612", "0.2.0+local", "1!0.2.0", "0.2"):
            wheel = directory / f"vane_ai-{version}-cp312-cp312-{PLATFORM}.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr(f"vane_ai-{version}.dist-info/METADATA", f"Name: vane-ai\nVersion: {version}\n")
            if version in {"0.2.0", "0.2.0rc1"}:
                if builder._require_production_runtime(wheel) != version:
                    raise AssertionError("production runtime version was not preserved")
            else:
                require_error(builder.QualificationError, lambda: builder._require_production_runtime(wheel))
        mismatch = directory / f"vane_ai-0.2.1-cp312-cp312-{PLATFORM}.whl"
        with zipfile.ZipFile(mismatch, "w") as archive:
            archive.writestr("vane_ai-0.2.1.dist-info/METADATA", "Name: vane-ai\nVersion: 0.2.0.dev612\n")
        require_error(builder.QualificationError, lambda: builder._require_production_runtime(mismatch))
        for relative in ("share/arrow/ArrowConfig.cmake", "share/arrowflight/ArrowFlightConfig.cmake"):
            fixture = directory / "dependencies/x64-linux" / relative
            fixture.parent.mkdir(parents=True, exist_ok=True)
            fixture.touch()
        with mock.patch.dict(os.environ, {"CMAKE_ARGS": "-DVANE_ENABLE_TEST_EXTENSION_SIGNING_KEY=ON"}):
            environment = builder._build_environment(
                extension_root=REPOSITORY_ROOT,
                build_directory=directory,
                vane_vcpkg_installed=directory / "dependencies",
                vcpkg_toolchain=directory / "toolchain.cmake",
                jobs=1,
                signing_cmake_option=None,
            )
        options = shlex.split(environment["CMAKE_ARGS"])
        for option in ("VANE_ENABLE_TEST_EXTENSION_SIGNING_KEY", "VANE_ENABLE_TESTPYPI_EXTENSION_SIGNING_KEY"):
            if f"-D{option}=OFF" not in options or f"-D{option}=ON" in options:
                raise AssertionError("production builds must explicitly disable both testing trust roots")


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
    preflight = load_script("vane_paimon_release_preflight", "scripts/vane_release_preflight.py")
    try:
        exercise_release_validator(validator)
        exercise_integration_pins()
        exercise_promotion_cli(validator)
        exercise_workflow_contract()
        exercise_preflight(preflight, validator)
        exercise_production_signing(builder)
        exercise_private_key_consumption(builder)
    finally:
        sys.modules.pop(validator.__name__, None)
        sys.modules.pop(builder.__name__, None)
        sys.modules.pop(preflight.__name__, None)


if __name__ == "__main__":
    main()
