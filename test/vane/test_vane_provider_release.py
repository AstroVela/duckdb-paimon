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

import ast
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
from types import ModuleType, SimpleNamespace
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
    prepare = jobs["vane-testpypi-prepare"]
    package = jobs["vane-testpypi-wheels"]
    signing = jobs["vane-testpypi-sign"]
    if prepare["needs"] != "vane-release-preflight" or signing["needs"] != "vane-testpypi-prepare":
        raise AssertionError("signing must wait for the source/index gate and unsigned native preparation")
    if set(package["needs"]) != {"vane-release-preflight", "vane-testpypi-prepare", "vane-testpypi-sign"}:
        raise AssertionError("fresh packaging requires the original unsigned and isolated signed artifacts")
    for job, phase in ((prepare, "prepare"), (package, "package")):
        scripts = "\n".join(step.get("run", "") for step in job["steps"])
        if "environment" in job or "secrets" in str(job) or job["permissions"] != {"contents": "read"}:
            raise AssertionError("native build and wheel dependencies must not have signing or publishing authority")
        if f"--phase {phase}" not in scripts or "--signing-private-key" in scripts:
            raise AssertionError("publishing must use explicit key-free builder phases")
    if "'production-signing'" not in signing["environment"]["name"]:
        raise AssertionError("production signing needs its separate protected environment")
    if signing["permissions"] != {"contents": "read"}:
        raise AssertionError("native signing must not have publishing OIDC")
    for step in signing["steps"]:
        if "uses" in step and not step["uses"].startswith(
            ("actions/checkout@", "actions/download-artifact@", "actions/upload-artifact@")
        ):
            raise AssertionError("signing may use only pinned checkout and artifact actions")
        if "run" in step and not step["run"].startswith(
            "/usr/bin/python3 -I -S extension/scripts/sign_vane_dynamic_bundle.py "
        ):
            raise AssertionError("the signer may execute only the isolated stdlib wrapper")
    source = next(step for step in signing["steps"] if step["name"] == "Checkout the committed Vane signing utility")
    if (
        source["with"]["repository"] != "AstroVela/vane"
        or source["with"]["ref"] != "${{ steps.manifest.outputs.vane_revision }}"
    ):
        raise AssertionError("the signer must choose official Vane source from its own committed manifest")
    signing_step = next(step for step in signing["steps"] if "VANE_PROVIDER_SIGNING_PRIVATE_KEY" in step.get("env", {}))
    if signing_step["env"]["VANE_PROVIDER_SIGNING_PRIVATE_KEY"] != (
        "${{ secrets[inputs.operation == 'release' && 'VANE_EXTENSION_SIGNING_PRIVATE_KEY' || "
        "'VANE_TESTPYPI_EXTENSION_SIGNING_PRIVATE_KEY'] }}"
    ):
        raise AssertionError("a missing production secret must never select the development secret")
    for job_name in (
        "vane-testpypi-sign",
        "vane-testpypi-wheels",
        "assemble-testpypi-paimon",
        "publish-testpypi-paimon",
        "verify-testpypi-paimon",
        "testpypi-local-paimon-integration",
        "testpypi-ray-paimon-integration",
        "verify-pypi-promotion",
        "publish-pypi-paimon",
        "verify-pypi-paimon",
    ):
        for step in jobs[job_name]["steps"]:
            if step.get("uses", "").startswith("actions/download-artifact@"):
                if "name" in step["with"] or not step["with"].get("artifact-ids", "").endswith(
                    ".outputs.artifact_id }}"
                ):
                    raise AssertionError("publishing consumers must use original immutable artifact IDs")
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
    promotion = jobs["verify-pypi-promotion"]
    if set(promotion["needs"]) != smoke_jobs | {"assemble-testpypi-paimon"}:
        raise AssertionError("PyPI promotion requires both successful smoke jobs and the same candidate set")
    if promotion["if"] != "inputs.operation == 'release'" or promotion["environment"]["name"] != "pypi":
        raise AssertionError("only release dispatches may enter the production approval environment")
    if promotion["permissions"] != {"contents": "read"}:
        raise AssertionError("release validation dependencies must never run with publishing OIDC")
    if "verify-promotion" not in promotion["steps"][-1]["run"]:
        raise AssertionError("promotion must be rechecked after approval")
    publisher = jobs["publish-pypi-paimon"]
    if (
        set(publisher["needs"]) != {"verify-pypi-promotion", "assemble-testpypi-paimon"}
        or publisher["environment"]["name"] != "pypi"
    ):
        raise AssertionError("the protected publisher must wait for immutable promotion verification")
    steps = publisher["steps"]
    if len(steps) != 2 or any("run" in step for step in steps):
        raise AssertionError(
            "the OIDC publisher must only download the original artifact and invoke the pinned upload action"
        )
    if (
        not steps[0]["uses"].startswith("actions/download-artifact@")
        or steps[0]["with"].get("artifact-ids") != "${{ needs.assemble-testpypi-paimon.outputs.artifact_id }}"
        or "name" in steps[0]["with"]
    ):
        raise AssertionError("the publisher must use the original tested distribution artifact")
    if (
        steps[1]["uses"] != "pypa/gh-action-pypi-publish@dc37677b2e1c63e2034f94d8a5b11f265b73ba33"
        or steps[1]["with"]["packages-dir"] != steps[0]["with"]["path"]
    ):
        raise AssertionError("the pinned publishing action must upload the unchanged artifact directory")
    indexed = jobs["verify-pypi-paimon"]
    if set(indexed["needs"]) != {"assemble-testpypi-paimon", "publish-pypi-paimon"} or indexed["permissions"] != {
        "contents": "read"
    }:
        raise AssertionError("post-upload index verification must be a separate OIDC-free job")
    if "--index pypi" not in indexed["steps"][-1]["run"]:
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
            mock.patch.object(preflight, "require_indexed_runtime") as indexed,
            redirect_stdout(io.StringIO()),
        ):
            preflight.main(command)
            indexed.assert_called_once_with(
                validator, "0.2.0", "release", REPOSITORY_ROOT / "vane-provider-release.toml"
            )
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


def exercise_production_signing(builder: ModuleType, signer: ModuleType) -> None:
    if builder.SIGNING_PROFILES["production"] != ("astrovela/vane", None):
        raise AssertionError("production must use the default native trust store")
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
        for profile in ("production", "testpypi"):
            require_error(ValueError, lambda: signer.require_key_fingerprint(private, profile))
            with mock.patch.dict(signer.KEY_FINGERPRINTS, {profile: hashlib.sha256(public).hexdigest()}):
                signer.require_key_fingerprint(private, profile)
            require_error(
                ValueError, lambda: signer.require_key_fingerprint(bytearray(b"invalid private key"), profile)
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


def exercise_runtime_index(preflight: ModuleType, validator: ModuleType) -> None:
    config = REPOSITORY_ROOT / "vane-provider-release.toml"
    files = [
        {
            "filename": f"vane_ai-0.2.0-{interpreter}-{interpreter}-{PLATFORM}.whl",
            "packagetype": "bdist_wheel",
            "digests": {"sha256": "a" * 64},
            "yanked": False,
        }
        for interpreter in INTERPRETERS
    ]
    for channel, host in (("release", "pypi.org"), ("testpypi-dev", "test.pypi.org")):
        with mock.patch.object(validator, "_request_json", return_value=(200, {"urls": files})) as request:
            preflight.require_indexed_runtime(validator, "0.2.0", channel, config)
            request.assert_called_once_with(f"https://{host}/pypi/vane-ai/0.2.0/json")
    for response in (
        (404, {}),
        (200, {"urls": files[:-1]}),
        (200, {"urls": [{**record, "yanked": True} for record in files]}),
    ):
        with mock.patch.object(validator, "_request_json", return_value=response):
            require_error(ValueError, lambda: preflight.require_indexed_runtime(validator, "0.2.0", "release", config))


def exercise_phase_boundaries(builder: ModuleType) -> None:
    with tempfile.TemporaryDirectory(prefix="vane-paimon-phase-") as value:
        root = Path(value)
        native = root / "build/duckdb/extension/paimon/paimon.duckdb_extension"
        native.parent.mkdir(parents=True)
        native.write_bytes(b"native-data" * 128 + b"\0" * 256)
        license_file = root / "LICENSE.txt"
        license_file.write_text("Apache-2.0")
        dependencies = root / "dependencies"
        dependencies.mkdir()
        unsigned = root / "unsigned"
        signed = root / "signed"
        signed.mkdir()
        arguments = SimpleNamespace(
            phase="prepare",
            signing_profile="testpypi",
            signing_private_key=None,
            consume_signing_private_key=False,
            package_local_runtime=False,
            runtime_python=[],
            runtime_wheel=[],
            unsigned_directory=None,
            signed_directory=None,
            extension_root=REPOSITORY_ROOT,
            vane_source=root,
            vane_revision="a" * 40,
            vane_vcpkg_installed=dependencies,
            vcpkg_toolchain=root / "toolchain.cmake",
            build_directory=root / "build",
            output_directory=unsigned,
            jobs=1,
        )
        builder._require_phase(arguments)
        for overrides in (
            {"phase": "full"},
            {"signing_private_key": root / "secret.pem"},
            {"consume_signing_private_key": True},
            {"package_local_runtime": True},
            {"runtime_python": [Path(sys.executable)]},
            {"signing_profile": "ci-test"},
        ):
            invalid = SimpleNamespace(**{**vars(arguments), **overrides})
            require_error(builder.QualificationError, lambda: builder._require_phase(invalid))
        with (
            mock.patch.object(builder, "_parse_arguments", return_value=arguments),
            mock.patch.object(builder, "_require_git_revision"),
            mock.patch.object(builder, "_platform_tag", return_value=PLATFORM),
            mock.patch.object(builder, "_require_vcpkg_toolchain", return_value=root / "toolchain.cmake"),
            mock.patch.object(builder, "_build_environment", return_value={}),
            mock.patch.object(builder, "_one_wheel", return_value=root / "base.whl"),
            mock.patch.object(builder, "_require_base_wheel_free_of_paimon"),
            mock.patch.object(builder, "_require_self_contained_artifact") as audit,
            mock.patch.object(builder, "_stage_license_files", return_value=(license_file,)),
            mock.patch.object(builder, "_run") as run,
            mock.patch.object(builder, "_read_signing_private_key") as key,
            mock.patch.object(builder, "_package_provider_matrix") as package,
        ):
            if builder.main() != 0:
                raise AssertionError("unsigned native preparation failed")
            audit.assert_called_once_with(native)
            key.assert_not_called()
            package.assert_not_called()
            if run.call_count != 2 or any("sign_test_dynamic_extension.py" in str(call) for call in run.call_args_list):
                raise AssertionError("prepare must only perform the native build and no signing")
        if (unsigned / "artifacts/paimon.duckdb_extension").read_bytes() != native.read_bytes():
            raise AssertionError("prepare must preserve the native bytes")
        if (unsigned / "licenses/paimon/LICENSE.txt").read_bytes() != license_file.read_bytes():
            raise AssertionError("prepare must carry licenses forward without executable metadata")
        signed_artifact = signed / native.name
        signed_artifact.write_bytes(native.read_bytes()[:-256] + b"s" * 256)
        builder._prepared_inputs(unsigned, signed)
        signed_artifact.write_bytes(b"x" + signed_artifact.read_bytes()[1:])
        require_error(builder.QualificationError, lambda: builder._prepared_inputs(unsigned, signed))
        signed_artifact.write_bytes(native.read_bytes()[:-256] + b"s" * 256)
        arguments = SimpleNamespace(
            **{
                **vars(arguments),
                "phase": "package",
                "unsigned_directory": unsigned,
                "signed_directory": signed,
                "build_directory": None,
                "vcpkg_toolchain": None,
                "vane_vcpkg_installed": None,
                "runtime_python": [Path(sys.executable)],
                "runtime_wheel": [license_file],
                "output_directory": root / "dist",
            }
        )
        with (
            mock.patch.object(builder, "_parse_arguments", return_value=arguments),
            mock.patch.object(builder, "_require_git_revision"),
            mock.patch.object(builder, "_platform_tag", return_value=PLATFORM),
            mock.patch.object(builder, "_build_environment") as environment,
            mock.patch.object(builder, "_run") as run,
            mock.patch.object(builder, "_read_signing_private_key") as key,
            mock.patch.object(builder, "_package_provider_matrix") as package,
        ):
            if builder.main() != 0:
                raise AssertionError("fresh wheel packaging failed")
            package.assert_called_once()
            if package.call_args.kwargs["signed"] != signed_artifact:
                raise AssertionError("package did not consume the isolated signed artifact")
            environment.assert_not_called()
            run.assert_not_called()
            key.assert_not_called()


def exercise_isolated_signer(signer: ModuleType) -> None:
    source = (REPOSITORY_ROOT / "scripts/sign_vane_dynamic_bundle.py").read_text()
    allowed = {"__future__", "argparse", "hashlib", "os", "re", "stat", "subprocess", "tempfile", "tomllib", "pathlib"}
    for node in ast.walk(ast.parse(source)):
        modules = (
            [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else [node.module] if isinstance(node, ast.ImportFrom) else []
        )
        if any(module not in allowed for module in modules):
            raise AssertionError("the signing boundary must remain stdlib-only")
    for profile, filename in (("production", "vane-extension-release.toml"), ("testpypi", "vane-extension.toml")):
        with mock.patch.object(signer, "_git", return_value=(REPOSITORY_ROOT / filename).read_text()) as git:
            manifest = signer.committed_manifest(REPOSITORY_ROOT, profile)
            git.assert_called_once_with(REPOSITORY_ROOT, "show", f"HEAD:{filename}")
            if manifest["repository"] != "AstroVela/vane":
                raise AssertionError("the signing source must stay fixed to the official repository")
    with tempfile.TemporaryDirectory(prefix="vane-paimon-isolated-sign-") as value:
        root = Path(value)
        incoming = root / "unsigned/artifacts"
        incoming.mkdir(parents=True)
        native = incoming / "paimon.duckdb_extension"
        native.write_bytes(b"payload" * 128 + b"\0" * 256)
        signer.require_artifact(native)
        link = incoming / "link.duckdb_extension"
        link.symlink_to(native)
        require_error(ValueError, lambda: signer.require_artifact(link))
        with mock.patch.object(signer, "MAX_ARTIFACT_BYTES", 512):
            require_error(ValueError, lambda: signer.require_artifact(native))
        native.write_bytes(native.read_bytes()[:-256] + b"s" * 256)
        require_error(ValueError, lambda: signer.require_artifact(native))
        native.write_bytes(native.read_bytes()[:-256] + b"\0" * 256)
        temporary = root / "runner-temp"
        temporary.mkdir()
        arguments = SimpleNamespace(
            extension_root=REPOSITORY_ROOT,
            profile="production",
            vane_source=root / "vane",
            input_directory=root / "unsigned",
            output_directory=root / "signed",
        )
        observed_keys = []

        def fake_git(_root, *args):
            return (
                "a" * 40
                if args[0] == "rev-parse"
                else "https://github.com/AstroVela/vane" if args[0] == "remote" else ""
            )

        def failed_sign(command, **kwargs):
            if command[:3] != ["/usr/bin/python3", "-I", "-S"]:
                raise AssertionError("signer utility must run without site packages")
            if "VANE_PROVIDER_SIGNING_PRIVATE_KEY" in kwargs["env"]:
                raise AssertionError("the private key must not reach child process environments")
            key_path = Path(command[command.index("--private-key") + 1])
            if key_path.stat().st_mode & 0o777 != 0o600 or key_path.read_bytes() != b"throwaway test key":
                raise AssertionError("the signer must use a private bounded temporary key")
            observed_keys.append(key_path)
            raise RuntimeError("synthetic signer failure")

        with (
            mock.patch.dict(
                os.environ, {"RUNNER_TEMP": str(temporary), "VANE_PROVIDER_SIGNING_PRIVATE_KEY": "throwaway test key"}
            ),
            mock.patch.object(signer, "committed_manifest", return_value={"revision": "a" * 40}),
            mock.patch.object(signer, "_git", side_effect=fake_git),
            mock.patch.object(signer, "require_key_fingerprint"),
            mock.patch.object(signer.subprocess, "run", side_effect=failed_sign),
        ):
            require_error(
                RuntimeError,
                lambda: signer.main(
                    [
                        "sign",
                        "--profile",
                        "production",
                        "--extension-root",
                        str(REPOSITORY_ROOT),
                        "--vane-source",
                        str(arguments.vane_source),
                        "--input-directory",
                        str(arguments.input_directory),
                        "--output-directory",
                        str(arguments.output_directory),
                    ]
                ),
            )
            if "VANE_PROVIDER_SIGNING_PRIVATE_KEY" in os.environ:
                raise AssertionError("the secret must be removed before any subprocess")
        if not observed_keys or any(path.exists() for path in observed_keys) or any(temporary.iterdir()):
            raise AssertionError("private signing files must be removed after failure")


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
    signer = load_script("vane_paimon_isolated_signer", "scripts/sign_vane_dynamic_bundle.py")
    try:
        exercise_release_validator(validator)
        exercise_integration_pins()
        exercise_promotion_cli(validator)
        exercise_workflow_contract()
        exercise_preflight(preflight, validator)
        exercise_production_signing(builder, signer)
        exercise_runtime_index(preflight, validator)
        exercise_phase_boundaries(builder)
        exercise_isolated_signer(signer)
        exercise_private_key_consumption(builder)
    finally:
        sys.modules.pop(validator.__name__, None)
        sys.modules.pop(builder.__name__, None)
        sys.modules.pop(preflight.__name__, None)
        sys.modules.pop(signer.__name__, None)


if __name__ == "__main__":
    main()
