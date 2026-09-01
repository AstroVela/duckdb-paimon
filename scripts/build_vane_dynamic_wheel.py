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

"""Build and qualify a self-contained Paimon provider wheel for Vane."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Iterable, Sequence
from pathlib import Path

import tomllib
from packaging.tags import sys_tags

EXTENSION_NAME = "paimon"
TRUST_IDENTITY = "vane-ci-test-key"
LICENSE_EXPRESSION = (
    "0BSD AND Apache-2.0 AND BSD-2-Clause AND BSD-3-Clause AND BSL-1.0 AND "
    "ISC AND MIT AND NCSA AND Unicode-DFS-2015 AND Zlib AND curl"
)
EXPECTED_DUCKDB_LICENSE_PATTERNS = (
    "external/duckdb/LICENSE",
    "external/duckdb/src/include/duckdb/storage/compression/alp/algorithm/LICENSE",
    "external/duckdb/src/include/duckdb/storage/compression/alprd/algorithm/LICENSE",
    "external/duckdb/third_party/*/LICENSE",
    "external/duckdb/third_party/tdigest/NOTICES",
    "external/duckdb/third_party/thrift/thrift/LICENSE",
    "external/duckdb/third_party/jemalloc/LICENSE",
)
EXPECTED_VCPKG_LICENSE_COMPONENTS = frozenset({"curl", "openssl", "zlib"})
EXPECTED_BUNDLED_LICENSE_SOURCES = (
    ("Apache-Arrow-LICENSE.txt", "arrow_ep-prefix/src/arrow_ep/LICENSE.txt"),
    ("Apache-Arrow-NOTICE.txt", "arrow_ep-prefix/src/arrow_ep/NOTICE.txt"),
    ("Apache-Avro-LICENSE.txt", "avro_ep-prefix/src/avro_ep/LICENSE.txt"),
    ("Apache-Avro-NOTICE.txt", "avro_ep-prefix/src/avro_ep/NOTICE.txt"),
    ("AWS-C-Auth-LICENSE.txt", "aws-c-auth_ep-prefix/src/aws-c-auth_ep/LICENSE"),
    ("AWS-C-Auth-NOTICE.txt", "aws-c-auth_ep-prefix/src/aws-c-auth_ep/NOTICE"),
    ("AWS-C-Cal-LICENSE.txt", "aws-c-cal_ep-prefix/src/aws-c-cal_ep/LICENSE"),
    ("AWS-C-Cal-NOTICE.txt", "aws-c-cal_ep-prefix/src/aws-c-cal_ep/NOTICE"),
    ("AWS-C-Common-LICENSE.txt", "aws-c-common_ep-prefix/src/aws-c-common_ep/LICENSE"),
    ("AWS-C-Common-NOTICE.txt", "aws-c-common_ep-prefix/src/aws-c-common_ep/NOTICE"),
    (
        "AWS-C-Compression-LICENSE.txt",
        "aws-c-compression_ep-prefix/src/aws-c-compression_ep/LICENSE",
    ),
    (
        "AWS-C-Compression-NOTICE.txt",
        "aws-c-compression_ep-prefix/src/aws-c-compression_ep/NOTICE",
    ),
    ("AWS-C-HTTP-LICENSE.txt", "aws-c-http_ep-prefix/src/aws-c-http_ep/LICENSE"),
    ("AWS-C-HTTP-NOTICE.txt", "aws-c-http_ep-prefix/src/aws-c-http_ep/NOTICE"),
    ("AWS-C-IO-LICENSE.txt", "aws-c-io_ep-prefix/src/aws-c-io_ep/LICENSE"),
    ("AWS-C-IO-NOTICE.txt", "aws-c-io_ep-prefix/src/aws-c-io_ep/NOTICE"),
    (
        "AWS-C-SDKUtils-LICENSE.txt",
        "aws-c-sdkutils_ep-prefix/src/aws-c-sdkutils_ep/LICENSE",
    ),
    (
        "AWS-C-SDKUtils-NOTICE.txt",
        "aws-c-sdkutils_ep-prefix/src/aws-c-sdkutils_ep/NOTICE",
    ),
    ("AWS-S2N-LICENSE.txt", "s2n_ep-prefix/src/s2n_ep/LICENSE"),
    ("AWS-S2N-NOTICE.txt", "s2n_ep-prefix/src/s2n_ep/NOTICE"),
    ("DataSketches-LICENSE.txt", "datasketches_ep-prefix/src/datasketches_ep/LICENSE"),
    ("DataSketches-NOTICE.txt", "datasketches_ep-prefix/src/datasketches_ep/NOTICE"),
    ("fmt-LICENSE.txt", "fmt_ep-prefix/src/fmt_ep/LICENSE"),
    ("glog-LICENSE.txt", "glog_ep-prefix/src/glog_ep/COPYING"),
    ("LZ4-LICENSE.txt", "lz4_ep-prefix/src/lz4_ep/LICENSE"),
    ("Apache-ORC-LICENSE.txt", "orc_ep-prefix/cpp/LICENSE"),
    ("Apache-ORC-NOTICE.txt", "orc_ep-prefix/cpp/NOTICE"),
    ("Protobuf-LICENSE.txt", "protobuf_ep-prefix/src/protobuf_ep/LICENSE"),
    ("RapidJSON-LICENSE.txt", "rapidjson_ep-prefix/src/rapidjson_ep/license.txt"),
    ("RE2-LICENSE.txt", "re2_ep-prefix/src/re2_ep/LICENSE"),
    ("Snappy-LICENSE.txt", "snappy_ep-prefix/src/snappy_ep/COPYING"),
    ("oneTBB-LICENSE.txt", "tbb_ep-prefix/src/tbb_ep/LICENSE.txt"),
    ("zlib-LICENSE.txt", "zlib_ep-prefix/src/zlib_ep/LICENSE"),
    ("Zstandard-LICENSE.txt", "zstd_ep-prefix/src/zstd_ep/LICENSE"),
)
ALLOWED_RUNTIME_LIBRARIES = frozenset(
    {
        "ld-linux-x86-64.so.2",
        "libc.so.6",
        "libdl.so.2",
        "libgcc_s.so.1",
        "libm.so.6",
        "libpthread.so.0",
        "librt.so.1",
        "libstdc++.so.6",
    }
)
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_PLATFORM_TAG_RE = re.compile(r"^manylinux_[0-9]+_[0-9]+_x86_64$")


class QualificationError(RuntimeError):
    """Raised when a qualification input or artifact violates the fixed contract."""


def _run(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
) -> None:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def _capture(command: Sequence[str], *, cwd: Path) -> str:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def _require_directory(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_dir():
        raise QualificationError(f"{description} is not a directory: {resolved}")
    return resolved


def _require_file(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise QualificationError(f"{description} is not a file: {resolved}")
    return resolved


def _require_git_revision(source: Path, expected: str, description: str) -> None:
    if not _REVISION_RE.fullmatch(expected):
        raise QualificationError(
            f"{description} expected revision is not a complete commit SHA: {expected!r}"
        )
    actual = _capture(("git", "rev-parse", "HEAD^{commit}"), cwd=source)
    if actual != expected:
        raise QualificationError(
            f"{description} checkout is {actual}, expected {expected}"
        )
    status = _capture(
        ("git", "status", "--porcelain", "--untracked-files=no"), cwd=source
    )
    if status:
        raise QualificationError(
            f"{description} checkout has tracked working-tree changes"
        )


def _one_wheel(directory: Path, pattern: str, description: str) -> Path:
    wheels = sorted(directory.glob(pattern))
    if len(wheels) != 1:
        raise QualificationError(
            f"expected exactly one {description}, found {len(wheels)} below {directory}"
        )
    return wheels[0]


def _platform_tag() -> str:
    if sys.platform != "linux" or platform.machine() != "x86_64":
        raise QualificationError("dynamic wheel qualification requires Linux x86_64")
    tag = next(
        (
            candidate.platform
            for candidate in sys_tags()
            if candidate.platform.startswith("manylinux_")
        ),
        "",
    )
    if not _PLATFORM_TAG_RE.fullmatch(tag):
        raise QualificationError(
            f"could not resolve one exact manylinux x86_64 platform tag: {tag!r}"
        )
    return tag


def _compiler_launcher_arguments() -> list[str]:
    launcher = os.environ.get("VANE_CMAKE_COMPILER_LAUNCHER", "")
    if not launcher:
        return []
    if launcher != "ccache":
        raise QualificationError("VANE_CMAKE_COMPILER_LAUNCHER must be ccache when set")
    return [
        "-DCMAKE_C_COMPILER_LAUNCHER=ccache",
        "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache",
    ]


def _write_loadable_extension_config(
    extension_root: Path, build_directory: Path
) -> Path:
    extension_config = _require_file(
        extension_root / "extension_config_vane.cmake",
        "Vane extension configuration",
    )
    config = build_directory / "vane-paimon-loadable-config.cmake"
    config.write_text(
        "# Generated by scripts/build_vane_dynamic_wheel.py.\n"
        f'include("{extension_config.as_posix()}")\n'
        'list(FIND DUCKDB_EXTENSION_NAMES "paimon" _VANE_PAIMON_INDEX)\n'
        "if(_VANE_PAIMON_INDEX EQUAL -1)\n"
        '  message(FATAL_ERROR "Paimon extension config did not register paimon")\n'
        "endif()\n"
        "set(DUCKDB_EXTENSION_PAIMON_SHOULD_LINK FALSE)\n",
        encoding="utf-8",
    )
    return config


def _vcpkg_baseline(extension_root: Path) -> str:
    manifest = json.loads(
        _require_file(
            extension_root / "vcpkg.json", "extension vcpkg manifest"
        ).read_text(encoding="utf-8")
    )
    baseline = manifest.get("builtin-baseline")
    if not isinstance(baseline, str) or not _REVISION_RE.fullmatch(baseline):
        raise QualificationError(
            "vcpkg.json must contain one complete builtin-baseline"
        )
    return baseline


def _require_vcpkg_toolchain(path: Path, baseline: str) -> Path:
    toolchain = _require_file(path, "vcpkg toolchain")
    try:
        vcpkg_root = toolchain.parents[2]
    except IndexError:
        raise QualificationError(
            f"vcpkg toolchain does not have the expected repository layout: {toolchain}"
        ) from None
    expected = (vcpkg_root / "scripts/buildsystems/vcpkg.cmake").resolve()
    if toolchain != expected:
        raise QualificationError(
            f"vcpkg toolchain does not have the expected repository layout: {toolchain}"
        )
    _require_git_revision(vcpkg_root, baseline, "extension vcpkg")
    return toolchain


def _build_environment(
    *,
    extension_root: Path,
    build_directory: Path,
    vane_vcpkg_installed: Path,
    vcpkg_toolchain: Path,
    jobs: int,
) -> dict[str, str]:
    target_triplet = "x64-linux"
    dependency_prefix = vane_vcpkg_installed / target_triplet
    for relative in (
        "share/arrow/ArrowConfig.cmake",
        "share/arrowflight/ArrowFlightConfig.cmake",
    ):
        _require_file(
            dependency_prefix / relative, "Vane native dependency configuration"
        )

    prefix_config = build_directory / "vane-dynamic-wheel-dependency-prefix.cmake"
    prefix_config.write_text(
        "# Generated by scripts/build_vane_dynamic_wheel.py.\n"
        f'list(PREPEND CMAKE_PREFIX_PATH "{dependency_prefix}")\n',
        encoding="utf-8",
    )
    extension_config = _write_loadable_extension_config(
        extension_root, build_directory
    )
    cmake_arguments = [
        "--fresh",
        "-DBUILD_DISTRIBUTED_EXCHANGE=ON",
        "-DENABLE_EXTENSION_AUTOLOADING=OFF",
        "-DENABLE_EXTENSION_AUTOINSTALL=OFF",
        "-DEXTENSION_STATIC_BUILD=ON",
        "-DPAIMON_VANE_DISTRIBUTED=ON",
        "-DPAIMON_VANE_SELF_CONTAINED=ON",
        "-DVANE_ENABLE_TEST_EXTENSION_SIGNING_KEY=ON",
        f"-DDUCKDB_EXTENSION_CONFIGS={extension_config}",
        "-DVCPKG_BUILD=ON",
        f"-DCMAKE_TOOLCHAIN_FILE={vcpkg_toolchain}",
        f"-DVCPKG_MANIFEST_DIR={extension_root}",
        f"-DVCPKG_INSTALLED_DIR={build_directory / 'vcpkg_installed'}",
        f"-DVCPKG_TARGET_TRIPLET={target_triplet}",
        f"-DCMAKE_PROJECT_TOP_LEVEL_INCLUDES={prefix_config}",
        *_compiler_launcher_arguments(),
    ]

    environment = os.environ.copy()
    selection_variables = {
        "VCPKG_CHAINLOAD_TOOLCHAIN_FILE",
        "VCPKG_DEFAULT_HOST_TRIPLET",
        "VCPKG_DEFAULT_TRIPLET",
        "VCPKG_OVERLAY_PORTS",
        "VCPKG_OVERLAY_TRIPLETS",
    }
    for name in tuple(environment):
        if (
            name
            in {
                "CMAKE_ARGS",
                "CMAKE_PREFIX_PATH",
                "COVERAGE",
                "DONT_LINK",
                "GITHUB_BASE_REF",
                "GITHUB_REF_NAME",
                "VANE_CMAKE_PREFIX_PATH",
                "VANE_CMAKE_COMPILER_LAUNCHER",
                "VANE_VERSION_BRANCH",
            }
            or name in selection_variables
            or name.startswith(("SETUPTOOLS_SCM_PRETEND_VERSION", "SKBUILD_"))
            or (name[:7] == "DUCKDB_" and name.endswith("_DIRECTORY"))
        ):
            environment.pop(name)
    environment.update(
        {
            "CMAKE_ARGS": shlex.join(cmake_arguments),
            "CMAKE_BUILD_PARALLEL_LEVEL": str(jobs),
            "CMAKE_GENERATOR": "Ninja",
            "SKBUILD_BUILD_DIR": str(build_directory),
            "SKBUILD_CMAKE_BUILD_TYPE": "Release",
            "VCPKG_MAX_CONCURRENCY": str(jobs),
            "VCPKG_TARGET_TRIPLET": target_triplet,
            "VCPKG_TOOLCHAIN_PATH": str(vcpkg_toolchain),
        }
    )
    return environment


def _require_self_contained_artifact(artifact: Path) -> None:
    dynamic = _capture(("readelf", "--dynamic", str(artifact)), cwd=artifact.parent)
    needed = frozenset(re.findall(r"Shared library: \[([^]]+)]", dynamic))
    unexpected = sorted(needed - ALLOWED_RUNTIME_LIBRARIES)
    if unexpected:
        raise QualificationError(
            f"{artifact.name} retains non-platform runtime libraries: {unexpected}"
        )
    if "(RPATH)" in dynamic or "(RUNPATH)" in dynamic:
        raise QualificationError(f"{artifact.name} retains a runtime search path")

    symbols = _capture(
        ("nm", "--dynamic", "--demangle", "--undefined-only", str(artifact)),
        cwd=artifact.parent,
    )
    bundled_namespaces = (
        "Aws::",
        "arrow::",
        "avro::",
        "duckdb::",
        "fmt::",
        "google::protobuf::",
        "orc::",
        "paimon::",
        "parquet::",
        "re2::",
        "snappy::",
        "tbb::",
    )
    forbidden = tuple(
        line.strip()
        for line in symbols.splitlines()
        if any(namespace in line for namespace in bundled_namespaces)
    )
    if forbidden:
        details = "\n".join(f"  {symbol}" for symbol in forbidden)
        raise QualificationError(
            f"{artifact.name} has unresolved bundled C++ symbols:\n{details}"
        )


def _require_base_wheel_free_of_paimon(base_wheel: Path) -> None:
    with zipfile.ZipFile(base_wheel) as wheel:
        unexpected = sorted(
            name for name in wheel.namelist() if "paimon" in name.lower()
        )
    if unexpected:
        raise QualificationError(f"base Vane wheel contains Paimon paths: {unexpected}")


def _render_vcpkg_license_bundle(extension_root: Path, share_directory: Path) -> str:
    records = sorted(
        (
            path
            for path in share_directory.glob("*/copyright")
            if not path.parent.name.startswith("vcpkg-")
        ),
        key=lambda path: path.parent.name,
    )
    components = frozenset(record.parent.name for record in records)
    if components != EXPECTED_VCPKG_LICENSE_COMPONENTS:
        missing = sorted(EXPECTED_VCPKG_LICENSE_COMPONENTS - components)
        unexpected = sorted(components - EXPECTED_VCPKG_LICENSE_COMPONENTS)
        raise QualificationError(
            "extension vcpkg license closure differs from the reviewed Linux set: "
            f"missing={missing}, unexpected={unexpected}"
        )
    lines = [
        "Vane Paimon provider vcpkg binary dependency licenses",
        "======================================================",
        "",
        f"vcpkg builtin baseline: {_vcpkg_baseline(extension_root)}",
        "",
    ]
    for record in records:
        lines.extend(
            (
                "=" * 80,
                f"Component: {record.parent.name}",
                "=" * 80,
                record.read_text(encoding="utf-8", errors="replace").strip(),
                "",
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def _duckdb_license_sources(vane_source: Path) -> tuple[tuple[str, Path], ...]:
    project = tomllib.loads(
        (vane_source / "pyproject.toml").read_text(encoding="utf-8")
    ).get("project")
    if not isinstance(project, dict):
        raise QualificationError("Vane pyproject.toml must contain a project table")
    license_patterns = project.get("license-files")
    if not isinstance(license_patterns, list) or any(
        not isinstance(value, str) for value in license_patterns
    ):
        raise QualificationError("Vane project.license-files must be a list of strings")
    duckdb_patterns = tuple(
        value
        for value in license_patterns
        if value.startswith("external/duckdb/")
        and not value.startswith("external/duckdb/extension/")
    )
    if duckdb_patterns != EXPECTED_DUCKDB_LICENSE_PATTERNS:
        raise QualificationError(
            "Vane's declared DuckDB license patterns differ from the reviewed set: "
            f"{duckdb_patterns}"
        )

    sources: dict[str, Path] = {}
    for pattern in duckdb_patterns:
        matches = sorted(vane_source.glob(pattern))
        if not matches:
            raise QualificationError(
                f"Vane DuckDB license pattern has no matches: {pattern!r}"
            )
        for match in matches:
            source = _require_file(match, "Vane DuckDB license source")
            try:
                relative = source.relative_to(vane_source).as_posix()
            except ValueError:
                raise QualificationError(
                    f"Vane DuckDB license source escapes the checkout: {source}"
                ) from None
            sources[relative] = source
    return tuple(sorted(sources.items()))


def _render_duckdb_license_bundle(vane_source: Path) -> str:
    lines = [
        "DuckDB static-engine source and third-party licenses",
        "====================================================",
        "",
    ]
    for relative, source in _duckdb_license_sources(vane_source):
        lines.extend(
            (
                "=" * 80,
                f"Source: {relative}",
                "=" * 80,
                source.read_text(encoding="utf-8", errors="replace").strip(),
                "",
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def _copy_license(source: Path, destination: Path) -> Path:
    _require_file(source, "license source")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    return destination


def _stage_license_files(
    *,
    extension_root: Path,
    vane_source: Path,
    build_directory: Path,
) -> tuple[Path, ...]:
    license_directory = build_directory / "dynamic-extension-licenses"
    license_directory.mkdir(parents=True, exist_ok=True)
    files = [
        _copy_license(
            vane_source / "LICENSE", license_directory / "Vane-Apache-2.0.txt"
        ),
        _copy_license(vane_source / "NOTICE", license_directory / "Vane-NOTICE.txt"),
        _copy_license(
            extension_root / "LICENSE",
            license_directory / "DuckDB-Paimon-Apache-2.0.txt",
        ),
        _copy_license(
            extension_root / "NOTICE", license_directory / "DuckDB-Paimon-NOTICE.txt"
        ),
        _copy_license(
            extension_root / "third_party/paimon-cpp/LICENSE",
            license_directory / "Apache-Paimon-Cpp-LICENSE.txt",
        ),
        _copy_license(
            extension_root / "third_party/paimon-cpp/NOTICE",
            license_directory / "Apache-Paimon-Cpp-NOTICE.txt",
        ),
    ]
    for source in sorted(
        (extension_root / "third_party/paimon-cpp/licenses").glob("*")
    ):
        files.append(
            _copy_license(
                source, license_directory / f"Apache-Paimon-Cpp-{source.name}"
            )
        )

    duckdb_bundle = license_directory / "DuckDB-static-engine-licenses.txt"
    duckdb_bundle.write_text(
        _render_duckdb_license_bundle(vane_source), encoding="utf-8"
    )
    files.append(duckdb_bundle)

    vcpkg_bundle = license_directory / "vcpkg-binary-dependencies.txt"
    vcpkg_bundle.write_text(
        _render_vcpkg_license_bundle(
            extension_root,
            build_directory / "vcpkg_installed/x64-linux/share",
        ),
        encoding="utf-8",
    )
    files.append(vcpkg_bundle)

    paimon_cpp_build = (
        build_directory
        / "duckdb/extension/paimon/paimon_cpp_ep-prefix/src/paimon_cpp_ep-build"
    )
    for destination_name, relative_source in EXPECTED_BUNDLED_LICENSE_SOURCES:
        files.append(
            _copy_license(
                paimon_cpp_build / relative_source,
                license_directory / destination_name,
            )
        )
    return tuple(files)


def _builder_python(
    base_wheel: Path, parent: Path
) -> tuple[tempfile.TemporaryDirectory[str], Path]:
    temporary = tempfile.TemporaryDirectory(
        prefix="vane-paimon-wheel-builder-", dir=parent
    )
    environment_root = Path(temporary.name)
    _run((sys.executable, "-I", "-m", "venv", "--copies", str(environment_root)))
    python = environment_root / "bin/python"
    _run(
        (
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "packaging>=24.2",
            str(base_wheel),
        )
    )
    return temporary, python


def _build_provider_wheel(
    *,
    python: Path,
    vane_source: Path,
    artifact: Path,
    output_directory: Path,
    platform_tag: str,
    license_files: Iterable[Path],
) -> Path:
    command = [
        str(python),
        "-I",
        str(vane_source / "scripts/build_extension_wheel.py"),
        "--artifact",
        str(artifact),
        "--extension-name",
        EXTENSION_NAME,
        "--output-directory",
        str(output_directory),
        "--platform-tag",
        platform_tag,
        "--trust-identity",
        TRUST_IDENTITY,
        "--license-expression",
        LICENSE_EXPRESSION,
    ]
    for license_file in license_files:
        command.extend(("--license-file", str(license_file)))
    _run(command)
    return _one_wheel(
        output_directory,
        "vane_extension_paimon-*.whl",
        "vane-extension-paimon wheel",
    )


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension-root", required=True, type=Path)
    parser.add_argument("--vane-source", required=True, type=Path)
    parser.add_argument("--vane-revision", required=True)
    parser.add_argument("--vane-vcpkg-installed", required=True, type=Path)
    parser.add_argument("--vcpkg-toolchain", required=True, type=Path)
    parser.add_argument("--build-directory", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--jobs", default=8, type=int)
    return parser.parse_args()


def main() -> int:
    arguments = _parse_arguments()
    if arguments.jobs <= 0:
        raise QualificationError("--jobs must be a positive integer")

    extension_root = _require_directory(arguments.extension_root, "extension root")
    vane_source = _require_directory(arguments.vane_source, "Vane source")
    vane_vcpkg_installed = _require_directory(
        arguments.vane_vcpkg_installed, "Vane vcpkg installation"
    )
    vcpkg_toolchain = _require_vcpkg_toolchain(
        arguments.vcpkg_toolchain,
        _vcpkg_baseline(extension_root),
    )
    build_directory = arguments.build_directory.expanduser().resolve()
    output_directory = arguments.output_directory.expanduser().resolve()
    build_directory.mkdir(parents=True, exist_ok=True)
    output_directory.mkdir(parents=True, exist_ok=True)
    existing_wheels = sorted(output_directory.glob("*.whl"))
    if existing_wheels:
        raise QualificationError(
            f"output directory already contains a wheel: {existing_wheels[0]}"
        )

    _require_git_revision(vane_source, arguments.vane_revision, "Vane")
    platform_tag = _platform_tag()
    environment = _build_environment(
        extension_root=extension_root,
        build_directory=build_directory,
        vane_vcpkg_installed=vane_vcpkg_installed,
        vcpkg_toolchain=vcpkg_toolchain,
        jobs=arguments.jobs,
    )

    with tempfile.TemporaryDirectory(
        prefix="vane-base-wheel-", dir=build_directory.parent
    ) as base_output_value:
        base_output = Path(base_output_value)
        _run(
            (
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--no-isolation",
                "--outdir",
                str(base_output),
                str(vane_source),
            ),
            cwd=extension_root,
            environment=environment,
        )
        base_wheel = _one_wheel(base_output, "vane_ai-*.whl", "base Vane wheel")
        _require_base_wheel_free_of_paimon(base_wheel)
        _run(
            (
                "cmake",
                "--build",
                str(build_directory),
                "--target",
                "paimon_loadable_extension",
                "--parallel",
                str(arguments.jobs),
            ),
            cwd=extension_root,
            environment=environment,
        )

        unsigned = _require_file(
            build_directory
            / "duckdb/extension/paimon/paimon.duckdb_extension",
            "unsigned Paimon artifact",
        )
        _require_self_contained_artifact(unsigned)
        signed_directory = build_directory / "signed-vane-extensions"
        signed_directory.mkdir(parents=True, exist_ok=True)
        signed = signed_directory / unsigned.name
        _run(
            (
                sys.executable,
                str(vane_source / "scripts/sign_test_dynamic_extension.py"),
                "--private-key",
                str(vane_source / "external/duckdb/test/mbedtls/private.pem"),
                str(unsigned),
                str(signed),
            )
        )

        licenses = _stage_license_files(
            extension_root=extension_root,
            vane_source=vane_source,
            build_directory=build_directory,
        )
        with tempfile.TemporaryDirectory(
            prefix="vane-qualified-wheels-", dir=output_directory.parent
        ) as staging_value:
            staging = Path(staging_value)
            repaired_base_directory = staging / "base"
            provider_directory = staging / "extension"
            repaired_base_directory.mkdir()
            provider_directory.mkdir()
            _run(
                (
                    sys.executable,
                    "-m",
                    "auditwheel",
                    "repair",
                    "--plat",
                    platform_tag,
                    "--wheel-dir",
                    str(repaired_base_directory),
                    str(base_wheel),
                )
            )
            repaired_base = _one_wheel(
                repaired_base_directory, "vane_ai-*.whl", "repaired base Vane wheel"
            )

            builder_environment, builder_python = _builder_python(
                base_wheel, build_directory.parent
            )
            try:
                provider_wheel = _build_provider_wheel(
                    python=builder_python,
                    vane_source=vane_source,
                    artifact=signed,
                    output_directory=provider_directory,
                    platform_tag=platform_tag,
                    license_files=licenses,
                )
                _run(
                    (
                        str(builder_python),
                        "-I",
                        str(vane_source / "scripts/verify_extension_wheel.py"),
                        "--base-wheel",
                        str(repaired_base),
                        "--extension-wheel",
                        str(provider_wheel),
                        "--extension-name",
                        EXTENSION_NAME,
                        "--trust-identity",
                        TRUST_IDENTITY,
                    )
                )
            finally:
                builder_environment.cleanup()

            for wheel in (repaired_base, provider_wheel):
                destination = output_directory / wheel.name
                shutil.copyfile(wheel, destination)
                print(destination)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except QualificationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
