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

"""Create one symbol-isolated relocatable object from a static Paimon closure."""

from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

PRIVATE_SYMBOL_PREFIX = "vane_paimon_private_"
BUNDLED_NAMESPACE_MARKERS = (
    "Aws::",
    "arrow::",
    "avro::",
    "fmt::",
    "google::protobuf::",
    "orc::",
    "paimon::",
    "parquet::",
    "re2::",
    "snappy::",
    "tbb::",
)


class IsolationError(RuntimeError):
    """Raised when the reviewed static-bundle contract is violated."""


def _run(command: Sequence[str]) -> None:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    subprocess.run(command, check=True)


def _capture(command: Sequence[str]) -> str:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    return subprocess.run(
        command,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout


def _capture_bytes(command: Sequence[str]) -> bytes:
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    return subprocess.run(command, check=True, stdout=subprocess.PIPE).stdout


def _require_file(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise IsolationError(f"{description} is not a file: {resolved}")
    return resolved


def _require_tool(path: Path, description: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise IsolationError(f"{description} is not executable: {resolved}")
    return resolved


def _just_symbols(nm: Path, path: Path, *selection: str) -> frozenset[str]:
    output = _capture(
        (str(nm), "--extern-only", "--format=just-symbols", *selection, str(path))
    )
    symbols = frozenset(line.strip() for line in output.splitlines() if line.strip())
    if any(any(character.isspace() for character in symbol) for symbol in symbols):
        raise IsolationError(f"nm returned an unsupported symbol name for {path}")
    return symbols


def _undefined_symbols(nm: Path, path: Path) -> tuple[frozenset[str], frozenset[str]]:
    output = _capture(
        (
            str(nm),
            "--extern-only",
            "--undefined-only",
            "--format=posix",
            str(path),
        )
    )
    all_symbols: set[str] = set()
    strong_symbols: set[str] = set()
    for line in output.splitlines():
        fields = line.split()
        if not fields:
            continue
        if len(fields) < 2:
            raise IsolationError(f"could not parse nm undefined-symbol line: {line!r}")
        symbol, symbol_type = fields[:2]
        all_symbols.add(symbol)
        if symbol_type == "U":
            strong_symbols.add(symbol)
    return frozenset(all_symbols), frozenset(strong_symbols)


def _demangled_strong_undefined(nm: Path, path: Path) -> tuple[str, ...]:
    output = _capture(
        (
            str(nm),
            "--demangle",
            "--extern-only",
            "--undefined-only",
            str(path),
        )
    )
    return tuple(
        stripped[2:]
        for line in output.splitlines()
        if (stripped := line.strip()).startswith("U ")
    )


def _archive_members(archiver: Path, archive: Path) -> tuple[str, ...]:
    members = tuple(
        line.strip()
        for line in _capture((str(archiver), "t", str(archive))).splitlines()
        if line.strip()
    )
    if not members:
        raise IsolationError(f"component archive is empty: {archive}")
    if len(members) != len(set(members)):
        raise IsolationError(f"component archive contains duplicate member names: {archive}")
    return members


def _deduplicate_component_archives(
    archiver: Path,
    archives: tuple[Path, ...],
    directory: Path,
) -> tuple[Path, ...]:
    copied: list[Path] = []
    occurrences: dict[str, list[Path]] = {}
    for index, archive in enumerate(archives):
        destination = directory / f"{index:02d}-{archive.name}"
        shutil.copyfile(archive, destination)
        copied.append(destination)
        for member in _archive_members(archiver, destination):
            occurrences.setdefault(member, []).append(destination)

    for member, member_archives in occurrences.items():
        if len(member_archives) < 2:
            continue
        seen_digests: set[str] = set()
        for archive in member_archives:
            contents = _capture_bytes((str(archiver), "p", str(archive), member))
            digest = hashlib.sha256(contents).hexdigest()
            if digest in seen_digests:
                _run((str(archiver), "d", str(archive), member))
                print(
                    f"removed byte-identical duplicate archive member {member} "
                    f"from {archive.name}",
                    file=sys.stderr,
                )
            else:
                seen_digests.add(digest)

    for destination in copied:
        _run((str(archiver), "s", str(destination)))
    return tuple(copied)


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--linker", required=True, type=Path)
    parser.add_argument("--archiver", required=True, type=Path)
    parser.add_argument("--objcopy", required=True, type=Path)
    parser.add_argument("--nm", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--entry-symbol", required=True)
    parser.add_argument("--objects", required=True, nargs="+", type=Path)
    parser.add_argument("--component-archives", required=True, nargs="+", type=Path)
    parser.add_argument("--dependency-archives", required=True, nargs="+", type=Path)
    return parser.parse_args()


def main() -> int:
    arguments = _parse_arguments()
    linker = _require_tool(arguments.linker, "linker")
    archiver = _require_tool(arguments.archiver, "archiver")
    objcopy = _require_tool(arguments.objcopy, "objcopy")
    nm = _require_tool(arguments.nm, "nm")
    objects = tuple(_require_file(path, "extension object") for path in arguments.objects)
    components = tuple(
        _require_file(path, "Paimon component archive")
        for path in arguments.component_archives
    )
    dependencies = tuple(
        _require_file(path, "Paimon dependency archive")
        for path in arguments.dependency_archives
    )
    if len(components) != len(set(components)):
        raise IsolationError("Paimon component archive list contains duplicates")
    if len(dependencies) != len(set(dependencies)):
        raise IsolationError("Paimon dependency archive list contains duplicates")
    if set(components) & set(dependencies):
        raise IsolationError("component and dependency archive lists overlap")
    if not arguments.entry_symbol or any(
        character.isspace() for character in arguments.entry_symbol
    ):
        raise IsolationError("--entry-symbol must contain one non-whitespace symbol")

    output = arguments.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    all_inputs = set(objects) | set(components) | set(dependencies)
    if output in all_inputs:
        raise IsolationError("isolated output must not overwrite an input")

    dependency_definitions: set[str] = set()
    for dependency in dependencies:
        dependency_definitions.update(
            _just_symbols(nm, dependency, "--defined-only")
        )

    with tempfile.TemporaryDirectory(
        prefix="vane-paimon-isolation-", dir=output.parent
    ) as temporary_value:
        temporary = Path(temporary_value)
        sanitized_components = _deduplicate_component_archives(
            archiver, components, temporary
        )
        raw_bundle = temporary / "paimon-bundle.raw.o"
        prefixed_bundle = temporary / "paimon-bundle.prefixed.o"
        isolated_bundle = temporary / "paimon-bundle.isolated.o"
        symbol_map = temporary / "restore-symbols.map"

        _run(
            (
                str(linker),
                "-r",
                "-o",
                str(raw_bundle),
                *(str(path) for path in objects),
                "--whole-archive",
                *(str(path) for path in sanitized_components),
                "--no-whole-archive",
                "--start-group",
                *(str(path) for path in dependencies),
                "--end-group",
            )
        )

        defined = _just_symbols(nm, raw_bundle, "--defined-only")
        undefined, strong_undefined = _undefined_symbols(nm, raw_bundle)
        if arguments.entry_symbol not in defined:
            raise IsolationError(
                f"relocatable bundle does not define {arguments.entry_symbol}"
            )
        if any(symbol.startswith(PRIVATE_SYMBOL_PREFIX) for symbol in defined | undefined):
            raise IsolationError(
                f"input symbol already uses reserved prefix {PRIVATE_SYMBOL_PREFIX!r}"
            )
        unresolved_bundled = sorted(strong_undefined & dependency_definitions)
        if unresolved_bundled:
            preview = "\n".join(f"  {symbol}" for symbol in unresolved_bundled[:50])
            raise IsolationError(
                "static dependency group left bundled symbols unresolved:\n" + preview
            )
        unresolved_namespaces = tuple(
            symbol
            for symbol in _demangled_strong_undefined(nm, raw_bundle)
            if any(marker in symbol for marker in BUNDLED_NAMESPACE_MARKERS)
        )
        if unresolved_namespaces:
            preview = "\n".join(
                f"  {symbol}" for symbol in unresolved_namespaces[:50]
            )
            raise IsolationError(
                "relocatable bundle would resolve private C++ symbols outside its "
                "reviewed closure:\n" + preview
            )

        _run(
            (
                str(objcopy),
                f"--prefix-symbols={PRIVATE_SYMBOL_PREFIX}",
                str(raw_bundle),
                str(prefixed_bundle),
            )
        )
        restored = sorted(undefined | {arguments.entry_symbol})
        symbol_map.write_text(
            "".join(
                f"{PRIVATE_SYMBOL_PREFIX}{symbol} {symbol}\n" for symbol in restored
            ),
            encoding="utf-8",
        )
        _run(
            (
                str(objcopy),
                f"--redefine-syms={symbol_map}",
                str(prefixed_bundle),
                str(isolated_bundle),
            )
        )

        isolated_defined = _just_symbols(nm, isolated_bundle, "--defined-only")
        isolated_undefined, _ = _undefined_symbols(nm, isolated_bundle)
        unexpected_exports = sorted(
            symbol
            for symbol in isolated_defined
            if symbol != arguments.entry_symbol
            and not symbol.startswith(PRIVATE_SYMBOL_PREFIX)
        )
        if unexpected_exports:
            preview = "\n".join(f"  {symbol}" for symbol in unexpected_exports[:50])
            raise IsolationError(
                "symbol isolation left unexpected unprefixed definitions:\n" + preview
            )
        if isolated_undefined != undefined:
            raise IsolationError("symbol isolation changed the external undefined closure")

        os.replace(isolated_bundle, output)

    print(
        f"isolated {len(defined)} definitions; restored {len(undefined)} external "
        f"references and entry point {arguments.entry_symbol}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except IsolationError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
