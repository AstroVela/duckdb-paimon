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

"""Helpers for exercising the installed Paimon provider wheel."""

from __future__ import annotations

import os
from importlib import import_module
from importlib.metadata import entry_points


def load_packaged_dynamic_paimon(connection: object) -> None:
    """Validate and load the exact installed Paimon provider."""
    import vane
    from vane.extensions import LocalExtensionProvider

    trust_identity = os.environ.get("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY")
    if not trust_identity:
        raise AssertionError("VANE_EXPECTED_EXTENSION_TRUST_IDENTITY must name the explicit test trust root")

    installed = tuple(entry_points(group="vane.dynamic_extension_providers"))
    matches = [candidate for candidate in installed if candidate.name == "paimon"]
    if len(matches) != 1:
        raise AssertionError(f"expected exactly one installed 'paimon' provider entry point, found {len(matches)}")

    entry_point = matches[0]
    provider = entry_point.load()()
    if not isinstance(provider, LocalExtensionProvider):
        raise TypeError("the 'paimon' entry point did not return LocalExtensionProvider")
    descriptor = import_module(entry_point.module).descriptor()
    if descriptor.name != "paimon":
        raise AssertionError(f"the 'paimon' provider returned descriptor for {descriptor.name!r}")
    if descriptor.trust_identity != trust_identity:
        raise AssertionError(f"the Paimon descriptor uses unexpected trust identity {descriptor.trust_identity!r}")
    if descriptor.dependencies:
        raise AssertionError("the self-contained Paimon provider must not declare dynamic dependencies")
    artifact = provider.find(descriptor.identity)
    if artifact is None or artifact.descriptor != descriptor:
        raise AssertionError("the Paimon provider does not own its exact descriptor identity")

    security = connection.execute("""
        SELECT
            CAST(current_setting('allow_unsigned_extensions') AS BOOLEAN),
            CAST(current_setting('autoinstall_known_extensions') AS BOOLEAN),
            CAST(current_setting('autoload_known_extensions') AS BOOLEAN)
        """).fetchone()
    if security != (False, False, False):
        raise AssertionError(f"dynamic extension security settings are not fail-closed: {security!r}")

    state = connection.execute(
        "SELECT loaded, installed, install_mode FROM duckdb_extensions() WHERE extension_name = 'paimon'"
    ).fetchone()
    if state not in (None, (False, False, "NOT_INSTALLED")):
        raise AssertionError(f"Paimon was already installed or linked before provider loading: {state!r}")

    resolved = vane.load_installed_extension("paimon", connection=connection)
    if resolved.descriptor != descriptor:
        raise AssertionError("installed-provider loading did not return the exact Paimon descriptor")

    loaded = connection.execute(
        "SELECT loaded, installed, install_mode FROM duckdb_extensions() WHERE extension_name = 'paimon'"
    ).fetchone()
    if loaded != (True, False, "NOT_INSTALLED"):
        raise AssertionError(f"Paimon did not load dynamically from its provider wheel: {loaded!r}")


__all__ = ["load_packaged_dynamic_paimon"]
