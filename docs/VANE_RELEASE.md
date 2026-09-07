# Vane provider release tooling

`vane-provider-release.toml` declares Paimon's provider distribution, its complete
CPython/platform wheel matrix, and the TestPyPI per-file upload budget. It has no
provider dependencies; the generated wheels require the exact `vane-ai` version.

The release validator is owned by the pinned `vane-extension-ci-tools` submodule,
not copied into this repository. All native and release jobs use that same exact
tool revision. Initialize it and run the lightweight consumer checks with Python
3.11 or newer:

```bash
git submodule update --init vane-extension-ci-tools
python -m pip install -r vane-extension-ci-tools/requirements-release.txt
python -I test/vane/test_vane_provider_release.py
```

For a locally assembled TestPyPI candidate set:

```bash
python -I vane-extension-ci-tools/scripts/vane_provider_release.py validate \
  --manifest vane-extension.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory build/vane-testpypi-wheel-dist \
  --vane-version 0.2.0.dev612 \
  --require-testpypi-publishable
```

The Vane checkout must already exist at the exact manifest revision. The shared
gate verifies both official source revisions and rejects dirty or mismatched
Vane/tools checkouts before accepting wheels. Assembly and index-verification
jobs use a shallow checkout of that exact Vane revision; no native build or
version derivation is needed in these jobs.

`VaneExtension.yml` uses this shared gate before upload and compares the exact
indexed wheel filenames and SHA-256 digests after upload. The workflow's
`vane_version` and `paimon_version` outputs remain unchanged. Matrix, dependency
and index edge cases are tested in the shared repository; this repository tests
its real configuration and workflow integration plus private-key consumption.

The native Paimon builder, license collection, signing, exact Vane artifact
verification and local/two-worker Ray tests remain here. The final upload job
still runs in this repository's `VaneExtension.yml` with the existing `testpypi`
environment and Trusted Publisher; no signing key or publisher reconfiguration
is needed.

The current shared native integration uses manifest schema 2, requiring an
explicit official vcpkg revision. Paimon's `vane-extension.toml` keeps the same
revision as its existing `vcpkg.json`; this integration does not change the Vane
revision, native dependency versions, signing identity or package versioning.
