# Vane provider release tooling

`vane-provider-release.toml` declares Paimon's provider distribution, its complete
CPython/platform wheel matrix, and the per-file staging upload budget. It has no
provider dependencies; the generated wheels require the exact `vane-ai` version.

The release validator is owned by the pinned `vane-extension-ci-tools` submodule,
not copied into this repository. All native and release jobs use that same exact
tool revision. Initialize it and run the lightweight consumer checks with Python
3.11 or newer:

```bash
git submodule update --init vane-extension-ci-tools
python -m pip install -r vane-extension-ci-tools/requirements-release.txt "PyYAML>=6.0.2"
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
  --channel testpypi-dev \
  --require-publishable-on testpypi
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
verification and local/two-worker Ray tests remain here. Uploads run directly in
this repository's `VaneExtension.yml`; PyPI does not support a reusable workflow
as the Trusted Publisher. The existing development `testpypi` environment and
Trusted Publisher remain unchanged.

The current shared native integration uses manifest schema 2, requiring an
explicit official vcpkg revision. Paimon's `vane-extension.toml` keeps the same
revision as its existing `vcpkg.json`; this integration does not change the Vane
revision, native dependency versions or development package versioning.

## Production preparation and activation

`VaneExtension.yml` has three explicit operations:

| Operation | Runtime source | Native signing key | Upload destination |
| --- | --- | --- | --- |
| `build-only` (default; also push/PR CI) | Existing development source pin | Public CI fixture key | GitHub CI artifacts only |
| `testpypi-dev` | Exact development `vane-ai` wheels from TestPyPI | `astrovela/vane-testpypi` | TestPyPI only |
| `release` | Exact non-development `vane-ai` wheels from PyPI | `astrovela/vane` | TestPyPI, qualification, approval, then identical files to PyPI |

The development manifest `vane-extension.toml` remains pinned to dev612.
`vane-extension-release.toml` is a separate committed, exact source pin. Its
initial `033b549afcb498633fd6669b26c054c00363004e` commit contains the production
public key but **is not a published Vane release**. Consequently `release` fails
in a read-only preflight job before entering the signing environment or building.
This preparation does not publish or relabel development wheels as production.

To activate production later:

1. Publish a canonical non-development Vane runtime, including all CPython
   3.10–3.14 `manylinux_2_28_x86_64` wheels on PyPI. Its source must descend from
   the reviewed production-key commit above. A prerelease such as `0.2.0rc1` is
   allowed; development, local, epoch and non-`X.Y.Z` versions are rejected.
2. Submit and review a PR updating only the production Vane source pin and its
   preparation-pin regression assertion. Keep the development manifest unchanged.
3. Configure the `production-signing` GitHub environment to permit only protected
   `main_vane`, with required reviewers, and store the production private key in
   its `VANE_EXTENSION_SIGNING_PRIVATE_KEY` secret. Do not use the TestPyPI key or
   a repository-wide secret. The expected production public DER SHA-256 is
   `8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb`.
4. Configure the `pypi` environment with required reviewers, prevent self-review,
   and allow only protected `main_vane`. Register `vane-extension-paimon` on PyPI
   with owner `AstroVela`, repository `duckdb-paimon`, workflow `VaneExtension.yml`,
   environment `pypi`. Keep the existing TestPyPI registration and `testpypi`
   environment for staging. Restrict that environment to protected `main_vane` too.
5. Manually dispatch `VaneExtension.yml` on `main_vane`, operation `release`.
   No provider tag is required or created. Approve signing only after reviewing
   the exact source pin, then approve `pypi` only after qualification succeeds.

The release job builds and RSA-signs the Paimon binary once, packages its complete
wheel matrix against the exact indexed runtime wheels, and runs Vane's native
signature, SourceID, platform and dependency checks. Both testing trust roots are
explicitly disabled. Signing-key files are consumed before native subprocesses;
the private key is never an artifact. A missing production secret cannot select
the development secret.

Those same wheels are staged on TestPyPI. Both local and two-worker Ray tests
download the staged provider, compare its bytes to the digest-verified candidate
artifact, and use the production runtime from PyPI. No mixed-index resolver or
runtime fallback is used. Ordinary runtime dependencies still come from PyPI.

Only after both tests succeed does the `pypi` approval job start. After approval,
the shared `verify-promotion` gate rechecks the complete staged filenames and
SHA-256 hashes and rejects conflicting PyPI files. The upload uses that unchanged
directory; no rebuild, resigning or version rewrite occurs. A final
`verify-index --index pypi` checks the complete indexed matrix. Identical partial
uploads can be retried; different files under an existing version fail closed.

Environment protection, secrets and Trusted Publisher registration are explicit
administrator setup steps; merging this preparation does not configure them or
trigger a release. A `build-only` run exercises the guarded tooling and existing
development CI, not a production publication or production-native qualification.
