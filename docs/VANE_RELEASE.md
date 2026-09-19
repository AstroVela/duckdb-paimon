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
python -m pip install -r vane-extension-ci-tools/requirements-release.txt "PyYAML>=6.0.2" "setuptools-scm>=9.2.0"
python -I test/vane/test_vane_provider_release.py
```

For a locally assembled production candidate set:

```bash
python -I vane-extension-ci-tools/scripts/vane_provider_release.py validate \
  --manifest vane-extension-release.toml --extension-root . \
  --vane-source ../vane \
  --ci-tools-version "$(git rev-parse HEAD:vane-extension-ci-tools)" \
  --config vane-provider-release.toml \
  --directory build/vane-testpypi-wheel-dist \
  --vane-version 0.2.0 \
  --channel release \
  --require-publishable-on testpypi --require-publishable-on pypi
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
verification and default Ray smoke/two-worker tests remain here. Uploads run
directly in this repository's `VaneExtension.yml`; PyPI does not support a reusable workflow
as the Trusted Publisher. The existing development `testpypi` environment and
Trusted Publisher remain unchanged.

The current shared native integration uses manifest schema 2, requiring an
explicit official vcpkg revision. Paimon's `vane-extension.toml` keeps the same
revision as its existing `vcpkg.json`; this update changes the Vane pin without
changing native dependency versions or development package versioning.

## Development and production channels

`VaneExtension.yml` has three explicit operations:

| Operation | Runtime source | Native signing key | Upload destination |
| --- | --- | --- | --- |
| `build-only` (default; also push/PR CI) | Locally built Vane v0.2.0 source pin | Public CI fixture key | GitHub CI artifacts only |
| `testpypi-dev` | Exact development `vane-ai` wheels from TestPyPI | `astrovela/vane-testpypi` | TestPyPI only |
| `release` | Exact non-development `vane-ai` wheels from PyPI | `astrovela/vane` | TestPyPI, qualification, approval, then identical files to PyPI |

Both manifests pin Vane v0.2.0 at
`79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`. Build-only CI enables the public
CI test key in its locally built runtime and packages a matching runtime/provider
set. These are test artifacts even though the runtime reports `0.2.0`; do not
mix them with the PyPI runtime or publish them.

Production qualification uses the exact PyPI runtime wheels and production
signer. The preflight checks the complete runtime matrix and production-key
ancestry before native builds or signing. Updating the pin does not publish the
provider or establish production qualification.

Use `build-only` for PRs and `release` for production. With the stable pin,
`testpypi-dev` deliberately rejects `0.2.0`; a future development publication
requires a separately reviewed pin to its exact TestPyPI runtime. Ordinary
TestPyPI dev runtimes trust the dedicated TestPyPI key, not the public CI key.

Before the first production release:

1. Configure the `production-signing` GitHub environment to permit only protected
   `main_vane`, with required reviewers, and store the production private key in
   its `VANE_EXTENSION_SIGNING_PRIVATE_KEY` secret. Do not use the TestPyPI key or
   a repository-wide secret. The expected production public DER SHA-256 is
   `8729fbfbf5276be4b159c0b698c9e4214edd72eaad3e21bcefc03bcb36dffaeb`.
2. Configure the `pypi` environment with required reviewers, prevent self-review,
   and allow only protected `main_vane`. Register `vane-extension-paimon` on PyPI
   with owner `AstroVela`, repository `duckdb-paimon`, workflow `VaneExtension.yml`,
   environment `pypi`. Keep the existing TestPyPI registration and `testpypi`
   environment for staging. Restrict that environment to protected `main_vane` too.
3. Manually dispatch `VaneExtension.yml` on `main_vane`, operation `release`.
   No provider tag is required or created. Approve signing only after reviewing
   the exact source pin, then approve `pypi` only after qualification succeeds.

Both publishing operations use three isolated jobs. A key-free `prepare` phase
builds Paimon once, audits its native dependencies and uploads only the unsigned
`artifacts/paimon.duckdb_extension` plus the flat `licenses/paimon/` text bundle.
An environment-protected signer downloads that original artifact by ID, reads
the committed manifest itself, and uses only system Python (`-I -S`), its standard
library and system OpenSSL. It does not install dependencies or load native data.
It checks the selected public-key fingerprint, signs the fixed bounded native
file as data using the exact reviewed Vane utility, and removes its mode-600
temporary private key even on failure. Only the signed native file is uploaded.

A fresh key-free `package` job downloads both original artifact IDs, verifies
that signing changed only the signature footer, then packages the complete wheel
matrix against exact indexed runtime wheels. It reuses the prepared licenses,
without rebuilding or signing, and runs Vane's native signature, SourceID,
platform and dependency checks. Production explicitly disables both testing
trust roots. No mutable build, pip or native-verification dependency runs in a
job with the private key; the private key is never an artifact. The existing
CI-only `full` path can use only the public CI fixture key. A missing production
secret cannot select the development secret.

Those same wheels are staged on TestPyPI. Both smoke and two-worker default Ray
tests download the staged provider, compare its bytes to the digest-verified candidate
artifact, and use the production runtime from PyPI. No mixed-index resolver or
runtime fallback is used. Ordinary runtime dependencies still come from PyPI.

Only after both tests succeed does the `pypi` approval-gated verification job
start. After approval, the shared `verify-promotion` gate rechecks the complete
staged filenames and SHA-256 hashes and rejects conflicting PyPI files. This job
has read-only permissions and cannot obtain publishing OIDC tokens.

The dependent publisher is also gated by the `pypi` environment. It only downloads
the original digest-verified candidate artifact and invokes the pinned PyPI
publishing action. No repository checkout, Python tooling installation or release
validator runs in the OIDC-authorized publisher. No rebuild, resigning or version
rewrite occurs; GitHub may request another approval for that publisher job. A
separate read-only `verify-index --index pypi` job checks the
complete indexed matrix after upload. Identical partial uploads can be retried;
different files under an existing version fail closed.

Environment protection, secrets and Trusted Publisher registration are explicit
administrator setup steps; merging this preparation does not configure them or
trigger a release. A `build-only` run exercises the guarded tooling and existing
development CI, not a production publication or production-native qualification.

## Default Ray qualification

Both manifests pin Vane v0.2.0,
`79049f382ba6ee79d035c09cc8b5d3538e5bbe6a`.
The smoke and distributed suites require `VANE_RUNNER` to be absent and verify
Ray dispatch without a runner selection API. Fixtures and readback use that
same default runner. A test-owned two-worker cluster controls resources only.
The duplicate CTAS race pauses the Ray driver after physical planning, creates
the competing table through client SQL, and must fail before worker submission.
Duplicate-attempt validation corrupts one real Ray task's native identity so
two tasks produce valid envelopes for different attempts of one logical task.
It checks coordinator rejection, cleanup and successful explicit retry.
Paimon qualification currently uses filesystem tables; it does not claim S3 or
MinIO coverage.

`paimon_snapshots()` resolves completed snapshot rows during binding and sends
one portable scan to Ray. Reusing a bound plan preserves its snapshot rows;
creating a new query observes current metadata. Neither worker retries nor
serialization retain catalog pointers or credentials. Tables without UUIDs can
be read, but distributed writes reject them before committing any data.
