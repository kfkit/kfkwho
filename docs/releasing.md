# Releasing

A signed tag `vX.Y.Z` on `main` is a release. The `release` workflow
(`.github/workflows/release.yml`) builds and tests the jar at that version,
signs it and publishes a GitHub release with three files:

| File | What |
|---|---|
| `kfkwho-X.Y.Z.jar` | the jar, built with `./gradlew build -Pversion=X.Y.Z` |
| `kfkwho-X.Y.Z.jar.sha256` | its SHA-256, for `sha256sum -c` |
| `kfkwho-X.Y.Z.jar.sigstore.json` | the cosign bundle: signature, certificate and transparency-log entry |

The signature is keyless: the workflow signs with a short-lived certificate
that Sigstore issues to its GitHub OIDC identity, and logs it in Rekor. No
key is kept anywhere.

## The version

The build takes the version from `-Pversion`, which the workflow sets to the
tag without its `v`. It accepts only `X.Y.Z`: a `SNAPSHOT`, a suffix or a
leading `v` fails the build, and so does a tag that is not `vX.Y.Z`. Without
`-Pversion` the build is the development version, `0.1.0-SNAPSHOT` in
`build.gradle.kts`.

## Cutting a release

1. A `chore: vX.Y.Z` pull request:
   - `CHANGELOG.md`: `## Unreleased` becomes `## X.Y.Z - YYYY-MM-DD`, with a
     new empty `## Unreleased` above it;
   - `README.md`: the status line, and any jar name or version it shows;
   - `docs/install.md`: the jar names in the examples;
   - `build.gradle.kts`: the development version moves to the next one.
2. After it is merged, tag the squash commit on `main` and push the tag:

   ```bash
   git fetch origin
   git tag -s vX.Y.Z -m vX.Y.Z origin/main
   git push origin vX.Y.Z
   ```

3. Watch the `release` run. It fails, and publishes nothing, if the tag is
   not `vX.Y.Z`, the build or a test fails, or the signature does not verify
   with the command below.
4. Check the published files from another machine:

   ```bash
   gh release download vX.Y.Z --repo kfkit/kfkwho
   sha256sum -c kfkwho-X.Y.Z.jar.sha256
   cosign verify-blob kfkwho-X.Y.Z.jar \
     --bundle kfkwho-X.Y.Z.jar.sigstore.json \
     --certificate-identity-regexp '^https://github.com/kfkit/kfkwho/' \
     --certificate-oidc-issuer https://token.actions.githubusercontent.com
   ```

5. Add the release to `docs/provenance/release-hashes.txt` and, for the first
   release, timestamp the file; see [provenance](provenance/README.md).

A failed run publishes no release. Fix the cause on `main`, delete the tag
(`git push origin :refs/tags/vX.Y.Z`, `git tag -d vX.Y.Z`) and tag again, or
release the next patch version.
