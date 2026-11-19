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

The same jar then goes to Maven Central as `dev.kfkit:kfkwho:X.Y.Z`. No
workflow does that: it is the maintainer's step, [below](#maven-central).

## The version

The build takes the version from `-Pversion`, which the workflow sets to the
tag without its `v`. It accepts only `X.Y.Z`: a `SNAPSHOT`, a suffix or a
leading `v` fails the build, and so does a tag that is not `vX.Y.Z`. Without
`-Pversion` the build is the development version, `0.2.0-SNAPSHOT` in
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
6. Put the release on [Maven Central](#maven-central): the jar of step 4, the
   POM, the sources and the javadoc, each with its signature.

A failed run publishes no release. Fix the cause on `main`, delete the tag
(`git push origin :refs/tags/vX.Y.Z`, `git tag -d vX.Y.Z`) and tag again, or
release the next patch version. Once a version is on Maven Central only the
next patch version is left: Central replaces and removes nothing.

## Maven Central

The maintainer uploads each release through the
[Central Portal](https://central.sonatype.com), by hand. The release
workflow has no job for it, and neither the portal's token nor the OpenPGP
key is a secret of the repository.

| File | What |
|---|---|
| `kfkwho-X.Y.Z.jar` | the jar of the GitHub release, byte for byte: the one downloaded and checked in step 4, not one built again |
| `kfkwho-X.Y.Z.pom` | the POM, built from the tag |
| `kfkwho-X.Y.Z-sources.jar` | the sources, built from the tag |
| `kfkwho-X.Y.Z-javadoc.jar` | the javadoc, built from the tag |

Each of the four goes up with its `.asc`, a detached signature made with the
OpenPGP key `A4B825D253DB40AB` ("kfkit releases", on keyserver.ubuntu.com),
and with the checksums Central asks for. Gradle's module metadata
(`kfkwho-X.Y.Z.module`) is not among them, and the build does not produce
it: it would name the size and the hashes of a jar built here, not of the
released one.

The POM, the sources and the javadoc come from the tag:

```bash
git checkout vX.Y.Z
./gradlew publishToMavenLocal -Pversion=X.Y.Z -Dmaven.repo.local=/tmp/kfkwho-m2
ls /tmp/kfkwho-m2/dev/kfkit/kfkwho/X.Y.Z/
```

The directory also holds the `kfkwho-X.Y.Z.jar` this build made. It is not
the one to upload: put the released jar in its place, and check it against
`kfkwho-X.Y.Z.jar.sha256` once more after that. Sign the four files, add
the checksums, pack the directory into a zip under its path,
`dev/kfkit/kfkwho/X.Y.Z/`, upload it, let the portal validate it, and only
then publish.

How a user checks what is there is in
[provenance](provenance/README.md#from-maven-central).
