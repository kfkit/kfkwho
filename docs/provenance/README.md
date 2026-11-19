# Provenance

`release-hashes.txt` lists the signed tags of the first and the current
release with their commit, the SHA-256 of the source archive and the SHA-256
of the released jar. `release-hashes.txt.ots`, once it exists, is an
[OpenTimestamps](https://opentimestamps.org) proof that the file, and so the
code and the jar it names, existed no later than the Bitcoin block it is
attached to. The commit dates, the signatures on the tags and the cosign
signatures on the jars, logged in Rekor, are the earlier evidence; the proof
does not depend on any of them, nor on GitHub.

The first release, v0.1.0, and the current one, v0.1.1, are in the file;
there is no proof yet. The maintainer adds it, as below.

## Adding a release

For the tag `vX.Y.Z`, with git 2.55.0 or the version the file names:

```bash
git fetch origin --tags
tag=vX.Y.Z
echo "$tag tag=$(git rev-parse "$tag") commit=$(git rev-parse "$tag^{commit}")" \
  "archive_sha256=$(git archive --format=tar "$tag" | shasum -a 256 | cut -d' ' -f1)" \
  "jar_sha256=$(gh release download "$tag" --repo kfkit/kfkwho --pattern '*.jar.sha256' --output - | cut -d' ' -f1)"
```

Append the line to `release-hashes.txt`, keeping the first release's line and
replacing the previous current one, and fill in the first release's URL and
date once.

## Timestamping

After the first release, and again whenever the file should be re-attested:

```bash
uvx --from opentimestamps-client ots stamp docs/provenance/release-hashes.txt
# a few hours later, once the calendars have attached it to a block:
uvx --from opentimestamps-client ots upgrade docs/provenance/release-hashes.txt.ots
```

Commit `release-hashes.txt` and `release-hashes.txt.ots` together, in a
`docs:` pull request, and name the Bitcoin block(s) the proof is attested in
here.

## Verifying

`ots verify` checks the block headers against a local Bitcoin Core node;
without one, drop the two files into https://opentimestamps.org, or read the
proof and compare the merkle roots it names with any block explorer:

```bash
uvx --from opentimestamps-client ots info docs/provenance/release-hashes.txt.ots
```

Check that the hashes still match the tags and the jars:

```bash
git rev-parse vX.Y.Z vX.Y.Z^{commit}
git archive --format=tar vX.Y.Z | shasum -a 256
shasum -a 256 kfkwho-X.Y.Z.jar
```

## From Maven Central

From 0.1.1 the jar of each release is also on Maven Central as
`dev.kfkit:kfkwho`, the same file byte for byte: the SHA-256 of the release
checks it, and so does the cosign signature, with the commands of
[Installation](../install.md#before-you-start). Every file there, the POM,
sources and javadoc included, is also signed with the OpenPGP key
`A4B825D253DB40AB`, "kfkit releases", which is on keyserver.ubuntu.com:

```bash
X=X.Y.Z
base="https://repo1.maven.org/maven2/dev/kfkit/kfkwho/$X"
curl -fsSLO "$base/kfkwho-$X.jar" -O "$base/kfkwho-$X.jar.asc"
gpg --keyserver keyserver.ubuntu.com --recv-keys A4B825D253DB40AB
gpg --verify "kfkwho-$X.jar.asc" "kfkwho-$X.jar"
```

`gpg` prints `Good signature from "kfkit releases"`.

The maintainer uploads these by hand after the GitHub release; what goes up
and how it is built is in [Releasing](../releasing.md#maven-central).
