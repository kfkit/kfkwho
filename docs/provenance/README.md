# Provenance

`release-hashes.txt` lists the signed tags of the first and the current
release with their commit, the SHA-256 of the source archive and the SHA-256
of the released jar. `release-hashes.txt.ots`, once it exists, is an
[OpenTimestamps](https://opentimestamps.org) proof that the file, and so the
code and the jar it names, existed no later than the Bitcoin block it is
attached to. The commit dates, the signatures on the tags and the cosign
signatures on the jars, logged in Rekor, are the earlier evidence; the proof
does not depend on any of them, nor on GitHub.

There is no release yet, and so no proof. The maintainer adds both after the
first release, as below.

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
