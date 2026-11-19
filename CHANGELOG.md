# Changelog

Changes that matter to someone running kfkwho. Until v1.0 the major version
is 0 and breaking changes are listed here instead of bumping it.

## Unreleased

## 0.1.1 - 2026-11-19

### Added

- The jar of each release is on Maven Central as `dev.kfkit:kfkwho`, from
  0.1.1: the same file as on the releases page, with its POM, sources and
  javadoc, each signed with the OpenPGP key `A4B825D253DB40AB`. How to check
  it is in [Provenance](docs/provenance/README.md#from-maven-central).

## 0.1.0 - 2026-11-18

### Breaking

Nothing was released before these; they are listed so that anyone who built
the jar from `main` knows what to change.

- The bookkeeping MBean `kfkwho:type=authorizer` is now `kfkwho:type=series`.
- Its attributes `series-count`, `series-evicted-total` and
  `series-overflow-total` are now `access-series-count`,
  `access-series-evicted-total` and `access-series-overflow-total`, next to
  the `client-series-*` ones.
- The setting `kfkwho.labels` is now `kfkwho.tags`.
- The setting `kfkwho.ttl.seconds` is now `kfkwho.series.ttl.seconds`
  (`KAFKA_KFKWHO_SERIES_TTL_SECONDS` in the `apache/kafka` image).
- The `resource-type` tag carries the name of the broker's `ResourceType`,
  `TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`, where it was `topic`,
  `group`, `cluster`, `transactional-id`. Prometheus queries and dashboards
  that filter on `resource_type="topic"` need `resource_type="TOPIC"`.

### Added

- A release workflow: a tag `vX.Y.Z` publishes `kfkwho-X.Y.Z.jar` on the
  GitHub releases page with its SHA-256 and a cosign keyless signature
  (`.sigstore.json`); how to verify it is in `docs/install.md`, how to cut
  one in `docs/releasing.md`. The build takes a release version from
  `-Pversion=X.Y.Z` and refuses anything else.
- `AccessRecorder`, a public final class with which another authorizer
  publishes the same MBeans, tags and `kfkwho.*` settings from its own
  verdicts: `create(Map)`, `record(...)`, `close()`. What is stable is in
  `docs/embedding.md`. `MeteredStandardAuthorizer` now records through it,
  with no change in what it publishes.
