# Changelog

Changes that matter to someone running kfkwho. Until v1.0 the major version
is 0 and breaking changes are listed here instead of bumping it.

## Unreleased

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
