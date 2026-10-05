# Metrics

kfkwho publishes JMX MBeans in the `kfkwho` domain through the broker's own
metrics library and a `JmxReporter`, the way quota metrics are published.
Only brokers publish them; see [Brokers only](config.md#brokers-only). The
JMX Exporter rules in `jmx-exporter/kfkwho.yml` turn them into Prometheus
series.

`MetricsDocTest` fails when the two tables below and the MBeans the code
registers disagree on types, tags, their order or attributes, or when a
Prometheus name here is not one the rules produce; change both together.

## MBeans

The tag order is fixed: exporter rules depend on it, and changing it is a
breaking change.

| MBean | Tags, in order |
|---|---|
| `kfkwho:type=access` | `principal`, `client-id`, `resource-type`, `resource`, `operation`, `api`, `result` |
| `kfkwho:type=client` | `principal`, `client-id`, `listener`, `security-protocol`, `client-address` |
| `kfkwho:type=authorizer` | none |

- **access**: one MBean per principal, client id, resource, operation,
  request type and verdict, counted once per action the broker authorizes.
  One request may authorize several actions (a Produce to three topics is
  three); each is counted in its own series.
- **client**: one MBean per principal and client id, counted once per
  `authorize` call, which is about once per request that needs
  authorization. It answers who is connected without the resource
  dimension. `listener`, `security-protocol` and `client-address` are tags
  only when `kfkwho.labels` lists them; by default the first two are and
  `client-address` is not. A tag that is left out is not part of the
  series' identity: the requests of all its values are one series.
- **authorizer**: one MBean, kfkwho's own bookkeeping.

## Attributes

| MBean | Attribute | Prometheus | Meaning |
|---|---|---|---|
| `kfkwho:type=access` | `request-total` | `kfkwho_access_requests_total` | Actions authorized since the series was created. |
| `kfkwho:type=access` | `request-rate` | not exported | Actions per second, over the metrics library's default window (two samples of 30 s). |
| `kfkwho:type=access` | `last-seen-ms` | not exported | Epoch milliseconds of the latest action, by the broker's clock. |
| `kfkwho:type=client` | `request-total` | not exported | `authorize` calls since the series was created. |
| `kfkwho:type=client` | `request-rate` | not exported | `authorize` calls per second, same window. |
| `kfkwho:type=client` | `last-seen-ms` | not exported | Epoch milliseconds of the latest call. |
| `kfkwho:type=authorizer` | `series-count` | not exported | Access series that exist, `__other__` aside. |
| `kfkwho:type=authorizer` | `series-evicted-total` | not exported | Access series removed after the TTL. |
| `kfkwho:type=authorizer` | `series-overflow-total` | not exported | Actions recorded into an access `__other__` series. |
| `kfkwho:type=authorizer` | `client-series-count` | not exported | Client series that exist, `__other__` aside. |
| `kfkwho:type=authorizer` | `client-series-evicted-total` | not exported | Client series removed after the TTL. |
| `kfkwho:type=authorizer` | `client-series-overflow-total` | not exported | Calls recorded into the client `__other__` series. |

A series is created on the first request with its key, on a background
thread, and removed after `kfkwho.ttl.seconds` without one; seen again, it
starts from zero, which Prometheus' `rate()` and `increase()` read as a
counter reset. Requests that arrive while it is being created are counted
into it once it exists.

## Prometheus

The rules in `jmx-exporter/kfkwho.yml` are a first, minimal version: they
export `request-total` of the access series and nothing else; the full
rules are a separate issue. Each tag becomes a label of the same name with
`-` written as `_`:

```
kfkwho_access_requests_total{principal="User:alice",client_id="billing",resource_type="topic",resource="orders",operation="WRITE",api="PRODUCE",result="ALLOWED"} 10.0
```

The rules strip the quotes the `JmxReporter` puts around a value an
ObjectName does not allow bare (see [Quoted](#tag-values)); they do not undo
the escapes inside one, so the label value of the `*` wildcard topic is
`\*`.

Who produces to `orders`, per principal and client id:

```promql
sum by (principal, client_id) (
  rate(kfkwho_access_requests_total{resource_type="topic", resource="orders",
                                    operation="WRITE", api="PRODUCE", result="ALLOWED"}[5m]))
```

Who reads it, and who was denied anything in the last hour:

```promql
sum by (principal, client_id) (rate(kfkwho_access_requests_total{resource="orders", api="FETCH"}[5m]))
sum by (principal, client_id, resource, operation) (increase(kfkwho_access_requests_total{result="DENIED"}[1h])) > 0
```

## Reading the series

`api` is the name of the request's API key, as the broker's `ApiKeys` spell
it; an id the broker's client library does not know is `api=UNKNOWN`. What
a client looks like, by kind (`resource-type`, `operation`/`api`):

| Client | Series |
|---|---|
| Producer | `topic`, `WRITE`/`PRODUCE` |
| Idempotent producer | as above, plus `cluster`, `IDEMPOTENT_WRITE`/`INIT_PRODUCER_ID` |
| Transactional producer | as above, plus `topic`, `WRITE`/`ADD_PARTITIONS_TO_TXN` and `transactional-id`, `WRITE`/`INIT_PRODUCER_ID` |
| A client that only fetched metadata | `topic`, `DESCRIBE`/`METADATA`: it has not produced |
| Consumer | `topic`, `READ`/`FETCH`, plus `group`, `READ` with `OFFSET_COMMIT`, `JOIN_GROUP`, `HEARTBEAT` (`CONSUMER_GROUP_HEARTBEAT` for KIP-848 groups) |

For a `group` series the `resource` is the group id, which links client ids
to consumer groups. Nothing is filtered by client kind: an admin client's
`CREATE`/`CREATE_TOPICS` is counted the same way. `resource-type` is the
broker's `ResourceType` in lower case with `-` for `_`: `topic`, `group`,
`cluster`, `transactional-id`, `delegation-token`, `user`.

The principal is the whole `KafkaPrincipal`, type included: `User:alice`,
`User:CN=svc,OU=x,O=y` for an mTLS client, `User:ANONYMOUS`, or
`Service:billing` from a custom `KafkaPrincipalBuilder`. A cluster action is
`resource-type=cluster,resource=kafka-cluster`; a request for the `*`
wildcard topic is `resource-type=topic,resource="\*"`.

## Tag values

What clients send is reported as sent, with four exceptions.

- **Rewritten.** A `client-id` is first rewritten by the client id rules, by
  default stripping a per-instance suffix: `consumer-orders-app-3-<uuid>` and
  `billing-1` are `consumer-orders-app` and `billing`. See
  [Client id rules](config.md#client-id-rules). A resource whose name
  matches `kfkwho.resource.exclude` (by default `__.*`, the internal topics)
  is not recorded at all.
- **Missing.** A null or empty `client-id`, `resource` or `listener` is
  `unknown`. Both land in one series, together with a client that really
  calls itself `unknown`. (The `JmxReporter` drops an empty tag, which would
  shift the tags after it.)
- **Too long.** A `principal`, `client-id` or `resource` longer than 256
  characters (UTF-16 code units) is cut to at most 256: its beginning, `-`,
  and the first 12 hex digits of the SHA-256 of the whole value in UTF-8. Two
  long values with a common prefix stay two series, and the same value
  always gets the same tag. A surrogate pair is never split.
- **Quoted.** A value with characters an ObjectName does not allow bare
  (`:`, `,`, `=`, `*`, `"`, non-ASCII letters, ...) is quoted by the
  `JmxReporter`; `ObjectName.unquote` gives back the value. Nothing else is
  changed: whitespace, a trailing space included, and unicode are kept, so
  `платёжка 🚀 ` and `платёжка 🚀` are two clients.

## `__other__`

Past `kfkwho.max.series` series of a type, a new key is not given a series
of its own: it is recorded into that type's `__other__` series, whose tags
are all `__other__`. The access type has one per verdict, so
`result=ALLOWED` and `result=DENIED` stay apart; the client type has one.
The same happens, briefly, when more than 1024 new series are waiting to be
created at once. Every request recorded this way also counts in
`series-overflow-total` (`client-series-overflow-total`).

A non-zero overflow means some clients are not told apart: who they are is
lost for as long as the cap stays reached. Series that already exist keep
counting; a series freed by the TTL makes room for the next new key.

## Cardinality

The access series that exist at a time are the distinct combinations of
principal, client id, resource, operation, request type and verdict seen
within the TTL; the client series, those of principal, client id and the
selected connection tags. Each exported attribute is one Prometheus series
per MBean. Four settings bound it, all in [Configuration](config.md):

- **TTL** (`kfkwho.ttl.seconds`, 600): an idle series disappears. A shorter
  TTL forgets a client that comes back rarely, a nightly job say, and
  restarts its counter; longer than the scrape interval is enough for
  `rate()`, longer than the job's period keeps it in view.
- **Cap** (`kfkwho.max.series`, 10000 of each type): the hard bound. Watch
  `series-count` against it and `series-overflow-total` for growth; raise
  it, or find what multiplies the series, before overflow becomes the
  normal state.
- **Client id normalisation** (`kfkwho.client.id.rules`): the usual cause of
  churn is a client id with a per-instance part, a pod name, a UUID, a
  thread number. Write a rule that strips it, so that every instance of an
  application is one series.
- **Labels and exclusion** (`kfkwho.labels`, `kfkwho.resource.exclude`):
  leave `client-address` off unless clients are few and their addresses
  stable, and exclude topics nobody needs to see, temporary ones of tests
  or tools. Of the labels, `listener`, `security-protocol` and
  `client-address` apply to the client series today; dropping the others is
  validated and not applied yet.

As an order of magnitude: 50 applications, each with one client id after
normalisation, producing to or reading 10 topics, with metadata and group
requests alongside, make a few thousand access series; a client id that
changes on every restart multiplies that by the restarts within one TTL.
