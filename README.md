# kfkwho

Who talks to what on your Apache Kafka® cluster: a drop-in replacement for the
broker's `StandardAuthorizer` that publishes JMX metrics per principal, client
id, topic, operation and request type, with Prometheus JMX Exporter rules and a
Grafana dashboard to read them.

> Status: pre-alpha. The skeleton compiles and the plan is in the issues;
> nothing has run on a real cluster yet.

## Why

A broker knows, on every request, who sent it (principal, client id, address)
and what it touches (topic, group, operation). It never puts the two together
in a metric: quota metrics are per client with no topic, `BrokerTopicMetrics`
are per topic with no client, and client telemetry (KIP-714) needs new clients
and a reporter plugin. The question "which clients read this topic, and when
did they last show up?" is answered today by grepping request logs.

The question kfkwho answers first is **who produces to which topic**: the
producers of a topic are what you need to know before you change, move or
delete it. Consumers come right after, and admins are the same mechanism
applied to other operations; everything the broker authorizes is counted.

The authorizer is the one hook that sees both sides of every request, for
clients of any version, at no extra cost. `kfkwho` extends the standard one:
the verdict stays the broker's, the observation becomes a metric.

## What you get

- `dev.kfkit.kfkwho.MeteredStandardAuthorizer`: `StandardAuthorizer` plus
  metrics. Set `authorizer.class.name` and nothing else changes.
- Producers first: the `api` tag tells a client that really produced
  (`WRITE`, `api=PRODUCE`) from one that only fetched metadata about the topic
  (`DESCRIBE`, `api=METADATA`). Consumers and their groups are in the same
  series, see [Reading the series](#reading-the-series).
- MBeans with a fixed tag order, so exporter rules are one line:

      kfkwho:type=access,principal=…,client-id=…,resource-type=topic,resource=…,operation=…,api=…,result=ALLOWED
      kfkwho:type=client,principal=…,client-id=…,listener=…,security-protocol=…

- Cardinality under control: idle series expire, a hard cap folds the rest into
  `__other__`, client ids are normalised by rules you configure, internal
  topics are excluded.
- `jmx-exporter/kfkwho.yml` and `grafana/kfkwho.json`.

## Reading the series

`api` is the name of the request's API key, as the broker's `ApiKeys` spell
it; an id the broker's client library does not know is `api=UNKNOWN`. One
request may authorize several actions; each is its own series. What a client
looks like, by kind (`resource-type`, `operation`/`api`):

| Client | Series |
|---|---|
| Producer | `topic`, `WRITE`/`PRODUCE` |
| Idempotent producer | as above, plus `cluster`, `IDEMPOTENT_WRITE`/`INIT_PRODUCER_ID` |
| Transactional producer | as above, plus `topic`, `WRITE`/`ADD_PARTITIONS_TO_TXN` and `transactional-id`, `WRITE`/`INIT_PRODUCER_ID` |
| A client that only fetched metadata | `topic`, `DESCRIBE`/`METADATA`: it has not produced |
| Consumer | `topic`, `READ`/`FETCH`, plus `group`, `READ` with `OFFSET_COMMIT`, `JOIN_GROUP`, `HEARTBEAT` (`CONSUMER_GROUP_HEARTBEAT` for KIP-848 groups) |

For a `group` series the `resource` is the group id, which links client ids
to consumer groups. Nothing is filtered by client kind; this table moves to
the metrics reference once `docs/metrics.md` exists.

## Tag values

What clients send is reported as sent, with four exceptions. These rules
live here and nowhere else; they move to `docs/metrics.md` with the table
above.

- **Rewritten.** A `client-id` is first rewritten by the client id rules, by
  default stripping a per-instance suffix: `consumer-orders-app-3-<uuid>` and
  `billing-1` are `consumer-orders-app` and `billing`. See
  [Client id rules](docs/config.md#client-id-rules). A resource whose name
  matches `kfkwho.resource.exclude` (by default `__.*`, the internal topics)
  is not recorded at all.
- **Missing.** A null or empty `client-id` or `resource` is `unknown`. Both
  land in one series, together with a client that really calls itself
  `unknown`. (The `JmxReporter` drops an empty tag, which would shift the
  tags after it.)
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

The principal is the whole `KafkaPrincipal`, type included: `User:alice`,
`User:CN=svc,OU=x,O=y` for an mTLS client, `User:ANONYMOUS`, or
`Service:billing` from a custom `KafkaPrincipalBuilder`. A cluster action is
`resource-type=cluster,resource=kafka-cluster`; a request for the `*`
wildcard topic is `resource-type=topic,resource="\*"`.

## Usage

Not yet. Installation, configuration and the metrics reference land with the
first release; follow the [issues](https://github.com/kfkit/kfkwho/issues).

## Compatibility

Compiled against Kafka 4.3, Java 17. Kafka 4.1 and later is the target
(KRaft, `StandardAuthorizer`); earlier releases are not tested.

## Development

```bash
./gradlew check
./gradlew integrationTest -PbrokerVersion=4.1.2   # needs Docker; default: the compile version
./gradlew jmh                                     # the hot-path benchmarks, a few minutes
```

The integration test runs the built jar as the authorizer of an
`apache/kafka` broker with the JMX Exporter agent and the rules in
`jmx-exporter/`, and checks the exporter's output. Without Docker it is
skipped; `./gradlew check -PintegrationTest` makes it part of `check`. CI runs
it on every supported broker release. What the metrics cost per request is in
[docs/performance.md](docs/performance.md).

Rules for contributors, human or agent, are in [AGENTS.md](AGENTS.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Apache Kafka, Kafka, and the Kafka logo are trademarks of The Apache Software
Foundation. kfkwho is an independent project, not affiliated with or endorsed
by the ASF.
