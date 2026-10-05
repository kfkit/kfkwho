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

Which series a producer, a consumer or an admin leaves, what each tag value
is made of, and how to keep their number bounded is in the
[metrics reference](docs/metrics.md).

## Usage

```bash
./gradlew jar                                       # build/libs/kfkwho-<version>.jar
cp build/libs/kfkwho-*.jar "$KAFKA_HOME/libs/"      # on every node
echo 'authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer' >> "$KAFKA_HOME/config/server.properties"
export KAFKA_OPTS="-javaagent:jmx_prometheus_javaagent-1.0.1.jar=9404:jmx-exporter/kfkwho.yml"   # then restart, node by node
curl -s localhost:9404/metrics | grep kfkwho_access_requests_total
```

A cluster that ran without an authorizer starts enforcing ACLs: read
[Installation](docs/install.md) first, which also covers the `apache/kafka`
image and Strimzi. Settings are in [Configuration](docs/config.md), the
MBeans and Prometheus series in [Metrics](docs/metrics.md).

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
