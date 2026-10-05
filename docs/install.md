# Installation

From a running cluster to `kfkwho_*` series in Prometheus and Grafana. Three
ways to run a broker are covered: the `apache/kafka` image, a broker from
the release tarball, and Strimzi. Each takes the same four things:

1. the kfkwho jar on the broker's classpath;
2. `authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer`;
3. the Prometheus JMX Exporter agent with the rules in
   `jmx-exporter/kfkwho.yml`;
4. a rolling restart, one node at a time.

The `apache/kafka` steps are what the integration test (`BrokerIT`) does on
every supported broker release in CI. The tarball and Strimzi steps have not
been run by CI.

## Before you start

- **Kafka 4.1 or later, KRaft.** See [Compatibility](../README.md#compatibility).
- **The jar.** There is no release yet; build it with `./gradlew jar`, which
  writes `build/libs/kfkwho-<version>.jar`. It has no dependencies of its
  own.
- **The JMX Exporter agent.** `jmx_prometheus_javaagent-1.0.1.jar`, the
  version the integration test runs, from
  `https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/`.
- **ACLs.** kfkwho is the broker's `StandardAuthorizer` with metrics added;
  the verdict on every request is the `StandardAuthorizer`'s.
  - A cluster that already runs `StandardAuthorizer` changes only the class
    name; its ACLs, `super.users` and `allow.everyone.if.no.acl.found` work
    as before.
  - A cluster that runs **without an authorizer** starts enforcing ACLs the
    moment it gets one: with no ACLs, every request of a principal that is
    not a super user is denied. To observe without enforcing, set
    `allow.everyone.if.no.acl.found=true`, and put the principals the
    brokers and controllers use between themselves in `super.users`.
- **Every node.** Set the authorizer on brokers and controllers alike, as
  for `StandardAuthorizer`. A node that is not a broker passes the verdict
  through and publishes nothing; see [Brokers only](config.md#brokers-only).

The `kfkwho.*` settings, all optional, are in [Configuration](config.md).

## The apache/kafka image

Put the jar, the agent and the rules in the image, or mount them:

```dockerfile
FROM apache/kafka:4.1.2
COPY kfkwho-0.1.0-SNAPSHOT.jar /opt/kfkwho/kfkwho.jar
COPY jmx_prometheus_javaagent-1.0.1.jar /opt/kfkwho/jmx_prometheus_javaagent.jar
COPY jmx-exporter/kfkwho.yml /opt/kfkwho/kfkwho.yml
```

and add to the environment the broker already has:

```bash
CLASSPATH=/opt/kfkwho/kfkwho.jar
KAFKA_AUTHORIZER_CLASS_NAME=dev.kfkit.kfkwho.MeteredStandardAuthorizer
KAFKA_SUPER_USERS=User:admin
KAFKA_OPTS=-javaagent:/opt/kfkwho/jmx_prometheus_javaagent.jar=9404:/opt/kfkwho/kfkwho.yml
```

The image's start script adds `CLASSPATH` to the broker's classpath and
turns every `KAFKA_*` variable into a broker setting: `kfkwho.ttl.seconds`
is `KAFKA_KFKWHO_TTL_SECONDS`. Remember that the image applies its built-in
single-node defaults only when no `KAFKA_*` variable is set: the variables
above go next to a full configuration (`KAFKA_NODE_ID`,
`KAFKA_PROCESS_ROLES`, `KAFKA_LISTENERS`, ...), not instead of one. Publish
port 9404 to Prometheus.

## A broker from the tarball

On every node:

```bash
cp kfkwho-0.1.0-SNAPSHOT.jar "$KAFKA_HOME/libs/"
cat >> "$KAFKA_HOME/config/server.properties" <<'PROPERTIES'
authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer
super.users=User:admin
PROPERTIES
export KAFKA_OPTS="-javaagent:/opt/kfkwho/jmx_prometheus_javaagent-1.0.1.jar=9404:/opt/kfkwho/kfkwho.yml"
"$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties"
```

`kafka-run-class.sh` puts everything in `libs/` on the classpath. Where
`KAFKA_OPTS` is set depends on how the broker is started; in a systemd unit
it is an `Environment=` line. If the broker already runs a JMX Exporter
agent, add the rule from `jmx-exporter/kfkwho.yml` to its configuration
instead of starting a second agent, and leave out that file's
`includeObjectNames`, which would hide every other MBean.

## Strimzi

Strimzi runs its own images, so the jar goes in an image built on top of
one; the operator configures the authorizer and the exporter.

```dockerfile
FROM quay.io/strimzi/kafka:<strimzi-version>-kafka-<kafka-version>
USER root:root
COPY kfkwho-0.1.0-SNAPSHOT.jar /opt/kafka/libs/
USER 1001
```

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    image: registry.example.com/kafka-kfkwho:<tag>
    authorization:
      type: custom
      authorizerClass: dev.kfkit.kfkwho.MeteredStandardAuthorizer
      supportsAdminApi: true
      superUsers:
        - User:admin
    config:
      kfkwho.ttl.seconds: 900
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef:
          name: kafka-metrics
          key: kafka-metrics-config.yml
```

`supportsAdminApi: true` tells Strimzi the authorizer manages ACLs through
the Admin API, which `StandardAuthorizer` does, so the User Operator keeps
working. Strimzi already runs the JMX Exporter agent and exposes it on port
9404 of each pod; append the rule from `jmx-exporter/kfkwho.yml` to the
`rules` of the `kafka-metrics` ConfigMap, without its `includeObjectNames`.

## Prometheus

Scrape port 9404 of every broker:

```yaml
scrape_configs:
  - job_name: kfkwho
    static_configs:
      - targets: ["broker-1:9404", "broker-2:9404", "broker-3:9404"]
```

On Kubernetes, a `PodMonitor` for the broker pods' metrics port does the
same. Then check one broker by hand after some traffic:

```bash
curl -s http://broker-1:9404/metrics | grep '^kfkwho_access_requests_total'
```

## Grafana

Add the Prometheus above as a data source. The dashboard,
`grafana/kfkwho.json`, has not landed yet; until then the queries in
[Metrics](metrics.md#prometheus), who produces to a topic first, work in
Explore or in a panel of your own.

## Removing it

Set `authorizer.class.name` back to
`org.apache.kafka.metadata.authorizer.StandardAuthorizer`, or to nothing if
the cluster ran without an authorizer, and restart node by node. ACLs are
in the cluster metadata, not in kfkwho, and stay as they are.
