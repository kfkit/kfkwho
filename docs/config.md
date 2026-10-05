# Configuration

kfkwho reads its settings from the broker configuration, next to
`authorizer.class.name`: in `server.properties`, or as `KAFKA_KFKWHO_…`
environment variables in the `apache/kafka` image. Every key is declared in
one `ConfigDef` (`AuthorizerConfig`); a wrong value fails broker start with a
`ConfigException` that names the key. Settings of the parent
`StandardAuthorizer` (`super.users`, `allow.everyone.if.no.acl.found`) work as
before.

`AuthorizerConfigTest` fails when this table and the `ConfigDef` disagree on
keys, types or defaults; change both together.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `kfkwho.ttl.seconds` | int | `600` | Seconds without a request after which a series is removed; seen again, it starts from zero. At least 1. |
| `kfkwho.max.series` | int | `10000` | Access series that may exist at once; past it, new ones are recorded into `__other__`. At least 1. |
| `kfkwho.labels` | list | `principal,client-id,resource-type,resource,operation,api,result,listener,security-protocol` | The labels series carry, a subset of `principal`, `client-id`, `resource-type`, `resource`, `operation`, `api`, `result`, `listener`, `security-protocol`, `client-address`. `client-address` is off by default: its cardinality is that of the clients' addresses. Validated; not applied yet ([#7](https://github.com/kfkit/kfkwho/issues/7)). |
| `kfkwho.client.id.rules` | list | `^(.+?)(-\d+)?-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$=>$1,^(.+)-\d+$=>$1` | Rules that rewrite a client id before it becomes a label, each `pattern=>replacement` in Java regex syntax; the first whose pattern matches the whole id applies, and an id no rule matches is kept. Entries are separated by commas, so a pattern cannot contain one (write `\d+`, not `\d{1,3}`). The default strips per-instance suffixes, see [Client id rules](#client-id-rules). |
| `kfkwho.resource.exclude` | string | `__.*` | Resources of any type whose whole name matches this Java regex are not recorded at all: by default the internal topics `__consumer_offsets` and `__transaction_state`. An empty value matches no resource. |
| `kfkwho.count.denied` | boolean | `true` | Whether denied requests are recorded as well as allowed ones. Validated; not applied yet. |

## Client id rules

Consumer and Streams clients put a per-instance suffix in their client id;
left as is, every restart is a new series. The default rules, in order:

| Rule | Turns | Into |
|---|---|---|
| `^(.+?)(-\d+)?-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$=>$1` | `consumer-orders-app-3-6f1c2a4e-1b2c-4d5e-8f90-abcdef123456` | `consumer-orders-app` |
| `^(.+)-\d+$=>$1` | `consumer-orders-app-3`, `producer-1`, `billing-1` | `consumer-orders-app`, `producer`, `billing` |

The first rule whose pattern matches the whole client id rewrites it; `$1`,
`$2`, `${name}` are its groups, `\$` a dollar sign. A rule that refers to a
group its pattern does not have fails broker start. An id no rule matches is
kept; a rule that rewrites an id to nothing gives `unknown`.

Setting the key replaces the defaults, so copy the ones you want to keep. To
fold Kafka Streams threads into the application, put a Streams rule first:

```properties
kfkwho.client.id.rules=^(.*)-StreamThread-\\d+-(consumer|producer)$=>$1,\
  ^(.+?)(-\\d+)?-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$=>$1,\
  ^(.+)-\\d+$=>$1
```

In a `.properties` file a backslash is written twice. To keep client ids as
sent, set the key to an empty value.

Rules run once per distinct client id, not per request: the result is
remembered for up to 10000 ids, after which the memory is emptied and fills
again. The same holds for whether a resource is excluded.

## Example

```properties
authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer
super.users=User:admin
kfkwho.ttl.seconds=900
kfkwho.max.series=20000
```
