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
| `kfkwho.client.id.rules` | list | (empty) | Rules that rewrite a client id before it becomes a label, each `pattern=>replacement` in Java regex syntax; the first that matches applies. Entries are separated by commas, so a pattern cannot contain one (write `\d+`, not `\d{1,3}`). Validated; not applied yet ([#6](https://github.com/kfkit/kfkwho/issues/6)). |
| `kfkwho.resource.exclude` | string | `__.*` | Resources whose whole name matches this Java regex are not recorded: by default the internal topics `__consumer_offsets` and `__transaction_state`. Validated; not applied yet ([#6](https://github.com/kfkit/kfkwho/issues/6)). |
| `kfkwho.count.denied` | boolean | `true` | Whether denied requests are recorded as well as allowed ones. Validated; not applied yet. |

## Example

```properties
authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer
super.users=User:admin
kfkwho.ttl.seconds=900
kfkwho.max.series=20000
```
