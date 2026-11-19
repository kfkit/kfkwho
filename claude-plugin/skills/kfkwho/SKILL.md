---
name: kfkwho
description: Adopt and operate kfkwho, a drop-in replacement for the StandardAuthorizer of Apache Kafka that publishes JMX metrics on who accesses what, per principal, client id, topic, operation and request type. Use when putting its jar on brokers (the apache/kafka image, a tarball, Strimzi), turning it on in a cluster that ran without an authorizer, adding its rules to the Prometheus JMX Exporter, choosing the series TTL, the series cap, the client id rules or the tags, finding who produces to or reads a topic, or working out why clients are denied, a broker does not start, or series are missing, reset or folded into `__other__`.
---

# Adopting kfkwho

kfkwho is a drop-in replacement for the `StandardAuthorizer` of Apache Kafka® brokers. The verdict
on every request stays the `StandardAuthorizer`'s; kfkwho adds JMX metrics on who accesses what,
per principal, client id, resource, operation and request type, and rules for the Prometheus JMX
Exporter to read them. The question it answers first is who produces to which topic.

The project's documents are the source of truth; this skill is the order to do things in and the
mistakes to avoid. Read the section a step names before acting on it, and quote it to the user
rather than this file when the two differ:

- Install and remove ("the guide" below): https://github.com/kfkit/kfkwho/blob/main/docs/install.md
- Settings: https://github.com/kfkit/kfkwho/blob/main/docs/config.md
- MBeans, Prometheus series, how to read them: https://github.com/kfkit/kfkwho/blob/main/docs/metrics.md
- What it costs per request: https://github.com/kfkit/kfkwho/blob/main/docs/performance.md
- Another authorizer publishing the same metrics: https://github.com/kfkit/kfkwho/blob/main/docs/embedding.md
- The exporter rules: https://github.com/kfkit/kfkwho/blob/main/jmx-exporter/kfkwho.yml
- What changed between releases: https://github.com/kfkit/kfkwho/blob/main/CHANGELOG.md
- Status and compatibility: https://github.com/kfkit/kfkwho/blob/main/README.md

## Say this first

**A cluster that runs without an authorizer starts enforcing ACLs the moment it gets this one.**
With no ACLs, every request of a principal that is not a super user is denied. Ask whether the
cluster has an authorizer today before anything else. To observe without enforcing,
`allow.everyone.if.no.acl.found=true` goes in with the class name, and the principals the brokers
and controllers use between themselves go in `super.users` (ACLs under "Before you start").

kfkwho observes: it never changes a verdict. Before a production cluster, read the status line at
the top of the README to the user: it says where the jar has run and where it has not.

## Before installing, find out

1. **Whether the cluster runs an authorizer today.** With `StandardAuthorizer`, only the class
   name changes: its ACLs, `super.users` and `allow.everyone.if.no.acl.found` work as before.
   With none, see above. An authorizer that decides for itself does not switch to this class: it
   publishes the same metrics through `AccessRecorder`, see the embedding page.
2. **The Kafka release and the mode.** 4.1 or later, KRaft; earlier releases are not tested
   ("Compatibility" in the README).
3. **How the brokers are run**: the `apache/kafka` image, the release tarball, or Strimzi. The
   guide has a section for each. Only the `apache/kafka` steps are run by CI; say so when the
   user's way is one of the other two.
4. **Which nodes are dedicated controllers.** The authorizer is set on brokers and controllers
   alike, as for `StandardAuthorizer`, but only a node whose `process.roles` include `broker`
   publishes metrics ("Brokers only").
5. **Whether the brokers already run a JMX Exporter agent.** Strimzi does. Then the kfkwho rule
   goes into that agent's configuration; no second agent is started.
6. **What the client ids look like.** A per-instance part, a pod name, a UUID, a thread number,
   makes every restart a new series. The default rules strip a trailing number and a UUID suffix;
   Kafka Streams threads need a rule of their own ("Client id rules").
7. **How many applications and topics there are, and how rarely the rarest client shows up**, a
   nightly job say. They size the cap and the TTL ("Cardinality").

## Install

Every way of running a broker takes the same four things: the jar on the broker's classpath,
`authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer`, the JMX Exporter agent with
the rules in `jmx-exporter/kfkwho.yml`, and a rolling restart, one node at a time.

1. Take the jar from the releases page and check it before it goes on a broker: its SHA-256 and
   its cosign signature, with the commands under "Before you start". The agent to download is
   named there too.
2. Settle the ACL question (above) and choose the `kfkwho.*` settings, all optional.
3. Follow the section for the user's brokers: "The apache/kafka image", "A broker from the
   tarball" or "Strimzi". On Strimzi the jar goes in an image built on top of Strimzi's, and
   `supportsAdminApi: true` keeps the User Operator working.
4. Restart node by node, controllers included.
5. Scrape port 9404 of every broker ("Prometheus" in the guide). Then check one broker by hand
   after some traffic: its `/metrics` has lines that start with `kfkwho_access_requests_total`.
6. "Grafana" in the guide says whether the dashboard has landed. Until it has, the queries under
   "Prometheus" in the metrics reference, who produces to a topic first, work in Explore.

To take it out, see "Removing it": the class name goes back and the nodes restart one by one.
ACLs are in the cluster metadata, not in kfkwho, and stay as they are.

## Reading the series

"Reading the series" in the metrics reference has the table of what each kind of client leaves.

- A producer of a topic is `resource_type="TOPIC"`, `operation="WRITE"`, `api="PRODUCE"`.
- `DESCRIBE` with `api="METADATA"` is a client that only fetched metadata: it has not produced.
- A consumer is `READ` with `api="FETCH"` on the topic, plus `GROUP` series whose `resource` is
  the group id, which links client ids to consumer groups.
- `result="DENIED"` shows who was denied what.
- The shipped rules are a first, minimal version: they export `kfkwho_access_requests_total` and
  nothing else. The client series, `last-seen-ms` and the bookkeeping MBean `kfkwho:type=series`
  are in JMX only ("Attributes" lists what is exported).

## The settings, and what they trade

- **`kfkwho.series.ttl.seconds`** (600). An idle series is removed after it; seen again, it
  starts from zero, which `rate()` and `increase()` read as a counter reset. A shorter TTL
  forgets a client that comes back rarely, a nightly job say; longer than the job's period
  keeps it in view; longer than the scrape interval is enough for `rate()`.
- **`kfkwho.max.series`** (10000 of each type). The hard bound. Past it a new key is recorded
  into `__other__`, and who those clients are is lost for as long as the cap stays reached.
  Watch `access-series-count` against it and `access-series-overflow-total` for growth; raise
  it, or find what multiplies the series, before overflow becomes the normal state.
- **`kfkwho.client.id.rules`**. A rule that strips the per-instance part makes every instance
  of an application one series. Setting the key replaces the defaults, so copy the ones to
  keep; an empty value keeps client ids as sent.
- **`kfkwho.tags`**. `client-address` is off by default: its cardinality is that of the
  clients' addresses. Leave it off unless clients are few and their addresses stable.
- **`kfkwho.resource.exclude`** (`__.*`). A resource whose whole name matches is not recorded
  at all: by default the internal topics. Add topics nobody needs to see, temporary ones of
  tests or tools.
- **The cost per request** is measured, not guessed: "Against the budget" in the performance
  page states the budget and where the figures stand.

## Do not

- **Do not turn it on in a cluster without an authorizer** before
  `allow.everyone.if.no.acl.found` and `super.users` are decided: with no ACLs, every request
  of a principal that is not a super user is denied.
- **Do not give the `apache/kafka` image the kfkwho variables alone.** The image applies its
  built-in single-node defaults only when no `KAFKA_*` variable is set: they go next to a full
  configuration, not instead of one.
- **Do not start a second JMX Exporter agent** on a broker that already runs one. Add the rule
  to its configuration, and leave out the `includeObjectNames` of `jmx-exporter/kfkwho.yml`,
  which would hide every other MBean.
- **Do not guess a `kfkwho.*` value.** A wrong one fails broker start with a `ConfigException`
  that names the key. A client id rule that refers to a group its pattern does not have fails
  broker start too.
- **Do not put a comma in the pattern of a client id rule** (write `\d+`, not `\d{1,3}`):
  entries are separated by commas. In a `.properties` file a backslash is written twice.
- **Do not rely on `kfkwho.count.denied`, or on leaving out a tag** other than `listener`,
  `security-protocol` and `client-address`. They are validated and not applied yet.
- **Do not read a dedicated controller without `kfkwho` MBeans as broken.** It passes the
  verdict through and publishes nothing.
- **Do not wrap `StandardAuthorizer`** in an authorizer of your own. The broker feeds ACLs only
  to a `ClusterMetadataAuthorizer`; a wrapper gets none and denies everything without a word.
  Extend `StandardAuthorizer` and hold an `AccessRecorder` ("Embedding").
- **Do not upgrade without reading "Breaking" in the changelog.** Until 1.0 a setting or a
  metric may still be renamed, and every such change is listed there.

## When something is off

There is no troubleshooting page; each symptom has a section that explains it.

- Clients are denied right after the restart: the cluster ran without an authorizer and now
  enforces ACLs. See the ACLs under "Before you start".
- A broker does not start, with a `ConfigException`: it names the key. The table in the
  configuration page has each key's type, default and bounds.
- A node has no `kfkwho` lines: it is a dedicated controller ("Brokers only"), or no request
  came yet, since a series is created on the first request with its key. Internal topics are
  excluded by default.
- The exporter's other metrics are gone: `includeObjectNames` came along with the rule.
- A series whose labels are all `__other__` grows: the cap is reached, and new clients are not
  told apart. See "`__other__`" and "Cardinality".
- A counter starts from zero again, or a series is gone: it was idle for the TTL.
- A new series on every restart of one application: its client id has a per-instance part the
  rules do not strip. Write a rule ("Client id rules").
- A `client_id` is not what the client sends: the rules rewrote it, or it was null or empty and
  became `unknown`. A value longer than 256 characters is cut and ends in a hash. The label of
  the `*` wildcard topic is `\*`. See "Tag values" and "Prometheus" in the metrics reference.
