# Embedding

Another authorizer can publish exactly the metrics kfkwho publishes: the
same MBeans, tags and `kfkwho.*` settings, so that `jmx-exporter/kfkwho.yml`
and the Grafana dashboard work with it unchanged. It holds an
`AccessRecorder`, from the package of `MeteredStandardAuthorizer`, and hands
it its own verdicts.

The recorder records whatever verdict it is handed. Whether that verdict is
the one `StandardAuthorizer` would have given is up to the authorizer that
holds it; `MeteredStandardAuthorizer`, the recorder's first user, hands it the
parent's and nothing else.

## Example

```java
public class RoleAuthorizer extends StandardAuthorizer {
    private AccessRecorder recorder;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        recorder = AccessRecorder.create(configs);  // throws ConfigException on a wrong kfkwho.* value
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext context, List<Action> actions) {
        List<AuthorizationResult> results = decide(context, actions);   // your verdicts
        recorder.record(context, actions, results);
        return results;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            recorder.close();
        }
    }
}
```

- **Extend `StandardAuthorizer`, don't wrap it.** The broker feeds ACLs only
  to a `ClusterMetadataAuthorizer`; a wrapper that implements `Authorizer`
  and delegates gets none, and denies everything without a word.
- **One recorder per authorizer, created in `configure`.** It registers the
  MBeans under the `kfkwho` domain and starts one daemon thread,
  `kfkwho-series`, which creates and expires series off the request thread.
  Two recorders in one JVM would register the same names. Loading the class
  does nothing; an authorizer that wants no metrics creates no recorder.
- **Only a broker records.** On a node whose `process.roles` does not include
  `broker`, `create` returns a recorder whose `record` does nothing: no
  MBean, no thread. Hand it the whole broker configuration, as `configure`
  received it.
- **`record` is the hot path.** Call it on the request thread with the
  verdicts you return, in the order of the actions. A series seen before
  costs one lookup per action and allocates nothing; see
  [Performance](performance.md).
- **`close` unregisters every MBean** and stops the thread; calling it twice
  is harmless.
- **Relocation is fine.** Nothing looks a class up by name and no
  `ServiceLoader` is involved: the classes can be shaded into another
  package. The JMX domain and the thread name are constants and stay
  `kfkwho`.

## What is stable

Once released, these change only with a major version, and until v1.0 only
with an entry under "Breaking" in the changelog:

- the public types and their public members, exactly this list
  (`ApiSurfaceTest` fails when the package gains one that is not here):
  - `AccessRecorder`
  - `AccessRecorder.create(Map)`
  - `AccessRecorder.record(AuthorizableRequestContext, List, List)`
  - `AccessRecorder.close()`
  - `MeteredStandardAuthorizer`
  - `MeteredStandardAuthorizer()`
  - `MeteredStandardAuthorizer.configure(Map)`
  - `MeteredStandardAuthorizer.authorize(AuthorizableRequestContext, List)`
  - `MeteredStandardAuthorizer.close()`
- the `kfkwho.*` settings, their meaning and defaults, in
  [Configuration](config.md);
- the MBean names, their tags in their order, and their attributes, in
  [Metrics](metrics.md).

What is not: everything package-private, which may change in any release,
and how often the thread looks for idle series. `MeteredStandardAuthorizer`
is public so that a broker can load it; extend `StandardAuthorizer`, not it.
