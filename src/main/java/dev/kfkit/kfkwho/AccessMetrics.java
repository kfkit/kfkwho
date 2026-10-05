/*
 * Copyright 2026 Ivan Abramov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.kfkit.kfkwho;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import dev.kfkit.kfkwho.AuthorizerConfig.ClientIdRule;

/**
 * Two types of series, each its own {@link SeriesSet} with the TTL and cap
 * from the configuration.
 *
 * <p>Access: one series per principal, client id, resource, operation,
 * request type and verdict,
 * {@code kfkwho:type=access,principal=,client-id=,resource-type=,resource=,operation=,api=,result=}
 * with {@code request-total}, {@code request-rate} and {@code last-seen-ms},
 * counted once per action.
 *
 * <p>Client: one series per principal, client id and, as {@code kfkwho.labels}
 * selects them, listener, security protocol and client address,
 * {@code kfkwho:type=client,principal=,client-id=,listener=,security-protocol=,client-address=}
 * with the same three metrics, counted once per {@code authorize} call. It
 * answers who is connected without the resource dimension. A label that is
 * not selected is not a tag and not part of the key: the series of its
 * values are one.
 *
 * <p>The {@code api} tag is the name of the request's API key
 * ({@code PRODUCE}, {@code METADATA}, {@code INIT_PRODUCER_ID}, ...), which
 * tells a producer that wrote from one that only described the topic. Its
 * values are bounded by {@link ApiKeys}; an id the client library does not
 * know is reported as {@value #UNKNOWN_API}.
 *
 * <p>The tag order is fixed; exporter rules depend on it. The
 * {@code JmxReporter} quotes values that are not valid in an ObjectName.
 * What a tag value is made of (the rules are in docs/metrics.md, "Tag values"):
 * a null or empty client id, resource name or listener is {@value #UNKNOWN},
 * and counted together with a literal {@value #UNKNOWN}; anything else is kept
 * as sent, whitespace and unicode included; a principal, client id or
 * resource longer than {@value #MAX_TAG_LENGTH} characters is cut and ends
 * in a hash of the whole value, see {@link #truncate}.
 *
 * <p>Before a client id becomes a tag, the first client id rule whose
 * pattern matches all of it rewrites it; the result for each raw id is kept
 * in a {@link Memo} of at most {@value #MAX_MEMO} ids, so the rules run once
 * per distinct id, not per request. An action on a resource whose name
 * matches the exclude regex is not recorded at all; that is decided only
 * when no series is found, so a recorded resource costs nothing more.
 *
 * <p>A request whose series exist takes no lock of ours and allocates
 * nothing: one lookup for the client and one per action, with per-thread
 * probes that are copied only when a series is created.
 */
final class AccessMetrics {

    static final String JMX_PREFIX = "kfkwho";
    static final String GROUP = "access";
    static final String CLIENT_GROUP = "client";
    static final String SELF_GROUP = "authorizer";

    /**
     * A null or empty client id, resource name or listener. The
     * {@code JmxReporter} drops a tag whose value is empty, which would shift
     * the tags that follow.
     */
    static final String UNKNOWN = "unknown";

    /** The longest principal, client id or resource tag value, in UTF-16 chars, before JMX quoting. */
    static final int MAX_TAG_LENGTH = 256;

    /** Hex digits of the SHA-256 of the whole value that end a cut one. */
    private static final int HASH_LENGTH = 12;

    /** Every tag but the verdict of the series that takes what is over the cap. */
    static final String OTHER = "__other__";

    /** The {@code api} of a request type that is not an {@link ApiKeys} id. */
    static final String UNKNOWN_API = "UNKNOWN";

    /** Raw client ids, and resource names, whose rewrite or exclusion is remembered, at most. */
    static final int MAX_MEMO = 10_000;

    private final Metrics metrics;
    private final long ttlSeconds;
    /** Raw client id to tag value; null without rules. */
    private final Memo<String> clientIds;
    private final Memo<Boolean> excluded;
    private final boolean listener;
    private final boolean securityProtocol;
    private final boolean clientAddress;
    private final SeriesSet<Key> access;
    private final SeriesSet<ClientKey> clients;
    /** The lookup keys of each request thread; never stored in a {@link SeriesSet}. */
    private final ThreadLocal<Probes> probes = ThreadLocal.withInitial(Probes::new);

    /**
     * Under concurrent first sightings {@code maxSeries} may be overshot by
     * the number of request handler threads.
     *
     * @param labels the labels of {@code kfkwho.labels}; of them, this reads
     *        {@code listener}, {@code security-protocol} and {@code client-address}
     */
    AccessMetrics(Metrics metrics, long ttlSeconds, int maxSeries, List<String> labels,
            List<ClientIdRule> clientIdRules, Pattern resourceExclude, Executor creator) {
        this.metrics = metrics;
        this.ttlSeconds = ttlSeconds;
        this.clientIds = clientIdRules.isEmpty() ? null
                : new Memo<>(raw -> orUnknown(rewrite(clientIdRules, raw)), MAX_MEMO);
        this.excluded = new Memo<>(resource -> resourceExclude.matcher(resource).matches(), MAX_MEMO);
        this.listener = labels.contains("listener");
        this.securityProtocol = labels.contains("security-protocol");
        this.clientAddress = labels.contains("client-address");
        this.access = new SeriesSet<Key>(metrics, "", "Access", maxSeries, AuthorizationResult.values().length,
                creator, new SeriesSet.Sensors<Key>() {
                    @Override
                    public Sensor sensor(Key key) {
                        return accessSensor(key);
                    }

                    @Override
                    public Sensor overflow(int slot) {
                        return accessSensor(OTHER, OTHER, OTHER, OTHER, OTHER, OTHER,
                                AuthorizationResult.values()[slot]);
                    }
                });
        this.clients = new SeriesSet<ClientKey>(metrics, "client-", "Client", maxSeries, 1, creator,
                new SeriesSet.Sensors<ClientKey>() {
                    @Override
                    public Sensor sensor(ClientKey key) {
                        return clientSensor(key);
                    }

                    @Override
                    public Sensor overflow(int slot) {
                        return clientSensor(OTHER, OTHER, OTHER, OTHER, OTHER);
                    }
                });
    }

    void record(AuthorizableRequestContext context, List<Action> actions, List<AuthorizationResult> results,
            long nowMs) {
        Probes probes = this.probes.get();
        // Normalised before the lookup, so that values with one tag are one series.
        String clientId = clientId(context.clientId());
        ClientKey client = probes.client;
        client.set(context.principal(), clientId, listener ? orUnknown(context.listenerName()) : null,
                securityProtocol ? context.securityProtocol() : null, clientAddress ? context.clientAddress() : null);
        if (!clients.recordExisting(client, nowMs)) {
            clients.firstSight(client, 0, nowMs);
        }
        Key probe = probes.access;
        for (int i = 0; i < actions.size(); i++) {
            Action action = actions.get(i);
            AuthorizationResult result = results.get(i);
            String resource = orUnknown(action.resourcePattern().name());
            probe.set(context.principal(), clientId, action.resourcePattern().resourceType(), resource,
                    action.operation(), context.requestType(), result);
            if (!access.recordExisting(probe, nowMs) && !excluded.get(resource)) {
                access.firstSight(probe, result.ordinal(), nowMs);
            }
        }
    }

    /** The tag value of a raw client id: one lookup once its rewrite is known. */
    private String clientId(String raw) {
        String clientId = orUnknown(raw);
        // UNKNOWN stands for a missing id; no rule is meant for it.
        return clientIds == null || clientId == UNKNOWN ? clientId : clientIds.get(clientId);
    }

    /**
     * The first rule's rewrite, or the id as it came when none matches.
     * Called once per distinct raw id, not per request.
     */
    static String rewrite(List<ClientIdRule> rules, String clientId) {
        for (ClientIdRule rule : rules) {
            try {
                String rewritten = rule.rewrite(clientId);
                if (rewritten != null) {
                    return rewritten;
                }
            } catch (RuntimeException e) {
                // A replacement the validation let through; authorize must not throw.
            }
        }
        return clientId;
    }

    /** Removes the series of both types that saw no request for the TTL. */
    void expire() {
        access.expire();
        clients.expire();
    }

    /** No series is created after this returns; closing {@link Metrics} removes those that were. */
    void close() {
        access.close();
        clients.close();
    }

    /** The probes of one request thread. */
    private static final class Probes {
        final Key access = new Key();
        final ClientKey client = new ClientKey();
    }

    /**
     * The identity of an access series. The per-thread probe is set for
     * every action; a copy, never changed again, is what the set keeps.
     */
    private static final class Key implements SeriesSet.Key<Key> {
        private KafkaPrincipal principal;
        private String clientId;
        private ResourceType resourceType;
        private String resource;
        private AclOperation operation;
        private int api;
        private AuthorizationResult result;
        private int hash;

        void set(KafkaPrincipal principal, String clientId, ResourceType resourceType, String resource,
                AclOperation operation, int api, AuthorizationResult result) {
            this.principal = principal;
            this.clientId = clientId;
            this.resourceType = resourceType;
            this.resource = resource;
            this.operation = operation;
            this.api = api;
            this.result = result;
            int h = Objects.hashCode(principal);
            h = 31 * h + clientId.hashCode();
            h = 31 * h + resourceType.hashCode();
            h = 31 * h + resource.hashCode();
            h = 31 * h + operation.hashCode();
            h = 31 * h + api;
            this.hash = 31 * h + result.hashCode();
        }

        @Override
        public Key copy() {
            Key key = new Key();
            key.set(principal, clientId, resourceType, resource, operation, api, result);
            return key;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k && hash == k.hash && api == k.api && resourceType == k.resourceType
                    && operation == k.operation && result == k.result && resource.equals(k.resource)
                    && clientId.equals(k.clientId) && Objects.equals(principal, k.principal);
        }
    }

    /** The identity of a client series; a label that is not selected is null. */
    private static final class ClientKey implements SeriesSet.Key<ClientKey> {
        private KafkaPrincipal principal;
        private String clientId;
        private String listener;
        private SecurityProtocol securityProtocol;
        private InetAddress clientAddress;
        private int hash;

        void set(KafkaPrincipal principal, String clientId, String listener, SecurityProtocol securityProtocol,
                InetAddress clientAddress) {
            this.principal = principal;
            this.clientId = clientId;
            this.listener = listener;
            this.securityProtocol = securityProtocol;
            this.clientAddress = clientAddress;
            int h = Objects.hashCode(principal);
            h = 31 * h + clientId.hashCode();
            h = 31 * h + Objects.hashCode(listener);
            h = 31 * h + Objects.hashCode(securityProtocol);
            this.hash = 31 * h + Objects.hashCode(clientAddress);
        }

        @Override
        public ClientKey copy() {
            ClientKey key = new ClientKey();
            key.set(principal, clientId, listener, securityProtocol, clientAddress);
            return key;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ClientKey k && hash == k.hash && securityProtocol == k.securityProtocol
                    && clientId.equals(k.clientId) && Objects.equals(listener, k.listener)
                    && Objects.equals(principal, k.principal) && Objects.equals(clientAddress, k.clientAddress);
        }
    }

    private Sensor accessSensor(Key key) {
        return accessSensor(truncate(String.valueOf(key.principal)), truncate(key.clientId),
                key.resourceType.name().toLowerCase(Locale.ROOT).replace('_', '-'), truncate(key.resource),
                key.operation.name(), api(key.api), key.result);
    }

    private Sensor accessSensor(String principal, String clientId, String resourceType, String resource,
            String operation, String api, AuthorizationResult result) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("principal", principal);
        tags.put("client-id", clientId);
        tags.put("resource-type", resourceType);
        tags.put("resource", resource);
        tags.put("operation", operation);
        tags.put("api", api);
        tags.put("result", result.name());

        return sensor(GROUP, "Requests authorized", tags);
    }

    private Sensor clientSensor(ClientKey key) {
        return clientSensor(truncate(String.valueOf(key.principal)), truncate(key.clientId), key.listener,
                key.securityProtocol == null ? UNKNOWN : key.securityProtocol.name(),
                key.clientAddress == null ? UNKNOWN : key.clientAddress.getHostAddress());
    }

    /** Tags of labels that are not selected are left out, whatever is passed for them. */
    private Sensor clientSensor(String principal, String clientId, String listener, String securityProtocol,
            String clientAddress) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("principal", principal);
        tags.put("client-id", clientId);
        if (this.listener) {
            tags.put("listener", listener);
        }
        if (this.securityProtocol) {
            tags.put("security-protocol", securityProtocol);
        }
        if (this.clientAddress) {
            tags.put("client-address", clientAddress);
        }
        return sensor(CLIENT_GROUP, "Authorize calls", tags);
    }

    private Sensor sensor(String group, String what, Map<String, String> tags) {
        Sensor sensor = metrics.sensor(group + ":" + tags, null, ttlSeconds);
        sensor.add(metrics.metricName("request-total", group, what, tags), new CumulativeCount());
        sensor.add(metrics.metricName("request-rate", group, what + " per second", tags),
                new Rate(TimeUnit.SECONDS, new WindowedCount()));
        sensor.add(metrics.metricName("last-seen-ms", group, "Epoch milliseconds of the last request", tags),
                new Value());
        return sensor;
    }

    /** Called once per series, not per request. */
    static String api(int id) {
        // ApiKeys.forId throws on an id it does not know.
        return ApiKeys.hasId(id) ? ApiKeys.forId(id).name() : UNKNOWN_API;
    }

    /** On the request path: no allocation. */
    static String orUnknown(String value) {
        return value == null || value.isEmpty() ? UNKNOWN : value;
    }

    /**
     * A value of at most {@value #MAX_TAG_LENGTH} chars: one that is longer
     * keeps its beginning, then {@code -} and the first {@value #HASH_LENGTH}
     * hex digits of the SHA-256 of its UTF-8 bytes, so two long values that
     * share a prefix stay two series. A surrogate pair is never split.
     * Called once per series, not per request.
     */
    static String truncate(String value) {
        if (value.length() <= MAX_TAG_LENGTH) {
            return value;
        }
        int keep = MAX_TAG_LENGTH - HASH_LENGTH - 1;
        if (Character.isHighSurrogate(value.charAt(keep - 1))) {
            keep--;
        }
        byte[] hash = sha256().digest(value.getBytes(StandardCharsets.UTF_8));
        return value.substring(0, keep) + "-" + HexFormat.of().formatHex(hash, 0, HASH_LENGTH / 2);
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // Every Java platform is required to have it.
            throw new IllegalStateException(e);
        }
    }
}
