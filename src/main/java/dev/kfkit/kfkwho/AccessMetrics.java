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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.regex.Pattern;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import dev.kfkit.kfkwho.AuthorizerConfig.ClientIdRule;

/**
 * One series per principal, client id, resource, operation, request type and
 * verdict:
 * {@code kfkwho:type=access,principal=,client-id=,resource-type=,resource=,operation=,api=,result=}
 * with {@code request-total}, {@code request-rate} and {@code last-seen-ms}.
 *
 * <p>The {@code api} tag is the name of the request's API key
 * ({@code PRODUCE}, {@code METADATA}, {@code INIT_PRODUCER_ID}, ...), which
 * tells a producer that wrote from one that only described the topic. Its
 * values are bounded by {@link ApiKeys}; an id the client library does not
 * know is reported as {@value #UNKNOWN_API}.
 *
 * <p>The tag order is fixed; exporter rules depend on it. The
 * {@code JmxReporter} quotes values that are not valid in an ObjectName.
 * What a tag value is made of (the rules are in the README, "Tag values"):
 * a null or empty client id or resource name is {@value #UNKNOWN}, and
 * counted together with a literal {@value #UNKNOWN}; anything else is kept
 * as sent, whitespace and unicode included; a principal, client id or
 * resource longer than {@value #MAX_TAG_LENGTH} characters is cut and ends
 * in a hash of the whole value, see {@link #truncate}.
 *
 * <p>The number of series is bounded twice. A series that saw no request
 * for the TTL is removed by {@link #expire}; seen again, it starts from zero.
 * Once {@code maxSeries} series exist, new combinations are recorded into
 * one {@value #OTHER} series per verdict instead. Both are reported under
 * {@code kfkwho:type=authorizer}: {@code series-count},
 * {@code series-evicted-total} and {@code series-overflow-total}.
 *
 * <p>The request thread never creates a series: creating one registers
 * MBeans and costs tens of microseconds. On first sight of a key it leaves a
 * pending series and hands it to the {@code creator}; requests that arrive
 * before the series exists are counted in the pending series and recorded
 * into it once it is created, so each is counted once. At most
 * {@value #MAX_PENDING} series wait at a time; past that a new key is
 * recorded into {@value #OTHER}, as past the cap. A pending series counts
 * against the cap but not in {@code series-count}.
 *
 * <p>Before a client id becomes a tag, the first client id rule whose
 * pattern matches all of it rewrites it; the result for each raw id is kept
 * in a {@link Memo} of at most {@value #MAX_MEMO} ids, so the rules run once
 * per distinct id, not per request. An action on a resource whose name
 * matches the exclude regex is not recorded at all; that is decided only
 * when no series is found, so a recorded resource costs nothing more.
 *
 * <p>A request that finds its series takes no lock of ours and allocates
 * nothing: the lookup key is a per-thread {@link Key} that is copied only
 * when a series is created.
 */
final class AccessMetrics {

    static final String JMX_PREFIX = "kfkwho";
    static final String GROUP = "access";
    static final String SELF_GROUP = "authorizer";

    /**
     * A null or empty client id or resource name. The {@code JmxReporter}
     * drops a tag whose value is empty, which would shift the tags that follow.
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

    /** Series waiting for the creator, at most; a burst of new keys beyond it goes to {@value #OTHER}. */
    static final int MAX_PENDING = 1024;

    /** Raw client ids, and resource names, whose rewrite or exclusion is remembered, at most. */
    static final int MAX_MEMO = 10_000;

    private final Metrics metrics;
    private final long ttlSeconds;
    private final int maxSeries;
    private final Executor creator;
    /** Raw client id to tag value; null without rules. */
    private final Memo<String> clientIds;
    private final Memo<Boolean> excluded;
    private final Map<Key, Series> seriesByKey = new ConcurrentHashMap<>();
    /** The lookup key of each request thread; never stored in {@link #seriesByKey}. */
    private final ThreadLocal<Key> probes = ThreadLocal.withInitial(Key::new);
    private final Queue<Series> pending = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger();
    private final AtomicBoolean createScheduled = new AtomicBoolean();
    /** Guards creating a series against {@link #close}: none is registered after it. */
    private final Object createLock = new Object();
    private boolean closed;
    /** The {@value #OTHER} series, by verdict ordinal; created on the first overflow. */
    private final AtomicReferenceArray<Sensor> overflow =
            new AtomicReferenceArray<>(AuthorizationResult.values().length);
    private final Sensor evicted;
    private final Sensor overflowed;

    /**
     * Under concurrent first sightings {@code maxSeries} may be overshot by
     * the number of request handler threads.
     */
    AccessMetrics(Metrics metrics, long ttlSeconds, int maxSeries, List<ClientIdRule> clientIdRules,
            Pattern resourceExclude, Executor creator) {
        this.metrics = metrics;
        this.ttlSeconds = ttlSeconds;
        this.maxSeries = maxSeries;
        this.creator = creator;
        this.clientIds = clientIdRules.isEmpty() ? null
                : new Memo<>(raw -> orUnknown(rewrite(clientIdRules, raw)), MAX_MEMO);
        this.excluded = new Memo<>(resource -> resourceExclude.matcher(resource).matches(), MAX_MEMO);
        metrics.addMetric(metrics.metricName("series-count", SELF_GROUP, "Access series that exist, overflow aside"),
                (Measurable) (config, nowMs) -> Math.max(0, seriesByKey.size() - pendingCount.get()));
        evicted = metrics.sensor(SELF_GROUP + ":series-evicted");
        evicted.add(metrics.metricName("series-evicted-total", SELF_GROUP, "Access series removed after the TTL"),
                new CumulativeCount());
        overflowed = metrics.sensor(SELF_GROUP + ":series-overflow");
        overflowed.add(metrics.metricName("series-overflow-total", SELF_GROUP,
                "Requests recorded into the overflow series because the cap was reached"
                        + " or too many series were waiting to be created"), new CumulativeCount());
    }

    void record(AuthorizableRequestContext context, List<Action> actions, List<AuthorizationResult> results,
            long nowMs) {
        Key probe = probes.get();
        // Normalised before the lookup, so that values with one tag are one series.
        String clientId = clientId(context.clientId());
        for (int i = 0; i < actions.size(); i++) {
            Action action = actions.get(i);
            AuthorizationResult result = results.get(i);
            String resource = orUnknown(action.resourcePattern().name());
            probe.set(context.principal(), clientId, action.resourcePattern().resourceType(), resource,
                    action.operation(), context.requestType(), result);
            Series series = seriesByKey.get(probe);
            if (series != null) {
                series.record(nowMs);
            } else if (!excluded.get(resource)) {
                firstSight(probe, result, nowMs);
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

    /** On the request thread: leaves a pending series for the creator, or records into overflow. */
    private void firstSight(Key probe, AuthorizationResult result, long nowMs) {
        if (seriesByKey.size() >= maxSeries) {
            overflow(result, nowMs);
            return;
        }
        if (pendingCount.incrementAndGet() > MAX_PENDING) {
            pendingCount.decrementAndGet();
            overflow(result, nowMs);
            return;
        }
        Key key = probe.copy();
        Series series = new Series(key);
        Series raced = seriesByKey.putIfAbsent(key, series);
        if (raced != null) {
            pendingCount.decrementAndGet();
            raced.record(nowMs);
            return;
        }
        series.record(nowMs);
        pending.add(series);
        if (!createScheduled.get() && createScheduled.compareAndSet(false, true)) {
            try {
                creator.execute(this::createPending);
            } catch (RejectedExecutionException e) {
                // Closed; nothing will be created any more.
            }
        }
    }

    private void overflow(AuthorizationResult result, long nowMs) {
        overflow(result).record(nowMs, nowMs);
        overflowed.record(1, nowMs);
    }

    /**
     * On the creator: creates the pending series and records what each
     * counted meanwhile. A series queued after this started may be left to
     * the run its request scheduled, so that a stream of new keys cannot hold
     * the thread that also expires series.
     */
    void createPending() {
        createScheduled.set(false);
        Series series;
        for (int i = 0; i < MAX_PENDING && (series = pending.poll()) != null; i++) {
            try {
                synchronized (createLock) {
                    if (closed) {
                        return;
                    }
                    series.sensor = sensor(series.key);
                }
                series.flush();
            } catch (RuntimeException e) {
                // What it counted is lost; the next request for the key tries again.
                seriesByKey.remove(series.key, series);
            } finally {
                pendingCount.decrementAndGet();
            }
        }
    }

    /**
     * Removes the series, overflow included, that saw no request for the
     * TTL. A request racing with the removal of its series may be recorded
     * into the removed one and lost; that series had been idle for the TTL.
     * A pending series is left to the creator.
     */
    void expire() {
        for (Map.Entry<Key, Series> entry : seriesByKey.entrySet()) {
            Sensor sensor = entry.getValue().sensor;
            if (sensor != null && sensor.hasExpired() && seriesByKey.remove(entry.getKey(), entry.getValue())) {
                metrics.removeSensor(sensor.name());
                evicted.record(1);
            }
        }
        for (int i = 0; i < overflow.length(); i++) {
            Sensor sensor = overflow.get(i);
            if (sensor != null && sensor.hasExpired() && overflow.compareAndSet(i, sensor, null)) {
                metrics.removeSensor(sensor.name());
            }
        }
    }

    /** No series is created after this returns; closing {@link Metrics} removes those that were. */
    void close() {
        synchronized (createLock) {
            closed = true;
        }
        pending.clear();
    }

    /**
     * The identity of a series. The per-thread probe is set for every action;
     * a copy, never changed again, is what {@link #seriesByKey} keeps.
     */
    private static final class Key {
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

        Key copy() {
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

    /**
     * A series, created or pending. Until the creator sets {@link #sensor},
     * requests are counted in {@link #pending}; the creator then swaps in
     * {@link #CREATED} and records the count, so a request is counted either
     * there or directly into the sensor, never both and never neither.
     */
    private static final class Series {
        private static final long CREATED = Long.MIN_VALUE;

        final Key key;
        volatile Sensor sensor;
        private final AtomicLong pending = new AtomicLong();
        /** The time of the latest request counted in {@link #pending}. */
        private volatile long pendingMs;

        Series(Key key) {
            this.key = key;
        }

        void record(long nowMs) {
            Sensor s = sensor;
            if (s == null) {
                pendingMs = nowMs;
                for (long n = pending.get(); n != CREATED; n = pending.get()) {
                    if (pending.compareAndSet(n, n + 1)) {
                        return;
                    }
                }
                // Created between the two reads; the sensor was set before CREATED.
                s = sensor;
            }
            // CumulativeCount and WindowedCount count records; Value keeps the time.
            s.record(nowMs, nowMs);
        }

        /** After {@link #sensor} is set. */
        void flush() {
            long n = pending.getAndSet(CREATED);
            long ms = pendingMs;
            for (long i = 0; i < n; i++) {
                sensor.record(ms, ms);
            }
        }
    }

    private Sensor overflow(AuthorizationResult result) {
        int i = result.ordinal();
        Sensor sensor = overflow.get(i);
        if (sensor == null) {
            synchronized (overflow) {
                sensor = overflow.get(i);
                if (sensor == null) {
                    sensor = sensor(OTHER, OTHER, OTHER, OTHER, OTHER, OTHER, result);
                    overflow.set(i, sensor);
                }
            }
        }
        return sensor;
    }

    private Sensor sensor(Key key) {
        return sensor(truncate(String.valueOf(key.principal)), truncate(key.clientId),
                key.resourceType.name().toLowerCase(Locale.ROOT).replace('_', '-'), truncate(key.resource),
                key.operation.name(), api(key.api), key.result);
    }

    private Sensor sensor(String principal, String clientId, String resourceType, String resource, String operation,
            String api, AuthorizationResult result) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("principal", principal);
        tags.put("client-id", clientId);
        tags.put("resource-type", resourceType);
        tags.put("resource", resource);
        tags.put("operation", operation);
        tags.put("api", api);
        tags.put("result", result.name());

        Sensor sensor = metrics.sensor(GROUP + ":" + tags, null, ttlSeconds);
        sensor.add(metrics.metricName("request-total", GROUP, "Requests authorized", tags), new CumulativeCount());
        sensor.add(metrics.metricName("request-rate", GROUP, "Requests authorized per second", tags),
                new Rate(TimeUnit.SECONDS, new WindowedCount()));
        sensor.add(metrics.metricName("last-seen-ms", GROUP, "Epoch milliseconds of the last request", tags),
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
