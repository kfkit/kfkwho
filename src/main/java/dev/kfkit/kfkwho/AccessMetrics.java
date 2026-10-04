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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * One series per principal, client id, resource, operation and verdict:
 * {@code kfkwho:type=access,principal=,client-id=,resource-type=,resource=,operation=,result=}
 * with {@code request-total}, {@code request-rate} and {@code last-seen-ms}.
 *
 * <p>The tag order is fixed; exporter rules depend on it. The
 * {@code JmxReporter} quotes values that are not valid in an ObjectName.
 *
 * <p>The number of series is bounded twice. A series that saw no request
 * for the TTL is removed by {@link #expire}; seen again, it starts from zero.
 * Once {@code maxSeries} series exist, new combinations are recorded into
 * one {@value #OTHER} series per verdict instead. Both are reported under
 * {@code kfkwho:type=authorizer}: {@code series-count},
 * {@code series-evicted-total} and {@code series-overflow-total}.
 */
final class AccessMetrics {

    static final String JMX_PREFIX = "kfkwho";
    static final String GROUP = "access";
    static final String SELF_GROUP = "authorizer";

    /**
     * The {@code JmxReporter} drops a tag whose value is empty, which would
     * shift the tags that follow; an empty client id is reported as this.
     */
    static final String EMPTY = "-";

    /** Every tag but the verdict of the series that takes what is over the cap. */
    static final String OTHER = "__other__";

    static final long DEFAULT_TTL_SECONDS = 600;
    static final int DEFAULT_MAX_SERIES = 10_000;

    private record Key(KafkaPrincipal principal, String clientId, ResourceType resourceType, String resource,
            AclOperation operation, AuthorizationResult result) {
    }

    private final Metrics metrics;
    private final long ttlSeconds;
    private final int maxSeries;
    private final Map<Key, Sensor> sensors = new ConcurrentHashMap<>();
    /** The {@value #OTHER} series, by verdict ordinal; created on the first overflow. */
    private final AtomicReferenceArray<Sensor> overflow =
            new AtomicReferenceArray<>(AuthorizationResult.values().length);
    private final Sensor evicted;
    private final Sensor overflowed;

    /**
     * Under concurrent first sightings {@code maxSeries} may be overshot by
     * the number of request handler threads.
     */
    AccessMetrics(Metrics metrics, long ttlSeconds, int maxSeries) {
        this.metrics = metrics;
        this.ttlSeconds = ttlSeconds;
        this.maxSeries = maxSeries;
        metrics.addMetric(metrics.metricName("series-count", SELF_GROUP, "Access series that exist, overflow aside"),
                (Measurable) (config, nowMs) -> sensors.size());
        evicted = metrics.sensor(SELF_GROUP + ":series-evicted");
        evicted.add(metrics.metricName("series-evicted-total", SELF_GROUP, "Access series removed after the TTL"),
                new CumulativeCount());
        overflowed = metrics.sensor(SELF_GROUP + ":series-overflow");
        overflowed.add(metrics.metricName("series-overflow-total", SELF_GROUP,
                "Requests recorded into the overflow series because the cap was reached"), new CumulativeCount());
    }

    void record(AuthorizableRequestContext context, List<Action> actions, List<AuthorizationResult> results,
            long nowMs) {
        for (int i = 0; i < actions.size(); i++) {
            Action action = actions.get(i);
            AuthorizationResult result = results.get(i);
            Key key = new Key(context.principal(), context.clientId(), action.resourcePattern().resourceType(),
                    action.resourcePattern().name(), action.operation(), result);
            Sensor sensor = sensors.get(key);
            if (sensor == null) {
                if (sensors.size() >= maxSeries) {
                    sensor = overflow(result);
                    overflowed.record(1, nowMs);
                } else {
                    sensor = sensors.computeIfAbsent(key, this::sensor);
                }
            }
            // CumulativeCount and WindowedCount count records; Value keeps the time.
            sensor.record(nowMs, nowMs);
        }
    }

    /**
     * Removes the series, overflow included, that saw no request for the
     * TTL. A request racing with the removal of its series may be recorded
     * into the removed one and lost; that series had been idle for the TTL.
     */
    void expire() {
        for (Map.Entry<Key, Sensor> entry : sensors.entrySet()) {
            Sensor sensor = entry.getValue();
            if (sensor.hasExpired() && sensors.remove(entry.getKey(), sensor)) {
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

    private Sensor overflow(AuthorizationResult result) {
        int i = result.ordinal();
        Sensor sensor = overflow.get(i);
        if (sensor == null) {
            synchronized (overflow) {
                sensor = overflow.get(i);
                if (sensor == null) {
                    sensor = sensor(OTHER, OTHER, OTHER, OTHER, OTHER, result);
                    overflow.set(i, sensor);
                }
            }
        }
        return sensor;
    }

    private Sensor sensor(Key key) {
        return sensor(tag(String.valueOf(key.principal())), tag(key.clientId()),
                key.resourceType().name().toLowerCase(Locale.ROOT).replace('_', '-'), tag(key.resource()),
                key.operation().name(), key.result());
    }

    private Sensor sensor(String principal, String clientId, String resourceType, String resource, String operation,
            AuthorizationResult result) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("principal", principal);
        tags.put("client-id", clientId);
        tags.put("resource-type", resourceType);
        tags.put("resource", resource);
        tags.put("operation", operation);
        tags.put("result", result.name());

        Sensor sensor = metrics.sensor(GROUP + ":" + tags, null, ttlSeconds);
        sensor.add(metrics.metricName("request-total", GROUP, "Requests authorized", tags), new CumulativeCount());
        sensor.add(metrics.metricName("request-rate", GROUP, "Requests authorized per second", tags),
                new Rate(TimeUnit.SECONDS, new WindowedCount()));
        sensor.add(metrics.metricName("last-seen-ms", GROUP, "Epoch milliseconds of the last request", tags),
                new Value());
        return sensor;
    }

    private static String tag(String value) {
        return value == null || value.isEmpty() ? EMPTY : value;
    }
}
