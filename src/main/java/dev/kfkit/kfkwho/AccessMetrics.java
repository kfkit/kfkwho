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

import org.apache.kafka.common.acl.AclOperation;
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
 */
final class AccessMetrics {

    static final String JMX_PREFIX = "kfkwho";
    static final String GROUP = "access";

    /**
     * The {@code JmxReporter} drops a tag whose value is empty, which would
     * shift the tags that follow; an empty client id is reported as this.
     */
    static final String EMPTY = "-";

    /**
     * A hard bound on the number of series until expiry and a configurable
     * cap exist: past it, new combinations are not recorded. Under
     * concurrent first sightings it may be overshot by the number of
     * request handler threads.
     */
    static final int MAX_SERIES = 10_000;

    private record Key(KafkaPrincipal principal, String clientId, ResourceType resourceType, String resource,
            AclOperation operation, AuthorizationResult result) {
    }

    private final Metrics metrics;
    private final Map<Key, Sensor> sensors = new ConcurrentHashMap<>();

    AccessMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    void record(AuthorizableRequestContext context, List<Action> actions, List<AuthorizationResult> results,
            long nowMs) {
        for (int i = 0; i < actions.size(); i++) {
            Action action = actions.get(i);
            Key key = new Key(context.principal(), context.clientId(), action.resourcePattern().resourceType(),
                    action.resourcePattern().name(), action.operation(), results.get(i));
            Sensor sensor = sensors.get(key);
            if (sensor == null) {
                if (sensors.size() >= MAX_SERIES) {
                    continue;
                }
                sensor = sensors.computeIfAbsent(key, this::sensor);
            }
            // CumulativeCount and WindowedCount count records; Value keeps the time.
            sensor.record(nowMs, nowMs);
        }
    }

    private Sensor sensor(Key key) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("principal", tag(String.valueOf(key.principal())));
        tags.put("client-id", tag(key.clientId()));
        tags.put("resource-type", key.resourceType().name().toLowerCase(Locale.ROOT).replace('_', '-'));
        tags.put("resource", tag(key.resource()));
        tags.put("operation", key.operation().name());
        tags.put("result", key.result().name());

        Sensor sensor = metrics.sensor(GROUP + ":" + tags);
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
