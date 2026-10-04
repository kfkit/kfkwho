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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * The broker's {@link StandardAuthorizer} with metrics on who accesses what.
 *
 * <p>Every request the broker authorizes passes through {@link #authorize}
 * with the principal, the client id, the client address, the resource and
 * the operation. This class decides nothing itself: the verdict is the
 * parent's. It only observes, and publishes what it saw as JMX metrics
 * under the {@value AccessMetrics#JMX_PREFIX} domain.
 *
 * <p>Extending rather than wrapping is deliberate. The broker feeds ACLs
 * only to authorizers that are a {@code ClusterMetadataAuthorizer}; a plain
 * wrapper would silently never receive them.
 *
 * <p>Configure with {@code authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer}.
 * A series that saw no request for {@value #TTL_SECONDS_CONFIG} (default
 * 600) disappears; past {@value #MAX_SERIES_CONFIG} series (default 10000),
 * new ones are folded into an overflow series.
 */
public class MeteredStandardAuthorizer extends StandardAuthorizer {

    static final String TTL_SECONDS_CONFIG = "kfkwho.ttl.seconds";
    static final String MAX_SERIES_CONFIG = "kfkwho.max.series";

    /** How often idle series are looked for, at most; a shorter TTL looks more often. */
    private static final long MAX_EXPIRY_PERIOD_SECONDS = 30;

    private final Time time;
    private Metrics metrics;
    private AccessMetrics access;
    private ScheduledExecutorService expiry;

    public MeteredStandardAuthorizer() {
        this(Time.SYSTEM);
    }

    MeteredStandardAuthorizer(Time time) {
        this.time = time;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        metrics = new Metrics(new MetricConfig(), List.of(new JmxReporter()), time,
                new KafkaMetricsContext(AccessMetrics.JMX_PREFIX));
        // Metrics registers its own count of metrics; it is not ours to publish.
        metrics.removeMetric(metrics.metricName("count", "kafka-metrics-count"));
        long ttlSeconds = longConfig(configs, TTL_SECONDS_CONFIG, AccessMetrics.DEFAULT_TTL_SECONDS);
        access = new AccessMetrics(metrics, ttlSeconds,
                (int) longConfig(configs, MAX_SERIES_CONFIG, AccessMetrics.DEFAULT_MAX_SERIES));
        expiry = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "kfkwho-expiry");
            thread.setDaemon(true);
            return thread;
        });
        long period = Math.max(1, Math.min(ttlSeconds, MAX_EXPIRY_PERIOD_SECONDS));
        expiry.scheduleAtFixedRate(() -> {
            try {
                expireIdleSeries();
            } catch (RuntimeException e) {
                // A task that throws is never run again; the next round may succeed.
            }
        }, period, period, TimeUnit.SECONDS);
    }

    /** Runs on the expiry thread; package-private so tests can drive it with their own clock. */
    void expireIdleSeries() {
        access.expire();
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        List<AuthorizationResult> results = super.authorize(requestContext, actions);
        access.record(requestContext, actions, results, time.milliseconds());
        return results;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (expiry != null) {
                expiry.shutdownNow();
            }
            if (metrics != null) {
                metrics.close();
            }
        }
    }

    // Validation is a separate issue; until then a value that is not a number fails configure().
    private static long longConfig(Map<String, ?> configs, String name, long defaultValue) {
        Object value = configs.get(name);
        return value == null ? defaultValue : Long.parseLong(value.toString().trim());
    }
}
