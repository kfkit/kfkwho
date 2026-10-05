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
import java.util.concurrent.Executor;
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
 * A series that saw no request for {@code kfkwho.ttl.seconds} (default
 * 600) disappears; past {@code kfkwho.max.series} series (default 10000),
 * new ones are folded into an overflow series. Every setting is declared in
 * {@link AuthorizerConfig} and listed in {@code docs/config.md}; a wrong value
 * fails {@link #configure} with a message naming the key. Series are created and
 * expired on one background thread, {@code kfkwho-series}, never on the
 * request thread.
 */
public class MeteredStandardAuthorizer extends StandardAuthorizer {

    /** How often idle series are looked for, at most; a shorter TTL looks more often. */
    private static final long MAX_EXPIRY_PERIOD_SECONDS = 30;

    private final Time time;
    /** Where new series are created; null for the background thread. */
    private final Executor creator;
    private Metrics metrics;
    private AccessMetrics access;
    private ScheduledExecutorService background;

    public MeteredStandardAuthorizer() {
        this(Time.SYSTEM);
    }

    MeteredStandardAuthorizer(Time time) {
        this(time, null);
    }

    /** Tests pass their own creator to decide when series appear. */
    MeteredStandardAuthorizer(Time time, Executor creator) {
        this.time = time;
        this.creator = creator;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // A wrong value fails here, before the parent or any metric is set up.
        AuthorizerConfig config = AuthorizerConfig.parse(configs);
        super.configure(configs);
        metrics = new Metrics(new MetricConfig(), List.of(new JmxReporter()), time,
                new KafkaMetricsContext(AccessMetrics.JMX_PREFIX));
        // Metrics registers its own count of metrics; it is not ours to publish.
        metrics.removeMetric(metrics.metricName("count", "kafka-metrics-count"));
        long ttlSeconds = config.ttlSeconds();
        background = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "kfkwho-series");
            thread.setDaemon(true);
            return thread;
        });
        access = new AccessMetrics(metrics, ttlSeconds, config.maxSeries(), config.clientIdRules(),
                config.resourceExclude(), creator != null ? creator : background);
        long period = Math.max(1, Math.min(ttlSeconds, MAX_EXPIRY_PERIOD_SECONDS));
        background.scheduleAtFixedRate(() -> {
            try {
                expireIdleSeries();
            } catch (RuntimeException e) {
                // A task that throws is never run again; the next round may succeed.
            }
        }, period, period, TimeUnit.SECONDS);
    }

    /** Runs on the background thread; package-private so tests can drive it with their own clock. */
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
            if (access != null) {
                access.close();
            }
            if (background != null) {
                background.shutdownNow();
            }
            if (metrics != null) {
                metrics.close();
            }
        }
    }
}
