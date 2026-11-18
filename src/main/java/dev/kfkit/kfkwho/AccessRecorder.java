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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * Publishes who accessed what, and with which verdict, as the JMX metrics
 * of kfkwho: for any authorizer that hands it its own verdicts.
 *
 * <p>{@link MeteredStandardAuthorizer} is one user; an authorizer that
 * decides differently is another. It extends {@code StandardAuthorizer}
 * itself, so that the broker still feeds it ACLs, creates a recorder in
 * {@code configure}, hands every verdict to {@link #record} and closes the
 * recorder in {@code close}. The recorder records whatever verdict it is
 * handed; it decides nothing. {@code docs/embedding.md} has an example.
 *
 * <p>What is stable: this class and its three members, the {@code kfkwho.*}
 * configuration keys, the MBean names and their tag order, as listed in
 * {@code docs/embedding.md}, {@code docs/config.md} and
 * {@code docs/metrics.md}. Everything else in the package is package-private
 * and may change in any release.
 *
 * <p>The MBeans are registered under the {@code kfkwho} domain, and series
 * are created and expired on one daemon thread, {@code kfkwho-series}: both
 * are constants, whatever package this class is relocated to. Loading the
 * class does neither; {@link #create} does, on a broker.
 *
 * <p>One recorder per authorizer: two recorders in one JVM register the same
 * MBean names, and the second loses.
 */
public final class AccessRecorder {

    /** How often idle series are looked for, at most; a shorter TTL looks more often. */
    private static final long MAX_EXPIRY_PERIOD_SECONDS = 30;

    /** How long {@link #close} waits for the background thread to end. */
    private static final long CLOSE_WAIT_SECONDS = 5;

    private final Time time;
    /** Null on a node that is not a broker: then nothing is recorded or registered. */
    private final AccessMetrics access;
    private final Metrics metrics;
    private final ScheduledExecutorService background;
    private boolean closed;

    private AccessRecorder(Time time, AccessMetrics access, Metrics metrics, ScheduledExecutorService background) {
        this.time = time;
        this.access = access;
        this.metrics = metrics;
        this.background = background;
    }

    /**
     * A recorder for one authorizer, configured from the broker configuration
     * the authorizer's {@code configure(Map)} received: the {@code kfkwho.*}
     * keys of {@code docs/config.md} and {@code process.roles}.
     *
     * <p>On a node whose {@code process.roles} does not include
     * {@code broker} the recorder records nothing: no MBean is registered and
     * no thread started. Otherwise this registers the MBean of the series
     * counts and starts the {@code kfkwho-series} thread.
     *
     * @param brokerConfigs the whole broker configuration; only the keys above are read
     * @return a recorder to hand verdicts to, and to close with the authorizer
     * @throws ConfigException when a {@code kfkwho.*} value is wrong, naming the key
     */
    public static AccessRecorder create(Map<String, ?> brokerConfigs) {
        return create(brokerConfigs, Time.SYSTEM, null);
    }

    /**
     * Tests pass their own clock, and their own creator to decide when series
     * appear; a null creator is the background thread.
     */
    static AccessRecorder create(Map<String, ?> brokerConfigs, Time time, Executor creator) {
        RecorderConfig config = RecorderConfig.parse(brokerConfigs);
        if (!config.broker()) {
            return new AccessRecorder(time, null, null, null);
        }
        Metrics metrics = new Metrics(new MetricConfig(), List.of(new JmxReporter()), time,
                new KafkaMetricsContext(AccessMetrics.JMX_PREFIX));
        // Metrics registers its own count of metrics; it is not ours to publish.
        metrics.removeMetric(metrics.metricName("count", "kafka-metrics-count"));
        long ttlSeconds = config.ttlSeconds();
        ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "kfkwho-series");
            thread.setDaemon(true);
            return thread;
        });
        AccessMetrics access = new AccessMetrics(metrics, ttlSeconds, config.maxSeries(), config.tags(),
                config.clientIdRules(), config.resourceExclude(), creator != null ? creator : background);
        AccessRecorder recorder = new AccessRecorder(time, access, metrics, background);
        long period = Math.max(1, Math.min(ttlSeconds, MAX_EXPIRY_PERIOD_SECONDS));
        background.scheduleAtFixedRate(() -> {
            try {
                recorder.expireIdleSeries();
            } catch (RuntimeException e) {
                // A task that throws is never run again; the next round may succeed.
            }
        }, period, period, TimeUnit.SECONDS);
        return recorder;
    }

    /**
     * Counts one {@code authorize} call: the request, its actions and the
     * verdict for each, in the order of the actions. Call it with the
     * verdicts the authorizer returns, on the request thread. A series seen
     * before costs one lookup per action and allocates nothing; a new one is
     * created on the {@code kfkwho-series} thread.
     *
     * @param requestContext the request, as the broker passed it to {@code authorize}
     * @param actions the actions, as the broker passed them to {@code authorize}
     * @param results the verdicts the authorizer returns, one per action
     */
    public void record(AuthorizableRequestContext requestContext, List<Action> actions,
            List<AuthorizationResult> results) {
        AccessMetrics access = this.access;
        if (access != null) {
            access.record(requestContext, actions, results, time.milliseconds());
        }
    }

    /**
     * Unregisters every MBean of this recorder and stops its thread. No MBean
     * is registered after it returns, whatever {@link #record} is handed;
     * calling it again does nothing.
     */
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        if (access == null) {
            return;
        }
        try {
            access.close();
            background.shutdownNow();
            background.awaitTermination(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            metrics.close();
        }
    }

    /** Runs on the background thread; package-private so tests can drive it with their own clock. */
    void expireIdleSeries() {
        if (access != null) {
            access.expire();
        }
    }
}
