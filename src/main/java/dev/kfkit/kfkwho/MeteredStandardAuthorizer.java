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
 */
public class MeteredStandardAuthorizer extends StandardAuthorizer {

    private final Time time = Time.SYSTEM;
    private Metrics metrics;
    private AccessMetrics access;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        metrics = new Metrics(new MetricConfig(), List.of(new JmxReporter()), time,
                new KafkaMetricsContext(AccessMetrics.JMX_PREFIX));
        // Metrics registers its own count of metrics; it is not ours to publish.
        metrics.removeMetric(metrics.metricName("count", "kafka-metrics-count"));
        access = new AccessMetrics(metrics);
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
            if (metrics != null) {
                metrics.close();
            }
        }
    }
}
