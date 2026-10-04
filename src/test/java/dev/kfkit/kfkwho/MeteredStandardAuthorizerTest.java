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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The parent authorizer can be driven without a broker: configure, hand it
 * plugin metrics, finish the (empty) ACL load, authorize. That is how Kafka's
 * own tests do it and how every test here reaches the code that ships.
 *
 * <p>Since Kafka 4.1 the broker always calls {@code withPluginMetrics} on the
 * authorizer and {@code authorize} dereferences what it set; skip the call
 * and the parent throws a {@code NullPointerException}.
 */
class MeteredStandardAuthorizerTest {

    private static final ResourcePattern ORDERS = new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL);

    private final MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer();

    @AfterEach
    void close() throws Exception {
        authorizer.close();
    }

    @Test
    void verdictIsTheParents() {
        authorizer.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice"));
        authorizer.withPluginMetrics(new PluginMetricsImpl(new Metrics(), Map.of()));
        authorizer.completeInitialLoad();
        List<Action> read = List.of(new Action(AclOperation.READ, ORDERS, 1, true, true));

        assertEquals(List.of(AuthorizationResult.ALLOWED),
                authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), read));
        assertEquals(List.of(AuthorizationResult.DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), read));
    }

    @Test
    void staysAClusterMetadataAuthorizer() {
        // The broker's AclPublisher feeds ACLs only to this type; losing it
        // would mean an authorizer that denies everything, silently.
        assertEquals(true, ClusterMetadataAuthorizer.class.isInstance(authorizer));
    }
}
