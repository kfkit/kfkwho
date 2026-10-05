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
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

/**
 * Authorizers set up the way the broker sets them up, with one ACL that lets
 * {@code User:alice} read every topic whose name starts with {@code orders}:
 * no super user, so every call walks the ACLs as it would in production.
 */
final class Fixture {

    static final AuthorizableRequestContext ALICE = new Context(
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"), "billing-1");

    static final List<Action> READ_ORDERS = read("orders");

    private Fixture() {
    }

    static StandardAuthorizer parent() {
        return ready(new StandardAuthorizer(), Map.of());
    }

    static MeteredStandardAuthorizer metered(int maxSeries) {
        return ready(new MeteredStandardAuthorizer(),
                Map.of(MeteredStandardAuthorizer.MAX_SERIES_CONFIG, String.valueOf(maxSeries)));
    }

    static List<Action> read(String topic) {
        return List.of(new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic,
                PatternType.LITERAL), 1, true, true));
    }

    private static <T extends StandardAuthorizer> T ready(T authorizer, Map<String, String> configs) {
        authorizer.configure(configs);
        authorizer.withPluginMetrics(new PluginMetricsImpl(new Metrics(), Map.of()));
        authorizer.addAcl(Uuid.randomUuid(), new StandardAcl(ResourceType.TOPIC, "orders", PatternType.PREFIXED,
                "User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
        authorizer.completeInitialLoad();
        return authorizer;
    }

    /** A Fetch from {@code billing-1} as the broker would describe it to the authorizer. */
    private record Context(KafkaPrincipal principal, String clientId) implements AuthorizableRequestContext {

        @Override
        public String listenerName() {
            return "PLAINTEXT";
        }

        @Override
        public SecurityProtocol securityProtocol() {
            return SecurityProtocol.PLAINTEXT;
        }

        @Override
        public int requestType() {
            return ApiKeys.FETCH.id;
        }

        @Override
        public int requestVersion() {
            return ApiKeys.FETCH.latestVersion();
        }

        @Override
        public InetAddress clientAddress() {
            return InetAddress.getLoopbackAddress();
        }

        @Override
        public int correlationId() {
            return 0;
        }
    }
}
