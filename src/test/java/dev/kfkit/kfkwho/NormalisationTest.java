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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Client id rules and excluded resources, through the authorizer, read back from the MBean server. */
class NormalisationTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final String STREAMS_RULE = "^(.*)-StreamThread-\\d+-(consumer|producer)$=>$1";

    private final Metrics pluginMetrics = new Metrics();
    // Series are created on the request thread here; SeriesCreationTest covers the background thread.
    private final MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer(Time.SYSTEM, Runnable::run);

    @AfterEach
    void stop() throws Exception {
        authorizer.close();
        pluginMetrics.close();
    }

    @Test
    void defaultRulesFoldInstancesOfOneConsumerIntoOneSeries() throws Exception {
        start();

        read("consumer-orders-app-3-6f1c2a4e-1b2c-4d5e-8f90-abcdef123456", "orders");
        read("consumer-orders-app-7-0a9b8c7d-6e5f-4a3b-2c1d-0e9f8a7b6c5d", "orders");
        read("consumer-orders-app-4", "orders");

        assertEquals(Set.of("consumer-orders-app"), tags("client-id"));
        assertEquals(3.0, total(single(accessBeans())));
    }

    @Test
    void defaultRulesKeepAnIdWithoutASuffix() throws Exception {
        start();

        read("orders-app", "orders");
        read("payments", "orders");

        assertEquals(Set.of("orders-app", "payments"), tags("client-id"));
    }

    @Test
    void aStreamsRuleMapsTheThreadClientsToTheApplicationId() throws Exception {
        start(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, STREAMS_RULE);

        read("app-1-StreamThread-2-consumer", "orders");
        read("app-1-StreamThread-3-producer", "orders");

        assertEquals(Set.of("app-1"), tags("client-id"));
        assertEquals(2.0, total(single(accessBeans())));
    }

    @Test
    void configuredRulesReplaceTheDefaults() throws Exception {
        start(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, STREAMS_RULE);

        read("consumer-orders-app-3", "orders");

        assertEquals(Set.of("consumer-orders-app-3"), tags("client-id"));
    }

    @Test
    void anEmptyRuleListKeepsClientIdsAsSent() throws Exception {
        start(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, "");

        read("consumer-orders-app-3-6f1c2a4e-1b2c-4d5e-8f90-abcdef123456", "orders");

        assertEquals(Set.of("consumer-orders-app-3-6f1c2a4e-1b2c-4d5e-8f90-abcdef123456"), tags("client-id"));
    }

    @Test
    void theFirstRuleThatMatchesTheWholeIdWins() throws Exception {
        start(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, "billing=>partial,^billing-(\\d)$=>first-$1,^billing-.*=>second");

        read("billing-1", "orders");
        read("billing-x", "orders");

        assertEquals(Set.of("first-1", "second"), tags("client-id"));
    }

    @Test
    void aRuleThatRewritesToNothingGivesUnknown() throws Exception {
        start(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, "^tmp-.*=>");

        read("tmp-42", "orders");

        assertEquals(Set.of(AccessMetrics.UNKNOWN), tags("client-id"));
    }

    @Test
    void appliesTheRulesToMoreDistinctIdsThanItRemembers() throws Exception {
        start();

        for (int i = 0; i <= AccessMetrics.MAX_MEMO; i++) {
            read("billing-" + i, "orders");
        }

        assertEquals(Set.of("billing"), tags("client-id"));
        assertEquals(AccessMetrics.MAX_MEMO + 1.0, total(single(accessBeans())));
    }

    @Test
    void anInvalidRuleFailsConfigureWithTheRule() {
        for (String rule : List.of("^(consumer-.*=>$1", "^(consumer)-.*=>$2", "^consumer-.*=>${app}", "^c=>$",
                "^c=>\\")) {
            ConfigException e = assertThrows(ConfigException.class,
                    () -> authorizer.configure(broker(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, rule)), rule);
            assertTrue(e.getMessage().contains(rule), e.getMessage());
        }
    }

    @Test
    void aNamedGroupMayBeReplaced() throws Exception {
        start(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, "^(?<app>.+)-worker-\\d+$=>${app}");

        read("billing-worker-3", "orders");

        assertEquals(Set.of("billing"), tags("client-id"));
    }

    @Test
    void internalTopicsAreNotRecordedAndOthersAre() throws Exception {
        start();

        read("billing", "__consumer_offsets");
        read("billing", "__transaction_state");
        read("billing", "orders");

        assertEquals(Set.of("orders"), tags("resource"));
        assertEquals(1.0, self("series-count"));
        assertEquals(0.0, self("series-overflow-total"));
    }

    @Test
    void anExcludedResourceTakesNoPlaceUnderTheCap() throws Exception {
        start(AuthorizerConfig.MAX_SERIES_CONFIG, "1");

        for (int i = 0; i < 3; i++) {
            read("billing", "__consumer_offsets");
        }
        read("billing", "orders");

        assertEquals(Set.of("orders"), tags("resource"));
        assertEquals(0.0, self("series-overflow-total"));
    }

    @Test
    void theExcludeRegexIsConfigurable() throws Exception {
        start(AuthorizerConfig.RESOURCE_EXCLUDE_CONFIG, "orders|payments");

        read("billing", "orders");
        read("billing", "payments");
        read("billing", "__consumer_offsets");

        assertEquals(Set.of("__consumer_offsets"), tags("resource"));
    }

    @Test
    void theClusterIsRecordedUnderTheNameTheBrokerGivesIt() throws Exception {
        start();

        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.INIT_PRODUCER_ID), List.of(new Action(
                AclOperation.IDEMPOTENT_WRITE, new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster",
                        PatternType.LITERAL), 1, true, true)));

        assertEquals(Set.of("cluster"), tags("resource-type"));
        assertEquals(Set.of("kafka-cluster"), tags("resource"));
    }

    private void start(String... keyValues) {
        authorizer.configure(broker(keyValues));
        authorizer.withPluginMetrics(new PluginMetricsImpl(pluginMetrics, Map.of()));
        authorizer.completeInitialLoad();
    }

    private void read(String clientId, String topic) {
        authorizer.authorize(RequestContext.of("alice", clientId, ApiKeys.FETCH), List.of(new Action(
                AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), 1, true, true)));
    }

    private static Map<String, Object> broker(String... keyValues) {
        Map<String, Object> configs = new TreeMap<>(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice"));
        for (int i = 0; i < keyValues.length; i += 2) {
            configs.put(keyValues[i], keyValues[i + 1]);
        }
        return configs;
    }

    private static String single(Set<String> values) {
        assertEquals(1, values.size(), values.toString());
        return values.iterator().next();
    }

    private static Object total(String name) throws Exception {
        return SERVER.getAttribute(new ObjectName(name), "request-total");
    }

    private static Object self(String attribute) throws Exception {
        return SERVER.getAttribute(new ObjectName("kfkwho:type=authorizer"), attribute);
    }

    /** One tag, unquoted, across every access series. */
    private static Set<String> tags(String tag) throws Exception {
        Set<String> values = new TreeSet<>();
        for (String name : accessBeans()) {
            String value = new ObjectName(name).getKeyProperty(tag);
            values.add(value.startsWith("\"") ? ObjectName.unquote(value) : value);
        }
        return values;
    }

    private static Set<String> accessBeans() throws Exception {
        Set<String> names = new TreeSet<>();
        for (ObjectName name : SERVER.queryNames(new ObjectName("kfkwho:type=access,*"), null)) {
            names.add(name.getDomain() + ":" + name.getKeyPropertyListString());
        }
        return names;
    }
}
