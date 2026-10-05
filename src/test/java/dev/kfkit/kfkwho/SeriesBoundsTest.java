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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The TTL and the cap, on a clock the test moves, read back from the MBean server. */
class SeriesBoundsTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final long TTL_SECONDS = 60;
    private static final String SELF = "kfkwho:type=authorizer";
    private static final String OTHER_ALLOWED = "kfkwho:type=access,principal=__other__,client-id=__other__,"
            + "resource-type=__other__,resource=__other__,operation=__other__,api=__other__,result=ALLOWED";

    private final ManualTime time = new ManualTime();
    private final Metrics pluginMetrics = new Metrics();
    private final MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer(time, Runnable::run);

    @AfterEach
    void stop() throws Exception {
        authorizer.close();
        pluginMetrics.close();
    }

    @Test
    void expiresAnIdleSeriesAndKeepsABusyOne() throws Exception {
        start(Map.of(AuthorizerConfig.TTL_SECONDS_CONFIG, String.valueOf(TTL_SECONDS)));
        readTopic("orders");
        readTopic("billing");

        // billing keeps receiving requests, orders falls silent.
        for (int s = 0; s <= TTL_SECONDS; s += 10) {
            time.advanceSeconds(10);
            readTopic("billing");
            authorizer.expireIdleSeries();
        }
        time.advanceSeconds(1);
        readTopic("billing");
        authorizer.expireIdleSeries();

        assertEquals(Set.of(topicSeries("billing")), accessBeans());
        assertEquals(1.0, self("series-evicted-total"));
        assertEquals(1.0, self("series-count"));
    }

    @Test
    void keepsASeriesUntilTheTtlHasPassed() throws Exception {
        start(Map.of(AuthorizerConfig.TTL_SECONDS_CONFIG, String.valueOf(TTL_SECONDS)));
        readTopic("orders");

        time.advanceSeconds(TTL_SECONDS);
        authorizer.expireIdleSeries();
        assertEquals(Set.of(topicSeries("orders")), accessBeans());

        time.advanceSeconds(1);
        authorizer.expireIdleSeries();
        assertEquals(Set.of(), accessBeans());
    }

    @Test
    void foldsSeriesPastTheCapIntoOverflow() throws Exception {
        start(Map.of(AuthorizerConfig.MAX_SERIES_CONFIG, "3"));
        for (String topic : List.of("orders", "billing", "payments")) {
            readTopic(topic);
        }
        assertEquals(0.0, self("series-overflow-total"));

        // Past the cap the verdict is still the parent's.
        assertEquals(List.of(AuthorizationResult.ALLOWED), readTopic("refunds"));

        assertEquals(1.0, self("series-overflow-total"));
        assertEquals(3.0, self("series-count"));
        assertEquals(Set.of(topicSeries("orders"), topicSeries("billing"), topicSeries("payments"), OTHER_ALLOWED),
                accessBeans());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(OTHER_ALLOWED), "request-total"));
        assertEquals(Set.of(SELF), beans(SELF));

        // A series that exists keeps counting; a denial past the cap has its own overflow series.
        readTopic("orders");
        assertEquals(2.0, SERVER.getAttribute(new ObjectName(topicSeries("orders")), "request-total"));
        assertEquals(List.of(AuthorizationResult.DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), read("orders")));
        assertEquals(2.0, self("series-overflow-total"));
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(OTHER_ALLOWED.replace("ALLOWED", "DENIED")),
                "request-total"));
    }

    @Test
    void anEvictedSeriesStartsFromZero() throws Exception {
        start(Map.of(AuthorizerConfig.TTL_SECONDS_CONFIG, String.valueOf(TTL_SECONDS)));
        ObjectName orders = new ObjectName(topicSeries("orders"));
        readTopic("orders");
        readTopic("orders");
        assertEquals(2.0, SERVER.getAttribute(orders, "request-total"));
        assertEquals(0.0, self("series-evicted-total"));

        time.advanceSeconds(TTL_SECONDS + 1);
        authorizer.expireIdleSeries();
        assertTrue(accessBeans().isEmpty());

        readTopic("orders");
        assertEquals(1.0, SERVER.getAttribute(orders, "request-total"));
        assertEquals(1.0, self("series-evicted-total"));
    }

    @Test
    void evictionMakesRoomUnderTheCap() throws Exception {
        start(Map.of(AuthorizerConfig.TTL_SECONDS_CONFIG, String.valueOf(TTL_SECONDS),
                AuthorizerConfig.MAX_SERIES_CONFIG, "1"));
        readTopic("orders");
        readTopic("billing");
        assertEquals(Set.of(topicSeries("orders"), OTHER_ALLOWED), accessBeans());

        time.advanceSeconds(TTL_SECONDS + 1);
        authorizer.expireIdleSeries();
        assertEquals(Set.of(), accessBeans());

        readTopic("billing");
        assertEquals(Set.of(topicSeries("billing")), accessBeans());
    }

    @Test
    void defaultsToTenMinutesAndTenThousand() throws Exception {
        start(Map.of());
        readTopic("orders");

        time.advanceSeconds(AuthorizerConfig.DEFAULT_TTL_SECONDS);
        authorizer.expireIdleSeries();
        assertEquals(Set.of(topicSeries("orders")), accessBeans());
        time.advanceSeconds(1);
        authorizer.expireIdleSeries();
        assertEquals(Set.of(), accessBeans());
        assertEquals(10_000, AuthorizerConfig.DEFAULT_MAX_SERIES);
    }

    private void start(Map<String, String> configs) {
        Map<String, Object> all = new java.util.HashMap<>(configs);
        all.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice");
        authorizer.configure(all);
        authorizer.withPluginMetrics(new PluginMetricsImpl(pluginMetrics, Map.of()));
        authorizer.completeInitialLoad();
    }

    private List<AuthorizationResult> readTopic(String topic) {
        return authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), read(topic));
    }

    private static List<Action> read(String topic) {
        return List.of(new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic,
                PatternType.LITERAL), 1, true, true));
    }

    private static String topicSeries(String topic) {
        return "kfkwho:type=access,principal=\"User:alice\",client-id=billing,resource-type=topic,resource="
                + topic + ",operation=READ,api=FETCH,result=ALLOWED";
    }

    private static Object self(String attribute) throws Exception {
        return SERVER.getAttribute(new ObjectName(SELF), attribute);
    }

    private static Set<String> accessBeans() throws Exception {
        return beans("kfkwho:type=access,*");
    }

    private static Set<String> beans(String pattern) throws Exception {
        Set<String> names = new TreeSet<>();
        for (ObjectName name : SERVER.queryNames(new ObjectName(pattern), null)) {
            names.add(name.getDomain() + ":" + name.getKeyPropertyListString());
        }
        return names;
    }
}
