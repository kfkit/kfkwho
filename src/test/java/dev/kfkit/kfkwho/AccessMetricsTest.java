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
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** What the authorizer publishes, read back from the platform MBean server as an exporter would. */
class AccessMetricsTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final ResourcePattern ORDERS = new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL);
    private static final List<Action> READ_ORDERS = List.of(new Action(AclOperation.READ, ORDERS, 1, true, true));
    private static final String ALICE_READS_ORDERS = "kfkwho:type=access,principal=\"User:alice\",client-id=billing-1,"
            + "resource-type=topic,resource=orders,operation=READ,result=ALLOWED";

    private final Metrics pluginMetrics = new Metrics();
    private final MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer();

    @BeforeEach
    void start() {
        authorizer.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice"));
        authorizer.withPluginMetrics(new PluginMetricsImpl(pluginMetrics, Map.of()));
        authorizer.completeInitialLoad();
    }

    @AfterEach
    void stop() throws Exception {
        authorizer.close();
        pluginMetrics.close();
    }

    @Test
    void countsEachAuthorizedAction() throws Exception {
        long start = System.currentTimeMillis();
        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), READ_ORDERS);

        assertEquals(Set.of(ALICE_READS_ORDERS), accessBeans());
        ObjectName name = new ObjectName(ALICE_READS_ORDERS);
        assertEquals(1.0, SERVER.getAttribute(name, "request-total"));

        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), READ_ORDERS);
        long end = System.currentTimeMillis();

        assertEquals(2.0, SERVER.getAttribute(name, "request-total"));
        assertTrue((double) SERVER.getAttribute(name, "request-rate") > 0);
        double lastSeen = (double) SERVER.getAttribute(name, "last-seen-ms");
        assertTrue(lastSeen >= start && lastSeen <= end, start + " <= " + lastSeen + " <= " + end);
    }

    @Test
    void deniedIsASeparateSeries() throws Exception {
        assertEquals(List.of(AuthorizationResult.ALLOWED),
                authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), READ_ORDERS));
        assertEquals(List.of(AuthorizationResult.DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), READ_ORDERS));

        String bobDenied = "kfkwho:type=access,principal=\"User:bob\",client-id=billing-2,"
                + "resource-type=topic,resource=orders,operation=READ,result=DENIED";
        assertEquals(Set.of(ALICE_READS_ORDERS, bobDenied), accessBeans());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(bobDenied), "request-total"));
    }

    @Test
    void callsTheParentOncePerCall() throws Exception {
        // Count what the parent itself records, one per action it evaluates.
        var parentCount = pluginMetrics.metricName("parent-calls", "test");
        pluginMetrics.getSensor("authorizer-authorization-request").add(parentCount, new CumulativeCount());
        List<Action> two = List.of(new Action(AclOperation.READ, ORDERS, 1, true, true),
                new Action(AclOperation.WRITE, ORDERS, 1, true, true));

        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.PRODUCE), two);

        assertEquals(2.0, pluginMetrics.metric(parentCount).metricValue());
        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), READ_ORDERS);
        assertEquals(3.0, pluginMetrics.metric(parentCount).metricValue());
    }

    @Test
    void quotesAClientIdThatWouldBreakTheName() throws Exception {
        String clientId = "a,b=c:d\"e*f?";
        authorizer.authorize(RequestContext.of("alice", clientId, ApiKeys.FETCH), READ_ORDERS);

        ObjectName name = new ObjectName("kfkwho:type=access,principal=\"User:alice\",client-id="
                + ObjectName.quote(clientId) + ",resource-type=topic,resource=orders,operation=READ,result=ALLOWED");
        assertEquals(1.0, SERVER.getAttribute(name, "request-total"));
        assertEquals(clientId, ObjectName.unquote(name.getKeyProperty("client-id")));
    }

    @Test
    void keepsTheTagOrderForAnEmptyClientId() throws Exception {
        authorizer.authorize(RequestContext.of("alice", "", ApiKeys.FETCH), READ_ORDERS);

        assertEquals(Set.of("kfkwho:type=access,principal=\"User:alice\",client-id=-,"
                + "resource-type=topic,resource=orders,operation=READ,result=ALLOWED"), accessBeans());
    }

    @Test
    void namesEveryResourceType() throws Exception {
        var actions = List.of(
                new Action(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, "billing", PatternType.LITERAL), 1, true, true),
                new Action(AclOperation.WRITE, new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "billing-tx", PatternType.LITERAL), 1, true, true),
                new Action(AclOperation.CLUSTER_ACTION, new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL), 1, true, true));

        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), actions);

        Set<String> types = new TreeSet<>();
        for (String name : accessBeans()) {
            types.add(new ObjectName(name).getKeyProperty("resource-type"));
        }
        assertEquals(Set.of("group", "transactional-id", "cluster"), types);
    }

    @Test
    void capsTheNumberOfSeries() throws Exception {
        for (int i = 0; i <= AccessMetrics.DEFAULT_MAX_SERIES; i++) {
            var topic = new ResourcePattern(ResourceType.TOPIC, "orders-" + i, PatternType.LITERAL);
            authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH),
                    List.of(new Action(AclOperation.READ, topic, 1, true, true)));
        }

        // The default cap, plus the series the rest is folded into.
        assertEquals(AccessMetrics.DEFAULT_MAX_SERIES + 1, accessBeans().size());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName("kfkwho:type=access,principal=__other__,"
                + "client-id=__other__,resource-type=__other__,resource=__other__,operation=__other__,result=ALLOWED"),
                "request-total"));
        // Past the cap the verdict is still the parent's.
        assertEquals(List.of(AuthorizationResult.DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), READ_ORDERS));
    }

    @Test
    void closeUnregistersEverything() throws Exception {
        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), READ_ORDERS);
        assertEquals(Set.of(ALICE_READS_ORDERS, "kfkwho:type=authorizer"), beans("kfkwho:*"));

        authorizer.close();

        assertEquals(Set.of(), beans("kfkwho:*"));
    }

    private static Set<String> accessBeans() throws Exception {
        return beans("kfkwho:type=access,*");
    }

    private static Set<String> beans(String pattern) throws Exception {
        Set<String> names = new TreeSet<>();
        for (ObjectName name : SERVER.queryNames(new ObjectName(pattern), null)) {
            // The key property list as registered, in its original order.
            names.add(name.getDomain() + ":" + name.getKeyPropertyListString());
        }
        return names;
    }
}
