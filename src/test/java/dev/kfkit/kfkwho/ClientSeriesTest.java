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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashMap;
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
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The client series, the labels that shape it, and the node roles that record at all. */
class ClientSeriesTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final long TTL_SECONDS = 60;
    private static final String SELF = "kfkwho:type=authorizer";
    private static final String ALICE = "kfkwho:type=client,principal=\"User:alice\",client-id=billing";
    private static final List<Action> READ_ORDERS_AND_BILLING = List.of(read("orders"), read("billing"));
    private static final RequestContext FETCH = RequestContext.of("alice", "billing-1", ApiKeys.FETCH);

    private final ManualTime time = new ManualTime();
    private final Metrics pluginMetrics = new Metrics();
    private final MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer(time, Runnable::run);

    @AfterEach
    void stop() throws Exception {
        authorizer.close();
        pluginMetrics.close();
    }

    @Test
    void oneRequestCreatesAClientSeriesWithListenerAndProtocol() throws Exception {
        // AC1, with the client id kept as sent: by default the rules make billing-1 billing.
        start(Map.of(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, ""));
        authorizer.authorize(FETCH.over("INTERNAL", SecurityProtocol.PLAINTEXT), READ_ORDERS_AND_BILLING);

        String name = "kfkwho:type=client,principal=\"User:alice\",client-id=billing-1,listener=INTERNAL,"
                + "security-protocol=PLAINTEXT";
        assertEquals(Set.of(name), clientBeans());
        // One request of two actions: two access series, one count in the client series.
        assertEquals(2, beans("kfkwho:type=access,*").size());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(name), "request-total"));
        assertEquals((double) time.milliseconds(), SERVER.getAttribute(new ObjectName(name), "last-seen-ms"));
    }

    @Test
    void theClientIdIsTheOneTheAccessSeriesCarry() throws Exception {
        start(Map.of());
        authorizer.authorize(FETCH, READ_ORDERS_AND_BILLING);
        authorizer.authorize(RequestContext.of("alice", "billing-2", ApiKeys.PRODUCE), READ_ORDERS_AND_BILLING);

        String name = ALICE + ",listener=PLAINTEXT,security-protocol=PLAINTEXT";
        assertEquals(Set.of(name), clientBeans());
        assertEquals(2.0, SERVER.getAttribute(new ObjectName(name), "request-total"));
    }

    @Test
    void aControllerRegistersNothing() throws Exception {
        // AC2
        start(Map.of(AuthorizerConfig.PROCESS_ROLES_CONFIG, "controller"));

        assertEquals(List.of(AuthorizationResult.ALLOWED, AuthorizationResult.ALLOWED),
                authorizer.authorize(FETCH, READ_ORDERS_AND_BILLING));
        assertEquals(List.of(AuthorizationResult.DENIED, AuthorizationResult.DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), READ_ORDERS_AND_BILLING));
        authorizer.expireIdleSeries();

        assertEquals(Set.of(), beans("kfkwho:*"));
    }

    @Test
    void aCombinedNodeRecords() throws Exception {
        // AC3
        start(Map.of(AuthorizerConfig.PROCESS_ROLES_CONFIG, "broker,controller"));
        authorizer.authorize(FETCH, READ_ORDERS_AND_BILLING);

        assertEquals(Set.of(ALICE + ",listener=PLAINTEXT,security-protocol=PLAINTEXT"), clientBeans());
        assertEquals(2, beans("kfkwho:type=access,*").size());
        assertEquals(Set.of(SELF), beans(SELF));
    }

    @Test
    void aBrokerOnlyNodeRecords() throws Exception {
        start(Map.of(AuthorizerConfig.PROCESS_ROLES_CONFIG, "broker"));
        authorizer.authorize(FETCH, READ_ORDERS_AND_BILLING);

        assertEquals(1, clientBeans().size());
    }

    @Test
    void withoutTheListenerLabelThereIsNoListenerTag() throws Exception {
        // AC4: and requests over two listeners are one series.
        start(Map.of(AuthorizerConfig.LABELS_CONFIG, "principal,client-id,security-protocol"));
        authorizer.authorize(FETCH.over("INTERNAL", SecurityProtocol.PLAINTEXT), READ_ORDERS_AND_BILLING);
        authorizer.authorize(FETCH.over("REPLICATION", SecurityProtocol.PLAINTEXT), READ_ORDERS_AND_BILLING);

        String name = ALICE + ",security-protocol=PLAINTEXT";
        assertEquals(Set.of(name), clientBeans());
        assertEquals(2.0, SERVER.getAttribute(new ObjectName(name), "request-total"));
    }

    @Test
    void eachListenerAndProtocolIsItsOwnSeriesInTheFixedTagOrder() throws Exception {
        start(Map.of());
        authorizer.authorize(FETCH.over("INTERNAL", SecurityProtocol.PLAINTEXT), READ_ORDERS_AND_BILLING);
        authorizer.authorize(FETCH.over("EXTERNAL", SecurityProtocol.SASL_SSL), READ_ORDERS_AND_BILLING);
        authorizer.authorize(FETCH.over("", SecurityProtocol.SSL), READ_ORDERS_AND_BILLING);

        assertEquals(Set.of(ALICE + ",listener=INTERNAL,security-protocol=PLAINTEXT",
                ALICE + ",listener=EXTERNAL,security-protocol=SASL_SSL",
                ALICE + ",listener=unknown,security-protocol=SSL"), clientBeans());
    }

    @Test
    void theClientAddressIsATagOnlyWhenSelected() throws Exception {
        start(Map.of(AuthorizerConfig.LABELS_CONFIG, "principal,client-id,client-address"));
        authorizer.authorize(FETCH.from(InetAddress.getByName("192.0.2.10")), READ_ORDERS_AND_BILLING);
        authorizer.authorize(FETCH.from(InetAddress.getByName("192.0.2.11")), READ_ORDERS_AND_BILLING);
        authorizer.authorize(FETCH.from(InetAddress.getByName("192.0.2.11")), READ_ORDERS_AND_BILLING);

        assertEquals(Set.of(ALICE + ",client-address=192.0.2.10", ALICE + ",client-address=192.0.2.11"),
                clientBeans());
        assertEquals(2.0, SERVER.getAttribute(new ObjectName(ALICE + ",client-address=192.0.2.11"),
                "request-total"));
    }

    @Test
    void byDefaultTheClientAddressIsNotATag() throws Exception {
        start(Map.of());
        authorizer.authorize(FETCH.from(InetAddress.getByName("192.0.2.10")), READ_ORDERS_AND_BILLING);
        authorizer.authorize(FETCH.from(InetAddress.getByName("192.0.2.11")), READ_ORDERS_AND_BILLING);

        String name = ALICE + ",listener=PLAINTEXT,security-protocol=PLAINTEXT";
        assertEquals(Set.of(name), clientBeans());
        assertEquals(2.0, SERVER.getAttribute(new ObjectName(name), "request-total"));
    }

    @Test
    void foldsClientsPastTheCapIntoOverflow() throws Exception {
        start(Map.of(AuthorizerConfig.MAX_SERIES_CONFIG, "2"));
        for (String user : List.of("alice", "bob", "carol")) {
            authorizer.authorize(RequestContext.of(user, "billing-1", ApiKeys.FETCH), List.of(read("orders")));
        }

        String other = "kfkwho:type=client,principal=__other__,client-id=__other__,listener=__other__,"
                + "security-protocol=__other__";
        assertEquals(3, clientBeans().size());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(other), "request-total"));
        assertEquals(2.0, self("client-series-count"));
        assertEquals(1.0, self("client-series-overflow-total"));
        // The access series have a cap of their own.
        assertEquals(2.0, self("series-count"));
        assertEquals(1.0, self("series-overflow-total"));
    }

    @Test
    void expiresAnIdleClientSeries() throws Exception {
        start(Map.of(AuthorizerConfig.TTL_SECONDS_CONFIG, String.valueOf(TTL_SECONDS)));
        authorizer.authorize(FETCH, READ_ORDERS_AND_BILLING);
        assertEquals(1.0, self("client-series-count"));

        time.advanceSeconds(TTL_SECONDS + 1);
        authorizer.expireIdleSeries();

        assertEquals(Set.of(), clientBeans());
        assertEquals(0.0, self("client-series-count"));
        assertEquals(1.0, self("client-series-evicted-total"));
        assertEquals(2.0, self("series-evicted-total"));
    }

    @Test
    void closeUnregistersTheClientSeries() throws Exception {
        start(Map.of());
        authorizer.authorize(FETCH, READ_ORDERS_AND_BILLING);

        authorizer.close();

        assertEquals(Set.of(), beans("kfkwho:*"));
    }

    private void start(Map<String, Object> configs) {
        Map<String, Object> all = new HashMap<>(configs);
        all.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice");
        authorizer.configure(all);
        authorizer.withPluginMetrics(new PluginMetricsImpl(pluginMetrics, Map.of()));
        authorizer.completeInitialLoad();
    }

    private static Action read(String topic) {
        return new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                1, true, true);
    }

    private static Object self(String attribute) throws Exception {
        return SERVER.getAttribute(new ObjectName(SELF), attribute);
    }

    private static Set<String> clientBeans() throws Exception {
        return beans("kfkwho:type=client,*");
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
