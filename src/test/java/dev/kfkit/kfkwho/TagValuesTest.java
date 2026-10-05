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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** What real clients send, and what the series look like for it, read back from the MBean server. */
class TagValuesTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final List<Action> READ_ORDERS = List.of(read(ResourceType.TOPIC, "orders"));

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
    void nullAndEmptyClientIdAreOneUnknownSeries() throws Exception {
        authorizer.authorize(RequestContext.of("alice", null, ApiKeys.FETCH), READ_ORDERS);
        authorizer.authorize(RequestContext.of("alice", "", ApiKeys.FETCH), READ_ORDERS);
        authorizer.authorize(RequestContext.of("alice", AccessMetrics.UNKNOWN, ApiKeys.FETCH), READ_ORDERS);

        String unknown = "kfkwho:type=access,principal=\"User:alice\",client-id=unknown,"
                + "resource-type=topic,resource=orders,operation=READ,api=FETCH,result=ALLOWED";
        assertEquals(Set.of(unknown), accessBeans());
        assertEquals(3.0, SERVER.getAttribute(new ObjectName(unknown), "request-total"));
        assertEquals(1.0, self("series-count"));
    }

    @Test
    void anEmptyResourceNameIsUnknownToo() throws Exception {
        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.JOIN_GROUP),
                List.of(read(ResourceType.GROUP, "")));

        assertEquals(Set.of("unknown"), tags("resource"));
    }

    @Test
    void cutsALongClientIdAndKeepsTwoApart() throws Exception {
        String prefix = "billing-".repeat(128);
        String first = prefix + "a";
        String second = prefix + "b";
        assertEquals(1025, first.length());

        authorizer.authorize(RequestContext.of("alice", first, ApiKeys.FETCH), READ_ORDERS);
        authorizer.authorize(RequestContext.of("alice", second, ApiKeys.FETCH), READ_ORDERS);
        authorizer.authorize(RequestContext.of("alice", second, ApiKeys.FETCH), READ_ORDERS);

        Set<String> clientIds = tags("client-id");
        assertEquals(2, clientIds.size());
        for (String clientId : clientIds) {
            assertEquals(AccessMetrics.MAX_TAG_LENGTH, clientId.length(), clientId);
            assertTrue(clientId.startsWith(prefix.substring(0, 200)), clientId);
            assertTrue(clientId.matches(".*-[0-9a-f]{12}"), clientId);
        }
        List<Double> totals = new ArrayList<>();
        for (String name : accessBeans()) {
            totals.add((Double) SERVER.getAttribute(new ObjectName(name), "request-total"));
        }
        assertEquals(Set.of(1.0, 2.0), new TreeSet<>(totals));
    }

    @Test
    void cutsALongPrincipalWithoutThrowing() throws Exception {
        String dn = "CN=svc," + "OU=x,".repeat(2048) + "O=y";
        assertTrue(dn.length() > 10_000);

        assertEquals(List.of(AuthorizationResult.DENIED), authorizer.authorize(
                context(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, dn), "billing-1"), READ_ORDERS));

        String principal = single(tags("principal"));
        assertEquals(AccessMetrics.MAX_TAG_LENGTH, principal.length());
        assertTrue(principal.startsWith("User:CN=svc,OU=x,"), principal);
    }

    @Test
    void neverSplitsASurrogatePair() {
        int keep = AccessMetrics.MAX_TAG_LENGTH - 13;
        // The cut would fall between the two halves of the rocket.
        String value = "a".repeat(keep - 1) + "🚀".repeat(200);

        String cut = AccessMetrics.truncate(value);

        assertEquals(AccessMetrics.MAX_TAG_LENGTH - 1, cut.length());
        assertFalse(Character.isHighSurrogate(cut.charAt(keep - 2)), cut);
        assertEquals(cut, new String(cut.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8));
    }

    @Test
    void keepsUnicodeAndWhitespaceAsSent() throws Exception {
        String clientId = "платёжка 🚀 ";
        authorizer.authorize(RequestContext.of("alice", clientId, ApiKeys.PRODUCE),
                List.of(new Action(AclOperation.WRITE, topic("orders"), 1, true, true)));

        Set<String> names = accessBeans();
        assertEquals(1, names.size());
        ObjectName name = new ObjectName(single(names));
        assertFalse(name.isPattern());
        assertEquals(clientId, unquote(name.getKeyProperty("client-id")));
        assertEquals(1.0, SERVER.getAttribute(name, "request-total"));
        // The trailing space is a different client from the one without it.
        authorizer.authorize(RequestContext.of("alice", clientId.strip(), ApiKeys.PRODUCE),
                List.of(new Action(AclOperation.WRITE, topic("orders"), 1, true, true)));
        assertEquals(Set.of(clientId, clientId.strip()), tags("client-id"));
    }

    @Test
    void reportsThePrincipalWhole() throws Exception {
        List<KafkaPrincipal> principals = List.of(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice"),
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "CN=svc,OU=x,O=y"),
                KafkaPrincipal.ANONYMOUS,
                new ServicePrincipal("billing"));
        for (KafkaPrincipal principal : principals) {
            authorizer.authorize(context(principal, "billing-1"), READ_ORDERS);
        }

        assertEquals(Set.of("User:alice", "User:CN=svc,OU=x,O=y", "User:ANONYMOUS", "Service:billing"),
                tags("principal"));
        for (String name : accessBeans()) {
            assertFalse(new ObjectName(name).isPattern(), name);
            assertEquals(1.0, SERVER.getAttribute(new ObjectName(name), "request-total"), name);
        }
    }

    @Test
    void namesUnusualResources() throws Exception {
        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.METADATA), List.of(
                new Action(AclOperation.DESCRIBE, topic("*"), 1, true, true),
                new Action(AclOperation.IDEMPOTENT_WRITE,
                        new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL), 1, true, true),
                new Action(AclOperation.WRITE,
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "billing-tx.1", PatternType.LITERAL), 1, true, true),
                new Action(AclOperation.WRITE, topic("orders.v1-eu"), 1, true, true)));

        Set<String> series = new TreeSet<>();
        for (String name : accessBeans()) {
            ObjectName objectName = new ObjectName(name);
            assertFalse(objectName.isPattern(), name);
            series.add(objectName.getKeyProperty("resource-type") + " " + unquote(objectName.getKeyProperty("resource")));
            assertEquals(1.0, SERVER.getAttribute(objectName, "request-total"), name);
        }
        assertEquals(Set.of("topic *", "cluster kafka-cluster", "transactional-id billing-tx.1", "topic orders.v1-eu"),
                series);
    }

    @Test
    void countsExactlyUnderConcurrency() throws Exception {
        int threads = 16;
        int calls = 10_000;
        int keys = 100;
        List<List<Action>> actions = new ArrayList<>();
        for (int k = 0; k < keys; k++) {
            actions.add(List.of(read(ResourceType.TOPIC, "orders-" + k)));
        }

        ByteArrayOutputStream log = new ByteArrayOutputStream();
        PrintStream err = System.err;
        System.setErr(new PrintStream(log, true, StandardCharsets.UTF_8));
        List<Throwable> failures = new ArrayList<>();
        try {
            // slf4j-simple writes to System.err as it is now: prove the capture works.
            LoggerFactory.getLogger(TagValuesTest.class).error("capture works");
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<?>> done = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int offset = t;
                done.add(pool.submit(() -> {
                    go.await();
                    for (int i = 0; i < calls; i++) {
                        // Every thread walks every key, from a different start.
                        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH),
                                actions.get((i + offset) % keys));
                    }
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> future : done) {
                try {
                    future.get(2, TimeUnit.MINUTES);
                } catch (java.util.concurrent.ExecutionException e) {
                    failures.add(e.getCause());
                }
            }
            pool.shutdown();
        } finally {
            System.setErr(err);
        }

        assertEquals(List.of(), failures);
        String logged = log.toString(StandardCharsets.UTF_8);
        assertTrue(logged.contains("capture works"), logged);
        assertFalse(logged.contains("InstanceAlreadyExistsException"), logged);
        assertFalse(logged.contains("Exception"), logged);
        Set<String> names = accessBeans();
        assertEquals(keys, names.size());
        double total = 0;
        for (String name : names) {
            double perKey = (double) SERVER.getAttribute(new ObjectName(name), "request-total");
            assertEquals((double) threads * calls / keys, perKey, name);
            total += perKey;
        }
        assertEquals(160_000.0, total);
        assertEquals(0.0, self("series-overflow-total"));
    }

    /** A principal type of its own, as a custom {@code KafkaPrincipalBuilder} would return. */
    private static final class ServicePrincipal extends KafkaPrincipal {
        ServicePrincipal(String name) {
            super("Service", name);
        }
    }

    private static RequestContext context(KafkaPrincipal principal, String clientId) {
        return new RequestContext(principal, clientId, InetAddress.getLoopbackAddress(), ApiKeys.FETCH.id);
    }

    private static ResourcePattern topic(String name) {
        return new ResourcePattern(ResourceType.TOPIC, name, PatternType.LITERAL);
    }

    private static Action read(ResourceType type, String name) {
        return new Action(AclOperation.READ, new ResourcePattern(type, name, PatternType.LITERAL), 1, true, true);
    }

    private static String unquote(String value) {
        return value.startsWith("\"") ? ObjectName.unquote(value) : value;
    }

    private static String single(Set<String> values) {
        assertEquals(1, values.size(), values.toString());
        return values.iterator().next();
    }

    private static Object self(String attribute) throws Exception {
        return SERVER.getAttribute(new ObjectName("kfkwho:type=authorizer"), attribute);
    }

    /** One tag, unquoted, across every access series. */
    private static Set<String> tags(String tag) throws Exception {
        Set<String> values = new TreeSet<>();
        for (String name : accessBeans()) {
            values.add(unquote(new ObjectName(name).getKeyProperty(tag)));
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
