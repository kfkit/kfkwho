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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Series are created off the request thread; what is counted before one exists is not lost. */
class SeriesCreationTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final long TTL_SECONDS = 60;
    private static final String SELF = "kfkwho:type=authorizer";
    private static final String OTHER_ALLOWED = "kfkwho:type=access,principal=__other__,client-id=__other__,"
            + "resource-type=__other__,resource=__other__,operation=__other__,api=__other__,result=ALLOWED";

    private final ManualTime time = new ManualTime();
    private final ManualExecutor creator = new ManualExecutor();
    private final Metrics pluginMetrics = new Metrics();
    private MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer(time, creator);

    @AfterEach
    void stop() throws Exception {
        authorizer.close();
        pluginMetrics.close();
    }

    @Test
    void countsWhatCameBeforeTheSeriesExisted() throws Exception {
        start(Map.of());
        readTopic("orders");
        time.advanceSeconds(1);
        readTopic("orders");
        time.advanceSeconds(1);
        readTopic("orders");

        assertEquals(Set.of(), accessBeans());
        assertEquals(0.0, self("series-count"));

        creator.runAll();

        ObjectName orders = new ObjectName(topicSeries("orders"));
        assertEquals(Set.of(topicSeries("orders")), accessBeans());
        assertEquals(3.0, SERVER.getAttribute(orders, "request-total"));
        assertEquals((double) time.milliseconds(), SERVER.getAttribute(orders, "last-seen-ms"));
        assertEquals(1.0, self("series-count"));

        readTopic("orders");
        assertEquals(4.0, SERVER.getAttribute(orders, "request-total"));
        assertEquals(0, creator.size());
    }

    @Test
    void countsEveryRequestOnceUnderConcurrentFirstSight() throws Exception {
        int threads = 8;
        int topics = 100;
        int perTopic = 200;
        ExecutorService background = Executors.newSingleThreadExecutor();
        authorizer = new MeteredStandardAuthorizer(Time.SYSTEM, background);
        start(Map.of());

        // Every thread asks for the same new topics in the same order, so the
        // first requests of each race with the creation of its series.
        ExecutorService clients = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> done = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            done.add(clients.submit(() -> {
                go.await();
                for (int i = 0; i < topics; i++) {
                    for (int n = 0; n < perTopic; n++) {
                        readTopic("orders-" + i);
                    }
                }
                return null;
            }));
        }
        go.countDown();
        for (Future<?> f : done) {
            f.get(60, TimeUnit.SECONDS);
        }
        clients.shutdown();
        background.shutdown();
        assertTrue(background.awaitTermination(60, TimeUnit.SECONDS));

        assertEquals(topics, accessBeans().size());
        for (int i = 0; i < topics; i++) {
            assertEquals((double) threads * perTopic,
                    SERVER.getAttribute(new ObjectName(topicSeries("orders-" + i)), "request-total"), "orders-" + i);
        }
        assertEquals((double) topics, self("series-count"));
        assertEquals(0.0, self("series-overflow-total"));
    }

    @Test
    void aPendingSeriesCountsAgainstTheCapButNotInSeriesCount() throws Exception {
        start(Map.of(AuthorizerConfig.MAX_SERIES_CONFIG, "2"));
        readTopic("orders");
        readTopic("billing");
        readTopic("payments");

        assertEquals(0.0, self("series-count"));
        assertEquals(1.0, self("series-overflow-total"));
        assertEquals(Set.of(OTHER_ALLOWED), accessBeans());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(OTHER_ALLOWED), "request-total"));

        creator.runAll();

        assertEquals(2.0, self("series-count"));
        assertEquals(Set.of(topicSeries("orders"), topicSeries("billing"), OTHER_ALLOWED), accessBeans());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(topicSeries("orders")), "request-total"));
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(OTHER_ALLOWED), "request-total"));
    }

    @Test
    void aBurstBeyondThePendingLimitGoesToOther() throws Exception {
        start(Map.of());
        for (int i = 0; i <= SeriesSet.MAX_PENDING; i++) {
            readTopic("orders-" + i);
        }

        assertEquals(1.0, self("series-overflow-total"));
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(OTHER_ALLOWED), "request-total"));

        creator.runAll();
        assertEquals((double) SeriesSet.MAX_PENDING, self("series-count"));

        // Seen again once there is room, the key that overflowed gets its own series.
        String last = "orders-" + SeriesSet.MAX_PENDING;
        readTopic(last);
        creator.runAll();
        assertEquals((double) SeriesSet.MAX_PENDING + 1, self("series-count"));
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(topicSeries(last)), "request-total"));
    }

    @Test
    void aPendingSeriesIsNotExpired() throws Exception {
        start(Map.of(AuthorizerConfig.TTL_SECONDS_CONFIG, String.valueOf(TTL_SECONDS)));
        readTopic("orders");

        time.advanceSeconds(TTL_SECONDS + 1);
        authorizer.expireIdleSeries();
        creator.runAll();

        assertEquals(Set.of(topicSeries("orders")), accessBeans());
        assertEquals(1.0, SERVER.getAttribute(new ObjectName(topicSeries("orders")), "request-total"));
        assertEquals(0.0, self("series-evicted-total"));
    }

    @Test
    void closeWithPendingSeriesUnregistersEverything() throws Exception {
        start(Map.of());
        readTopic("orders");
        creator.runAll();
        readTopic("billing");
        readTopic("payments");

        authorizer.close();
        // A creation that was already scheduled runs after close and creates nothing.
        creator.runAll();

        assertEquals(Set.of(), beans("kfkwho:*"));
    }

    @Test
    void closeWhileTheBackgroundThreadCreatesUnregistersEverything() throws Exception {
        authorizer = new MeteredStandardAuthorizer(Time.SYSTEM);
        start(Map.of());
        for (int i = 0; i < SeriesSet.MAX_PENDING; i++) {
            readTopic("orders-" + i);
        }

        authorizer.close();

        assertEquals(Set.of(), beans("kfkwho:*"));
    }

    private void start(Map<String, String> configs) {
        Map<String, Object> all = new HashMap<>(configs);
        all.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice");
        authorizer.configure(all);
        authorizer.withPluginMetrics(new PluginMetricsImpl(pluginMetrics, Map.of()));
        authorizer.completeInitialLoad();
    }

    private List<AuthorizationResult> readTopic(String topic) {
        return authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH),
                List.of(new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic,
                        PatternType.LITERAL), 1, true, true)));
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

    /** Runs what it was given when the test says so, on the test thread. */
    private static final class ManualExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public synchronized void execute(Runnable task) {
            tasks.add(task);
        }

        void runAll() {
            for (Runnable task; (task = poll()) != null; ) {
                task.run();
            }
        }

        synchronized int size() {
            return tasks.size();
        }

        private synchronized Runnable poll() {
            return tasks.poll();
        }
    }
}
