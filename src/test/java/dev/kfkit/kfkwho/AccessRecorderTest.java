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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The recorder as another authorizer uses it: {@link OverridingAuthorizer}
 * decides for itself and hands its verdicts to an {@link AccessRecorder}.
 */
class AccessRecorderTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final String THREAD = "kfkwho-series";
    private static final List<Action> READ_ORDERS = List.of(new Action(AclOperation.READ,
            new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL), 1, true, true));

    private final Metrics pluginMetrics = new Metrics();
    private StandardAuthorizer authorizer;
    private AccessRecorder recorder;

    @AfterEach
    void stop() throws Exception {
        if (authorizer != null) {
            authorizer.close();
        }
        if (recorder != null) {
            recorder.close();
        }
        pluginMetrics.close();
    }

    @Test
    void recordsTheVerdictItIsHandedNotTheParents() throws Exception {
        // AC2: the parent denies carol, who has no ACL and is no super user; the authorizer allows her.
        start(new OverridingAuthorizer(configs -> AccessRecorder.create(configs, Time.SYSTEM, Runnable::run)),
                Map.of());

        assertEquals(List.of(AuthorizationResult.ALLOWED),
                authorizer.authorize(RequestContext.of("carol", "billing-1", ApiKeys.PRODUCE), READ_ORDERS));
        assertEquals(List.of(AuthorizationResult.DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.PRODUCE), READ_ORDERS));
        assertEquals(List.of(AuthorizationResult.ALLOWED),
                authorizer.authorize(RequestContext.of("alice", "billing-3", ApiKeys.PRODUCE), READ_ORDERS));

        assertEquals(Set.of("ALLOWED"), results("User:carol"));
        assertEquals(Set.of("DENIED"), results("User:bob"));
        assertEquals(Set.of("ALLOWED"), results("User:alice"));
    }

    @Test
    void theOtherAuthorizerStaysAClusterMetadataAuthorizer() {
        // AC2: extend, don't wrap, for the embedding authorizer too.
        assertTrue(ClusterMetadataAuthorizer.class.isInstance(new OverridingAuthorizer()));
    }

    @Test
    void recordsThroughThePublicFactory() throws Exception {
        // The factory another package can call: series appear on the background thread.
        start(new OverridingAuthorizer(), Map.of());
        authorizer.authorize(RequestContext.of("carol", "billing-1", ApiKeys.PRODUCE), READ_ORDERS);

        long deadline = System.nanoTime() + 10_000_000_000L;
        while (results("User:carol").isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(Set.of("ALLOWED"), results("User:carol"));
        assertTrue(seriesThreadRuns());
    }

    @Test
    void anAuthorizerWithoutARecorderHasNoSideEffect() throws Exception {
        // AC4: loading and initialising the classes registers nothing and starts nothing.
        Class.forName(AccessRecorder.class.getName(), true, getClass().getClassLoader());
        Class.forName(MeteredStandardAuthorizer.class.getName(), true, getClass().getClassLoader());
        start(new OverridingAuthorizer(null), Map.of());
        authorizer.authorize(RequestContext.of("carol", "billing-1", ApiKeys.PRODUCE), READ_ORDERS);
        authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.PRODUCE), READ_ORDERS);

        assertEquals(Set.of(), beans("kfkwho:*"));
        assertEquals(false, seriesThreadRuns());
    }

    @Test
    void onANodeThatIsNoBrokerRecordIsANoOp() throws Exception {
        // AC5
        recorder = AccessRecorder.create(Map.of(RecorderConfig.PROCESS_ROLES_CONFIG, "controller"));
        recorder.record(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), READ_ORDERS,
                List.of(AuthorizationResult.ALLOWED));
        recorder.record(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), READ_ORDERS,
                List.of(AuthorizationResult.DENIED));

        assertEquals(Set.of(), beans("kfkwho:*"));
        assertEquals(false, seriesThreadRuns());
        recorder.close();
    }

    @Test
    void closeUnregistersEverythingStopsTheThreadAndMayBeRepeated() throws Exception {
        // AC6
        recorder = AccessRecorder.create(Map.of(), Time.SYSTEM, Runnable::run);
        recorder.record(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), READ_ORDERS,
                List.of(AuthorizationResult.ALLOWED));
        assertEquals(Set.of("access", "client", "series"), types());
        assertTrue(seriesThreadRuns());

        recorder.close();

        assertEquals(Set.of(), beans("kfkwho:*"));
        assertEquals(false, seriesThreadRuns());
        assertDoesNotThrow(recorder::close);
        // Nothing appears for what is handed to a closed recorder.
        recorder.record(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), READ_ORDERS,
                List.of(AuthorizationResult.DENIED));
        assertEquals(Set.of(), beans("kfkwho:*"));
    }

    @Test
    void theFactoryNamesAWrongKey() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> AccessRecorder.create(Map.of(RecorderConfig.MAX_SERIES_CONFIG, "many")));
        assertTrue(e.getMessage().contains(RecorderConfig.MAX_SERIES_CONFIG), e.getMessage());
        assertEquals(Set.of(), beans("kfkwho:*"));
        assertEquals(false, seriesThreadRuns());
    }

    private void start(StandardAuthorizer authorizer, Map<String, Object> configs) {
        this.authorizer = authorizer;
        Map<String, Object> all = new HashMap<>(configs);
        all.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice");
        authorizer.configure(all);
        authorizer.withPluginMetrics(new PluginMetricsImpl(pluginMetrics, Map.of()));
        authorizer.completeInitialLoad();
    }

    /** The `result` tags of the access series of one principal. */
    private static Set<String> results(String principal) throws Exception {
        Set<String> results = new TreeSet<>();
        for (ObjectName name : SERVER.queryNames(
                new ObjectName("kfkwho:type=access,principal=" + ObjectName.quote(principal) + ",*"), null)) {
            results.add(name.getKeyProperty("result"));
        }
        return results;
    }

    private static Set<String> types() throws Exception {
        Set<String> types = new TreeSet<>();
        for (ObjectName name : SERVER.queryNames(new ObjectName("kfkwho:*"), null)) {
            types.add(name.getKeyProperty("type"));
        }
        return types;
    }

    private static Set<ObjectName> beans(String pattern) {
        try {
            return SERVER.queryNames(new ObjectName(pattern), null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static boolean seriesThreadRuns() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().equals(THREAD) && thread.isAlive());
    }
}
