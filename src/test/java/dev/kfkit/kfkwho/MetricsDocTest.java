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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanAttributeInfo;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code docs/metrics.md} against what the authorizer registers: every MBean
 * type with its tags in order and every attribute, read back from the
 * platform MBean server, and the Prometheus names against the exporter rules.
 */
class MetricsDocTest {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final Path DOC = Path.of("docs/metrics.md");
    private static final Path RULES = Path.of("jmx-exporter/kfkwho.yml");
    /** A row of the MBeans table: type, then its tags in order or {@code none}. */
    private static final Pattern MBEAN_ROW = Pattern.compile("^\\| `kfkwho:type=([a-z-]+)` \\| ([^|]+) \\|$");
    /** A row of the attributes table: type, attribute, Prometheus name or {@code not exported}. */
    private static final Pattern ATTRIBUTE_ROW =
            Pattern.compile("^\\| `kfkwho:type=([a-z-]+)` \\| `([a-z-]+)` \\| ([^|]+) \\|");
    private static final Pattern RULE_NAME = Pattern.compile("^\\s+name: (\\S+)\\s*$");
    private static final String NOT_EXPORTED = "not exported";

    private final Metrics pluginMetrics = new Metrics();
    private final MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer(Time.SYSTEM, Runnable::run);

    @AfterEach
    void stop() throws Exception {
        authorizer.close();
        pluginMetrics.close();
    }

    @Test
    void docsListEveryMBeanTypeWithItsTagsInOrder() throws Exception {
        registerEveryType();
        Map<String, List<String>> registered = new TreeMap<>();
        for (ObjectName name : SERVER.queryNames(new ObjectName("kfkwho:*"), null)) {
            List<String> tags = new ArrayList<>();
            // The key property list as registered, in its original order; no value here holds a comma.
            for (String property : name.getKeyPropertyListString().split(",")) {
                tags.add(property.substring(0, property.indexOf('=')));
            }
            assertEquals("type", tags.remove(0), name.toString());
            List<String> previous = registered.put(name.getKeyProperty("type"), tags);
            assertTrue(previous == null || previous.equals(tags), name + " has tags " + tags + ", not " + previous);
        }

        assertEquals(registered, documentedMBeans());
    }

    @Test
    void docsListEveryAttributeOfEveryType() throws Exception {
        registerEveryType();
        Set<String> registered = new TreeSet<>();
        for (ObjectName name : SERVER.queryNames(new ObjectName("kfkwho:*"), null)) {
            for (MBeanAttributeInfo attribute : SERVER.getMBeanInfo(name).getAttributes()) {
                registered.add(name.getKeyProperty("type") + " " + attribute.getName());
            }
        }

        assertEquals(registered, documentedAttributes().keySet());
    }

    @Test
    void thePrometheusNamesAreTheOnesTheRulesProduce() throws IOException {
        Set<String> rules = new TreeSet<>();
        for (String line : Files.readAllLines(RULES)) {
            Matcher name = RULE_NAME.matcher(line);
            if (name.matches()) {
                rules.add(name.group(1));
            }
        }
        Set<String> documented = new TreeSet<>();
        for (String prometheus : documentedAttributes().values()) {
            if (!prometheus.equals(NOT_EXPORTED)) {
                documented.add(prometheus.replace("`", ""));
            }
        }

        assertEquals(rules, documented);
    }

    /**
     * Configures every optional tag on and a cap of one, then sends two
     * clients, so that each type has a series and an overflow series.
     */
    private void registerEveryType() throws Exception {
        authorizer.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice",
                AuthorizerConfig.LABELS_CONFIG, String.join(",", AuthorizerConfig.KNOWN_LABELS),
                AuthorizerConfig.MAX_SERIES_CONFIG, "1"));
        authorizer.withPluginMetrics(new PluginMetricsImpl(pluginMetrics, Map.of()));
        authorizer.completeInitialLoad();
        List<Action> readOrders = List.of(new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL), 1, true, true));
        authorizer.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH)
                .from(InetAddress.getByName("192.0.2.10")), readOrders);
        authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.FETCH)
                .from(InetAddress.getByName("192.0.2.11")), readOrders);
        assertEquals(Set.of("access", "client", "authorizer"), types());
        assertEquals(2, SERVER.queryNames(new ObjectName("kfkwho:type=access,*"), null).size());
        assertEquals(2, SERVER.queryNames(new ObjectName("kfkwho:type=client,*"), null).size());
    }

    private static Set<String> types() throws Exception {
        Set<String> types = new TreeSet<>();
        for (ObjectName name : SERVER.queryNames(new ObjectName("kfkwho:*"), null)) {
            types.add(name.getKeyProperty("type"));
        }
        return types;
    }

    private static Map<String, List<String>> documentedMBeans() throws IOException {
        Map<String, List<String>> documented = new TreeMap<>();
        for (String line : section("MBeans")) {
            Matcher row = MBEAN_ROW.matcher(line);
            if (row.matches()) {
                String tags = row.group(2).trim();
                documented.put(row.group(1), tags.equals("none") ? List.of()
                        : Arrays.stream(tags.split(",")).map(tag -> tag.trim().replace("`", "")).toList());
            }
        }
        return documented;
    }

    /** {@code type attribute} to the Prometheus column. */
    private static Map<String, String> documentedAttributes() throws IOException {
        Map<String, String> documented = new TreeMap<>();
        for (String line : section("Attributes")) {
            Matcher row = ATTRIBUTE_ROW.matcher(line);
            if (row.find()) {
                documented.put(row.group(1) + " " + row.group(2), row.group(3).trim());
            }
        }
        return documented;
    }

    /** The lines under the {@code ##} heading of that name, up to the next one. */
    private static List<String> section(String heading) throws IOException {
        List<String> lines = Files.readAllLines(DOC);
        int start = lines.indexOf("## " + heading);
        assertTrue(start >= 0, "no section " + heading + " in " + DOC);
        List<String> section = new ArrayList<>();
        for (String line : lines.subList(start + 1, lines.size())) {
            if (line.startsWith("## ")) {
                break;
            }
            section.add(line);
        }
        return section;
    }
}
