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

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

/**
 * Every {@code kfkwho.*} setting, declared once with its type, default,
 * validator and documentation. {@code docs/config.md} lists the same keys
 * and defaults; {@code AuthorizerConfigTest} fails when the two disagree.
 *
 * <p>{@link #parse} takes the whole broker configuration, as
 * {@code configure(Map)} receives it, and reads only the keys declared
 * here. A wrong value throws {@link ConfigException} naming the key, which
 * fails broker start.
 *
 * <p>Parsing goes through {@link ConfigDef#parse} rather than an
 * {@code AbstractConfig}: the latter would also resolve the broker's
 * {@code config.providers} a second time.
 */
final class AuthorizerConfig {

    static final String TTL_SECONDS_CONFIG = "kfkwho.ttl.seconds";
    static final String MAX_SERIES_CONFIG = "kfkwho.max.series";
    static final String LABELS_CONFIG = "kfkwho.labels";
    static final String CLIENT_ID_RULES_CONFIG = "kfkwho.client.id.rules";
    static final String RESOURCE_EXCLUDE_CONFIG = "kfkwho.resource.exclude";
    static final String COUNT_DENIED_CONFIG = "kfkwho.count.denied";

    static final int DEFAULT_TTL_SECONDS = 600;
    static final int DEFAULT_MAX_SERIES = 10_000;

    /** Every label a series may carry, in the order the MBean names use. */
    static final List<String> KNOWN_LABELS = List.of("principal", "client-id", "resource-type", "resource",
            "operation", "api", "result", "listener", "security-protocol", "client-address");

    /** All known labels but {@code client-address}: its cardinality is that of the clients' addresses. */
    static final List<String> DEFAULT_LABELS = KNOWN_LABELS.stream()
            .filter(label -> !label.equals("client-address")).toList();

    /** Between the pattern and the replacement of a client id rule. */
    static final String RULE_SEPARATOR = "=>";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TTL_SECONDS_CONFIG, Type.INT, DEFAULT_TTL_SECONDS, Range.atLeast(1), Importance.MEDIUM,
                    "Seconds without a request after which a series is removed; seen again, it starts from zero.")
            .define(MAX_SERIES_CONFIG, Type.INT, DEFAULT_MAX_SERIES, Range.atLeast(1), Importance.MEDIUM,
                    "Access series that may exist at once; past it, new ones are recorded into __other__.")
            .define(LABELS_CONFIG, Type.LIST, DEFAULT_LABELS, AuthorizerConfig::validateLabels, Importance.LOW,
                    "The labels series carry, a subset of " + String.join(", ", KNOWN_LABELS) + ".")
            .define(CLIENT_ID_RULES_CONFIG, Type.LIST, List.of(), AuthorizerConfig::validateRules, Importance.LOW,
                    "Rules that rewrite a client id before it becomes a label, each pattern" + RULE_SEPARATOR
                            + "replacement in Java regex syntax; the first that matches applies.")
            .define(RESOURCE_EXCLUDE_CONFIG, Type.STRING, "__.*", AuthorizerConfig::validateRegex, Importance.LOW,
                    "Resources whose whole name matches this Java regex are not recorded.")
            .define(COUNT_DENIED_CONFIG, Type.BOOLEAN, true, Importance.LOW,
                    "Whether denied requests are recorded as well as allowed ones.");

    private final int ttlSeconds;
    private final int maxSeries;
    private final List<String> labels;
    private final List<ClientIdRule> clientIdRules;
    private final Pattern resourceExclude;
    private final boolean countDenied;

    private AuthorizerConfig(Map<String, Object> values) {
        ttlSeconds = (Integer) values.get(TTL_SECONDS_CONFIG);
        maxSeries = (Integer) values.get(MAX_SERIES_CONFIG);
        labels = List.copyOf(strings(values.get(LABELS_CONFIG)));
        clientIdRules = strings(values.get(CLIENT_ID_RULES_CONFIG)).stream().map(ClientIdRule::parse).toList();
        resourceExclude = Pattern.compile((String) values.get(RESOURCE_EXCLUDE_CONFIG));
        countDenied = (Boolean) values.get(COUNT_DENIED_CONFIG);
    }

    /** The broker configuration in, the {@code kfkwho.*} settings out; throws {@link ConfigException}. */
    static AuthorizerConfig parse(Map<String, ?> configs) {
        return new AuthorizerConfig(CONFIG_DEF.parse(configs));
    }

    int ttlSeconds() {
        return ttlSeconds;
    }

    int maxSeries() {
        return maxSeries;
    }

    List<String> labels() {
        return labels;
    }

    List<ClientIdRule> clientIdRules() {
        return clientIdRules;
    }

    Pattern resourceExclude() {
        return resourceExclude;
    }

    boolean countDenied() {
        return countDenied;
    }

    /** One {@code pattern=>replacement} entry of {@value #CLIENT_ID_RULES_CONFIG}, compiled. */
    record ClientIdRule(Pattern pattern, String replacement) {

        /** Splits at the last separator, so a pattern may contain one; assumes the entry was validated. */
        static ClientIdRule parse(String rule) {
            int at = rule.lastIndexOf(RULE_SEPARATOR);
            return new ClientIdRule(Pattern.compile(rule.substring(0, at)),
                    rule.substring(at + RULE_SEPARATOR.length()));
        }
    }

    private static void validateLabels(String name, Object value) {
        for (String label : strings(value)) {
            if (!KNOWN_LABELS.contains(label)) {
                throw new ConfigException(name, label, "Unknown label; known: " + String.join(", ", KNOWN_LABELS));
            }
        }
    }

    private static void validateRules(String name, Object value) {
        for (String rule : strings(value)) {
            int at = rule.lastIndexOf(RULE_SEPARATOR);
            if (at <= 0) {
                throw new ConfigException(name, rule, "A rule is pattern" + RULE_SEPARATOR + "replacement");
            }
            compile(name, rule, rule.substring(0, at));
        }
    }

    private static void validateRegex(String name, Object value) {
        compile(name, value, (String) value);
    }

    private static void compile(String name, Object value, String regex) {
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new ConfigException(name, value, "Not a valid Java regex: " + e.getDescription());
        }
    }

    /** A {@link Type#LIST} value, parsed by {@link ConfigDef} into strings. */
    @SuppressWarnings("unchecked")
    private static List<String> strings(Object value) {
        return (List<String>) value;
    }
}
