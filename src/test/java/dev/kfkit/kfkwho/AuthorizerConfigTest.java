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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The {@code kfkwho.*} settings: validation through {@code configure}, defaults, and the docs that list them. */
class AuthorizerConfigTest {

    /** A key row of the table in {@code docs/config.md}: key, type, default. */
    private static final Pattern ROW = Pattern.compile("^\\| `(kfkwho\\.[^`]+)` \\| (\\w+) \\| ([^|]+) \\|");
    private static final String EMPTY = "(empty)";

    private final MeteredStandardAuthorizer authorizer = new MeteredStandardAuthorizer();

    @AfterEach
    void close() throws Exception {
        authorizer.close();
    }

    @Test
    void aZeroTtlFailsConfigureNamingTheKey() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> authorizer.configure(broker(AuthorizerConfig.TTL_SECONDS_CONFIG, "0")));
        assertTrue(e.getMessage().contains("kfkwho.ttl.seconds"), e.getMessage());
    }

    @Test
    void aValueOfTheWrongTypeFailsConfigureNamingTheKey() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> authorizer.configure(broker(AuthorizerConfig.MAX_SERIES_CONFIG, "many")));
        assertTrue(e.getMessage().contains("kfkwho.max.series"), e.getMessage());
    }

    @Test
    void anUnknownLabelFailsConfigureWithTheLabel() {
        ConfigException e = assertThrows(ConfigException.class, () -> authorizer.configure(
                broker(AuthorizerConfig.LABELS_CONFIG, "principal,client-id,topic-owner")));
        assertTrue(e.getMessage().contains("topic-owner"), e.getMessage());
        assertTrue(e.getMessage().contains("kfkwho.labels"), e.getMessage());
    }

    @Test
    void anInvalidClientIdRuleFailsConfigureWithTheRule() {
        ConfigException bad = assertThrows(ConfigException.class, () -> authorizer.configure(
                broker(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, "^(consumer-.*=>$1")));
        assertTrue(bad.getMessage().contains("^(consumer-.*=>$1"), bad.getMessage());
        ConfigException noReplacement = assertThrows(ConfigException.class,
                () -> AuthorizerConfig.parse(Map.of(AuthorizerConfig.CLIENT_ID_RULES_CONFIG, "^consumer-.*")));
        assertTrue(noReplacement.getMessage().contains("^consumer-.*"), noReplacement.getMessage());
    }

    @Test
    void anInvalidExcludeRegexFailsConfigureNamingTheKey() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> authorizer.configure(broker(AuthorizerConfig.RESOURCE_EXCLUDE_CONFIG, "__[")));
        assertTrue(e.getMessage().contains("kfkwho.resource.exclude"), e.getMessage());
    }

    @Test
    void appliesEachDefaultWhenKeysAreAbsent() {
        AuthorizerConfig config = AuthorizerConfig.parse(broker());

        assertEquals(600, config.ttlSeconds());
        assertEquals(10_000, config.maxSeries());
        assertEquals(List.of("principal", "client-id", "resource-type", "resource", "operation", "api", "result",
                "listener", "security-protocol"), config.labels());
        assertEquals(AuthorizerConfig.DEFAULT_CLIENT_ID_RULES, config.clientIdRules().stream()
                .map(rule -> rule.pattern().pattern() + AuthorizerConfig.RULE_SEPARATOR + rule.replacement()).toList());
        assertEquals("__.*", config.resourceExclude().pattern());
        assertTrue(config.resourceExclude().matcher("__consumer_offsets").matches());
        assertFalse(config.resourceExclude().matcher("orders").matches());
        assertTrue(config.countDenied());
    }

    @Test
    void readsEveryKey() {
        AuthorizerConfig config = AuthorizerConfig.parse(Map.of(
                AuthorizerConfig.TTL_SECONDS_CONFIG, "30",
                AuthorizerConfig.MAX_SERIES_CONFIG, "5",
                AuthorizerConfig.LABELS_CONFIG, "principal, resource ,client-address",
                AuthorizerConfig.CLIENT_ID_RULES_CONFIG, "^(.*)-StreamThread-\\d+-(consumer|producer)$=>$1,^x=>y",
                AuthorizerConfig.RESOURCE_EXCLUDE_CONFIG, "_.*",
                AuthorizerConfig.COUNT_DENIED_CONFIG, "false"));

        assertEquals(30, config.ttlSeconds());
        assertEquals(5, config.maxSeries());
        assertEquals(List.of("principal", "resource", "client-address"), config.labels());
        assertEquals(2, config.clientIdRules().size());
        AuthorizerConfig.ClientIdRule streams = config.clientIdRules().get(0);
        assertEquals("orders-app", streams.pattern().matcher("orders-app-StreamThread-2-consumer")
                .replaceFirst(streams.replacement()));
        assertEquals("_.*", config.resourceExclude().pattern());
        assertFalse(config.countDenied());
    }

    @Test
    void docsListEveryKeyWithItsTypeAndDefault() throws IOException {
        Map<String, String> documented = new TreeMap<>();
        for (String line : Files.readAllLines(Path.of("docs/config.md"))) {
            Matcher row = ROW.matcher(line);
            if (row.find()) {
                documented.put(row.group(1), row.group(2) + " " + row.group(3).trim());
            }
        }
        Map<String, String> declared = new TreeMap<>();
        for (ConfigDef.ConfigKey key : AuthorizerConfig.CONFIG_DEF.configKeys().values()) {
            declared.put(key.name, key.type.name().toLowerCase(Locale.ROOT) + " " + rendered(key.defaultValue));
        }
        assertEquals(declared, documented);
    }

    /** A default as {@code docs/config.md} writes it. */
    private static String rendered(Object defaultValue) {
        if (defaultValue instanceof List<?> list) {
            return list.isEmpty() ? EMPTY : "`" + String.join(",", list.stream().map(String::valueOf).toList()) + "`";
        }
        return "`" + defaultValue + "`";
    }

    /** A broker configuration, unrelated keys included, with the given kfkwho settings. */
    private static Map<String, Object> broker(String... keyValues) {
        Map<String, Object> configs = new TreeMap<>(Map.of(
                StandardAuthorizer.SUPER_USERS_CONFIG, "User:alice",
                "process.roles", "broker",
                "listeners", "PLAINTEXT://:9092"));
        for (int i = 0; i < keyValues.length; i += 2) {
            configs.put(keyValues[i], keyValues[i + 1]);
        }
        return configs;
    }
}
