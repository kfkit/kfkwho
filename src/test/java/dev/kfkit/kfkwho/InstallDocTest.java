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
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.junit.jupiter.api.Test;

/** The class name operators copy from the README and {@code docs/} into their broker configuration. */
class InstallDocTest {

    private static final Pattern CLASS_NAME = Pattern.compile("dev\\.kfkit\\.kfkwho\\.[A-Za-z0-9_.]+[A-Za-z0-9_]");

    @Test
    void theDocsNameTheAuthorizerThatShips() throws Exception {
        Set<String> named = new TreeSet<>();
        try (Stream<Path> docs = Files.list(Path.of("docs"))) {
            for (Path doc : Stream.concat(Stream.of(Path.of("README.md")), docs).toList()) {
                named.addAll(classNames(doc));
            }
        }

        assertEquals(Set.of(MeteredStandardAuthorizer.class.getName()), named);
        Class<?> authorizer = Class.forName(named.iterator().next());
        assertTrue(Modifier.isPublic(authorizer.getModifiers()), authorizer + " is not public");
        assertTrue(StandardAuthorizer.class.isAssignableFrom(authorizer), authorizer + " is no StandardAuthorizer");
    }

    private static Set<String> classNames(Path doc) throws IOException {
        Set<String> names = new TreeSet<>();
        Matcher name = CLASS_NAME.matcher(Files.readString(doc));
        while (name.find()) {
            names.add(name.group());
        }
        return names;
    }
}
