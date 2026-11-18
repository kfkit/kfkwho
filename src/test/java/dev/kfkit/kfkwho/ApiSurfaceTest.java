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

import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * A public type or member is a compatibility promise: the package's public
 * surface is exactly what {@code docs/embedding.md} lists under "What is
 * stable", read from the classes that ship.
 */
class ApiSurfaceTest {

    private static final Path DOC = Path.of("docs", "embedding.md");
    private static final String PACKAGE = AccessRecorder.class.getPackageName();
    /** An entry of the nested list: a type, a constructor or a member, simple names only. */
    private static final Pattern ENTRY = Pattern.compile("^\\s+- `([A-Z][A-Za-z0-9_.]*(?:\\([A-Za-z0-9_, ]*\\))?)`$",
            Pattern.MULTILINE);

    @Test
    void thePublicSurfaceIsWhatTheDocsList() throws Exception {
        assertEquals(listed(), shipped());
    }

    @Test
    void theRecorderIsFinalWithThreeMembers() {
        assertTrue(Modifier.isFinal(AccessRecorder.class.getModifiers()));
        assertTrue(Modifier.isPublic(AccessRecorder.class.getModifiers()));
        assertEquals(Set.of("AccessRecorder.create(Map)", "AccessRecorder.close()",
                "AccessRecorder.record(AuthorizableRequestContext, List, List)"), members(AccessRecorder.class));
    }

    /** What the section "What is stable" of the doc lists. */
    private static Set<String> listed() throws Exception {
        String doc = Files.readString(DOC);
        int start = doc.indexOf("## What is stable");
        assertTrue(start >= 0, DOC + " has no section \"What is stable\"");
        int end = doc.indexOf("\n## ", start + 1);
        Matcher entry = ENTRY.matcher(doc.substring(start, end < 0 ? doc.length() : end));
        Set<String> listed = new TreeSet<>();
        while (entry.find()) {
            listed.add(entry.group(1));
        }
        return listed;
    }

    /** Every public type of the package, from the compiled classes, and the public and protected members of each. */
    private static Set<String> shipped() throws Exception {
        Path root = Path.of(AccessRecorder.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path dir = root.resolve(PACKAGE.replace('.', '/'));
        assertTrue(Files.isDirectory(dir), "the main classes are not a directory: " + root);
        Set<String> shipped = new TreeSet<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.filter(f -> f.toString().endsWith(".class")).toList()) {
                String name = file.getFileName().toString();
                Class<?> type = Class.forName(PACKAGE + "." + name.substring(0, name.length() - ".class".length()),
                        false, ApiSurfaceTest.class.getClassLoader());
                if (exposed(type)) {
                    shipped.add(type.getSimpleName());
                    shipped.addAll(members(type));
                }
            }
        }
        return shipped;
    }

    /** Public, and so is every type that encloses it. */
    private static boolean exposed(Class<?> type) {
        for (Class<?> t = type; t != null; t = t.getEnclosingClass()) {
            if (!Modifier.isPublic(t.getModifiers()) || t.isSynthetic() || t.isAnonymousClass()) {
                return false;
            }
        }
        return true;
    }

    private static Set<String> members(Class<?> type) {
        Set<String> members = new TreeSet<>();
        Stream.of(type.getDeclaredConstructors(), type.getDeclaredMethods(), type.getDeclaredFields())
                .flatMap(Arrays::stream)
                .filter(member -> !member.isSynthetic() && !(member instanceof Method m && m.isBridge()))
                .filter(member -> (member.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0)
                .forEach(member -> members.add(signature(type, member)));
        return members;
    }

    private static String signature(Class<?> type, Member member) {
        if (member instanceof Field) {
            return type.getSimpleName() + "." + member.getName();
        }
        String parameters = Arrays.stream(((Executable) member).getParameterTypes()).map(Class::getSimpleName)
                .collect(Collectors.joining(", ", "(", ")"));
        return member instanceof Method ? type.getSimpleName() + "." + member.getName() + parameters
                : type.getSimpleName() + parameters;
    }
}
