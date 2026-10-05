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

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/** The bound on what is remembered of client ids and resource names. */
class MemoTest {

    @Test
    void callsTheFunctionOncePerKeyWhileItFits() {
        AtomicInteger calls = new AtomicInteger();
        Memo<String> memo = new Memo<>(key -> {
            calls.incrementAndGet();
            return key.toUpperCase(Locale.ROOT);
        }, 3);

        for (int i = 0; i < 5; i++) {
            assertEquals("BILLING", memo.get("billing"));
            assertEquals("ORDERS", memo.get("orders"));
        }

        assertEquals(2, calls.get());
    }

    @Test
    void neverHoldsMoreThanItsCapacity() {
        AtomicInteger calls = new AtomicInteger();
        Memo<Integer> memo = new Memo<>(key -> calls.incrementAndGet(), 100);

        for (int i = 0; i < 10_000; i++) {
            memo.get("billing-" + i);
            assertTrue(memo.size() <= 100, String.valueOf(memo.size()));
        }

        assertEquals(10_000, calls.get());
    }
}
