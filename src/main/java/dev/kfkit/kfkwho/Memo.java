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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * What a function returned for each string it was asked about, so that the
 * function runs once per distinct string rather than once per request. Holds
 * at most {@code capacity} entries: when full, it is emptied and refills, so
 * a stream of distinct strings costs one call each and no more memory
 * (threads that miss at the same moment may overshoot it by one each).
 *
 * <p>A hit is one lookup and allocates nothing. Two threads that miss on the
 * same string may both call the function; it must be pure.
 */
final class Memo<V> {

    private final Function<String, V> function;
    private final int capacity;
    private final Map<String, V> values = new ConcurrentHashMap<>();

    Memo(Function<String, V> function, int capacity) {
        this.function = function;
        this.capacity = capacity;
    }

    V get(String key) {
        V value = values.get(key);
        if (value == null) {
            value = function.apply(key);
            if (values.size() >= capacity) {
                values.clear();
            }
            values.put(key, value);
        }
        return value;
    }

    /** For tests. */
    int size() {
        return values.size();
    }
}
