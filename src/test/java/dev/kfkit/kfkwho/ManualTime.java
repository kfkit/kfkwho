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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.kafka.common.utils.Time;

/** A clock that moves only when a test says so. */
final class ManualTime implements Time {

    private volatile long nowMs = 1_700_000_000_000L;

    void advanceSeconds(long seconds) {
        nowMs += TimeUnit.SECONDS.toMillis(seconds);
    }

    @Override
    public long milliseconds() {
        return nowMs;
    }

    @Override
    public long nanoseconds() {
        return TimeUnit.MILLISECONDS.toNanos(nowMs);
    }

    @Override
    public void sleep(long ms) {
        nowMs += ms;
    }

    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) {
        throw new UnsupportedOperationException();
    }
}
