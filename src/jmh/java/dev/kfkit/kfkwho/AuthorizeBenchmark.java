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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * What one {@code authorize()} of one action costs, single-threaded, in
 * nanoseconds. Subtract the matching {@code parent*} score to get what the
 * metrics add.
 *
 * <ul>
 * <li>{@code parent}, {@code metered}: the same key every call, the steady
 * state of a consumer fetching one topic.</li>
 * <li>{@code parentChurn}, {@code meteredChurn}: a topic never seen before on
 * every call, so every call creates a series; the authorizer is replaced each
 * iteration so the series do not pile up across the run.</li>
 * <li>{@code meteredChurnCapped}: a new topic every call with the cap already
 * reached, so every call goes to the overflow series.</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class AuthorizeBenchmark {

    @State(Scope.Benchmark)
    public static class Steady {
        StandardAuthorizer parent;
        MeteredStandardAuthorizer metered;

        @Setup(Level.Trial)
        public void setUp() {
            parent = Fixture.parent();
            metered = Fixture.metered(AuthorizerConfig.DEFAULT_MAX_SERIES);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            parent.close();
            metered.close();
        }
    }

    @State(Scope.Benchmark)
    public static class Churn {
        StandardAuthorizer parent;
        MeteredStandardAuthorizer metered;
        MeteredStandardAuthorizer capped;
        long next;

        @Setup(Level.Iteration)
        public void setUp() {
            parent = Fixture.parent();
            metered = Fixture.metered(Integer.MAX_VALUE);
            capped = Fixture.metered(1);
            capped.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            parent.close();
            metered.close();
            capped.close();
        }

        /** A topic no call has asked about yet; the name is built the same way for parent and metered. */
        List<Action> fresh() {
            return Fixture.read("orders-" + next++);
        }
    }

    @Benchmark
    public List<AuthorizationResult> parent(Steady state) {
        return state.parent.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }

    @Benchmark
    public List<AuthorizationResult> metered(Steady state) {
        return state.metered.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }

    @Benchmark
    public List<AuthorizationResult> parentChurn(Churn state) {
        return state.parent.authorize(Fixture.ALICE, state.fresh());
    }

    @Benchmark
    public List<AuthorizationResult> meteredChurn(Churn state) {
        return state.metered.authorize(Fixture.ALICE, state.fresh());
    }

    @Benchmark
    public List<AuthorizationResult> meteredChurnCapped(Churn state) {
        return state.capped.authorize(Fixture.ALICE, state.fresh());
    }
}
