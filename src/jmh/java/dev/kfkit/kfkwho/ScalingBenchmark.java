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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Throughput of the steady state, the same key from every thread, at 1, 4
 * and 16 threads: request handler threads authorizing the same client and
 * topic at once. Contention on the one series shows as metered throughput
 * that stops growing while the parent's still grows.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class ScalingBenchmark {

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

    @Benchmark
    @Threads(1)
    public List<AuthorizationResult> parent01() {
        return parent.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }

    @Benchmark
    @Threads(4)
    public List<AuthorizationResult> parent04() {
        return parent.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }

    @Benchmark
    @Threads(16)
    public List<AuthorizationResult> parent16() {
        return parent.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }

    @Benchmark
    @Threads(1)
    public List<AuthorizationResult> metered01() {
        return metered.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }

    @Benchmark
    @Threads(4)
    public List<AuthorizationResult> metered04() {
        return metered.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }

    @Benchmark
    @Threads(16)
    public List<AuthorizationResult> metered16() {
        return metered.authorize(Fixture.ALICE, Fixture.READ_ORDERS);
    }
}
