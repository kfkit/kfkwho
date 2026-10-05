# Performance

`authorize()` runs for every topic of every Fetch and Produce, so what the
metrics add to it is measured, not guessed. The benchmarks are in
`src/jmh/java`; run them with

```bash
./gradlew jmh
```

which writes `build/results/jmh/results.json`, allocation per call (the `gc`
profiler) included. `./gradlew check` compiles the benchmarks but does not run
them.

## What is measured

Every call authorizes one action (`READ` on a topic) for `User:alice`, client
id `billing-1`, against a `StandardAuthorizer` with plugin metrics and one
prefixed `ALLOW` ACL on `orders`, no super users: the parent walks its ACLs as
it would on a broker. Each metered scenario has a parent-only twin; the
difference is what kfkwho costs.

| Scenario | Benchmark | What it is |
|---|---|---|
| Parent alone | `AuthorizeBenchmark.parent` | the baseline |
| Steady state | `AuthorizeBenchmark.metered` | the same key every call: a consumer fetching one topic |
| First sight | `AuthorizeBenchmark.meteredChurn` vs `parentChurn` | a topic never seen before on every call, so every call creates a series |
| First sight, cap reached | `AuthorizeBenchmark.meteredChurnCapped` vs `parentChurn` | a new topic every call with `kfkwho.max.series` reached: every call goes to `__other__` |
| Threads | `ScalingBenchmark.{parent,metered}{01,04,16}` | the steady state from 1, 4 and 16 threads at once, throughput |

## Results

One run, 2026-10-05, with this change: 3 warm-up and 5 measured iterations
of 2 s, one fork. The run before it, on `main` at b9e3130, on the same
machine, is in brackets.

- Machine: cloud VM, 4 vCPU Intel Xeon @ 2.80 GHz, 15 GiB RAM, Linux 6.18.
- JDK: Temurin 17.0.20.1+1 (the Gradle toolchain), default JVM flags.
- Kafka 4.3.1 on the classpath, JMH 1.37.

Single thread, time per call (lower is better):

| Scenario | Parent | Metered | Added | Allocation, parent → metered |
|---|---:|---:|---:|---:|
| Steady state | 338 ± 53 ns | 525 ± 32 ns | **187 ns** (129) | 624 → 624 B/op, +0 (+40) |
| First sight | 349 ± 35 ns | 1 495 ± 1 612 ns | **≈ 1.1 µs** (≈ 41 µs) | 728 → 1 493 B/op (18 490) |
| First sight, cap reached | 349 ± 35 ns | 610 ± 48 ns | **261 ns** (197) | 728 → 728 B/op, +0 (+40) |

Steady state from several threads, total throughput (higher is better):

| Threads | Parent | Metered | Metered / parent |
|---:|---:|---:|---:|
| 1 | 3.03 ± 0.44 ops/µs | 1.89 ± 0.16 ops/µs | 0.62 (0.73) |
| 4 | 1.85 ± 0.05 ops/µs | 1.52 ± 0.10 ops/µs | 0.82 (0.85) |
| 16 | 2.40 ± 0.26 ops/µs | 1.59 ± 0.11 ops/µs | 0.66 (0.82) |

The steady-state difference between the two runs is within their error
bars. Run on its own (`java -jar build/libs/*-jmh.jar
'AuthorizeBenchmark\.(metered|parent)$' -prof gc`), the same build measured
459 ± 56 ns against the parent's 337 ± 53 ns: 122 ns added, as before.

## Against the budget

The budget, the same as in `AGENTS.md`: under 1 µs added per action in the
steady state and under 3 µs on the request thread on first sight of a key,
no allocation per call, under 2% broker CPU under load.

- **Steady state, 187 ns added: within budget.**
- **Allocation, 0 B/op added: within budget.** The lookup key is a mutable
  per-thread object, copied only when a series is created. (A plain class
  allocated per call measured 0 B/op too, scalar-replaced, as the record
  before it was not; the per-thread key does not depend on escape analysis.)
- **First sight, about 1.1 µs on the request thread: within budget.** The
  request thread no longer creates the series: it leaves a pending series
  that counts what arrives until the series exists, and the background
  thread `kfkwho-series` creates it, at about 40 µs each (a `Sensor` with
  three metrics, each of which makes the `JmxReporter` register the MBean
  again). The CPU is spent all the same, on that thread instead.
- **What the benchmark's churn does to the series.** It asks for a new topic
  on every call, about a million per iteration. The background thread
  creates 20 000 to 30 000 series a second, so at most
  `AccessMetrics.MAX_PENDING` (1024) wait at a time and the rest, about 95%
  in this run, are counted under `__other__` and in `series-overflow-total`.
  The figure above is the mean over both paths. A broker does not see a
  million new keys in two seconds; a burst of up to 1024 is counted in full.
- **Contention: not measurable here, and the parent does not scale either.**
  On 4 vCPUs neither the parent nor the metered authorizer gets faster with
  more threads; the parent records its own authorizer metrics into one
  `Sensor`, which is `synchronized`, and so is ours, per series. The ratios
  move between runs by more than the change could explain. A machine with 16
  or more cores would answer this properly.

What is left to change, if a measurement asks for it:

1. **Cheaper creation.** Registering the MBean once per series instead of
   once per metric would cut the background thread's cost and let more of
   a burst get its own series.
2. **Counters without a lock**, if a many-core run shows contention: a
   `LongAdder` per series read by a `Measurable` instead of a `Sensor`
   with stats, at the cost of computing the rate ourselves.

Sampling remains the last resort and a separate issue.
