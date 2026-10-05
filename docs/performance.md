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

One run, 2026-10-05: 3 warm-up and 5 measured iterations of 2 s, one fork.

- Machine: cloud VM, 4 vCPU Intel Xeon @ 2.80 GHz, 15 GiB RAM, Linux 6.18.
- JDK: Temurin 17.0.20.1+1 (the Gradle toolchain), default JVM flags.
- Kafka 4.3.1 on the classpath, JMH 1.37.

Single thread, time per call (lower is better):

| Scenario | Parent | Metered | Added | Allocation, parent → metered |
|---|---:|---:|---:|---:|
| Steady state | 362 ± 32 ns | 520 ± 59 ns | **158 ns** | 624 → 664 B/op (+40) |
| First sight | 427 ± 78 ns | 46 220 ± 29 680 ns | **≈ 46 µs** | 728 → 16 282 B/op |
| First sight, cap reached | 427 ± 78 ns | 635 ± 95 ns | **207 ns** | 728 → 768 B/op (+40) |

Steady state from several threads, total throughput (higher is better):

| Threads | Parent | Metered | Metered / parent |
|---:|---:|---:|---:|
| 1 | 2.70 ± 0.23 ops/µs | 2.02 ± 0.20 ops/µs | 0.75 |
| 4 | 1.39 ± 0.50 ops/µs | 1.28 ± 0.68 ops/µs | 0.92 |
| 16 | 1.86 ± 0.79 ops/µs | 1.46 ± 0.73 ops/µs | 0.78 |

## Against the budget

The budget: under 1 µs added per action in the steady state, under 3 µs on
first sight of a key, no allocation per call in the steady state, no lock
contention at 16 threads.

- **Steady state, 158 ns added: within budget.**
- **First sight, about 46 µs: over budget, by an order of magnitude.** The
  time is creating the series: a `Sensor` with three metrics, each of which
  makes the `JmxReporter` unregister and register the MBean again. It is paid
  once per series per TTL (600 s by default), not per request, but on the
  request thread. Once the cap is reached, a new key costs 207 ns.
- **Allocation, 40 B/op in the steady state: over budget.** It is the lookup
  key, a record of six references, built on every call to look up the series.
  Escape analysis does not remove it.
- **Contention: not measurable here, and the parent does not scale either.**
  On 4 vCPUs neither the parent nor the metered authorizer gets faster with
  more threads; the parent records its own authorizer metrics into one
  `Sensor`, which is `synchronized`. The metered throughput stays at 0.75–0.92
  of the parent's at every thread count, so kfkwho adds no contention of its
  own that this machine can show, while its `Sensor.record` is just as
  `synchronized` per series. A machine with 16 or more cores would answer
  this properly.

What to change, in the order they would pay off:

1. **Create series off the request thread.** On first sight, record into the
   `__other__` series (or a small pending counter) and hand the key to the
   expiry thread, which creates the sensor; the next request finds it. That
   takes first sight to the cost of the capped path, about 200 ns, at the
   price of the first request of a series being counted under `__other__`.
2. **Look up without allocating.** A per-thread mutable lookup key, copied
   into an immutable one only when a series is created, or a two-level map
   (principal and client id, then resource, operation and verdict) whose
   inner lookup needs no new object.
3. **Counters without a lock**, if a many-core run shows contention: a
   `LongAdder` per series read by a `Measurable` instead of a `Sensor`
   with stats, at the cost of computing the rate ourselves.

Sampling remains the last resort and a separate issue.
