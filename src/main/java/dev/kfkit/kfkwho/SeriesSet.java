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

import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;

/**
 * The series of one type, {@code access} or {@code client}, and what bounds
 * them: the TTL, the cap and the overflow series past it, creation off the
 * request thread. What a series is called and which tags it carries is the
 * caller's, through {@link Sensors}.
 *
 * <p>The number of series is bounded twice. A series that saw no request
 * for the TTL is removed by {@link #expire}; seen again, it starts from zero.
 * Once {@code maxSeries} series exist, new keys are recorded into an
 * {@value AccessMetrics#OTHER} series instead, one per overflow slot (the
 * access series have one per verdict). Both are reported under
 * {@code kfkwho:type=authorizer} as {@code <prefix>series-count},
 * {@code <prefix>series-evicted-total} and {@code <prefix>series-overflow-total}.
 *
 * <p>The request thread never creates a series: creating one registers
 * MBeans and costs tens of microseconds. On first sight of a key it leaves a
 * pending series and hands it to the {@code creator}; requests that arrive
 * before the series exists are counted in the pending series and recorded
 * into it once it is created, so each is counted once. At most
 * {@value #MAX_PENDING} series wait at a time; past that a new key is
 * recorded into overflow, as past the cap. A pending series counts against
 * the cap but not in {@code series-count}.
 *
 * <p>A request that finds its series takes no lock of ours and allocates
 * nothing: the lookup key is the caller's per-thread probe, copied only when
 * a series is created.
 *
 * @param <K> the identity of a series; equal keys are one series
 */
final class SeriesSet<K extends SeriesSet.Key<K>> {

    /** Series waiting for the creator, at most; a burst of new keys beyond it goes to overflow. */
    static final int MAX_PENDING = 1024;

    /** A series identity with value equality; the probe the caller sets per request is copied for keeping. */
    interface Key<K> {
        /** A copy that is never changed again. */
        K copy();
    }

    /** How the caller's series become sensors; called on the creator and on first overflow. */
    interface Sensors<K> {
        Sensor sensor(K key);

        Sensor overflow(int slot);
    }

    private final Metrics metrics;
    private final int maxSeries;
    private final Executor creator;
    private final Sensors<K> sensors;
    private final Map<K, Series<K>> seriesByKey = new ConcurrentHashMap<>();
    private final Queue<Series<K>> pending = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingCount = new AtomicInteger();
    private final AtomicBoolean createScheduled = new AtomicBoolean();
    /** Guards creating a series against {@link #close}: none is registered after it. */
    private final Object createLock = new Object();
    private boolean closed;
    /** The overflow series, by slot; created on the first overflow. */
    private final AtomicReferenceArray<Sensor> overflow;
    private final Sensor evicted;
    private final Sensor overflowed;

    /**
     * Under concurrent first sightings {@code maxSeries} may be overshot by
     * the number of request handler threads.
     *
     * @param prefix what the self metrics of this type start with: empty for access, {@code client-}
     * @param what the series, capitalised, for the descriptions of the self metrics
     */
    SeriesSet(Metrics metrics, String prefix, String what, int maxSeries, int overflowSlots, Executor creator,
            Sensors<K> sensors) {
        this.metrics = metrics;
        this.maxSeries = maxSeries;
        this.creator = creator;
        this.sensors = sensors;
        this.overflow = new AtomicReferenceArray<>(overflowSlots);
        String self = AccessMetrics.SELF_GROUP;
        metrics.addMetric(metrics.metricName(prefix + "series-count", self, what + " series that exist, overflow aside"),
                (Measurable) (config, nowMs) -> Math.max(0, seriesByKey.size() - pendingCount.get()));
        evicted = metrics.sensor(self + ":" + prefix + "series-evicted");
        evicted.add(metrics.metricName(prefix + "series-evicted-total", self, what + " series removed after the TTL"),
                new CumulativeCount());
        overflowed = metrics.sensor(self + ":" + prefix + "series-overflow");
        overflowed.add(metrics.metricName(prefix + "series-overflow-total", self,
                "Requests recorded into the " + what.toLowerCase(Locale.ROOT)
                        + " overflow series because the cap was reached"
                        + " or too many series were waiting to be created"), new CumulativeCount());
    }

    /**
     * Records into the series of the probe, created or pending; one lookup.
     * False when there is none, and nothing was recorded.
     */
    boolean recordExisting(K probe, long nowMs) {
        Series<K> series = seriesByKey.get(probe);
        if (series == null) {
            return false;
        }
        series.record(nowMs);
        return true;
    }

    /**
     * On the request thread, after {@link #recordExisting} found nothing:
     * leaves a pending series for the creator, or records into the overflow
     * series of the slot.
     */
    void firstSight(K probe, int slot, long nowMs) {
        if (seriesByKey.size() >= maxSeries) {
            overflow(slot, nowMs);
            return;
        }
        if (pendingCount.incrementAndGet() > MAX_PENDING) {
            pendingCount.decrementAndGet();
            overflow(slot, nowMs);
            return;
        }
        K key = probe.copy();
        Series<K> series = new Series<>(key);
        Series<K> raced = seriesByKey.putIfAbsent(key, series);
        if (raced != null) {
            pendingCount.decrementAndGet();
            raced.record(nowMs);
            return;
        }
        series.record(nowMs);
        pending.add(series);
        if (!createScheduled.get() && createScheduled.compareAndSet(false, true)) {
            try {
                creator.execute(this::createPending);
            } catch (RejectedExecutionException e) {
                // Closed; nothing will be created any more.
            }
        }
    }

    private void overflow(int slot, long nowMs) {
        overflowSensor(slot).record(nowMs, nowMs);
        overflowed.record(1, nowMs);
    }

    private Sensor overflowSensor(int slot) {
        Sensor sensor = overflow.get(slot);
        if (sensor == null) {
            synchronized (overflow) {
                sensor = overflow.get(slot);
                if (sensor == null) {
                    sensor = sensors.overflow(slot);
                    overflow.set(slot, sensor);
                }
            }
        }
        return sensor;
    }

    /**
     * On the creator: creates the pending series and records what each
     * counted meanwhile. A series queued after this started may be left to
     * the run its request scheduled, so that a stream of new keys cannot hold
     * the thread that also expires series.
     */
    void createPending() {
        createScheduled.set(false);
        Series<K> series;
        for (int i = 0; i < MAX_PENDING && (series = pending.poll()) != null; i++) {
            try {
                synchronized (createLock) {
                    if (closed) {
                        return;
                    }
                    series.sensor = sensors.sensor(series.key);
                }
                series.flush();
            } catch (RuntimeException e) {
                // What it counted is lost; the next request for the key tries again.
                seriesByKey.remove(series.key, series);
            } finally {
                pendingCount.decrementAndGet();
            }
        }
    }

    /**
     * Removes the series, overflow included, that saw no request for the
     * TTL. A request racing with the removal of its series may be recorded
     * into the removed one and lost; that series had been idle for the TTL.
     * A pending series is left to the creator.
     */
    void expire() {
        for (Map.Entry<K, Series<K>> entry : seriesByKey.entrySet()) {
            Sensor sensor = entry.getValue().sensor;
            if (sensor != null && sensor.hasExpired() && seriesByKey.remove(entry.getKey(), entry.getValue())) {
                metrics.removeSensor(sensor.name());
                evicted.record(1);
            }
        }
        for (int i = 0; i < overflow.length(); i++) {
            Sensor sensor = overflow.get(i);
            if (sensor != null && sensor.hasExpired() && overflow.compareAndSet(i, sensor, null)) {
                metrics.removeSensor(sensor.name());
            }
        }
    }

    /** No series is created after this returns; closing {@link Metrics} removes those that were. */
    void close() {
        synchronized (createLock) {
            closed = true;
        }
        pending.clear();
    }

    /**
     * A series, created or pending. Until the creator sets {@link #sensor},
     * requests are counted in {@link #pending}; the creator then swaps in
     * {@link #CREATED} and records the count, so a request is counted either
     * there or directly into the sensor, never both and never neither.
     */
    private static final class Series<K> {
        private static final long CREATED = Long.MIN_VALUE;

        final K key;
        volatile Sensor sensor;
        private final AtomicLong pending = new AtomicLong();
        /** The time of the latest request counted in {@link #pending}. */
        private volatile long pendingMs;

        Series(K key) {
            this.key = key;
        }

        void record(long nowMs) {
            Sensor s = sensor;
            if (s == null) {
                pendingMs = nowMs;
                for (long n = pending.get(); n != CREATED; n = pending.get()) {
                    if (pending.compareAndSet(n, n + 1)) {
                        return;
                    }
                }
                // Created between the two reads; the sensor was set before CREATED.
                s = sensor;
            }
            // CumulativeCount and WindowedCount count records; Value keeps the time.
            s.record(nowMs, nowMs);
        }

        /** After {@link #sensor} is set. */
        void flush() {
            long n = pending.getAndSet(CREATED);
            long ms = pendingMs;
            for (long i = 0; i < n; i++) {
                sensor.record(ms, ms);
            }
        }
    }
}
