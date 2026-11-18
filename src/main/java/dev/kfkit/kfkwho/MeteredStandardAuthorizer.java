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
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * The broker's {@link StandardAuthorizer} with metrics on who accesses what.
 *
 * <p>Every request the broker authorizes passes through {@link #authorize}
 * with the principal, the client id, the client address, the resource and
 * the operation. This class decides nothing itself: the verdict is the
 * parent's. It only observes, and publishes what it saw as JMX metrics
 * under the {@value AccessMetrics#JMX_PREFIX} domain.
 *
 * <p>Extending rather than wrapping is deliberate. The broker feeds ACLs
 * only to authorizers that are a {@code ClusterMetadataAuthorizer}; a plain
 * wrapper would silently never receive them.
 *
 * <p>Configure with {@code authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer}.
 * A series that saw no request for {@code kfkwho.series.ttl.seconds} (default
 * 600) disappears; past {@code kfkwho.max.series} series (default 10000),
 * new ones are folded into an overflow series. Every setting is declared in
 * {@link RecorderConfig} and listed in {@code docs/config.md}; a wrong value
 * fails {@link #configure} with a message naming the key. Series are created and
 * expired on one background thread, {@code kfkwho-series}, never on the
 * request thread.
 *
 * <p>The recording is an {@link AccessRecorder}, created in {@link #configure},
 * handed the parent's verdicts in {@link #authorize} and closed in
 * {@link #close}; another authorizer can publish the same metrics through it.
 *
 * <p>Only a broker records. On a node whose {@code process.roles} does not
 * include {@code broker}, a controller, the requests come from brokers and
 * would be noise: no metrics registry is created, no thread started and no
 * MBean registered, and {@link #authorize} is the parent's alone.
 */
public class MeteredStandardAuthorizer extends StandardAuthorizer {

    private final Time time;
    /** Where new series are created; null for the background thread. */
    private final Executor creator;
    private AccessRecorder recorder;

    public MeteredStandardAuthorizer() {
        this(Time.SYSTEM);
    }

    MeteredStandardAuthorizer(Time time) {
        this(time, null);
    }

    /** Tests pass their own creator to decide when series appear. */
    MeteredStandardAuthorizer(Time time, Executor creator) {
        this.time = time;
        this.creator = creator;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // A wrong value fails here, before the parent or any metric is set up.
        RecorderConfig.parse(configs);
        super.configure(configs);
        recorder = AccessRecorder.create(configs, time, creator);
    }

    /** Runs on the background thread; package-private so tests can drive it with their own clock. */
    void expireIdleSeries() {
        if (recorder != null) {
            recorder.expireIdleSeries();
        }
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        List<AuthorizationResult> results = super.authorize(requestContext, actions);
        AccessRecorder recorder = this.recorder;
        if (recorder != null) {
            recorder.record(requestContext, actions, results);
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (recorder != null) {
                recorder.close();
            }
        }
    }
}
