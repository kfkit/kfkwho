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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * Another authorizer, as {@code docs/embedding.md} shows one: it extends
 * {@link StandardAuthorizer}, so that the broker feeds it ACLs, decides
 * differently for one principal, and publishes its own verdicts through an
 * {@link AccessRecorder}. Here {@code User:carol} is allowed everything,
 * whatever the ACLs say.
 */
class OverridingAuthorizer extends StandardAuthorizer {

    static final String ALLOWED_PRINCIPAL = "User:carol";

    /** Creates the recorder from the broker configuration; null for an authorizer without one. */
    private final Function<Map<String, ?>, AccessRecorder> recorders;
    private AccessRecorder recorder;

    /** Records through {@link AccessRecorder#create(Map)}, as an authorizer outside this package would. */
    OverridingAuthorizer() {
        this(AccessRecorder::create);
    }

    OverridingAuthorizer(Function<Map<String, ?>, AccessRecorder> recorders) {
        this.recorders = recorders;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        if (recorders != null) {
            recorder = recorders.apply(configs);
        }
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext context, List<Action> actions) {
        List<AuthorizationResult> results = decide(context, actions);
        if (recorder != null) {
            recorder.record(context, actions, results);
        }
        return results;
    }

    private List<AuthorizationResult> decide(AuthorizableRequestContext context, List<Action> actions) {
        if (context.principal().toString().equals(ALLOWED_PRINCIPAL)) {
            return Collections.nCopies(actions.size(), AuthorizationResult.ALLOWED);
        }
        return super.authorize(context, actions);
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
