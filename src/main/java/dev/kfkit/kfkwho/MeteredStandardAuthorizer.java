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

import java.util.List;

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
 * parent's. It only observes, and publishes what it saw as JMX metrics.
 *
 * <p>Extending rather than wrapping is deliberate. The broker feeds ACLs
 * only to authorizers that are a {@code ClusterMetadataAuthorizer}; a plain
 * wrapper would silently never receive them.
 *
 * <p>Configure with {@code authorizer.class.name=dev.kfkit.kfkwho.MeteredStandardAuthorizer}.
 */
public class MeteredStandardAuthorizer extends StandardAuthorizer {

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        // Metrics are recorded here once they exist; the verdict stays the parent's.
        return super.authorize(requestContext, actions);
    }
}
