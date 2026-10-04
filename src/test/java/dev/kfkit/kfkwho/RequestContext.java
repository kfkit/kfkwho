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

import java.net.InetAddress;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

/** A request as the broker would describe it to the authorizer. */
record RequestContext(KafkaPrincipal principal, String clientId, InetAddress clientAddress, ApiKeys api)
        implements AuthorizableRequestContext {

    static RequestContext of(String user, String clientId, ApiKeys api) {
        return new RequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), clientId,
                InetAddress.getLoopbackAddress(), api);
    }

    @Override
    public String listenerName() {
        return "PLAINTEXT";
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    @Override
    public int requestType() {
        return api.id;
    }

    @Override
    public int requestVersion() {
        return api.latestVersion();
    }

    @Override
    public int correlationId() {
        return 0;
    }
}
