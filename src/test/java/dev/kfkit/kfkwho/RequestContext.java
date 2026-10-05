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

/** A request as the broker would describe it to the authorizer; by default over a PLAINTEXT listener of that name. */
record RequestContext(KafkaPrincipal principal, String clientId, InetAddress clientAddress, int requestType,
        String listenerName, SecurityProtocol securityProtocol) implements AuthorizableRequestContext {

    RequestContext(KafkaPrincipal principal, String clientId, InetAddress clientAddress, int requestType) {
        this(principal, clientId, clientAddress, requestType, "PLAINTEXT", SecurityProtocol.PLAINTEXT);
    }

    static RequestContext of(String user, String clientId, ApiKeys api) {
        return of(user, clientId, api.id);
    }

    /** A request whose type may be an id no {@link ApiKeys} has, as from a newer client. */
    static RequestContext of(String user, String clientId, int requestType) {
        return new RequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), clientId,
                InetAddress.getLoopbackAddress(), requestType);
    }

    /** The same request over another listener and security protocol. */
    RequestContext over(String listener, SecurityProtocol protocol) {
        return new RequestContext(principal, clientId, clientAddress, requestType, listener, protocol);
    }

    /** The same request from another address. */
    RequestContext from(InetAddress address) {
        return new RequestContext(principal, clientId, address, requestType, listenerName, securityProtocol);
    }

    @Override
    public int requestVersion() {
        return ApiKeys.hasId(requestType) ? ApiKeys.forId(requestType).latestVersion() : 0;
    }

    @Override
    public int correlationId() {
        return 0;
    }
}
