/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cluster group get nodes endpoints request.
 */
public class ClientClusterGroupGetNodesEndpointsRequest extends ClientRequest {
    /** Start topology version. -1 for earliest. */
    private final long startTopVer;

    /** End topology version. -1 for latest. */
    private final long endTopVer;

    /** Data center ID. */
    private final String dcId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientClusterGroupGetNodesEndpointsRequest(BinaryRawReader reader, ClientProtocolContext protocolCtx) {
        super(reader);
        startTopVer = reader.readLong();
        endTopVer = reader.readLong();

        if (protocolCtx.isFeatureSupported(ClientBitmaskFeature.DC_AWARE_REQUESTS))
            dcId = reader.readString();
        else
            dcId = null;
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        return new ClientClusterGroupGetNodesEndpointsResponse(requestId(), startTopVer, endTopVer, dcId);
    }
}
