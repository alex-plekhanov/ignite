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

package org.apache.ignite.internal.processors.platform.client.service;

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientLongResponse;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceDescriptor;

/**
 * Request to invoke service method.
 */
public class ClientServiceProxyRequest extends ClientRequest {
    /** Service name. */
    private final String name;

    /** Flags. */
    private final byte flags;

    /** Timeout. */
    private final long timeout;

    /** Nodes. */
    private final Set<UUID> nodeIds;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientServiceProxyRequest(BinaryRawReaderEx reader) {
        super(reader);

        name = reader.readString();

        flags = reader.readByte();

        timeout = reader.readLong();

        int cnt = reader.readInt();

        nodeIds = U.newHashSet(cnt);

        for (int i = 0; i < cnt; i++)
            nodeIds.add(reader.readUuid());
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        if (F.isEmpty(name))
            throw new IgniteException("Service name can't be empty");

        ServiceDescriptor desc = null;

        for (ServiceDescriptor desc0 : ctx.kernalContext().service().serviceDescriptors()) {
            if (name.equals(desc0.name())) {
                desc = desc0;

                break;
            }
        }

        if (desc == null)
            throw new IgniteException("Service not found: " + name);

        Class<?> svcCls = desc.serviceClass();

        ClusterGroupAdapter grp = ctx.kernalContext().cluster().get();

        if (ctx.securityContext() != null)
            grp = (ClusterGroupAdapter)grp.forSubjectId(ctx.securityContext().subject().id());

        grp = (ClusterGroupAdapter)(nodeIds.isEmpty() ? grp.forServers() : grp.forNodeIds(nodeIds));

        IgniteServices services = grp.services();

        Object proxy = PlatformService.class.isAssignableFrom(svcCls) ?
            services.serviceProxy(name, PlatformService.class, false, timeout) :
            new GridServiceProxy<>(grp, name, Service.class, false, timeout, ctx.kernalContext());

        long resId = ctx.resources().put(new ClientServiceProxy(proxy, svcCls, flags));

        return new ClientLongResponse(requestId(), resId);
    }
}
