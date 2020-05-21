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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientObjectResponse;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.processors.platform.services.PlatformServices;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.service.GridServiceProxy;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceDescriptor;

/**
 * Request to invoke service method.
 */
public class ClientServiceInvokeRequest extends ClientRequest {
    /** Flag keep binary mask. */
    private static final byte FLAG_KEEP_BINARY_MASK = 0x01;

    /** Methods cache. */
    private static final Map<MethodDescriptor, Method> methodsCache = new ConcurrentHashMap<>();

    /** Service name. */
    private final String name;

    /** Flags. */
    private final byte flags;

    /** Timeout. */
    private final long timeout;

    /** Nodes. */
    private final Set<UUID> nodeIds;

    /** Method name. */
    private final String methodName;

    /** Method parameter type IDs. */
    private int[] paramTypeIds;

    /** Service arguments. */
    private final Object[] args;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientServiceInvokeRequest(BinaryRawReaderEx reader) {
        super(reader);

        name = reader.readString();

        flags = reader.readByte();

        timeout = reader.readLong();

        int cnt = reader.readInt();

        nodeIds = U.newHashSet(cnt);

        for (int i = 0; i < cnt; i++)
            nodeIds.add(reader.readUuid());

        methodName = reader.readString();

        paramTypeIds = reader.readIntArray();

        int argCnt = reader.readInt();

        args = new Object[argCnt];

        for (int i = 0; i < argCnt; i++)
            args[i] = reader.readObjectDetached();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        if (F.isEmpty(name))
            throw new IgniteException("Service name can't be empty");

        if (F.isEmpty(methodName))
            throw new IgniteException("Method name can't be empty");

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

        boolean keepBinary = (flags & FLAG_KEEP_BINARY_MASK) != 0;

        try {
            Object res;

            if (PlatformService.class.isAssignableFrom(desc.serviceClass())) {
                PlatformService proxy = services.serviceProxy(name, PlatformService.class, false, timeout);

                res = proxy.invokeMethod(methodName, keepBinary, args);
            }
            else {
                GridServiceProxy<?> proxy = new GridServiceProxy<>(grp, name, Service.class, false, timeout,
                    ctx.kernalContext());

                Object[] args = keepBinary ? this.args : PlatformUtils.unwrapBinariesInArray(this.args);

                Method method = resolveMethod(ctx, svcCls);

                res = proxy.invokeMethod(method, args);
            }

            return new ClientObjectResponse(requestId(), res);
        }
        catch (Throwable e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Resolve method by method name and parameter types or parameter values.
     */
    private Method resolveMethod(ClientConnectionContext ctx, Class<?> cls) throws ReflectiveOperationException {
        if (paramTypeIds != null) {
            MethodDescriptor desc = new MethodDescriptor(cls, methodName, paramTypeIds);

            Method method = methodsCache.get(desc);

            if (method != null)
                return method;

            BinaryContext bctx = ((CacheObjectBinaryProcessorImpl)ctx.kernalContext().cacheObjects()).binaryContext();

            for (Method method0 : cls.getMethods()) {
                if (methodName.equals(method0.getName())) {
                    MethodDescriptor desc0 = MethodDescriptor.forMethod(bctx, method0);

                    methodsCache.putIfAbsent(desc0, method0);

                    if (desc0.equals(desc))
                        return method0;
                }
            }

            throw new NoSuchMethodException("Method not found: " + desc);
        }

        // Try to find method by name and parameter values.
        return PlatformServices.getMethod(cls, methodName, args);
    }

    /**
     *
     */
    private static class MethodDescriptor {
        /** Class. */
        private final Class<?> cls;

        /** Method name. */
        private final String methodName;

        /** Parameter type IDs. */
        private final int[] paramTypeIds;

        /** Hash code. */
        private final int hash;

        /**
         * @param cls Class.
         * @param methodName Method name.
         * @param paramTypeIds Parameter type ids.
         */
        private MethodDescriptor(Class<?> cls, String methodName, int[] paramTypeIds) {
            assert cls != null;
            assert methodName != null;
            assert paramTypeIds != null;

            this.cls = cls;
            this.methodName = methodName;
            this.paramTypeIds = paramTypeIds;

            // Precalculate hash in constructor, since we need it for all objects of this class.
            hash = 31 * ((31 * cls.hashCode()) + methodName.hashCode()) + Arrays.hashCode(paramTypeIds);
        }

        /**
         * @param ctx Binary context.
         * @param method Method.
         */
        private static MethodDescriptor forMethod(BinaryContext ctx, Method method) {
            Class<?>[] paramTypes = method.getParameterTypes();

            int[] paramTypeIds = new int[paramTypes.length];

            for (int i = 0; i < paramTypes.length; i++)
                paramTypeIds[i] = ctx.typeId(paramTypes[i].getName());

            return new MethodDescriptor(method.getDeclaringClass(), method.getName(), paramTypeIds);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            MethodDescriptor that = (MethodDescriptor)o;

            return cls.equals(that.cls) && methodName.equals(that.methodName)
                && Arrays.equals(paramTypeIds, that.paramTypeIds);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MethodDescriptor.class, this, "paramTypeIds", paramTypeIds);
        }
    }
}
