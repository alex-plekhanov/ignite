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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ClientCompute}.
 */
class ClientComputeImpl implements ClientCompute, NotificationListener {
    /** No failover flag mask. */
    private static final byte NO_FAILOVER_FLAG_MASK = 0x01;

    /** No result cache flag mask. */
    private static final byte NO_RESULT_CACHE_FLAG_MASK = 0x02;

    /** Channel. */
    private final ReliableChannel ch;

    /** Binary marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Utils for serialization/deserialization. */
    private final ClientUtils utils;

    /** Active tasks. */
    private final Map<ClientChannel, Map<Long, ClientComputeTask<Object>>> activeTasks = new ConcurrentHashMap<>();

    /** Guard lock. */
    private final ReadWriteLock guard = new ReentrantReadWriteLock();

    /** Constructor. */
    ClientComputeImpl(ReliableChannel ch, ClientBinaryMarshaller marsh) {
        this.ch = ch;
        this.marsh = marsh;

        utils = new ClientUtils(marsh);

        ch.addNotificationListener(this);

        ch.addChannelCloseListener(clientCh -> {
            guard.writeLock().lock();

            try {
                Map<Long, ClientComputeTask<Object>> chTasks = activeTasks.remove(clientCh);

                if (!F.isEmpty(chTasks)) {
                    for (ClientComputeTask<?> task : chTasks.values())
                        task.fut.onDone(new ClientException("Channel to server is closed"));
                }
            }
            finally {
                guard.writeLock().unlock();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(String taskName, @Nullable T arg) throws ClientException {
        return (R)executeAsync(taskName, arg).get();
    }

    /** {@inheritDoc} */
    @Override public <T, R> IgniteFuture<R> executeAsync(String taskName, @Nullable T arg) throws ClientException {
        return executeAsync0(taskName, arg, null, (byte)0, 0L);
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withTimeout(long timeout) {
        return timeout == 0L ? this : new ClientComputeModificator(this, null, (byte)0, timeout);
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withNoFailover() {
        return new ClientComputeModificator(this, null, NO_FAILOVER_FLAG_MASK, 0L);
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withNoResultCache() {
        return new ClientComputeModificator(this, null, NO_RESULT_CACHE_FLAG_MASK, 0L);
    }

    /**
     * Gets compute facade over the specified cluster group.
     *
     * @param grp Cluster group.
     */
    public ClientCompute withClusterGroup(ClientClusterGroupImpl grp) {
        return new ClientComputeModificator(this, grp, (byte)0, 0L);
    }

    /**
     * @param taskName Task name.
     * @param arg Argument.
     */
    private <T, R> IgniteFuture<R> executeAsync0(
        String taskName,
        @Nullable T arg,
        ClientClusterGroupImpl clusterGrp,
        byte flags,
        long timeout
    ) throws ClientException {
        guard.readLock().lock();

        try {
            Collection<UUID> nodeIds = clusterGrp == null ? null : clusterGrp.nodeIds();

            ClientComputeTask<Object> task = ch.service(ClientOperation.COMPUTE_TASK_EXECUTE,
                ch -> {
                    try (BinaryRawWriterEx w = new BinaryWriterExImpl(marsh.context(), ch.out(), null, null)) {
                        if (F.isEmpty(nodeIds))
                            w.writeInt(0); // Nodes count.
                        else {
                            w.writeInt(nodeIds.size());

                            for (UUID nodeId : nodeIds)
                                w.writeUuid(nodeId);
                        }

                        w.writeByte(flags);
                        w.writeLong(timeout);
                        w.writeString(taskName);
                        utils.writeObject(ch.out(), arg);
                    }
                },
                ch -> new ClientComputeTask<>(ch.clientChannel(), ch.in().readLong()));

            Map<Long, ClientComputeTask<Object>> chTasks = activeTasks.computeIfAbsent(task.ch, ch -> new ConcurrentHashMap<>());

            chTasks.put(task.taskId, task);

            task.fut.listen(f -> removeTaskIfExists(task.ch, task.taskId));

            return new IgniteFutureImpl<>((IgniteInternalFuture<R>)task.fut);
        }
        finally {
            guard.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptNotification(ClientChannel ch, ClientOperation op, long rsrcId, byte[] payload) {
        if (op == ClientOperation.COMPUTE_TASK_FINISHED) {
            Object res = payload == null ? null : utils.readObject(new BinaryHeapInputStream(payload), false);

            ClientComputeTask<Object> task = removeTaskIfExists(ch, rsrcId);

            if (task != null)
                task.fut.onDone(res);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptError(ClientChannel ch, ClientOperation op, long rsrcId, Throwable err) {
        if (op == ClientOperation.COMPUTE_TASK_FINISHED) {
            ClientComputeTask<Object> task = removeTaskIfExists(ch, rsrcId);

            if (task != null)
                task.fut.onDone(err);
        }
    }

    /**
     * @param ch Client channel.
     * @param taskId Task id.
     */
    private ClientComputeTask<Object> removeTaskIfExists(ClientChannel ch, long taskId) {
        Map<Long, ClientComputeTask<Object>> chTasks = activeTasks.get(ch);

        if (!F.isEmpty(chTasks))
            return chTasks.remove(taskId);

        return null;
    }

    /**
     * TODO
     */
    private static class ClientComputeModificator implements ClientCompute {
        /** Delegate. */
        private final ClientComputeImpl delegate;

        /** Cluster group. */
        private final ClientClusterGroupImpl clusterGrp;

        /** Task flags. */
        private final byte flags;

        /** Task timeout. */
        private final long timeout;

        /**
         * Constructor.
         */
        private ClientComputeModificator(ClientComputeImpl delegate, ClientClusterGroupImpl grp, byte flags, long timeout) {
            this.delegate = delegate;
            clusterGrp = grp;
            this.flags = flags;
            this.timeout = timeout;
        }

        /** {@inheritDoc} */
        @Override public <T, R> R execute(String taskName, @Nullable T arg) throws ClientException {
            return (R)executeAsync(taskName, arg).get();
        }

        /** {@inheritDoc} */
        @Override public <T, R> IgniteFuture<R> executeAsync(String taskName, @Nullable T arg) throws ClientException {
            return delegate.executeAsync0(taskName, arg, clusterGrp, flags, timeout);
        }

        /** {@inheritDoc} */
        @Override public ClientCompute withTimeout(long timeout) {
            return timeout == this.timeout ? this : new ClientComputeModificator(delegate, clusterGrp, flags, timeout);
        }

        /** {@inheritDoc} */
        @Override public ClientCompute withNoFailover() {
            return (flags & NO_FAILOVER_FLAG_MASK) != 0 ? this :
                new ClientComputeModificator(delegate, clusterGrp, (byte) (flags | NO_FAILOVER_FLAG_MASK), timeout);
        }

        /** {@inheritDoc} */
        @Override public ClientCompute withNoResultCache() {
            return (flags & NO_RESULT_CACHE_FLAG_MASK) != 0 ? this :
                new ClientComputeModificator(delegate, clusterGrp, (byte) (flags | NO_RESULT_CACHE_FLAG_MASK), timeout);
        }
    }

    /**
     * Compute task internal class.
     *
     * @param <R> Result type.
     */
    private static class ClientComputeTask<R> {
        /** Client channel. */
        private final ClientChannel ch;

        /** Task id. */
        private final long taskId;

        /** Future. */
        private final GridFutureAdapter<R> fut;

        /**
         * @param ch Client channel.
         * @param taskId Task id.
         */
        private ClientComputeTask(ClientChannel ch, long taskId) {
            this.ch = ch;
            this.taskId = taskId;
            fut = new GridFutureAdapter<>();
        }
    }
}
