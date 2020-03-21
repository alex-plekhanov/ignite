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

import java.util.Map;
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
class TcpClientCompute implements ClientCompute, NotificationListener {
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

    /** Task timeout. */
    private volatile long timeout = 0;

    /** No failover flag. */
    private volatile boolean noFailover;

    /** No result cache flag. */
    private volatile boolean noResultCache;

    /** Active tasks. */
    private final Map<ClientChannel, Map<Long, ClientComputeTask<Object>>> activeTasks = new ConcurrentHashMap<>();

    /** Guard lock. */
    private final ReadWriteLock guard = new ReentrantReadWriteLock();

    /** Constructor. */
    TcpClientCompute(ReliableChannel ch, ClientBinaryMarshaller marsh) {
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
        IgniteFuture<R> fut = executeAsync0(taskName, arg);

        return fut.get();
    }

    /** {@inheritDoc} */
    @Override public <T, R> IgniteFuture<R> executeAsync(String taskName, @Nullable T arg) throws ClientException {
        return executeAsync0(taskName, arg);
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withTimeout(long timeout) {
        this.timeout = timeout;

        return this;
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withNoFailover() {
        noFailover = true;

        return this;
    }

    /** {@inheritDoc} */
    @Override public ClientCompute withNoResultCache() {
        noResultCache = true;

        return this;
    }

    /**
     * @param taskName Task name.
     * @param arg Argument.
     */
    private <T, R> IgniteFuture<R> executeAsync0(String taskName, @Nullable T arg) throws ClientException {
        guard.readLock().lock();

        try {
            ClientComputeTask<Object> task = ch.service(ClientOperation.COMPUTE_TASK_EXECUTE,
                ch -> {
                    try (BinaryRawWriterEx w = new BinaryWriterExImpl(marsh.context(), ch.out(), null, null)) {
                        byte flags = (byte) ((noFailover ? NO_FAILOVER_FLAG_MASK : 0) |
                            (noResultCache ? NO_RESULT_CACHE_FLAG_MASK : 0));

                        w.writeInt(0); // Nodes cnt.
                        w.writeByte(flags);
                        w.writeLong(timeout);
                        w.writeString(taskName);
                        utils.writeObject(ch.out(), arg);
                    }
                },
                ch -> new ClientComputeTask<>(ch.clientChannel(), ch.in().readLong()));

            timeout = 0;
            noFailover = false;
            noResultCache = false;

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
