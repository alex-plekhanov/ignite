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

package org.apache.ignite.client;

import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Thin client compute facade. Defines compute grid functionality for executing tasks over nodes
 * in the {@link ClientClusterGroup}
 */
public interface ClientCompute {
    /**
     * Gets cluster group to which this {@code ClientCompute} instance belongs.
     *
     * @return Cluster group to which this {@code ClientCompute} instance belongs.
     */
    public ClientClusterGroup clusterGroup();

    /**
     * Executes given task within the cluster group. For step-by-step explanation of task execution process
     * refer to {@link ComputeTask} documentation.
     *
     * @param taskName Name of the task to execute.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task result.
     * @throws ClientException If task failed.
     * @see ComputeTask for information about task execution.
     */
    public <T, R> R execute(String taskName, @Nullable T arg) throws ClientException;

    /**
     * Executes given task asynchronously within the cluster group. For step-by-step explanation of task execution
     * process refer to {@link ComputeTask} documentation.
     *
     * @param taskName Name of the task to execute.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return a Future representing pending completion of the task.
     * @throws ClientException If task failed.
     * @see ComputeTask for information about task execution.
     */
    public <T, R> IgniteFuture<R> executeAsync(String taskName, @Nullable T arg) throws ClientException;

    /**
     * Executes task within the cluster group using cache affinity key for routing. This way the task will try to
     * start executing on the node where this affinity key is cached. Affinity routing only works when
     * partition awareness is enabled. If partition awareness is disabled task will be routed to the default channel.
     *
     * @param cacheName Name of the cache on which affinity should be calculated.
     * @param affKey Affinity key.
     * @param taskName Name of the task to execute.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return Task result.
     * @throws ClientException If task failed.
     * @see ClientConfiguration#setPartitionAwarenessEnabled
     */
    public <T, R> R affinityExecute(String cacheName, Object affKey, String taskName, @Nullable T arg)
        throws ClientException;

    /**
     * Executes task asynchronously within the cluster group using cache affinity key for routing. This way the task
     * will try to start executing on the node where this affinity key is cached. Affinity routing only works when
     * partition awareness is enabled. If partition awareness is disabled task will be routed to the default channel.
     *
     * @param cacheName Name of the cache on which affinity should be calculated.
     * @param affKey Affinity key.
     * @param taskName Name of the task to execute.
     * @param arg Optional argument of task execution, can be {@code null}.
     * @return a Future representing pending completion of the task.
     * @throws ClientException If task failed.
     * @see ClientConfiguration#setPartitionAwarenessEnabled
     */
    public <T, R> IgniteFuture<R> affinityExecuteAsync(String cacheName, Object affKey, String taskName, @Nullable T arg)
        throws ClientException;

    /**
     * Sets timeout for tasks executed by returned {@code ClientCompute} instance.
     *
     * @return {@code ClientCompute} instance with given timeout.
     */
    public ClientCompute withTimeout(long timeout);

    /**
     * Sets no-failover flag for tasks executed by returned {@code ClientCompute} instance.
     * If flag is set, job will be never failed over even if remote node crashes or rejects execution.
     *
     * @return {@code ClientCompute} instance with no-failover flag.
     */
    public ClientCompute withNoFailover();

    /**
     * Disables result caching for tasks executed by returned {@code ClientCompute} instance.
     *
     * @return {@code ClientCompute} instance with "no result cache" flag.
     */
    public ClientCompute withNoResultCache();
}
