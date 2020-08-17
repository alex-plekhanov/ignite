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

import javax.cache.event.EventType;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteAsyncCallback;

/**
 * API for configuring continuous cache queries.
 * <p>
 * Continuous queries allow registering a remote filter and a local listener
 * for cache updates. If an update event passes the filter, it will be sent to
 * the node that executed the query, and local listener will be notified.
 */
public class ClientContinuousQuery<K, V> {
    /**
     * Default page size. Size of {@code 1} means that all entries
     * will be sent to master node immediately (buffering is disabled).
     */
    public static final int DFLT_PAGE_SIZE = 1;

    /** Maximum default time interval after which buffer will be flushed (if buffering is enabled). */
    public static final long DFLT_TIME_INTERVAL = 0;

    /** Page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Disconnect listener. */
    private Runnable disconnectLsnr;

    /** Local listener. */
    private ClientContinuousQueryListener<K, V> locLsnr;

    /** Remote filter. */
    private CacheEntryEventSerializableFilter<K, V> rmtFilter;

    /** Time interval. */
    private long timeInterval = DFLT_TIME_INTERVAL;

    /** Whether to notify about {@link EventType#EXPIRED} events. */
    private boolean includeExpired;

    /**
     * Gets optional page size, if {@code 0}, then default is used.
     *
     * @return Optional page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets optional page size, if {@code 0}, then default is used.
     *
     * @param pageSize Optional page size.
     * @return {@code this} for chaining.
     */
    public ClientContinuousQuery<K, V> setPageSize(int pageSize) {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size must be above zero.");

        this.pageSize = pageSize == 0 ? DFLT_PAGE_SIZE : pageSize;

        return this;
    }

    /**
     * Sets a local callback. This callback is called on client when new updates are received.
     * <p>
     * Note that for removed entries values will be {@code null}.
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking (e.g.,
     * synchronization or transactional cache operations), should be executed asynchronously without
     * blocking the thread that called the callback. Otherwise, you can get deadlocks.
     *
     * @param locLsnr Local callback.
     * @return {@code this} for chaining.
     */
    public ClientContinuousQuery<K, V> setLocalListener(ClientContinuousQueryListener<K, V> locLsnr) {
        this.locLsnr = locLsnr;

        return this;
    }

    /**
     * @return Local listener.
     */
    public ClientContinuousQueryListener<K, V> getLocalListener() {
        return locLsnr;
    }

    /**
     * Sets optional key-value filter. This filter is called remotely before entry is sent to the client.
     * <p>
     * <b>WARNING:</b> all operations that involve any kind of JVM-local or distributed locking
     * (e.g., synchronization or transactional cache operations), should be executed asynchronously
     * without blocking the thread that called the filter. Otherwise, you can get deadlocks.
     * <p>
     * If remote filter are annotated with {@link IgniteAsyncCallback} then it is executed in async callback
     * pool (see {@link IgniteConfiguration#getAsyncCallbackPoolSize()}) that allow to perform a cache operations.
     *
     * @param rmtFilter Key-value filter.
     * @return {@code this} for chaining.
     *
     * @see IgniteAsyncCallback
     * @see IgniteConfiguration#getAsyncCallbackPoolSize()
     */
    public ClientContinuousQuery<K, V> setRemoteFilter(CacheEntryEventSerializableFilter<K, V> rmtFilter) {
        this.rmtFilter = rmtFilter;

        return this;
    }

    /**
     * Gets remote filter.
     *
     * @return Remote filter.
     */
    public CacheEntryEventSerializableFilter<K, V> getRemoteFilter() {
        return rmtFilter;
    }

    /**
     * Sets time interval.
     * <p>
     * When a cache update happens, entry is first put into a buffer. Entries from buffer will
     * be sent to the master node only if the buffer is full (its size can be provided via {@link #setPageSize(int)}
     * method) or time provided via this method is exceeded.
     * <p>
     * Default time interval is {@code 0} which means that
     * time check is disabled and entries will be sent only when buffer is full.
     *
     * @param timeInterval Time interval.
     * @return {@code this} for chaining.
     */
    public ClientContinuousQuery<K, V> setTimeInterval(long timeInterval) {
        if (timeInterval < 0)
            throw new IllegalArgumentException("Time interval can't be negative.");

        this.timeInterval = timeInterval;

        return this;
    }

    /**
     * Gets time interval.
     *
     * @return Time interval.
     */
    public long getTimeInterval() {
        return timeInterval;
    }

    /**
     * Sets the flag value defining whether to notify about {@link EventType#EXPIRED} events.
     * If {@code true}, then the remote listener will get notifications about entries
     * expired in cache. Otherwise, only {@link EventType#CREATED}, {@link EventType#UPDATED}
     * and {@link EventType#REMOVED} events will be fired in the remote listener.
     * <p>
     * This flag is {@code false} by default, so {@link EventType#EXPIRED} events are disabled.
     *
     * @param includeExpired Whether to notify about {@link EventType#EXPIRED} events.
     */
    public void setIncludeExpired(boolean includeExpired) {
        this.includeExpired = includeExpired;
    }

    /**
     * Gets the flag value defining whether to notify about {@link EventType#EXPIRED} events.
     *
     * @return Whether to notify about {@link EventType#EXPIRED} events.
     */
    public boolean isIncludeExpired() {
        return includeExpired;
    }
}
