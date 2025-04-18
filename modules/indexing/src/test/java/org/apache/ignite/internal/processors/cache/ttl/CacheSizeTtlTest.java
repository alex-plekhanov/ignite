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

package org.apache.ignite.internal.processors.cache.ttl;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import javax.cache.Cache;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;

/**
 * Tests for cache.size() with ttl enabled.
 */
public class CacheSizeTtlTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "TestCache";

    /** Entry expiry duration. */
    private static final Duration ENTRY_EXPIRY_DURATION = new Duration(SECONDS, 1);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests that cache.size() works correctly for massive amount of puts and ttl.
     */
    @Test
    public void testCacheSizeWorksCorrectlyWithTtl() throws IgniteInterruptedCheckedException {
        startIgniteServer();

        Ignite client = startIgniteClient();

        try (IgniteDataStreamer dataStreamer = client.dataStreamer(CACHE_NAME)) {
            IntStream.range(0, 100_000)
                .forEach(i -> dataStreamer.addData(1, LocalDateTime.now()));
        }

        assertTrue(GridTestUtils.waitForCondition(() -> client.cache(CACHE_NAME).size(CachePeekMode.PRIMARY) == 0,
            getTestTimeout()));
    }

    /**
     * Tests that cache.size() works correctly for massive amount of puts and ttl.
     */
    @Test
    public void testCachePutWorksCorrectlyWithTtl() throws Exception {
        startIgniteServer();

        Ignite client = startIgniteClient();

        multithreaded(() -> IntStream.range(0, 20_000)
            .forEach(i -> client.cache(CACHE_NAME).put(1, LocalDateTime.now())), 8);

        assertTrue(GridTestUtils.waitForCondition(() -> client.cache(CACHE_NAME).size(CachePeekMode.PRIMARY) == 0,
            getTestTimeout()));
    }

    /** */
    private static Ignite startIgniteServer() {
        IgniteConfiguration configuration = new IgniteConfiguration()
            .setClientMode(false)
            .setIgniteInstanceName(UUID.randomUUID().toString())
            .setCacheConfiguration(cacheConfiguration())
            .setDiscoverySpi(discoveryConfiguration());
        return Ignition.start(configuration);
    }

    /** */
    private static Ignite startIgniteClient() {
        IgniteConfiguration configuration = new IgniteConfiguration()
            .setClientMode(true)
            .setIgniteInstanceName(UUID.randomUUID().toString())
            .setDiscoverySpi(discoveryConfiguration());
        return Ignition.start(configuration);
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "4000")
    public void testEntriesLeak() throws Exception {
        IgniteEx srv = startGrid(getConfiguration().setIncludeEventTypes(EVT_CACHE_OBJECT_READ));

        srv.events().localListen(evt -> {
            doSleep(2000L);
            return true;
        }, EVT_CACHE_OBJECT_READ);

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME)
            .withExpiryPolicy(new TouchedExpiryPolicy(new Duration(SECONDS, 1)));

        cache.put(0, 0);

        doSleep(2000);
        //cache.get(0);

        //cache.remove(0);

        doSleep(2000);

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size(CachePeekMode.PRIMARY) == 0, 5_000L));
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "4000")
    public void testEntriesLeak0() throws Exception {
        IgniteEx srv = startGrid(getConfiguration().setIncludeEventTypes(EVT_CACHE_OBJECT_READ));

/*
        srv.events().localListen(evt -> {
            doSleep(2000L);
            return true;
        }, EVT_CACHE_OBJECT_READ);
*/

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME)
            .withExpiryPolicy(new TouchedExpiryPolicy(new Duration(SECONDS, 1)));

        cache.put(0, 0);

        doSleep(2000);

        cache.get(0);

        doSleep(5_000L);
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "4000")
    public void testEntriesLeak1() throws Exception {
        IgniteEx srv = startGrid(getConfiguration().setIncludeEventTypes(EVT_CACHE_OBJECT_READ));

        srv.events().localListen(evt -> {
            doSleep(2000L);
            return true;
        }, EVT_CACHE_OBJECT_READ);

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME)
            .withExpiryPolicy(new TouchedExpiryPolicy(new Duration(SECONDS, 1)));

        cache.put(0, 0);

        cache.get(0);

        doSleep(5_000L);
    }

    /** */
    @Test
    public void testEntriesLeak2() throws Exception {
        IgniteEx srv = startGrid();

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        IgniteInternalCache<Object, Object> cachex = srv.cachex(DEFAULT_CACHE_NAME);

        GridCacheVersion ver = new GridCacheVersion(1, 1, 1, 2);
        CacheObjectImpl val = new CacheObjectImpl(1, null);

        Map<KeyCacheObject, GridCacheDrInfo> map = new HashMap<>();

        for (int i = 0; i < 100_000; i++)
            map.put(
                new KeyCacheObjectImpl(i, null, -1),
                new GridCacheDrExpirationInfo(val, ver, 1, CU.toExpireTime(1)));

        cachex.putAllConflict(map);

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size() == 0, 10_000L));

        GridTestUtils.waitForCondition(() -> cache.size() == 0, 10_000L);

        int cnt = 0;
        for (Cache.Entry<Object, Object> entry : cache)
            cnt++;

        System.out.println(cnt);

        assertEquals(0, cache.size());

    }

    /** */
    @Test
    public void testEntriesLeak3() throws Exception {
        IgniteEx srv = startGrid();

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL))
            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 100)));

        cache.put(0, 0);

        try (Transaction tx = srv.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            cache.get(0);

            doSleep(1_000);

            tx.commit();
        }

        assertEquals(0, cache.size());
    }

    /** */
    @Test
    public void testEntriesLeak4() throws Exception {
        IgniteEx srv = startGrid();

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL))
            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 1)));

        Map<Integer, Integer> map = new TreeMap<>();

        for (int i = 0; i < 100_000; i++)
            map.put(i, i);

        cache.putAll(map);

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size() == 0, 10_000L));
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "3000")
    public void testEntriesLeak5() throws Exception {
        IgniteEx srv = startGrid(getConfiguration().setIncludeEventTypes(EVT_CACHE_OBJECT_READ));

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME)
            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(SECONDS, 2)));

        cache.put(0, 0);

        log.info(">>>> Put 0");

        doSleep(4000);

        log.info(">>>> Put 1");

        cache.put(0, "1");

        doSleep(5000);

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size(CachePeekMode.PRIMARY) == 0, 5_000L));
    }

    /** */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "3000")
    public void testEntriesLeak6() throws Exception {
        IgniteEx srv = startGrid(getConfiguration().setIncludeEventTypes(EVT_CACHE_OBJECT_READ));

        IgniteCache<Object, Object> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(0, 0);
        cache.remove(0);

        IgniteInternalCache<Object, Object> cachex = srv.cachex(DEFAULT_CACHE_NAME);

        GridCacheVersion ver = new GridCacheVersion(1, 1, 1, 2);
        KeyCacheObject key = new KeyCacheObjectImpl(0, null, -1);
        CacheObjectImpl val = new CacheObjectImpl(0, null);
        GridCacheDrInfo drInfo = new GridCacheDrExpirationInfo(val, ver, 2000, CU.toExpireTime(2000));

        Map<KeyCacheObject, GridCacheDrInfo> map = F.asMap(key, drInfo);

        cachex.putAllConflict(map);

        log.info(">>>> Put 0");

        doSleep(4000);

        cachex.removeAllConflict(F.asMap(key, ver));
        cachex.putAllConflict(map);

        log.info(">>>> Put 1");

        doSleep(5000);

        assertTrue(GridTestUtils.waitForCondition(() -> cache.size(CachePeekMode.PRIMARY) == 0, 5_000L));
    }

    /** */
    @NotNull
    private static CacheConfiguration<String, LocalDateTime> cacheConfiguration() {
        return new CacheConfiguration<String, LocalDateTime>()
            .setName(CACHE_NAME)
            .setCacheMode(REPLICATED)
            .setEagerTtl(true)
            .setExpiryPolicyFactory(ModifiedExpiryPolicy.factoryOf(ENTRY_EXPIRY_DURATION));
    }

    /** */
    private static TcpDiscoverySpi discoveryConfiguration() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(singleton("127.0.0.1:48550..48551"));
        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setIpFinder(ipFinder);
        tcpDiscoverySpi.setLocalPort(48550);
        tcpDiscoverySpi.setLocalPortRange(1);

        return tcpDiscoverySpi;
    }
}
