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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.ServiceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ALL;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;

/**
 * Full API cache test.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public abstract class GridCacheAbstractFullApiSelfTest extends GridCacheAbstractSelfTest {
    /** Test timeout */
    private static final long TEST_TIMEOUT = 60 * 1000;

    /** Service name. */
    private static final String SERVICE_NAME1 = "testService1";

    /** */
    public static final CacheEntryProcessor<String, Integer, String> ERR_PROCESSOR =
        new CacheEntryProcessor<String, Integer, String>() {
            /** */
            private static final long serialVersionUID = 0L;

            @Override public String process(MutableEntry<String, Integer> e, Object... args) {
                throw new RuntimeException("Failed!");
            }
        };

    /** Increment processor for invoke operations. */
    public static final EntryProcessor<String, Integer, String> INCR_PROCESSOR = new IncrementEntryProcessor();

    /** Increment processor for invoke operations with IgniteEntryProcessor. */
    public static final CacheEntryProcessor<String, Integer, String> INCR_IGNITE_PROCESSOR =
        new CacheEntryProcessor<String, Integer, String>() {
            /** */
            private static final long serialVersionUID = 0L;

            @Override public String process(MutableEntry<String, Integer> e, Object... args) {
                return INCR_PROCESSOR.process(e, args);
            }
        };

    /** Increment processor for invoke operations. */
    public static final EntryProcessor<String, Integer, String> RMV_PROCESSOR = new RemoveEntryProcessor();

    /** Increment processor for invoke operations with IgniteEntryProcessor. */
    public static final CacheEntryProcessor<String, Integer, String> RMV_IGNITE_PROCESSOR =
        new CacheEntryProcessor<String, Integer, String>() {
            /** */
            private static final long serialVersionUID = 0L;

            @Override public String process(MutableEntry<String, Integer> e, Object... args) {
                return RMV_PROCESSOR.process(e, args);
            }
        };

    /** Dflt grid. */
    protected static transient Ignite dfltIgnite;

    /** */
    private static Map<String, CacheConfiguration[]> cacheCfgMap;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes(
            EVT_CACHE_OBJECT_READ,
            EVT_CACHE_OBJECT_LOCKED,
            EVT_CACHE_OBJECT_UNLOCKED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        initStoreStrategy();

        if (cacheStartType() == CacheStartMode.STATIC)
            super.beforeTestsStarted();
        else {
            cacheCfgMap = Collections.synchronizedMap(new HashMap<String, CacheConfiguration[]>());

            if (cacheStartType() == CacheStartMode.NODES_THEN_CACHES) {
                super.beforeTestsStarted();

                for (Map.Entry<String, CacheConfiguration[]> entry : cacheCfgMap.entrySet()) {
                    Ignite ignite = grid(entry.getKey());

                    for (CacheConfiguration cfg : entry.getValue())
                        ignite.getOrCreateCache(cfg);
                }

                awaitPartitionMapExchange();
            }
            else {
                int cnt = gridCount();

                assert cnt >= 1 : "At least one grid must be started";

                for (int i = 0; i < cnt; i++) {
                    Ignite ignite = startGrid(i);

                    CacheConfiguration[] cacheCfgs = cacheCfgMap.get(ignite.name());

                    for (CacheConfiguration cfg : cacheCfgs)
                        ignite.createCache(cfg);
                }

                if (cnt > 1)
                    checkTopology(cnt);

                awaitPartitionMapExchange();
            }

            cacheCfgMap = null;
        }

        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + grid(i).localNode().id());
    }

    /**
     * Checks that any invoke returns result.
     *
     * @throws Exception if something goes bad.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-4380")
    @Test
    public void testInvokeAllMultithreaded() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();
        final int threadCnt = 4;
        final int cnt = 5000;

        final Set<String> keys = Collections.singleton("myKey");

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                for (int i = 0; i < cnt; i++) {
                    final Map<String, EntryProcessorResult<String>> res = cache.invokeAll(keys, INCR_PROCESSOR);

                    assertEquals(1, res.size());
                }
            }
        }, threadCnt, "testInvokeAllMultithreaded");

        assertEquals(cnt * threadCnt, (int)cache.get("myKey"));
    }

    /**
     * Checks that skipStore flag gets overridden inside a transaction.
     */
    @Test
    public void testWriteThroughTx() {
        String key = "writeThroughKey";

        storeStgy.removeFromStore(key);

        IgniteCache<String, Integer> cache = jcache();

        try (Transaction tx =
                 cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                     defaultInstance().transactions().txStart() : null) {
            // retrieve market type from the grid
            Integer old = cache.withSkipStore().get(key);

            assertNull(old);

            // update the grid
            cache.put(key, 2);

            // finally commit the transaction
            if (tx != null)
                tx.commit();
        }

        assertEquals(2, storeStgy.getFromStore(key));
    }

    /**
     * Checks that skipStore flag gets overridden inside a transaction.
     */
    @Test
    public void testNoReadThroughTx() {
        String key = "writeThroughKey";

        IgniteCache<String, Integer> cache = jcache();

        storeStgy.resetStore();

        cache.put(key, 1);

        storeStgy.putToStore(key, 2);

        try (Transaction tx =
                 cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                     defaultInstance().transactions().txStart() : null) {
            Integer old = cache.get(key);

            assertEquals((Integer)1, old);

            // update the grid
            cache.put(key, 2);

            // finally commit the transaction
            if (tx != null)
                tx.commit();
        }

        assertEquals(0, storeStgy.getReads());
    }

    /** {@inheritDoc} */
    @Override protected Ignite startGrid(String igniteInstanceName, GridSpringResourceContext ctx) throws Exception {
        if (cacheCfgMap == null)
            return super.startGrid(igniteInstanceName, ctx);

        IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        cacheCfgMap.put(igniteInstanceName, cfg.getCacheConfiguration());

        cfg.setCacheConfiguration();

        if (!isRemoteJvm(igniteInstanceName))
            return IgnitionEx.start(optimize(cfg), ctx);
        else
            return startRemoteGrid(igniteInstanceName, optimize(cfg), ctx);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        assertEquals(0, cache.localSize());
        assertEquals(0, cache.size());

        super.beforeTest();

        assertEquals(0, cache.localSize());
        assertEquals(0, cache.size());

        dfltIgnite = defaultInstance();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteCache<String, Integer> cache = jcache();

        assertEquals(0, cache.localSize());
        assertEquals(0, cache.size());
        assertEquals(0, cache.size(ONHEAP));

        dfltIgnite = null;
    }

    /**
     * @return A not near-only cache.
     */
    protected IgniteCache<String, Integer> fullCache() {
        return jcache();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testSize() throws Exception {
        assert jcache().localSize() == 0;

        int size = 10;

        final Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < size; i++)
            map.put("key" + i, i);

        // Put in primary nodes to avoid near readers which will prevent entry from being cleared.
        Map<ClusterNode, Collection<String>> mapped = defaultInstance().<String>affinity(DEFAULT_CACHE_NAME).mapKeysToNodes(map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keys = mapped.get(grid(i).localNode());

            if (!F.isEmpty(keys)) {
                for (String key : keys)
                    jcache(i).put(key, map.get(key));
            }
        }

        map.remove("key0");

        mapped = defaultInstance().<String>affinity(DEFAULT_CACHE_NAME).mapKeysToNodes(map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            // Will actually delete entry from map.
            CU.invalidate(jcache(i), "key0");

            assertNull("Failed check for grid: " + i, jcache(i).localPeek("key0", ONHEAP));

            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assert jcache(i).localSize() != 0 || F.isEmpty(keysCol);
        }

        for (int i = 0; i < gridCount(); i++)
            executeOnLocalOrRemoteJvm(i, new CheckCacheSizeTask(map));

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assertEquals("Failed check for grid: " + i, !F.isEmpty(keysCol) ? keysCol.size() : 0,
                jcache(i).localSize(PRIMARY));
        }

        int globalPrimarySize = map.size();

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalPrimarySize, jcache(i).size(PRIMARY));

        // Check how many instances of any given key there is in the cluster.
        int globalSize = 0;

        for (String key : map.keySet())
            globalSize += affinity(jcache()).mapKeyToPrimaryAndBackups(key).size();

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalSize, jcache(i).size(ALL));
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testContainsKey() throws Exception {
        jcache().put("testContainsKey", 1);

        checkContainsKey(true, "testContainsKey");
        checkContainsKey(false, "testContainsKeyWrongKey");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKeyTx() throws Exception {
        if (!txEnabled())
            return;

        IgniteCache<String, Integer> cache = jcache();

        IgniteTransactions txs = defaultInstance().transactions();

        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(i);

            try (Transaction tx =
                     cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                         txs.txStart() : null) {
                assertNull(key, cache.get(key));

                assertFalse(cache.containsKey(key));

                if (tx != null)
                    tx.commit();
            }

            try (Transaction tx =
                     cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                         txs.txStart() : null) {
                assertNull(key, cache.get(key));

                cache.put(key, i);

                assertTrue(cache.containsKey(key));

                if (tx != null)
                    tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testContainsKeysTx() throws Exception {
        if (!txEnabled())
            return;

        IgniteCache<String, Integer> cache = jcache();

        IgniteTransactions txs = defaultInstance().transactions();

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(i);

            keys.add(key);
        }

        try (Transaction tx =
                 cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ? txs.txStart() : null) {
            for (String key : keys)
                assertNull(key, cache.get(key));

            assertFalse(cache.containsKeys(keys));

            if (tx != null)
                tx.commit();
        }

        try (Transaction tx =
                 cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ? txs.txStart() : null) {
            for (String key : keys)
                assertNull(key, cache.get(key));

            for (String key : keys)
                cache.put(key, 0);

            assertTrue(cache.containsKeys(keys));

            if (tx != null)
                tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveInExplicitLocks() throws Exception {
        if (lockingEnabled()) {
            IgniteCache<String, Integer> cache = jcache();

            cache.put("a", 1);

            Lock lock = cache.lockAll(ImmutableSet.of("a", "b", "c", "d"));

            lock.lock();

            try {
                cache.remove("a");

                // Make sure single-key operation did not remove lock.
                cache.putAll(F.asMap("b", 2, "c", 3, "d", 4));
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAllSkipStore() throws Exception {
        IgniteCache<String, Integer> jcache = jcache();

        jcache.putAll(F.asMap("1", 1, "2", 2, "3", 3));

        jcache.withSkipStore().removeAll();

        assertEquals((Integer)1, jcache.get("1"));
        assertEquals((Integer)2, jcache.get("2"));
        assertEquals((Integer)3, jcache.get("3"));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testAtomicOps() throws IgniteCheckedException {
        IgniteCache<String, Integer> c = jcache();

        final int cnt = 10;

        for (int i = 0; i < cnt; i++)
            assertNull(c.getAndPutIfAbsent("k" + i, i));

        for (int i = 0; i < cnt; i++) {
            boolean wrong = i % 2 == 0;

            String key = "k" + i;

            boolean res = c.replace(key, wrong ? i + 1 : i, -1);

            assertEquals(wrong, !res);
        }

        for (int i = 0; i < cnt; i++) {
            boolean success = i % 2 != 0;

            String key = "k" + i;

            boolean res = c.remove(key, -1);

            assertTrue(success == res);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGet() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;
        assert cache.get("wrongKey") == null;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetEntry() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        CacheEntry<String, Integer> key1e = cache.getEntry("key1");
        CacheEntry<String, Integer> key2e = cache.getEntry("key2");
        CacheEntry<String, Integer> wrongKeye = cache.getEntry("wrongKey");

        assert key1e.getValue() == 1;
        assert key1e.getKey().equals("key1");
        assert key1e.version() != null;

        assert key2e.getValue() == 2;
        assert key2e.getKey().equals("key2");
        assert key2e.version() != null;

        assert wrongKeye == null;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        IgniteFuture<Integer> fut1 = cache.getAsync("key1");

        IgniteFuture<Integer> fut2 = cache.getAsync("key2");

        IgniteFuture<Integer> fut3 = cache.getAsync("wrongKey");

        assert fut1.get() == 1;
        assert fut2.get() == 2;
        assert fut3.get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.get("key1");

        IgniteFuture<Integer> fut1 = cacheAsync.future();

        cacheAsync.get("key2");

        IgniteFuture<Integer> fut2 = cacheAsync.future();

        cacheAsync.get("wrongKey");

        IgniteFuture<Integer> fut3 = cacheAsync.future();

        assert fut1.get() == 1;
        assert fut2.get() == 2;
        assert fut3.get() == null;
    }


    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAll() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        final IgniteCache<String, Integer> cache = jcache();

        try {
            cache.put("key1", 1);
            cache.put("key2", 2);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAll(null).isEmpty();

                return null;
            }
        }, NullPointerException.class, null);

        assert cache.getAll(Collections.<String>emptySet()).isEmpty();

        Map<String, Integer> map1 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved map1: " + map1);

        assert 2 == map1.size() : "Invalid map: " + map1;

        assertEquals(1, (int)map1.get("key1"));
        assertEquals(2, (int)map1.get("key2"));
        assertNull(map1.get("key9999"));

        Map<String, Integer> map2 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved map2: " + map2);

        assert 2 == map2.size() : "Invalid map: " + map2;

        assertEquals(1, (int)map2.get("key1"));
        assertEquals(2, (int)map2.get("key2"));
        assertNull(map2.get("key9999"));

        // Now do the same checks but within transaction.
        if (txShouldBeUsed()) {
            try (Transaction tx0 = transactions().txStart()) {
                assert cache.getAll(Collections.<String>emptySet()).isEmpty();

                map1 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

                info("Retrieved map1: " + map1);

                assert 2 == map1.size() : "Invalid map: " + map1;

                assertEquals(1, (int)map1.get("key1"));
                assertEquals(2, (int)map1.get("key2"));
                assertNull(map1.get("key9999"));

                map2 = cache.getAll(ImmutableSet.of("key1", "key2", "key9999"));

                info("Retrieved map2: " + map2);

                assert 2 == map2.size() : "Invalid map: " + map2;

                assertEquals(1, (int)map2.get("key1"));
                assertEquals(2, (int)map2.get("key2"));
                assertNull(map2.get("key9999"));

                tx0.commit();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetEntries() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        final IgniteCache<String, Integer> cache = jcache();

        try {
            cache.put("key1", 1);
            cache.put("key2", 2);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getEntries(null).isEmpty();

                return null;
            }
        }, NullPointerException.class, null);

        assert cache.getEntries(Collections.<String>emptySet()).isEmpty();

        Collection<CacheEntry<String, Integer>> c1 = cache.getEntries(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved c1: " + c1);

        assert 2 == c1.size() : "Invalid collection: " + c1;

        boolean b1 = false;
        boolean b2 = false;

        for (CacheEntry<String, Integer> e : c1) {
            if (e.getKey().equals("key1") && e.getValue().equals(1))
                b1 = true;

            if (e.getKey().equals("key2") && e.getValue().equals(2))
                b2 = true;
        }

        assertTrue(b1 && b2);

        Collection<CacheEntry<String, Integer>> c2 = cache.getEntries(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved c2: " + c2);

        assert 2 == c2.size() : "Invalid collection: " + c2;

        b1 = false;
        b2 = false;

        for (CacheEntry<String, Integer> e : c2) {
            if (e.getKey().equals("key1") && e.getValue().equals(1))
                b1 = true;

            if (e.getKey().equals("key2") && e.getValue().equals(2))
                b2 = true;
        }

        assertTrue(b1 && b2);

        // Now do the same checks but within transaction.
        if (txShouldBeUsed()) {
            try (Transaction tx0 = transactions().txStart()) {
                assert cache.getEntries(Collections.<String>emptySet()).isEmpty();

                c1 = cache.getEntries(ImmutableSet.of("key1", "key2", "key9999"));

                info("Retrieved c1: " + c1);

                assert 2 == c1.size() : "Invalid collection: " + c1;

                b1 = false;
                b2 = false;

                for (CacheEntry<String, Integer> e : c1) {
                    if (e.getKey().equals("key1") && e.getValue().equals(1))
                        b1 = true;

                    if (e.getKey().equals("key2") && e.getValue().equals(2))
                        b2 = true;
                }

                assertTrue(b1 && b2);

                c2 = cache.getEntries(ImmutableSet.of("key1", "key2", "key9999"));

                info("Retrieved c2: " + c2);

                assert 2 == c2.size() : "Invalid collection: " + c2;

                b1 = false;
                b2 = false;

                for (CacheEntry<String, Integer> e : c2) {
                    if (e.getKey().equals("key1") && e.getValue().equals(1))
                        b1 = true;

                    if (e.getKey().equals("key2") && e.getValue().equals(2))
                        b2 = true;
                }

                assertTrue(b1 && b2);

                tx0.commit();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAllWithLastNull() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final Set<String> c = new LinkedHashSet<>();

        c.add("key1");
        c.add(null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAll(c);

                return null;
            }
        }, NullPointerException.class, null);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAllWithFirstNull() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final Set<String> c = new LinkedHashSet<>();

        c.add(null);
        c.add("key1");

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAll(c);

                return null;
            }
        }, NullPointerException.class, null);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAllWithInTheMiddle() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final Set<String> c = new LinkedHashSet<>();

        c.add("key1");
        c.add(null);
        c.add("key2");

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAll(c);

                return null;
            }
        }, NullPointerException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetTxNonExistingKey() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction ignored = transactions().txStart()) {
                assert jcache().get("key999123") == null;
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAllAsync() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAllAsync(null);

                return null;
            }
        }, NullPointerException.class, null);

        IgniteFuture<Map<String, Integer>> fut2 = cache.getAllAsync(Collections.<String>emptySet());

        IgniteFuture<Map<String, Integer>> fut3 = cache.getAllAsync(ImmutableSet.of("key1", "key2"));

        assert fut2.get().isEmpty();
        assert fut3.get().size() == 2 : "Invalid map: " + fut3.get();
        assert fut3.get().get("key1") == 1;
        assert fut3.get().get("key2") == 2;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAllAsyncOld() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cacheAsync.getAll(null);

                return null;
            }
        }, NullPointerException.class, null);

        cacheAsync.getAll(Collections.<String>emptySet());
        IgniteFuture<Map<String, Integer>> fut2 = cacheAsync.future();

        cacheAsync.getAll(ImmutableSet.of("key1", "key2"));
        IgniteFuture<Map<String, Integer>> fut3 = cacheAsync.future();

        assert fut2.get().isEmpty();
        assert fut3.get().size() == 2 : "Invalid map: " + fut3.get();
        assert fut3.get().get("key1") == 1;
        assert fut3.get().get("key2") == 2;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPut() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        assert cache.getAndPut("key1", 1) == null;
        assert cache.getAndPut("key2", 2) == null;

        // Check inside transaction.
        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;

        // Put again to check returned values.
        assert cache.getAndPut("key1", 1) == 1;
        assert cache.getAndPut("key2", 2) == 2;

        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");

        assert cache.get("key1") != null;
        assert cache.get("key2") != null;
        assert cache.get("wrong") == null;

        // Check outside transaction.
        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;
        assert cache.get("wrong") == null;

        assertEquals((Integer)1, cache.getAndPut("key1", 10));
        assertEquals((Integer)2, cache.getAndPut("key2", 11));
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutTx() throws Exception {
        if (txShouldBeUsed()) {
            IgniteCache<String, Integer> cache = jcache();

            try (Transaction tx = transactions().txStart()) {
                assert cache.getAndPut("key1", 1) == null;
                assert cache.getAndPut("key2", 2) == null;

                // Check inside transaction.
                assert cache.get("key1") == 1;
                assert cache.get("key2") == 2;

                // Put again to check returned values.
                assert cache.getAndPut("key1", 1) == 1;
                assert cache.getAndPut("key2", 2) == 2;

                assert cache.get("key1") != null;
                assert cache.get("key2") != null;
                assert cache.get("wrong") == null;

                tx.commit();
            }

            // Check outside transaction.
            checkContainsKey(true, "key1");
            checkContainsKey(true, "key2");

            assert cache.get("key1") == 1;
            assert cache.get("key2") == 2;
            assert cache.get("wrong") == null;

            assertEquals((Integer)1, cache.getAndPut("key1", 10));
            assertEquals((Integer)2, cache.getAndPut("key2", 11));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformOptimisticReadCommitted() throws Exception {
        checkTransform(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformOptimisticRepeatableRead() throws Exception {
        checkTransform(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformPessimisticReadCommitted() throws Exception {
        checkTransform(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformPessimisticRepeatableRead() throws Exception {
        checkTransform(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteTransformOptimisticReadCommitted() throws Exception {
        checkIgniteTransform(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteTransformOptimisticRepeatableRead() throws Exception {
        checkIgniteTransform(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteTransformPessimisticReadCommitted() throws Exception {
        checkIgniteTransform(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteTransformPessimisticRepeatableRead() throws Exception {
        checkIgniteTransform(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkIgniteTransform(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        Transaction tx = txShouldBeUsed() ? defaultInstance().transactions().txStart(concurrency, isolation) : null;

        try {
            assertEquals("null", cache.invoke("key1", INCR_IGNITE_PROCESSOR));
            assertEquals("1", cache.invoke("key2", INCR_IGNITE_PROCESSOR));
            assertEquals("3", cache.invoke("key3", RMV_IGNITE_PROCESSOR));

            if (tx != null)
                tx.commit();
        }
        catch (Exception e) {
            e.printStackTrace();

            throw e;
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull("Failed for cache: " + i, jcache(i).localPeek("key3", ONHEAP));

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        assertEquals("null", cache.invoke("key1", INCR_IGNITE_PROCESSOR));
        assertEquals("1", cache.invoke("key2", INCR_IGNITE_PROCESSOR));
        assertEquals("3", cache.invoke("key3", RMV_IGNITE_PROCESSOR));

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("key3", ONHEAP));
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkTransform(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        Transaction tx = txShouldBeUsed() ? defaultInstance().transactions().txStart(concurrency, isolation) : null;

        try {
            assertEquals("null", cache.invoke("key1", INCR_PROCESSOR));
            assertEquals("1", cache.invoke("key2", INCR_PROCESSOR));
            assertEquals("3", cache.invoke("key3", RMV_PROCESSOR));

            if (tx != null)
                tx.commit();
        }
        catch (Exception e) {
            e.printStackTrace();

            throw e;
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull("Failed for cache: " + i, jcache(i).localPeek("key3", ONHEAP));

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        assertEquals("null", cache.invoke("key1", INCR_PROCESSOR));
        assertEquals("1", cache.invoke("key2", INCR_PROCESSOR));
        assertEquals("3", cache.invoke("key3", RMV_PROCESSOR));

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("key3", ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformAllOptimisticReadCommitted() throws Exception {
        checkTransformAll(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformAllOptimisticRepeatableRead() throws Exception {
        checkTransformAll(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformAllPessimisticReadCommitted() throws Exception {
        checkTransformAll(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformAllPessimisticRepeatableRead() throws Exception {
        checkTransformAll(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void checkTransformAll(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        if (txShouldBeUsed()) {
            Map<String, EntryProcessorResult<String>> res;

            try (Transaction tx = defaultInstance().transactions().txStart(concurrency, isolation)) {
                res = cache.invokeAll(F.asSet("key1", "key2", "key3"), INCR_PROCESSOR);

                tx.commit();
            }

            assertEquals((Integer)1, cache.get("key1"));
            assertEquals((Integer)2, cache.get("key2"));
            assertEquals((Integer)4, cache.get("key3"));

            assertEquals("null", res.get("key1").get());
            assertEquals("1", res.get("key2").get());
            assertEquals("3", res.get("key3").get());

            assertEquals(3, res.size());

            cache.remove("key1");
            cache.put("key2", 1);
            cache.put("key3", 3);
        }

        Map<String, EntryProcessorResult<String>> res = cache.invokeAll(F.asSet("key1", "key2", "key3"), RMV_PROCESSOR);

        for (int i = 0; i < gridCount(); i++) {
            assertNull(jcache(i).localPeek("key1", ONHEAP));
            assertNull(jcache(i).localPeek("key2", ONHEAP));
            assertNull(jcache(i).localPeek("key3", ONHEAP));
        }

        assertEquals("null", res.get("key1").get());
        assertEquals("1", res.get("key2").get());
        assertEquals("3", res.get("key3").get());

        assertEquals(3, res.size());

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        res = cache.invokeAll(F.asSet("key1", "key2", "key3"), INCR_PROCESSOR);

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertEquals((Integer)4, cache.get("key3"));

        assertEquals("null", res.get("key1").get());
        assertEquals("1", res.get("key2").get());
        assertEquals("3", res.get("key3").get());

        assertEquals(3, res.size());

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        res = cache.invokeAll(F.asMap("key1", INCR_PROCESSOR, "key2", INCR_PROCESSOR, "key3", INCR_PROCESSOR));

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertEquals((Integer)4, cache.get("key3"));

        assertEquals("null", res.get("key1").get());
        assertEquals("1", res.get("key2").get());
        assertEquals("3", res.get("key3").get());

        assertEquals(3, res.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invokeAll((Set<String>)null, INCR_PROCESSOR);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invokeAll(F.asSet("key1"), null);

                return null;
            }
        }, NullPointerException.class, null);

        {
            final Set<String> keys = new LinkedHashSet<>(2);

            keys.add("key1");
            keys.add(null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invokeAll(keys, INCR_PROCESSOR);

                    return null;
                }
            }, NullPointerException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invokeAll(F.asSet("key1"), null);

                    return null;
                }
            }, NullPointerException.class, null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformSequentialOptimisticNoStart() throws Exception {
        checkTransformSequential0(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformSequentialPessimisticNoStart() throws Exception {
        checkTransformSequential0(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformSequentialOptimisticWithStart() throws Exception {
        checkTransformSequential0(true, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformSequentialPessimisticWithStart() throws Exception {
        checkTransformSequential0(true, PESSIMISTIC);
    }

    /**
     * @param startVal Whether to put value.
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkTransformSequential0(boolean startVal, TransactionConcurrency concurrency)
        throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        final String key = primaryKeysForCache(cache, 1).get(0);

        Transaction tx = txShouldBeUsed() ? defaultInstance().transactions().txStart(concurrency, READ_COMMITTED) : null;

        try {
            if (startVal)
                cache.put(key, 2);
            else
                assertEquals(null, cache.get(key));

            Integer expRes = startVal ? 2 : null;

            assertEquals(String.valueOf(expRes), cache.invoke(key, INCR_PROCESSOR));

            expRes = startVal ? 3 : 1;

            assertEquals(String.valueOf(expRes), cache.invoke(key, INCR_PROCESSOR));

            expRes++;

            assertEquals(String.valueOf(expRes), cache.invoke(key, INCR_PROCESSOR));

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        Integer exp = (startVal ? 2 : 0) + 3;

        assertEquals(exp, cache.get(key));

        for (int i = 0; i < gridCount(); i++) {
            if (ignite(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key))
                assertEquals(exp, peek(jcache(i), key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformAfterRemoveOptimistic() throws Exception {
        checkTransformAfterRemove(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformAfterRemovePessimistic() throws Exception {
        checkTransformAfterRemove(PESSIMISTIC);
    }

    /**
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkTransformAfterRemove(TransactionConcurrency concurrency) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 4);

        Transaction tx = txShouldBeUsed() ? defaultInstance().transactions().txStart(concurrency, READ_COMMITTED) : null;

        try {
            cache.remove("key");

            cache.invoke("key", INCR_PROCESSOR);
            cache.invoke("key", INCR_PROCESSOR);
            cache.invoke("key", INCR_PROCESSOR);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)3, cache.get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformReturnValueGetOptimisticReadCommitted() throws Exception {
        checkTransformReturnValue(false, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformReturnValueGetOptimisticRepeatableRead() throws Exception {
        checkTransformReturnValue(false, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformReturnValueGetPessimisticReadCommitted() throws Exception {
        checkTransformReturnValue(false, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformReturnValueGetPessimisticRepeatableRead() throws Exception {
        checkTransformReturnValue(false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformReturnValuePutInTx() throws Exception {
        checkTransformReturnValue(true, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @param put Whether to put value.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkTransformReturnValue(boolean put,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation)
        throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        if (!put)
            cache.put("key", 1);

        Transaction tx = txShouldBeUsed() ? defaultInstance().transactions().txStart(concurrency, isolation) : null;

        try {
            if (put)
                cache.put("key", 1);

            cache.invoke("key", INCR_PROCESSOR);

            assertEquals((Integer)2, cache.get("key"));

            if (tx != null) {
                // Second get inside tx. Make sure read value is not transformed twice.
                assertEquals((Integer)2, cache.get("key"));

                tx.commit();
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAndPutAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);

        cacheAsync.getAndPut("key1", 10);

        IgniteFuture<Integer> fut1 = cacheAsync.future();

        cacheAsync.getAndPut("key2", 11);

        IgniteFuture<Integer> fut2 = cacheAsync.future();

        assertEquals((Integer)1, fut1.get(5000));
        assertEquals((Integer)2, fut2.get(5000));

        assertEquals((Integer)10, cache.get("key1"));
        assertEquals((Integer)11, cache.get("key2"));
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAndPutAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        IgniteFuture<Integer> fut1 = cache.getAndPutAsync("key1", 10);

        IgniteFuture<Integer> fut2 = cache.getAndPutAsync("key2", 11);

        assertEquals((Integer)1, fut1.get(5000));
        assertEquals((Integer)2, fut2.get(5000));

        assertEquals((Integer)10, cache.get("key1"));
        assertEquals((Integer)11, cache.get("key2"));
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutAsyncOld0() throws Exception {
        IgniteCache<String, Integer> cacheAsync = jcache().withAsync();

        cacheAsync.getAndPut("key1", 0);

        IgniteFuture<Integer> fut1 = cacheAsync.future();

        cacheAsync.getAndPut("key2", 1);

        IgniteFuture<Integer> fut2 = cacheAsync.future();

        assert fut1.get(5000) == null;
        assert fut2.get(5000) == null;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutAsync0() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteFuture<Integer> fut1 = cache.getAndPutAsync("key1", 0);

        IgniteFuture<Integer> fut2 = cache.getAndPutAsync("key2", 1);

        assert fut1.get(5000) == null;
        assert fut2.get(5000) == null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        assertNull(cacheAsync.invoke("key1", INCR_PROCESSOR));

        IgniteFuture<?> fut0 = cacheAsync.future();

        assertNull(cacheAsync.invoke("key2", INCR_PROCESSOR));

        IgniteFuture<?> fut1 = cacheAsync.future();

        assertNull(cacheAsync.invoke("key3", RMV_PROCESSOR));

        IgniteFuture<?> fut2 = cacheAsync.future();

        fut0.get();
        fut1.get();
        fut2.get();

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("key3", ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        IgniteFuture<?> fut0 = cache.invokeAsync("key1", INCR_PROCESSOR);

        IgniteFuture<?> fut1 = cache.invokeAsync("key2", INCR_PROCESSOR);

        IgniteFuture<?> fut2 = cache.invokeAsync("key3", RMV_PROCESSOR);

        fut0.get();
        fut1.get();
        fut2.get();

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("key3", ONHEAP));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvoke() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        assertEquals("null", cache.invoke("k0", INCR_PROCESSOR));

        assertEquals((Integer)1, cache.get("k0"));

        assertEquals("1", cache.invoke("k0", INCR_PROCESSOR));

        assertEquals((Integer)2, cache.get("k0"));

        cache.put("k1", 1);

        assertEquals("1", cache.invoke("k1", INCR_PROCESSOR));

        assertEquals((Integer)2, cache.get("k1"));

        assertEquals("2", cache.invoke("k1", INCR_PROCESSOR));

        assertEquals((Integer)3, cache.get("k1"));

        EntryProcessor<String, Integer, Integer> c = new RemoveAndReturnNullEntryProcessor();

        assertNull(cache.invoke("k1", c));
        assertNull(cache.get("k1"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(jcache(i).localPeek("k1", ONHEAP));

        final EntryProcessor<String, Integer, Integer> errProc = new FailedEntryProcessor();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.invoke("k1", errProc);

                return null;
            }
        }, EntryProcessorException.class, "Test entry processor exception.");
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutx() throws Exception {
        if (txShouldBeUsed())
            checkPut(true);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutxNoTx() throws Exception {
        checkPut(false);
    }

    /**
     * @param inTx Whether to start transaction.
     * @throws Exception If failed.
     */
    private void checkPut(boolean inTx) throws Exception {
        Transaction tx = inTx ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        try {
            cache.put("key1", 1);
            cache.put("key2", 2);

            // Check inside transaction.
            assert cache.get("key1") == 1;
            assert cache.get("key2") == 2;

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkSize(F.asSet("key1", "key2"));

        // Check outside transaction.
        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");
        checkContainsKey(false, "wrong");

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;
        assert cache.get("wrong") == null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAsyncOld() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cacheAsync = jcache().withAsync();

        try {
            jcache().put("key2", 1);

            cacheAsync.put("key1", 10);

            IgniteFuture<?> fut1 = cacheAsync.future();

            cacheAsync.put("key2", 11);

            IgniteFuture<?> fut2 = cacheAsync.future();

            IgniteFuture<Transaction> f = null;

            if (tx != null) {
                tx = (Transaction)tx.withAsync();

                tx.commit();

                f = tx.future();
            }

            assertNull(fut1.get());
            assertNull(fut2.get());

            assert f == null || f.get().state() == COMMITTED;
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkSize(F.asSet("key1", "key2"));

        assert jcache().get("key1") == 10;
        assert jcache().get("key2") == 11;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAsync() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            jcache().put("key2", 1);

            IgniteFuture<?> fut1 = jcache().putAsync("key1", 10);

            IgniteFuture<?> fut2 = jcache().putAsync("key2", 11);

            IgniteFuture<Void> f = null;

            if (tx != null)
                f = tx.commitAsync();

            assertNull(fut1.get());
            assertNull(fut2.get());

            try {
                if (f != null)
                    f.get();
            }
            catch (Throwable t) {
                assert false : "Unexpected exception " + t;
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkSize(F.asSet("key1", "key2"));

        assert jcache().get("key1") == 10;
        assert jcache().get("key2") == 11;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutAll() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        IgniteCache<String, Integer> cache = jcache();

        cache.putAll(map);

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 1;
        assert cache.get("key2") == 2;

        map.put("key1", 10);
        map.put("key2", 20);

        cache.putAll(map);

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 10;
        assert cache.get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testNullInTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        final IgniteCache<String, Integer> cache = jcache();

        for (int i = 0; i < 100; i++) {
            final String key = "key-" + i;

            assertNull(cache.get(key));

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteTransactions txs = transactions();

                    try (Transaction tx = txs.txStart()) {
                        cache.put(key, 1);

                        cache.put(null, 2);

                        tx.commit();
                    }

                    return null;
                }
            }, NullPointerException.class, null);

            assertNull(cache.get(key));

            cache.put(key, 1);

            assertEquals(1, (int)cache.get(key));

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteTransactions txs = transactions();

                    try (Transaction tx = txs.txStart()) {
                        cache.put(key, 2);

                        cache.remove(null);

                        tx.commit();
                    }

                    return null;
                }
            }, NullPointerException.class, null);

            assertEquals(1, (int)cache.get(key));

            cache.put(key, 2);

            assertEquals(2, (int)cache.get(key));

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteTransactions txs = transactions();

                    Map<String, Integer> map = new LinkedHashMap<>();

                    map.put("k1", 1);
                    map.put("k2", 2);
                    map.put(null, 3);

                    try (Transaction tx = txs.txStart()) {
                        cache.put(key, 1);

                        cache.putAll(map);

                        tx.commit();
                    }

                    return null;
                }
            }, NullPointerException.class, null);

            assertNull(cache.get("k1"));
            assertNull(cache.get("k2"));

            assertEquals(2, (int)cache.get(key));

            cache.put(key, 3);

            assertEquals(3, (int)cache.get(key));
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        {
            final Map<String, Integer> m = new LinkedHashMap<>(2);

            m.put("key1", 1);
            m.put(null, 2);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.putAll(m);

                    return null;
                }
            }, NullPointerException.class, null);

            cache.put("key1", 1);

            assertEquals(1, (int)cache.get("key1"));
        }

        {
            final Map<String, Integer> m = new LinkedHashMap<>(2);

            m.put("key3", 3);
            m.put("key4", null);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.putAll(m);

                    return null;
                }
            }, NullPointerException.class, null);

            m.put("key4", 4);

            cache.putAll(m);

            assertEquals(3, (int)cache.get("key3"));
            assertEquals(4, (int)cache.get("key4"));
        }

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.put("key1", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.getAndPut("key1", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.put(null, 1);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace(null, 1);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.getAndReplace(null, 1);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace("key", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.getAndReplace("key", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace(null, 1, 2);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace("key", null, 2);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.replace("key", 1, null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutAllAsyncOld() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.putAll(map);

        IgniteFuture<?> f1 = cacheAsync.future();

        map.put("key1", 10);
        map.put("key2", 20);

        cacheAsync.putAll(map);

        IgniteFuture<?> f2 = cacheAsync.future();

        assertNull(f2.get());
        assertNull(f1.get());

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 10;
        assert cache.get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutAllAsync() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        IgniteCache<String, Integer> cache = jcache();

        IgniteFuture<?> f1 = cache.putAllAsync(map);

        map.put("key1", 10);
        map.put("key2", 20);

        IgniteFuture<?> f2 = cache.putAllAsync(map);

        assertNull(f2.get());
        assertNull(f1.get());

        checkSize(F.asSet("key1", "key2"));

        assert cache.get("key1") == 10;
        assert cache.get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAndPutIfAbsent() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        try {
            assert cache.getAndPutIfAbsent("key", 1) == null;

            assert cache.get("key") != null;
            assert cache.get("key") == 1;

            assert cache.getAndPutIfAbsent("key", 2) != null;
            assert cache.getAndPutIfAbsent("key", 2) == 1;

            assert cache.get("key") != null;
            assert cache.get("key") == 1;

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.getAndPutIfAbsent("key", 2) != null;

        for (int i = 0; i < gridCount(); i++) {
            info("Peek on node [i=" + i + ", id=" + grid(i).localNode().id() + ", val=" +
                grid(i).cache(DEFAULT_CACHE_NAME).localPeek("key", ONHEAP) + ']');
        }

        assertEquals((Integer)1, cache.getAndPutIfAbsent("key", 2));

        assert cache.get("key") != null;
        assert cache.get("key") == 1;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        assertEquals((Integer)1, cache.getAndPutIfAbsent("key2", 3));

        // Check db.
        if (!isMultiJvm()) {
            storeStgy.putToStore("key3", 3);

            assertEquals((Integer)3, cache.getAndPutIfAbsent("key3", 4));

            assertEquals((Integer)3, cache.get("key3"));
        }

        assertEquals((Integer)1, cache.get("key2"));

        cache.localEvict(Collections.singleton("key2"));

        // Same checks inside tx.
        tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assertEquals((Integer)1, cache.getAndPutIfAbsent("key2", 3));

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache.get("key2"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPutIfAbsentAsyncOld() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        try {
            cacheAsync.getAndPutIfAbsent("key", 1);

            IgniteFuture<Integer> fut1 = cacheAsync.future();

            assertNull(fut1.get());
            assertEquals((Integer)1, cache.get("key"));

            cacheAsync.getAndPutIfAbsent("key", 2);

            IgniteFuture<Integer> fut2 = cacheAsync.future();

            assertEquals((Integer)1, fut2.get());
            assertEquals((Integer)1, cache.get("key"));

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        cacheAsync.getAndPutIfAbsent("key2", 3);

        assertEquals((Integer)1, cacheAsync.<Integer>future().get());

        // Check db.
        if (!isMultiJvm()) {
            storeStgy.putToStore("key3", 3);

            cacheAsync.getAndPutIfAbsent("key3", 4);

            assertEquals((Integer)3, cacheAsync.<Integer>future().get());
        }

        cache.localEvict(Collections.singleton("key2"));

        // Same checks inside tx.
        tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            cacheAsync.getAndPutIfAbsent("key2", 3);

            assertEquals(1, cacheAsync.future().get());

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache.get("key2"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPutIfAbsentAsync() throws Exception {
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        IgniteCache<String, Integer> cache = jcache();

        try {
            IgniteFuture<Integer> fut1 = cache.getAndPutIfAbsentAsync("key", 1);

            assertNull(fut1.get());
            assertEquals((Integer)1, cache.get("key"));

            IgniteFuture<Integer> fut2 = cache.getAndPutIfAbsentAsync("key", 2);

            assertEquals((Integer)1, fut2.get());
            assertEquals((Integer)1, cache.get("key"));

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        assertEquals((Integer)1, cache.getAndPutIfAbsentAsync("key2", 3).get());

        // Check db.
        if (!isMultiJvm()) {
            storeStgy.putToStore("key3", 3);

            assertEquals((Integer)3, cache.getAndPutIfAbsentAsync("key3", 4).get());
        }

        cache.localEvict(Collections.singleton("key2"));

        // Same checks inside tx.
        tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assertEquals(1, (int)cache.getAndPutIfAbsentAsync("key2", 3).get());

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache.get("key2"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsent() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        assertNull(cache.get("key"));
        assert cache.putIfAbsent("key", 1);
        assert cache.get("key") != null && cache.get("key") == 1;
        assert !cache.putIfAbsent("key", 2);
        assert cache.get("key") != null && cache.get("key") == 1;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        assertFalse(cache.putIfAbsent("key2", 3));

        // Check db.
        if (!isMultiJvm()) {
            storeStgy.putToStore("key3", 3);

            assertFalse(cache.putIfAbsent("key3", 4));
        }

        cache.localEvict(Collections.singleton("key2"));

        // Same checks inside tx.
        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assertFalse(cache.putIfAbsent("key2", 3));

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache.get("key2"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutxIfAbsentAsync() throws Exception {
        if (txShouldBeUsed())
            checkPutxIfAbsentAsync(true);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutxIfAbsentAsyncNoTx() throws Exception {
        checkPutxIfAbsentAsync(false);
    }

    /**
     * @param inTx In tx flag.
     * @throws Exception If failed.
     */
    private void checkPutxIfAbsentAsyncOld(boolean inTx) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cacheAsync.putIfAbsent("key", 1);

        IgniteFuture<Boolean> fut1 = cacheAsync.future();

        assert fut1.get();
        assert cache.get("key") != null && cache.get("key") == 1;

        cacheAsync.putIfAbsent("key", 2);

        IgniteFuture<Boolean> fut2 = cacheAsync.future();

        assert !fut2.get();
        assert cache.get("key") != null && cache.get("key") == 1;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        cacheAsync.putIfAbsent("key2", 3);

        assertFalse(cacheAsync.<Boolean>future().get());

        // Check db.
        if (!isMultiJvm()) {
            storeStgy.putToStore("key3", 3);

            cacheAsync.putIfAbsent("key3", 4);

            assertFalse(cacheAsync.<Boolean>future().get());
        }

        cache.localEvict(Collections.singletonList("key2"));

        // Same checks inside tx.
        Transaction tx = inTx ? transactions().txStart() : null;

        try {
            cacheAsync.putIfAbsent("key2", 3);

            assertFalse(cacheAsync.<Boolean>future().get());

            if (!isMultiJvm()) {
                cacheAsync.putIfAbsent("key3", 4);

                assertFalse(cacheAsync.<Boolean>future().get());
            }

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache.get("key2"));

        if (!isMultiJvm())
            assertEquals((Integer)3, cache.get("key3"));
    }

    /**
     * @param inTx In tx flag.
     * @throws Exception If failed.
     */
    private void checkPutxIfAbsentAsync(boolean inTx) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteFuture<Boolean> fut1 = cache.putIfAbsentAsync("key", 1);

        assert fut1.get();
        assert cache.get("key") != null && cache.get("key") == 1;

        IgniteFuture<Boolean> fut2 = cache.putIfAbsentAsync("key", 2);

        assert !fut2.get();
        assert cache.get("key") != null && cache.get("key") == 1;

        // Check swap.
        cache.put("key2", 1);

        cache.localEvict(Collections.singleton("key2"));

        assertFalse(cache.putIfAbsentAsync("key2", 3).get());

        // Check db.
        if (!isMultiJvm()) {
            storeStgy.putToStore("key3", 3);

            assertFalse(cache.putIfAbsentAsync("key3", 4).get());
        }

        cache.localEvict(Collections.singletonList("key2"));

        // Same checks inside tx.
        Transaction tx = inTx ? transactions().txStart() : null;

        try {
            assertFalse(cache.putIfAbsentAsync("key2", 3).get());

            if (!isMultiJvm())
                assertFalse(cache.putIfAbsentAsync("key3", 4).get());

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache.get("key2"));

        if (!isMultiJvm())
            assertEquals((Integer)3, cache.get("key3"));
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutIfAbsentAsyncConcurrentOld() throws Exception {
        IgniteCache<String, Integer> cacheAsync = jcache().withAsync();

        cacheAsync.putIfAbsent("key1", 1);

        IgniteFuture<Boolean> fut1 = cacheAsync.future();

        cacheAsync.putIfAbsent("key2", 2);

        IgniteFuture<Boolean> fut2 = cacheAsync.future();

        assert fut1.get();
        assert fut2.get();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPutIfAbsentAsyncConcurrent() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteFuture<Boolean> fut1 = cache.putIfAbsentAsync("key1", 1);

        IgniteFuture<Boolean> fut2 = cache.putIfAbsentAsync("key2", 2);

        assert fut1.get();
        assert fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndReplace() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        info("key 1 -> 2");

        assert cache.getAndReplace("key", 2) == 1;

        assert cache.get("key") == 2;

        assert cache.getAndReplace("wrong", 0) == null;

        assert cache.get("wrong") == null;

        info("key 0 -> 3");

        assert !cache.replace("key", 0, 3);

        assert cache.get("key") == 2;

        info("key 0 -> 3");

        assert !cache.replace("key", 0, 3);

        assert cache.get("key") == 2;

        info("key 2 -> 3");

        assert cache.replace("key", 2, 3);

        assert cache.get("key") == 3;

        info("evict key");

        cache.localEvict(Collections.singleton("key"));

        info("key 3 -> 4");

        assert cache.replace("key", 3, 4);

        assert cache.get("key") == 4;

        if (!isMultiJvm()) {
            storeStgy.putToStore("key2", 5);

            info("key2 5 -> 6");

            assert cache.replace("key2", 5, 6);
        }

        for (int i = 0; i < gridCount(); i++) {
            info("Peek key on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).cache(DEFAULT_CACHE_NAME).localPeek("key", ONHEAP) + ']');

            info("Peek key2 on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).cache(DEFAULT_CACHE_NAME).localPeek("key2", ONHEAP) + ']');
        }

        if (!isMultiJvm())
            assertEquals((Integer)6, cache.get("key2"));

        cache.localEvict(Collections.singleton("key"));

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assert cache.replace("key", 4, 5);

            if (tx != null)
                tx.commit();

            assert cache.get("key") == 5;
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplace() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        assert cache.replace("key", 2);

        assert cache.get("key") == 2;

        assert !cache.replace("wrong", 2);

        cache.localEvict(Collections.singleton("key"));

        assert cache.replace("key", 4);

        assert cache.get("key") == 4;

        if (!isMultiJvm()) {
            storeStgy.putToStore("key2", 5);

            assert cache.replace("key2", 6);

            assertEquals((Integer)6, cache.get("key2"));
        }

        cache.localEvict(Collections.singleton("key"));

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assert cache.replace("key", 5);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndReplaceAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        cacheAsync.getAndReplace("key", 2);

        assert cacheAsync.<Integer>future().get() == 1;

        assert cache.get("key") == 2;

        cacheAsync.getAndReplace("wrong", 0);

        assert cacheAsync.future().get() == null;

        assert cache.get("wrong") == null;

        cacheAsync.replace("key", 0, 3);

        assert !cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 2;

        cacheAsync.replace("key", 0, 3);

        assert !cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 2;

        cacheAsync.replace("key", 2, 3);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 3;

        cache.localEvict(Collections.singleton("key"));

        cacheAsync.replace("key", 3, 4);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 4;

        if (!isMultiJvm()) {
            storeStgy.putToStore("key2", 5);

            cacheAsync.replace("key2", 5, 6);

            assert cacheAsync.<Boolean>future().get();

            assertEquals((Integer)6, cache.get("key2"));
        }

        cache.localEvict(Collections.singleton("key"));

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            cacheAsync.replace("key", 4, 5);

            assert cacheAsync.<Boolean>future().get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndReplaceAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        assert cache.getAndReplaceAsync("key", 2).get() == 1;

        assert cache.get("key") == 2;

        assert cache.getAndReplaceAsync("wrong", 0).get() == null;

        assert cache.get("wrong") == null;

        assert !cache.replaceAsync("key", 0, 3).get();

        assert cache.get("key") == 2;

        assert !cache.replaceAsync("key", 0, 3).get();

        assert cache.get("key") == 2;

        assert cache.replaceAsync("key", 2, 3).get();

        assert cache.get("key") == 3;

        cache.localEvict(Collections.singleton("key"));

        assert cache.replaceAsync("key", 3, 4).get();

        assert cache.get("key") == 4;

        if (!isMultiJvm()) {
            storeStgy.putToStore("key2", 5);

            assert cache.replaceAsync("key2", 5, 6).get();

            assertEquals((Integer)6, cache.get("key2"));
        }

        cache.localEvict(Collections.singleton("key"));

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assert cache.replaceAsync("key", 4, 5).get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplacexAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        cacheAsync.replace("key", 2);

        assert cacheAsync.<Boolean>future().get();

        info("Finished replace.");

        assertEquals((Integer)2, cache.get("key"));

        cacheAsync.replace("wrond", 2);

        assert !cacheAsync.<Boolean>future().get();

        cache.localEvict(Collections.singleton("key"));

        cacheAsync.replace("key", 4);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key") == 4;

        if (!isMultiJvm()) {
            storeStgy.putToStore("key2", 5);

            cacheAsync.replace("key2", 6);

            assert cacheAsync.<Boolean>future().get();

            assert cache.get("key2") == 6;
        }

        cache.localEvict(Collections.singleton("key"));

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            cacheAsync.replace("key", 5);

            assert cacheAsync.<Boolean>future().get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplacexAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key", 1);

        assert cache.get("key") == 1;

        assert cache.replaceAsync("key", 2).get();

        info("Finished replace.");

        assertEquals((Integer)2, cache.get("key"));

        assert !cache.replaceAsync("wrond", 2).get();

        cache.localEvict(Collections.singleton("key"));

        assert cache.replaceAsync("key", 4).get();

        assert cache.get("key") == 4;

        if (!isMultiJvm()) {
            storeStgy.putToStore("key2", 5);

            assert cache.replaceAsync("key2", 6).get();

            assert cache.get("key2") == 6;
        }

        cache.localEvict(Collections.singleton("key"));

        Transaction tx = txShouldBeUsed() ? transactions().txStart() : null;

        try {
            assert cache.replaceAsync("key", 5).get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache.get("key") == 5;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGetAndRemove() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        assert !cache.remove("key1", 0);
        assert cache.get("key1") != null && cache.get("key1") == 1;
        assert cache.remove("key1", 1);
        assert cache.get("key1") == null;
        assert cache.getAndRemove("key2") == 2;
        assert cache.get("key2") == null;
        assert cache.getAndRemove("key2") == null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndRemoveObject() throws Exception {
        IgniteCache<String, TestValue> cache = defaultInstance().cache(DEFAULT_CACHE_NAME);

        TestValue val1 = new TestValue(1);
        TestValue val2 = new TestValue(2);

        cache.put("key1", val1);
        cache.put("key2", val2);

        assert !cache.remove("key1", new TestValue(0));

        TestValue oldVal = cache.get("key1");

        assert oldVal != null && Objects.equals(val1, oldVal);

        assert cache.remove("key1");

        assert cache.get("key1") == null;

        TestValue oldVal2 = cache.getAndRemove("key2");

        assert Objects.equals(val2, oldVal2);

        assert cache.get("key2") == null;
        assert cache.getAndRemove("key2") == null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAndPutObject() throws Exception {
        IgniteCache<String, TestValue> cache = defaultInstance().cache(DEFAULT_CACHE_NAME);

        TestValue val1 = new TestValue(1);
        TestValue val2 = new TestValue(2);

        cache.put("key1", val1);

        TestValue oldVal = cache.get("key1");

        assertEquals(val1, oldVal);

        oldVal = cache.getAndPut("key1", val2);

        assertEquals(val1, oldVal);

        TestValue updVal = cache.get("key1");

        assertEquals(val2, updVal);
    }

    /**
     * TODO: GG-11241.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeletedEntriesFlag() throws Exception {
        if (cacheMode() == PARTITIONED) {
            final int cnt = 3;

            IgniteCache<String, Integer> cache = jcache();

            for (int i = 0; i < cnt; i++)
                cache.put(String.valueOf(i), i);

            for (int i = 0; i < cnt; i++)
                cache.remove(String.valueOf(i));

            for (int g = 0; g < gridCount(); g++)
                executeOnLocalOrRemoteJvm(g, new CheckEntriesDeletedTask(cnt));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveLoad() throws Exception {
        int cnt = 10;

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < cnt; i++)
            keys.add(String.valueOf(i));

        jcache().removeAll(keys);

        for (String key : keys)
            storeStgy.putToStore(key, Integer.parseInt(key));

        for (int g = 0; g < gridCount(); g++)
            grid(g).cache(DEFAULT_CACHE_NAME).localLoadCache(null);

        for (int g = 0; g < gridCount(); g++) {
            for (int i = 0; i < cnt; i++) {
                String key = String.valueOf(i);

                if (defaultInstance().affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode()))
                    assertEquals((Integer)i, peek(jcache(g), key));
                else
                    assertNull(peek(jcache(g), key));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveLoadAsync() throws Exception {
        if (isMultiJvm())
            return;

        int cnt = 10;

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < cnt; i++)
            keys.add(String.valueOf(i));

        jcache().removeAllAsync(keys).get();

        for (String key : keys)
            storeStgy.putToStore(key, Integer.parseInt(key));

        for (int g = 0; g < gridCount(); g++)
            grid(g).cache(DEFAULT_CACHE_NAME).localLoadCacheAsync(null).get();

        for (int g = 0; g < gridCount(); g++) {
            for (int i = 0; i < cnt; i++) {
                String key = String.valueOf(i);

                if (defaultInstance().affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode()))
                    assertEquals((Integer)i, peek(jcache(g), key));
                else
                    assertNull(peek(jcache(g), key));
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);

        cacheAsync.remove("key1", 0);

        assert !cacheAsync.<Boolean>future().get();

        assert cache.get("key1") != null && cache.get("key1") == 1;

        cacheAsync.remove("key1", 1);

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key1") == null;

        cacheAsync.getAndRemove("key2");

        assert cacheAsync.<Integer>future().get() == 2;

        assert cache.get("key2") == null;

        cacheAsync.getAndRemove("key2");

        assert cacheAsync.future().get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        assert !cache.removeAsync("key1", 0).get();

        assert cache.get("key1") != null && cache.get("key1") == 1;

        assert cache.removeAsync("key1", 1).get();

        assert cache.get("key1") == null;

        assert cache.getAndRemoveAsync("key2").get() == 2;

        assert cache.get("key2") == null;

        assert cache.getAndRemoveAsync("key2").get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemove() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);

        assert cache.remove("key1");
        assert cache.get("key1") == null;
        assert !cache.remove("key1");
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemovexAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);

        cacheAsync.remove("key1");

        assert cacheAsync.<Boolean>future().get();

        assert cache.get("key1") == null;

        cacheAsync.remove("key1");

        assert !cacheAsync.<Boolean>future().get();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemovexAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);

        assert cache.removeAsync("key1").get();

        assert cache.get("key1") == null;

        assert !cache.removeAsync("key1").get();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGlobalRemoveAll() throws Exception {
        globalRemoveAll(false);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testGlobalRemoveAllAsync() throws Exception {
        globalRemoveAll(true);
    }

    /**
     * @param async If {@code true} uses asynchronous operation.
     * @throws Exception In case of error.
     */
    private void globalRemoveAllOld(boolean async) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        IgniteCache<String, Integer> asyncCache = cache.withAsync();

        if (async) {
            asyncCache.removeAll(F.asSet("key1", "key2"));

            asyncCache.future().get();
        }
        else
            cache.removeAll(F.asSet("key1", "key2"));

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");

        // Put values again.
        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        if (async) {
            IgniteCache<String, Integer> asyncCache0 = jcache(gridCount() > 1 ? 1 : 0).withAsync();

            asyncCache0.removeAll();

            asyncCache0.future().get();
        }
        else
            jcache(gridCount() > 1 ? 1 : 0).removeAll();

        assertEquals(0, cache.localSize());
        long entryCnt = hugeRemoveAllEntryCount();

        for (int i = 0; i < entryCnt; i++)
            cache.put(String.valueOf(i), i);

        for (int i = 0; i < entryCnt; i++)
            assertEquals(Integer.valueOf(i), cache.get(String.valueOf(i)));

        if (async) {
            asyncCache.removeAll();

            asyncCache.future().get();
        }
        else
            cache.removeAll();

        for (int i = 0; i < entryCnt; i++)
            assertNull(cache.get(String.valueOf(i)));
    }

    /**
     * @param async If {@code true} uses asynchronous operation.
     * @throws Exception In case of error.
     */
    private void globalRemoveAll(boolean async) throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        if (async)
            cache.removeAllAsync(F.asSet("key1", "key2")).get();
        else
            cache.removeAll(F.asSet("key1", "key2"));

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");

        // Put values again.
        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        if (async)
            jcache(gridCount() > 1 ? 1 : 0).removeAllAsync().get();
        else
            jcache(gridCount() > 1 ? 1 : 0).removeAll();

        assertEquals(0, cache.localSize());
        long entryCnt = hugeRemoveAllEntryCount();

        for (int i = 0; i < entryCnt; i++)
            cache.put(String.valueOf(i), i);

        for (int i = 0; i < entryCnt; i++)
            assertEquals(Integer.valueOf(i), cache.get(String.valueOf(i)));

        if (async)
            cache.removeAllAsync().get();
        else
            cache.removeAll();

        for (int i = 0; i < entryCnt; i++)
            assertNull(cache.get(String.valueOf(i)));
    }

    /**
     * @return Count of entries to be removed in removeAll() test.
     */
    protected long hugeRemoveAllEntryCount() {
        return 1000L;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAllWithNulls() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final Set<String> c = new LinkedHashSet<>();

        c.add("key1");
        c.add(null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(c);

                return null;
            }
        }, NullPointerException.class, null);

        assertEquals(0, defaultInstance().cache(DEFAULT_CACHE_NAME).localSize());

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(null);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.remove(null);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.getAndRemove(null);

                return null;
            }
        }, NullPointerException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.remove("key1", null);

                return null;
            }
        }, NullPointerException.class, null);
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAllDuplicates() throws Exception {
        jcache().removeAll(ImmutableSet.of("key1", "key1", "key1"));
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAllDuplicatesTx() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart()) {
                jcache().removeAll(ImmutableSet.of("key1", "key1", "key1"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAllEmpty() throws Exception {
        jcache().removeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAllAsyncOld() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheAsync = cache.withAsync();

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        cacheAsync.removeAll(F.asSet("key1", "key2"));

        assertNull(cacheAsync.future().get());

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testRemoveAllAsync() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);
        cache.put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        assertNull(cache.removeAllAsync(F.asSet("key1", "key2")).get());

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testLoadAll() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        Set<String> keys = new HashSet<>(primaryKeysForCache(cache, 2));

        for (String key : keys)
            assertNull(cache.localPeek(key, ONHEAP));

        Map<String, Integer> vals = new HashMap<>();

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        cache.clear();

        for (String key : keys)
            assertNull(peek(cache, key));

        loadAll(cache, keys, true);

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAfterClear() throws Exception {
        IgniteEx ignite = defaultInstance();

        boolean affNode = ignite.context().cache().internalCache(DEFAULT_CACHE_NAME).context().affinityNode();

        if (!affNode) {
            if (gridCount() < 2)
                return;

            ignite = grid(1);
        }

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int key = 0;

        Collection<Integer> keys = new ArrayList<>();

        for (int k = 0; k < 2; k++) {
            while (!ignite.affinity(DEFAULT_CACHE_NAME).isPrimary(ignite.localNode(), key))
                key++;

            keys.add(key);

            key++;
        }

        info("Keys: " + keys);

        for (Integer k : keys)
            cache.put(k, k);

        cache.clear();

        for (int g = 0; g < gridCount(); g++) {
            Ignite grid0 = grid(g);

            grid0.cache(DEFAULT_CACHE_NAME).removeAll();

            assertTrue(grid0.cache(DEFAULT_CACHE_NAME).localSize() == 0);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testClear() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        Set<String> keys = new HashSet<>(primaryKeysForCache(cache, 3));

        for (String key : keys)
            assertNull(cache.get(key));

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        cache.clear();

        for (String key : keys)
            assertNull(peek(cache, key));

        for (i = 0; i < gridCount(); i++)
            jcache(i).clear();

        for (i = 0; i < gridCount(); i++)
            assert jcache(i).localSize() == 0;

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            cache.put(entry.getKey(), entry.getValue());

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        String first = F.first(keys);

        if (lockingEnabled()) {
            Lock lock = cache.lock(first);

            lock.lock();

            try {
                cache.clear();

                GridCacheContext<String, Integer> cctx = context(0);

                GridCacheEntryEx entry = cctx.isNear() ? cctx.near().dht().peekEx(first) :
                    cctx.cache().peekEx(first);

                assertNotNull(entry);
            }
            finally {
                lock.unlock();
            }
        }
        else {
            cache.clear();

            cache.put(first, vals.get(first));
        }

        cache.clear();

        assert cache.localSize() == 0 : "Values after clear.";

        i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        cache.put("key1", 1);
        cache.put("key2", 2);

        cache.localEvict(Sets.union(ImmutableSet.of("key1", "key2"), keys));

        assert cache.localSize(ONHEAP) == 0;

        cache.clear();

        assert cache.localPeek("key1", ONHEAP) == null;
        assert cache.localPeek("key2", ONHEAP) == null;
    }

    /**
     * @param keys0 Keys to check.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkUnlocked(final Collection<String> keys0) throws IgniteCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    for (int i = 0; i < gridCount(); i++) {
                        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)ignite(i)).internalCache(DEFAULT_CACHE_NAME);

                        for (String key : keys0) {
                            GridCacheEntryEx entry = cache.peekEx(key);

                            if (entry != null) {
                                if (entry.lockedByAny()) {
                                    info("Entry is still locked [i=" + i + ", entry=" + entry + ']');

                                    return false;
                                }
                            }

                            if (cache.isNear()) {
                                entry = cache.context().near().dht().peekEx(key);

                                if (entry != null) {
                                    if (entry.lockedByAny()) {
                                        info("Entry is still locked [i=" + i + ", entry=" + entry + ']');

                                        return false;
                                    }
                                }
                            }
                        }
                    }

                    return true;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    info("Entry was removed, will retry");

                    return false;
                }
            }
        }, 10_000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearAll() throws Exception {
        globalClearAll(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearAllAsyncOld() throws Exception {
        globalClearAll(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearAllAsync() throws Exception {
        globalClearAll(true, false);
    }

    /**
     * @param async If {@code true} uses async method.
     * @param oldAsync Use old async API.
     * @throws Exception If failed.
     */
    protected void globalClearAll(boolean async, boolean oldAsync) throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < gridCount(); i++) {
            for (String key : primaryKeysForCache(jcache(i), 3, 100_000))
                jcache(i).put(key, 1);
        }

        if (async) {
            if (oldAsync) {
                IgniteCache<String, Integer> asyncCache = jcache().withAsync();

                asyncCache.clear();

                asyncCache.future().get();
            }
            else
                jcache().clearAsync().get();
        }
        else
            jcache().clear();

        for (int i = 0; i < gridCount(); i++)
            assert jcache(i).localSize() == 0;
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    @Test
    public void testLockUnlock() throws Exception {
        if (lockingEnabled()) {
            final CountDownLatch lockCnt = new CountDownLatch(1);
            final CountDownLatch unlockCnt = new CountDownLatch(1);

            defaultInstance().events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_LOCKED:
                            lockCnt.countDown();

                            break;
                        case EVT_CACHE_OBJECT_UNLOCKED:
                            unlockCnt.countDown();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            IgniteCache<String, Integer> cache = jcache();

            String key = primaryKeysForCache(cache, 1).get(0);

            cache.put(key, 1);

            assert !cache.isLocalLocked(key, false);

            Lock lock = cache.lock(key);

            lock.lock();

            try {
                lockCnt.await();

                assert cache.isLocalLocked(key, false);
            }
            finally {
                lock.unlock();
            }

            unlockCnt.await();

            for (int i = 0; i < 100; i++)
                if (cache.isLocalLocked(key, false))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocalLocked(key, false);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    @Test
    public void testLockUnlockAll() throws Exception {
        if (lockingEnabled()) {
            IgniteCache<String, Integer> cache = jcache();

            cache.put("key1", 1);
            cache.put("key2", 2);

            assert !cache.isLocalLocked("key1", false);
            assert !cache.isLocalLocked("key2", false);

            Lock lock1_2 = cache.lockAll(ImmutableSet.of("key1", "key2"));

            lock1_2.lock();

            try {
                assert cache.isLocalLocked("key1", false);
                assert cache.isLocalLocked("key2", false);
            }
            finally {
                lock1_2.unlock();
            }

            for (int i = 0; i < 100; i++)
                if (cache.isLocalLocked("key1", false) || cache.isLocalLocked("key2", false))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocalLocked("key1", false);
            assert !cache.isLocalLocked("key2", false);

            lock1_2.lock();

            try {
                assert cache.isLocalLocked("key1", false);
                assert cache.isLocalLocked("key2", false);
            }
            finally {
                lock1_2.unlock();
            }

            for (int i = 0; i < 100; i++)
                if (cache.isLocalLocked("key1", false) || cache.isLocalLocked("key2", false))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocalLocked("key1", false);
            assert !cache.isLocalLocked("key2", false);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testPeek() throws Exception {
        Ignite ignite = primaryIgnite("key");
        IgniteCache<String, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        assert peek(cache, "key") == null;

        cache.put("key", 1);

        cache.replace("key", 2);

        assertEquals(2, peek(cache, "key").intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPeekTxRemoveOptimistic() throws Exception {
        checkPeekTxRemove(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPeekTxRemovePessimistic() throws Exception {
        checkPeekTxRemove(PESSIMISTIC);
    }

    /**
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkPeekTxRemove(TransactionConcurrency concurrency) throws Exception {
        if (txShouldBeUsed()) {
            Ignite ignite = primaryIgnite("key");
            IgniteCache<String, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

            cache.put("key", 1);

            try (Transaction tx = ignite.transactions().txStart(concurrency, READ_COMMITTED)) {
                cache.remove("key");

                assertNull(cache.get("key")); // localPeek ignores transactions.
                assertNotNull(peek(cache, "key")); // localPeek ignores transactions.

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPeekRemove() throws Exception {
        IgniteCache<String, Integer> cache = primaryCache("key");

        cache.put("key", 1);
        cache.remove("key");

        assertNull(peek(cache, "key"));
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testEvictExpired() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        long ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        defaultInstance().cache(DEFAULT_CACHE_NAME).withExpiryPolicy(expiry).put(key, 1);

        final Affinity<String> aff = defaultInstance().affinity(DEFAULT_CACHE_NAME);

        boolean wait = waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < gridCount(); i++) {
                    if (peek(jcache(i), key) != null)
                        return false;
                }

                return true;
            }
        }, ttl + 1000);

        assertTrue("Failed to wait for entry expiration.", wait);

        // Expired entry should not be swapped.
        cache.localEvict(Collections.singleton(key));

        assertNull(peek(cache, "key"));

        assertNull(cache.localPeek(key, ONHEAP));

        assertTrue(cache.localSize() == 0);

        load(cache, key, true);

        for (int i = 0; i < gridCount(); i++) {
            if (aff.isPrimary(grid(i).cluster().localNode(), key))
                assertEquals((Integer)1, peek(jcache(i), key));

            if (aff.isBackup(grid(i).cluster().localNode(), key))
                assertEquals((Integer)1, peek(jcache(i), key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPeekExpired() throws Exception {
        final IgniteCache<String, Integer> c = jcache();

        final String key = primaryKeysForCache(c, 1).get(0);

        info("Using key: " + key);

        c.put(key, 1);

        assertEquals(Integer.valueOf(1), peek(c, key));

        int ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        c.withExpiryPolicy(expiry).put(key, 1);

        Thread.sleep(ttl + 100);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return peek(c, key) == null;
            }
        }, 2000);

        assert peek(c, key) == null;

        assert c.localSize() == 0 : "Cache is not empty.";
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPeekExpiredTx() throws Exception {
        if (txShouldBeUsed()) {
            final IgniteCache<String, Integer> c = jcache();

            final String key = "1";
            int ttl = 500;

            try (Transaction tx = defaultInstance().transactions().txStart()) {
                final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

                defaultInstance().cache(DEFAULT_CACHE_NAME).withExpiryPolicy(expiry).put(key, 1);

                tx.commit();
            }

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return peek(c, key) == null;
                }
            }, 2000);

            assertNull(peek(c, key));

            assert c.localSize() == 0;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTtlTx() throws Exception {
        if (txShouldBeUsed())
            checkTtl(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTtlNoTx() throws Exception {
        checkTtl(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTtlNoTxOldEntry() throws Exception {
        checkTtl(false, true);
    }

    /**
     * @param inTx In tx flag.
     * @param oldEntry {@code True} to check TTL on old entry, {@code false} on new.
     * @throws Exception If failed.
     */
    private void checkTtl(boolean inTx, boolean oldEntry) throws Exception {
        int ttl = 4000;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        final IgniteCache<String, Integer> c = jcache();

        final String key = primaryKeysForCache(jcache(), 1).get(0);

        IgnitePair<Long> entryTtl;

        if (oldEntry) {
            c.put(key, 1);

            entryTtl = entryTtl(fullCache(), key);

            assertNotNull(entryTtl.get1());
            assertNotNull(entryTtl.get2());
            assertEquals((Long)0L, entryTtl.get1());
            assertEquals((Long)0L, entryTtl.get2());
        }

        long startTime = System.currentTimeMillis();

        if (inTx) {
            // Rollback transaction for the first time.
            Transaction tx = transactions().txStart();

            try {
                jcache().withExpiryPolicy(expiry).put(key, 1);
            }
            finally {
                tx.rollback();
            }

            if (oldEntry) {
                entryTtl = entryTtl(fullCache(), key);

                assertEquals((Long)0L, entryTtl.get1());
                assertEquals((Long)0L, entryTtl.get2());
            }
        }

        // Now commit transaction and check that ttl and expire time have been saved.
        Transaction tx = inTx ? transactions().txStart() : null;

        try {
            jcache().withExpiryPolicy(expiry).put(key, 1);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        long[] expireTimes = new long[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                IgnitePair<Long> curEntryTtl = entryTtl(jcache(i), key);

                assertNotNull(curEntryTtl.get1());
                assertNotNull(curEntryTtl.get2());
                assertTrue(curEntryTtl.get2() > startTime);

                expireTimes[i] = curEntryTtl.get2();
            }
        }

        // One more update from the same cache entry to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().withExpiryPolicy(expiry).put(key, 2);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                IgnitePair<Long> curEntryTtl = entryTtl(jcache(i), key);

                assertNotNull(curEntryTtl.get1());
                assertNotNull(curEntryTtl.get2());
                assertTrue(curEntryTtl.get2() > startTime);

                expireTimes[i] = curEntryTtl.get2();
            }
        }

        // And one more direct update to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().withExpiryPolicy(expiry).put(key, 3);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                IgnitePair<Long> curEntryTtl = entryTtl(jcache(i), key);

                assertNotNull(curEntryTtl.get1());
                assertNotNull(curEntryTtl.get2());
                assertTrue(curEntryTtl.get2() > startTime);

                expireTimes[i] = curEntryTtl.get2();
            }
        }

        // And one more update to ensure that ttl is not changed and expire time is not shifted forward.
        U.sleep(100);

        log.info("Put 4");

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().put(key, 4);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        log.info("Put 4 done");

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(i).localNode(), key)) {
                IgnitePair<Long> curEntryTtl = entryTtl(jcache(i), key);

                assertNotNull(curEntryTtl.get1());
                assertNotNull(curEntryTtl.get2());
                assertEquals(expireTimes[i], (long)curEntryTtl.get2());
            }
        }

        // Avoid reloading from store.
        storeStgy.removeFromStore(key);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() {
                try {
                    Integer val = c.get(key);

                    if (val != null) {
                        info("Value is in cache [key=" + key + ", val=" + val + ']');

                        return false;
                    }

                    // Get "cache" field from GridCacheProxyImpl.
                    GridCacheAdapter c0 = cacheFromCtx(c);

                    if (!c0.context().deferredDelete()) {
                        GridCacheEntryEx e0 = c0.peekEx(key);

                        return e0 == null || (e0.rawGet() == null && e0.valueBytes() == null);
                    }
                    else
                        return true;
                }
                catch (GridCacheEntryRemovedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, Math.min(ttl * 10, getTestTimeout())));

        assert c.get(key) == null;

        // Ensure that old TTL and expire time are not longer "visible".
        entryTtl = entryTtl(fullCache(), key);

        assertNotNull(entryTtl.get1());
        assertNotNull(entryTtl.get2());
        assertEquals(0, (long)entryTtl.get1());
        assertEquals(0, (long)entryTtl.get2());

        // Ensure that next update will not pick old expire time.

        tx = inTx ? transactions().txStart() : null;

        try {
            jcache().put(key, 10);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        U.sleep(2000);

        entryTtl = entryTtl(fullCache(), key);

        assertEquals((Integer)10, c.get(key));

        assertNotNull(entryTtl.get1());
        assertNotNull(entryTtl.get2());
        assertEquals(0, (long)entryTtl.get1());
        assertEquals(0, (long)entryTtl.get2());
    }

    /**
     * @throws Exception In case of error.
     */
    @Test(timeout = 10050000)
    public void testLocalEvict() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        List<String> keys = primaryKeysForCache(cache, 3);

        String key1 = keys.get(0);
        String key2 = keys.get(1);
        String key3 = keys.get(2);

        cache.put(key1, 1);
        cache.put(key2, 2);
        cache.put(key3, 3);

        assert peek(cache, key1) == 1;
        assert peek(cache, key2) == 2;
        assert peek(cache, key3) == 3;

        cache.localEvict(F.asList(key1, key2));

        assert cache.localPeek(key1, ONHEAP) == null;
        assert cache.localPeek(key2, ONHEAP) == null;
        assert peek(cache, key3) == 3;

        loadAll(cache, ImmutableSet.of(key1, key2), true);

        Affinity<String> aff = defaultInstance().affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < gridCount(); i++) {
            if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key1))
                assertEquals("node name = " + grid(i).name(), (Integer)1, peek(jcache(i), key1));

            if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key2))
                assertEquals((Integer)2, peek(jcache(i), key2));

            if (aff.isPrimaryOrBackup(grid(i).cluster().localNode(), key3))
                assertEquals((Integer)3, peek(jcache(i), key3));
        }
    }

    /**
     * JUnit.
     */
    @Test
    public void testCacheProxy() {
        IgniteCache<String, Integer> cache = jcache();

        assert cache instanceof IgniteCacheProxy;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCompactExpired() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        final String key = F.first(primaryKeysForCache(cache, 1));

        cache.put(key, 1);

        long ttl = 500;

        final ExpiryPolicy expiry = new TouchedExpiryPolicy(new Duration(MILLISECONDS, ttl));

        defaultInstance().cache(DEFAULT_CACHE_NAME).withExpiryPolicy(expiry).put(key, 1);

        waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.localPeek(key) == null;
            }
        }, ttl + 1000);

        // Peek will actually remove entry from cache.
        assertNull(cache.localPeek(key));

        assertEquals(0, cache.localSize());

        // Clear readers, if any.
        cache.remove(key);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTxMissingKey() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTxMissingKeyNoCommit() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkRemovexInTx(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        if (txShouldBeUsed()) {
            final int cnt = 10;

            CU.inTx(defaultInstance(), jcache(), concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        cache.put("key" + i, i);
                }
            });

            CU.inTx(defaultInstance(), jcache(), concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        assertEquals(new Integer(i), cache.get("key" + i));
                }
            });

            CU.inTx(defaultInstance(), jcache(), concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        assertTrue("Failed to remove key: key" + i, cache.remove("key" + i));
                }
            });

            CU.inTx(defaultInstance(), jcache(), concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        assertNull(cache.get("key" + i));
                }
            });
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxMissingKey() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxMissingKeyNoCommit() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction tx = transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(jcache().remove(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxRepeatableRead() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction ignored = transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                jcache().put("key", 1);

                assert jcache().get("key") == 1;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticTxRepeatableReadOnUpdate() throws Exception {
        if (txShouldBeUsed()) {
            try (Transaction ignored = transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                jcache().put("key", 1);

                assert jcache().getAndPut("key", 2) == 1;
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testToMap() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        cache.put("key1", 1);
        cache.put("key2", 2);

        Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < gridCount(); i++) {
            for (Cache.Entry<String, Integer> entry : jcache(i))
                map.put(entry.getKey(), entry.getValue());
        }

        assert map.size() == 2;
        assert map.get("key1") == 1;
        assert map.get("key2") == 2;
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkSize(final Collection<String> keys) throws Exception {
        if (nearEnabled())
            assertEquals(keys.size(), jcache().localSize(CachePeekMode.ALL));
        else {
            for (int i = 0; i < gridCount(); i++)
                executeOnLocalOrRemoteJvm(i, new CheckEntriesTask(keys));
        }
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkKeySize(final Collection<String> keys) throws Exception {
        if (nearEnabled())
            assertEquals("Invalid key size: " + jcache().localSize(ALL),
                keys.size(), jcache().localSize(ALL));
        else {
            for (int i = 0; i < gridCount(); i++)
                executeOnLocalOrRemoteJvm(i, new CheckKeySizeTask(keys));
        }
    }

    /**
     * @param exp Expected value.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkContainsKey(boolean exp, String key) throws Exception {
        if (nearEnabled())
            assertEquals(exp, jcache().containsKey(key));
        else {
            boolean contains = false;

            for (int i = 0; i < gridCount(); i++)
                if (containsKey(jcache(i), key)) {
                    contains = true;

                    break;
                }

            assertEquals("Key: " + key, exp, contains);
        }
    }

    /**
     * @param key Key.
     * @return Ignite instance for primary node.
     */
    protected Ignite primaryIgnite(String key) {
        ClusterNode node = defaultInstance().affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key);

        if (node == null)
            throw new IgniteException("Failed to find primary node.");

        UUID nodeId = node.id();

        for (int i = 0; i < gridCount(); i++) {
            if (grid(i).localNode().id().equals(nodeId))
                return ignite(i);
        }

        throw new IgniteException("Failed to find primary node.");
    }

    /**
     * @param key Key.
     * @return Cache.
     */
    protected IgniteCache<String, Integer> primaryCache(String key) {
        return primaryIgnite(key).cache(DEFAULT_CACHE_NAME);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Begin value ofthe key.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<String> primaryKeysForCache(IgniteCache<String, Integer> cache, int cnt, int startFrom) {
        return executeOnLocalOrRemoteJvm(cache, new CheckPrimaryKeysTask(startFrom, cnt));
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     * @throws IgniteCheckedException If failed.
     */
    protected List<String> primaryKeysForCache(IgniteCache<String, Integer> cache, int cnt)
        throws IgniteCheckedException {
        return primaryKeysForCache(cache, cnt, 1);
    }

    /**
     * @param cache Cache.
     * @param key Entry key.
     * @return Pair [ttl, expireTime]; both values null if entry not found
     */
    protected IgnitePair<Long> entryTtl(IgniteCache cache, String key) {
        return executeOnLocalOrRemoteJvm(cache, new EntryTtlTask(key, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIterator() throws Exception {
        IgniteCache<Integer, Integer> cache = defaultInstance().cache(DEFAULT_CACHE_NAME);

        final int KEYS = 1000;

        for (int i = 0; i < KEYS; i++)
            cache.put(i, i);

        // Try to initialize readers in case when near cache is enabled.
        for (int i = 0; i < gridCount(); i++) {
            cache = grid(i).cache(DEFAULT_CACHE_NAME);

            for (int k = 0; k < KEYS; k++)
                assertEquals((Object)k, cache.get(k));
        }

        int cnt = 0;

        for (Cache.Entry e : cache)
            cnt++;

        assertEquals(KEYS, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteCacheIterator() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        Iterator<Cache.Entry<String, Integer>> it = cache.iterator();

        boolean hasNext = it.hasNext();

        if (hasNext)
            assertFalse("Cache has value: " + it.next(), hasNext);

        final int SIZE = 10_000;

        Map<String, Integer> entries = new HashMap<>();

        Map<String, Integer> putMap = new HashMap<>();

        for (int i = 0; i < SIZE; ++i) {
            String key = Integer.toString(i);

            putMap.put(key, i);

            entries.put(key, i);

            if (putMap.size() == 500) {
                cache.putAll(putMap);

                info("Puts finished: " + (i + 1));

                putMap.clear();
            }
        }

        cache.putAll(putMap);

        checkIteratorHasNext();

        checkIteratorCache(entries);

        checkIteratorRemove(cache, entries);

        checkIteratorEmpty(cache);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIteratorLeakOnCancelCursor() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        final int SIZE = 10_000;

        Map<String, Integer> putMap = new HashMap<>();

        for (int i = 0; i < SIZE; ++i) {
            String key = Integer.toString(i);

            putMap.put(key, i);

            if (putMap.size() == 500) {
                cache.putAll(putMap);

                info("Puts finished: " + (i + 1));

                putMap.clear();
            }
        }

        cache.putAll(putMap);

        QueryCursor<Cache.Entry<String, Integer>> cur = cache.query(new ScanQuery<String, Integer>());

        cur.iterator().next();

        cur.close();

        waitForIteratorsCleared(cache, 10);
    }

    /**
     * If hasNext() is called repeatedly, it should return the same result.
     */
    private void checkIteratorHasNext() {
        Iterator<Cache.Entry<String, Integer>> iter = jcache().iterator();

        assertEquals(iter.hasNext(), iter.hasNext());

        while (iter.hasNext())
            iter.next();

        assertFalse(iter.hasNext());
    }

    /**
     * @param cache Cache.
     * @param entries Expected entries in the cache.
     */
    private void checkIteratorRemove(IgniteCache<String, Integer> cache, Map<String, Integer> entries) {
        // Check that we can remove element.
        String rmvKey = Integer.toString(5);

        removeCacheIterator(cache, rmvKey);

        entries.remove(rmvKey);

        assertFalse(cache.containsKey(rmvKey));
        assertNull(cache.get(rmvKey));

        checkIteratorCache(entries);

        // Check that we cannot call Iterator.remove() without next().
        final Iterator<Cache.Entry<String, Integer>> iter = jcache().iterator();

        assertTrue(iter.hasNext());

        iter.next();

        iter.remove();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Void call() throws Exception {
                iter.remove();

                return null;
            }
        }, IllegalStateException.class, null);
    }

    /**
     * @param cache Cache.
     * @param key Key to remove.
     */
    private void removeCacheIterator(IgniteCache<String, Integer> cache, String key) {
        Iterator<Cache.Entry<String, Integer>> iter = cache.iterator();

        int delCnt = 0;

        while (iter.hasNext()) {
            Cache.Entry<String, Integer> cur = iter.next();

            if (cur.getKey().equals(key)) {
                iter.remove();

                delCnt++;
            }
        }

        assertEquals(1, delCnt);
    }

    /**
     * @param entries Expected entries in the cache.
     */
    private void checkIteratorCache(Map<String, Integer> entries) {
        for (int i = 0; i < gridCount(); ++i)
            checkIteratorCache(jcache(i), entries);
    }

    /**
     * @param cache Cache.
     * @param entries Expected entries in the cache.
     */
    private void checkIteratorCache(IgniteCache<String, Integer> cache, Map<String, Integer> entries) {
        Iterator<Cache.Entry<String, Integer>> iter = cache.iterator();

        int cnt = 0;

        while (iter.hasNext()) {
            Cache.Entry<String, Integer> cur = iter.next();

            assertTrue(entries.containsKey(cur.getKey()));
            assertEquals(entries.get(cur.getKey()), cur.getValue());

            cnt++;
        }

        assertEquals(entries.size(), cnt);
    }

    /**
     * Checks iterators are cleared.
     */
    private void checkIteratorsCleared() {
        for (int j = 0; j < gridCount(); j++)
            executeOnLocalOrRemoteJvm(j, new CheckIteratorTask());
    }

    /**
     * Checks iterators are cleared.
     */
    private void waitForIteratorsCleared(IgniteCache<String, Integer> cache, int secs) throws InterruptedException {
        for (int i = 0; i < secs; i++) {
            try {
                cache.size(); // Trigger weak queue poll.

                checkIteratorsCleared();
            }
            catch (Throwable t) {
                // If AssertionError is in the chain, assume we need to wait and retry.
                if (!X.hasCause(t, AssertionError.class))
                    throw t;

                if (i == 9) {
                    for (int j = 0; j < gridCount(); j++)
                        executeOnLocalOrRemoteJvm(j, new PrintIteratorStateTask());

                    throw t;
                }

                log.info("Iterators not cleared, will wait");

                Thread.sleep(1000);
            }
        }
    }

    /**
     * Checks iterators are cleared after using.
     *
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkIteratorEmpty(IgniteCache<String, Integer> cache) throws Exception {
        int cnt = 5;

        for (int i = 0; i < cnt; ++i) {
            Iterator<Cache.Entry<String, Integer>> iter = cache.iterator();

            iter.next();

            assert iter.hasNext();
        }

        System.gc();

        waitForIteratorsCleared(cache, 10);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalClearKey() throws Exception {
        addKeys();

        String keyToRmv = "key" + 25;

        Ignite g = primaryIgnite(keyToRmv);

        g.<String, Integer>cache(DEFAULT_CACHE_NAME).localClear(keyToRmv);

        checkLocalRemovedKey(keyToRmv);

        g.<String, Integer>cache(DEFAULT_CACHE_NAME).put(keyToRmv, 1);

        String keyToEvict = "key" + 30;

        g = primaryIgnite(keyToEvict);

        g.<String, Integer>cache(DEFAULT_CACHE_NAME).localEvict(Collections.singleton(keyToEvict));

        g.<String, Integer>cache(DEFAULT_CACHE_NAME).localClear(keyToEvict);

        checkLocalRemovedKey(keyToEvict);
    }

    /**
     * @param keyToRmv Removed key.
     */
    protected void checkLocalRemovedKey(String keyToRmv) {
        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            boolean found = primaryIgnite(key).cache(DEFAULT_CACHE_NAME).localPeek(key) != null;

            if (keyToRmv.equals(key)) {
                Collection<ClusterNode> nodes = defaultInstance().affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key);

                for (int j = 0; j < gridCount(); ++j) {
                    if (nodes.contains(grid(j).localNode()) && grid(j) != primaryIgnite(key))
                        assertTrue("Not found on backup removed key ", grid(j).cache(DEFAULT_CACHE_NAME).localPeek(key) != null);
                }

                assertFalse("Found removed key " + key, found);
            }
            else
                assertTrue("Not found key " + key, found);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalClearKeys() throws Exception {
        Map<String, List<String>> keys = addKeys();

        Ignite g = defaultInstance();

        Set<String> keysToRmv = new HashSet<>();

        for (int i = 0; i < gridCount(); ++i) {
            List<String> gridKeys = keys.get(grid(i).name());

            if (gridKeys.size() > 2) {
                keysToRmv.add(gridKeys.get(0));

                keysToRmv.add(gridKeys.get(1));

                g = grid(i);

                break;
            }
        }

        assert keysToRmv.size() > 1;

        info("Will clear keys on node: " + g.cluster().localNode().id());

        g.<String, Integer>cache(DEFAULT_CACHE_NAME).localClearAll(keysToRmv);

        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            Ignite ignite = primaryIgnite(key);

            boolean found = ignite.cache(DEFAULT_CACHE_NAME).localPeek(key) != null;

            if (keysToRmv.contains(key))
                assertFalse("Found removed key [key=" + key + ", node=" + ignite.cluster().localNode().id() + ']',
                    found);
            else
                assertTrue("Not found key " + key, found);
        }
    }

    /**
     * Add 500 keys to cache only on primaries nodes.
     *
     * @return Map grid's name to its primary keys.
     */
    protected Map<String, List<String>> addKeys() {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        Map<String, List<String>> keys = new HashMap<>();

        for (int i = 0; i < gridCount(); ++i)
            keys.put(grid(i).name(), new ArrayList<String>());

        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            Ignite g = primaryIgnite(key);

            g.cache(DEFAULT_CACHE_NAME).put(key, "value" + i);

            keys.get(g.name()).add(key);
        }

        return keys;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearKey() throws Exception {
        testGlobalClearKey(false, Arrays.asList("key25"), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearKeyAsyncOld() throws Exception {
        testGlobalClearKey(true, Arrays.asList("key25"), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearKeyAsync() throws Exception {
        testGlobalClearKey(true, Arrays.asList("key25"), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearKeys() throws Exception {
        testGlobalClearKey(false, Arrays.asList("key25", "key100", "key150"), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearKeysAsyncOld() throws Exception {
        testGlobalClearKey(true, Arrays.asList("key25", "key100", "key150"), true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGlobalClearKeysAsync() throws Exception {
        testGlobalClearKey(true, Arrays.asList("key25", "key100", "key150"), false);
    }

    /**
     * @param async If {@code true} uses async method.
     * @param keysToRmv Keys to remove.
     * @param oldAsync Use old async API.
     * @throws Exception If failed.
     */
    protected void testGlobalClearKey(boolean async, Collection<String> keysToRmv, boolean oldAsync) throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearLocally() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            Ignite g = primaryIgnite(key);

            g.cache(DEFAULT_CACHE_NAME).put(key, "value" + i);
        }

        if (async) {
            if (oldAsync) {
                IgniteCache<String, Integer> asyncCache = jcache().withAsync();

                if (keysToRmv.size() == 1)
                    asyncCache.clear(F.first(keysToRmv));
                else
                    asyncCache.clearAll(new HashSet<>(keysToRmv));

                asyncCache.future().get();
            }
            else {
                if (keysToRmv.size() == 1)
                    jcache().clearAsync(F.first(keysToRmv)).get();
                else
                    jcache().clearAllAsync(new HashSet<>(keysToRmv)).get();
            }
        }
        else {
            if (keysToRmv.size() == 1)
                jcache().clear(F.first(keysToRmv));
            else
                jcache().clearAll(new HashSet<>(keysToRmv));
        }

        for (int i = 0; i < 500; ++i) {
            String key = "key" + i;

            boolean found = false;

            for (int j = 0; j < gridCount(); j++) {
                if (jcache(j).localPeek(key) != null)
                    found = true;
            }

            if (!keysToRmv.contains(key))
                assertTrue("Not found key " + key, found);
            else
                assertFalse("Found removed key " + key, found);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithSkipStore() throws Exception {
        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheSkipStore = cache.withSkipStore();

        List<String> keys = primaryKeysForCache(cache, 10);

        for (int i = 0; i < keys.size(); ++i)
            storeStgy.putToStore(keys.get(i), i);

        assertFalse(cacheSkipStore.iterator().hasNext());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));

            assertNotNull(cache.get(key));
        }

        for (String key : keys) {
            cacheSkipStore.remove(key);

            assertNotNull(cache.get(key));
        }

        cache.removeAll(new HashSet<>(keys));

        for (String key : keys)
            assertNull(cache.get(key));

        final int KEYS = 250;

        // Put/remove data from multiple nodes.

        keys = new ArrayList<>(KEYS);

        for (int i = 0; i < KEYS; i++)
            keys.add("key_" + i);

        for (int i = 0; i < keys.size(); ++i)
            cache.put(keys.get(i), i);

        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);

            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertEquals(i, storeStgy.getFromStore(key));
        }

        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);

            Integer val1 = -1;

            cacheSkipStore.put(key, val1);
            assertEquals(i, storeStgy.getFromStore(key));
            assertEquals(val1, cacheSkipStore.get(key));

            Integer val2 = -2;

            assertEquals(val1, cacheSkipStore.invoke(key, new SetValueProcessor(val2)));
            assertEquals(i, storeStgy.getFromStore(key));
            assertEquals(val2, cacheSkipStore.get(key));
        }

        for (String key : keys) {
            cacheSkipStore.remove(key);

            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        for (String key : keys) {
            cache.remove(key);

            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(storeStgy.isInStore(key));

            storeStgy.putToStore(key, 0);

            Integer val = -1;

            assertNull(cacheSkipStore.invoke(key, new SetValueProcessor(val)));
            assertEquals(0, storeStgy.getFromStore(key));
            assertEquals(val, cacheSkipStore.get(key));

            cache.remove(key);

            storeStgy.putToStore(key, 0);

            assertTrue(cacheSkipStore.putIfAbsent(key, val));
            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(0, storeStgy.getFromStore(key));

            cache.remove(key);

            storeStgy.putToStore(key, 0);

            assertNull(cacheSkipStore.getAndPut(key, val));
            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(0, storeStgy.getFromStore(key));

            cache.remove(key);
        }

        assertFalse(cacheSkipStore.iterator().hasNext());
        assertTrue(storeStgy.getStoreSize() == 0);
        assertTrue(cache.size(ALL) == 0);

        // putAll/removeAll from multiple nodes.

        Map<String, Integer> data = new LinkedHashMap<>();

        for (int i = 0; i < keys.size(); i++)
            data.put(keys.get(i), i);

        cacheSkipStore.putAll(data);

        for (String key : keys) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertFalse(storeStgy.isInStore(key));
        }

        cache.putAll(data);

        for (String key : keys) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        cacheSkipStore.removeAll(data.keySet());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        cacheSkipStore.putAll(data);

        for (String key : keys) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        cacheSkipStore.removeAll(data.keySet());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        cache.removeAll(data.keySet());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(storeStgy.isInStore(key));
        }

        assertTrue(storeStgy.getStoreSize() == 0);

        // Miscellaneous checks.

        String newKey = "New key";

        assertFalse(storeStgy.isInStore(newKey));

        cacheSkipStore.put(newKey, 1);

        assertFalse(storeStgy.isInStore(newKey));

        cache.put(newKey, 1);

        assertTrue(storeStgy.isInStore(newKey));

        Iterator<Cache.Entry<String, Integer>> it = cacheSkipStore.iterator();

        assertTrue(it.hasNext());

        Cache.Entry<String, Integer> entry = it.next();

        String rmvKey = entry.getKey();

        assertTrue(storeStgy.isInStore(rmvKey));

        it.remove();

        assertNull(cacheSkipStore.get(rmvKey));

        assertTrue(storeStgy.isInStore(rmvKey));

        assertTrue(cache.size(ALL) == 0);
        assertTrue(cacheSkipStore.size(ALL) == 0);

        cache.remove(rmvKey);

        assertTrue(storeStgy.getStoreSize() == 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithSkipStoreRemoveAll() throws Exception {
        if (atomicityMode() == TRANSACTIONAL || (atomicityMode() == ATOMIC && nearEnabled())) // TODO IGNITE-373.
            return;

        IgniteCache<String, Integer> cache = jcache();

        IgniteCache<String, Integer> cacheSkipStore = cache.withSkipStore();

        Map<String, Integer> data = new HashMap<>();

        for (int i = 0; i < 100; i++)
            data.put("key_" + i, i);

        cache.putAll(data);

        for (String key : data.keySet()) {
            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        cacheSkipStore.removeAll();

        for (String key : data.keySet()) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        cache.removeAll();

        for (String key : data.keySet()) {
            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(storeStgy.isInStore(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWithSkipStoreTx() throws Exception {
        if (txShouldBeUsed()) {
            IgniteCache<String, Integer> cache = defaultInstance().cache(DEFAULT_CACHE_NAME);

            IgniteCache<String, Integer> cacheSkipStore = cache.withSkipStore();

            final int KEYS = 250;

            // Put/remove data from multiple nodes.

            List<String> keys = new ArrayList<>(KEYS);

            for (int i = 0; i < KEYS; i++)
                keys.add("key_" + i);

            Map<String, Integer> data = new LinkedHashMap<>();

            for (int i = 0; i < keys.size(); i++)
                data.put(keys.get(i), i);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, OPTIMISTIC, READ_COMMITTED);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, OPTIMISTIC, REPEATABLE_READ);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, OPTIMISTIC, SERIALIZABLE);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, PESSIMISTIC, READ_COMMITTED);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, PESSIMISTIC, REPEATABLE_READ);

            checkSkipStoreWithTransaction(cache, cacheSkipStore, data, keys, PESSIMISTIC, SERIALIZABLE);
        }
    }

    /**
     * @param cache Cache instance.
     * @param cacheSkipStore Cache skip store projection.
     * @param data Data set.
     * @param keys Keys list.
     * @param txConcurrency Concurrency mode.
     * @param txIsolation Isolation mode.
     * @throws Exception If failed.
     */
    private void checkSkipStoreWithTransaction(IgniteCache<String, Integer> cache,
        IgniteCache<String, Integer> cacheSkipStore,
        Map<String, Integer> data,
        List<String> keys,
        TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation)
        throws Exception {
        info("Test tx skip store [concurrency=" + txConcurrency + ", isolation=" + txIsolation + ']');

        cache.removeAll(data.keySet());
        checkEmpty(cache, cacheSkipStore);

        IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

        Integer val = -1;

        // Several put check.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key : keys)
                cacheSkipStore.put(key, val);

            for (String key : keys) {
                assertEquals(val, cacheSkipStore.get(key));
                assertEquals(val, cache.get(key));
                assertFalse(storeStgy.isInStore(key));
            }

            tx.commit();
        }

        for (String key : keys) {
            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
            assertFalse(storeStgy.isInStore(key));
        }

        assertEquals(0, storeStgy.getStoreSize());

        // cacheSkipStore putAll(..)/removeAll(..) check.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            cacheSkipStore.putAll(data);

            tx.commit();
        }

        for (String key : keys) {
            val = data.get(key);

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
            assertFalse(storeStgy.isInStore(key));
        }

        storeStgy.putAllToStore(data);

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            cacheSkipStore.removeAll(data.keySet());

            tx.commit();
        }

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));

            cache.remove(key);
        }

        assertTrue(storeStgy.getStoreSize() == 0);

        // cache putAll(..)/removeAll(..) check.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            cache.putAll(data);

            for (String key : keys) {
                assertNotNull(cacheSkipStore.get(key));
                assertNotNull(cache.get(key));
                assertFalse(storeStgy.isInStore(key));
            }

            cache.removeAll(data.keySet());

            for (String key : keys) {
                assertNull(cacheSkipStore.get(key));
                assertNull(cache.get(key));
                assertFalse(storeStgy.isInStore(key));
            }

            tx.commit();
        }

        assertTrue(storeStgy.getStoreSize() == 0);

        // putAll(..) from both cacheSkipStore and cache.
        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            Map<String, Integer> subMap = new HashMap<>();

            for (int i = 0; i < keys.size() / 2; i++)
                subMap.put(keys.get(i), i);

            cacheSkipStore.putAll(subMap);

            subMap.clear();

            for (int i = keys.size() / 2; i < keys.size(); i++)
                subMap.put(keys.get(i), i);

            cache.putAll(subMap);

            for (String key : keys) {
                assertNotNull(cacheSkipStore.get(key));
                assertNotNull(cache.get(key));
                assertFalse(storeStgy.isInStore(key));
            }

            tx.commit();
        }

        for (int i = 0; i < keys.size() / 2; i++) {
            String key = keys.get(i);

            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertFalse(storeStgy.isInStore(key));
        }

        for (int i = keys.size() / 2; i < keys.size(); i++) {
            String key = keys.get(i);

            assertNotNull(cacheSkipStore.get(key));
            assertNotNull(cache.get(key));
            assertTrue(storeStgy.isInStore(key));
        }

        cache.removeAll(data.keySet());

        for (String key : keys) {
            assertNull(cacheSkipStore.get(key));
            assertNull(cache.get(key));
            assertFalse(storeStgy.isInStore(key));
        }

        // Check that read-through is disabled when cacheSkipStore is used.
        for (int i = 0; i < keys.size(); i++)
            storeStgy.putToStore(keys.get(i), i);

        assertTrue(cacheSkipStore.size(ALL) == 0);
        assertTrue(cache.size(ALL) == 0);
        assertTrue(storeStgy.getStoreSize() != 0);

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            assertTrue(cacheSkipStore.getAll(data.keySet()).isEmpty());

            for (String key : keys) {
                assertNull(cacheSkipStore.get(key));

                if (txIsolation == READ_COMMITTED) {
                    assertNotNull(cache.get(key));
                    assertNotNull(cacheSkipStore.get(key));
                }
            }

            tx.commit();
        }

        cache.removeAll(data.keySet());

        val = -1;

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key : data.keySet()) {
                storeStgy.putToStore(key, 0);

                assertNull(cacheSkipStore.invoke(key, new SetValueProcessor(val)));
            }

            tx.commit();
        }

        for (String key : data.keySet()) {
            assertEquals(0, storeStgy.getFromStore(key));

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
        }

        cache.removeAll(data.keySet());

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key : data.keySet()) {
                storeStgy.putToStore(key, 0);

                assertTrue(cacheSkipStore.putIfAbsent(key, val));
            }

            tx.commit();
        }

        for (String key : data.keySet()) {
            assertEquals(0, storeStgy.getFromStore(key));

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
        }

        cache.removeAll(data.keySet());

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            for (String key : data.keySet()) {
                storeStgy.putToStore(key, 0);

                assertNull(cacheSkipStore.getAndPut(key, val));
            }

            tx.commit();
        }

        for (String key : data.keySet()) {
            assertEquals(0, storeStgy.getFromStore(key));

            assertEquals(val, cacheSkipStore.get(key));
            assertEquals(val, cache.get(key));
        }

        cache.removeAll(data.keySet());
        checkEmpty(cache, cacheSkipStore);
    }

    /**
     * @param cache Cache instance.
     * @param cacheSkipStore Cache skip store projection.
     * @throws Exception If failed.
     */
    private void checkEmpty(IgniteCache<String, Integer> cache, IgniteCache<String, Integer> cacheSkipStore)
        throws Exception {
        assertTrue(cache.size(ALL) == 0);
        assertTrue(cacheSkipStore.size(ALL) == 0);
        assertTrue(storeStgy.getStoreSize() == 0);
    }

    /**
     * @return Cache start mode.
     */
    protected CacheStartMode cacheStartType() {
        String mode = System.getProperty("cache.start.mode");

        if (CacheStartMode.NODES_THEN_CACHES.name().equalsIgnoreCase(mode))
            return CacheStartMode.NODES_THEN_CACHES;

        if (CacheStartMode.ONE_BY_ONE.name().equalsIgnoreCase(mode))
            return CacheStartMode.ONE_BY_ONE;

        return CacheStartMode.STATIC;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOutTx() throws Exception {
        checkGetOutTx(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOutTxAsync() throws Exception {
        checkGetOutTx(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGetOutTx(boolean async) throws Exception {
        final AtomicInteger lockEvtCnt = new AtomicInteger();

        IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                lockEvtCnt.incrementAndGet();

                return true;
            }
        };

        try {
            IgniteCache<String, Integer> cache = jcache();

            List<String> keys = primaryKeysForCache(cache, 2);

            assertEquals(2, keys.size());

            cache.put(keys.get(0), 0);
            cache.put(keys.get(1), 1);

            defaultInstance().events().localListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            try (Transaction ignored =
                     cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                         transactions().txStart(PESSIMISTIC, REPEATABLE_READ) : null) {
                Integer val0;

                if (async)
                    val0 = cache.getAsync(keys.get(0)).get();
                else
                     val0 = cache.get(keys.get(0));

                assertEquals(0, val0.intValue());

                Map<String, Integer> allOutTx;

                if (async)
                    allOutTx = cache.getAllOutTxAsync(F.asSet(keys.get(1))).get();
                else
                    allOutTx = cache.getAllOutTx(F.asSet(keys.get(1)));

                assertEquals(1, allOutTx.size());

                assertTrue(allOutTx.containsKey(keys.get(1)));

                assertEquals(1, allOutTx.get(keys.get(1)).intValue());
            }

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    info("Lock event count: " + lockEvtCnt.get());
                    if (atomicityMode() == ATOMIC)
                        return lockEvtCnt.get() == 0;

                    if (cacheMode() == PARTITIONED && nearEnabled()) {
                        if (!defaultInstance().configuration().isClientMode())
                            return lockEvtCnt.get() == 4;
                    }

                    return lockEvtCnt.get() == 2;
                }
            }, 15000));
        }
        finally {
            defaultInstance().events().stopLocalListen(lsnr, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformException() throws Exception {
        final IgniteCache<String, Integer> cache = jcache();

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteFuture fut = cache.invokeAsync("key2", ERR_PROCESSOR).chain(new IgniteClosure<IgniteFuture, Object>() {
                    @Override public Object apply(IgniteFuture o) {
                        return o.get();
                    }
                });

                fut.get();

                return null;
            }
        }, EntryProcessorException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockInsideTransaction() throws Exception {
        if (txEnabled()) {
            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        IgniteCache<String, Integer> cache = jcache();

                        try (Transaction ignored =
                                 cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                                     defaultInstance().transactions().txStart() : null) {
                            cache.lock("key").lock();
                        }

                        return null;
                    }
                },
                CacheException.class,
                atomicityMode() == TRANSACTIONAL ?
                    "Explicit lock can't be acquired within a transaction." :
                    "Locks are not supported for CacheAtomicityMode.ATOMIC mode"
            );

            GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        IgniteCache<String, Integer> cache = jcache();

                        try (Transaction tx =
                                 cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                                     defaultInstance().transactions().txStart() : null) {
                            cache.lockAll(Arrays.asList("key1", "key2")).lock();
                        }

                        return null;
                    }
                },
                CacheException.class,
                atomicityMode() == TRANSACTIONAL ?
                    "Explicit lock can't be acquired within a transaction." :
                    "Locks are not supported for CacheAtomicityMode.ATOMIC mode"
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformResourceInjection() throws Exception {
        ClusterGroup servers = defaultInstance().cluster().forServers();

        if (F.isEmpty(servers.nodes()))
            return;

        defaultInstance().services( defaultInstance().cluster()).deployNodeSingleton(SERVICE_NAME1, new DummyServiceImpl());

        IgniteCache<String, Integer> cache = jcache();
        Ignite ignite = defaultInstance();

        doTransformResourceInjection(ignite, cache, false, false);
        doTransformResourceInjection(ignite, cache, true, false);
        doTransformResourceInjection(ignite, cache, true, true);

        if (txEnabled()) {
            doTransformResourceInjectionInTx(ignite, cache, false, false);
            doTransformResourceInjectionInTx(ignite, cache, true, false);
            doTransformResourceInjectionInTx(ignite, cache, true, true);
        }
    }

    /**
     * @param ignite Node.
     * @param cache Cache.
     * @param async Use async API.
     * @param oldAsync Use old async API.
     * @throws Exception If failed.
     */
    private void doTransformResourceInjectionInTx(Ignite ignite, IgniteCache<String, Integer> cache, boolean async,
        boolean oldAsync) throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                IgniteTransactions txs = ignite.transactions();

                try (Transaction tx =
                         cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL ?
                             txs.txStart(concurrency, isolation) : null) {
                    doTransformResourceInjection(ignite, cache, async, oldAsync);

                    if (tx != null)
                        tx.commit();
                }
            }
        }
    }

    /**
     * @param ignite Node.
     * @param cache Cache.
     * @param async Use async API.
     * @param oldAsync Use old async API.
     * @throws Exception If failed.
     */
    private void doTransformResourceInjection(Ignite ignite, IgniteCache<String, Integer> cache, boolean async,
        boolean oldAsync) throws Exception {
        final Collection<ResourceType> required = Arrays.asList(ResourceType.IGNITE_INSTANCE,
            ResourceType.CACHE_NAME,
            ResourceType.LOGGER);

        final CacheEventListener lsnr = new CacheEventListener();

        IgniteEvents evts = ignite.events(ignite.cluster());

        UUID opId = evts.remoteListen(lsnr, null, EVT_CACHE_OBJECT_READ);

        try {
            checkResourceInjectionOnInvoke(cache, required, async, oldAsync);

            checkResourceInjectionOnInvokeAll(cache, required, async, oldAsync);

            checkResourceInjectionOnInvokeAllMap(cache, required, async, oldAsync);
        }
        finally {
            evts.stopRemoteListen(opId);
        }
    }

    /**
     * Tests invokeAll method for map of pairs (key, entryProcessor).
     *
     * @param cache Cache.
     * @param required Expected injected resources.
     * @param async Use async API.
     * @param oldAsync Use old async API.
     */
    private void checkResourceInjectionOnInvokeAllMap(IgniteCache<String, Integer> cache,
        Collection<ResourceType> required, boolean async, boolean oldAsync) {
        Map<String, EntryProcessorResult<Integer>> results;

        Map<String, EntryProcessor<String, Integer, Integer>> map = new HashMap<>();

        map.put(UUID.randomUUID().toString(), new ResourceInjectionEntryProcessor());
        map.put(UUID.randomUUID().toString(), new ResourceInjectionEntryProcessor());
        map.put(UUID.randomUUID().toString(), new ResourceInjectionEntryProcessor());
        map.put(UUID.randomUUID().toString(), new ResourceInjectionEntryProcessor());

        if (async) {
            if (oldAsync) {
                IgniteCache<String, Integer> acache = cache.withAsync();

                acache.invokeAll(map);

                results = acache.<Map<String, EntryProcessorResult<Integer>>>future().get();
            }
            else
                results = cache.invokeAllAsync(map).get();
        }
        else
            results = cache.invokeAll(map);

        assertEquals(map.size(), results.size());

        for (EntryProcessorResult<Integer> res : results.values()) {
            Collection<ResourceType> notInjected = ResourceInfoSet.valueOf(res.get()).notInjected(required);

            if (!notInjected.isEmpty())
                fail("Can't inject resource(s): " + Arrays.toString(notInjected.toArray()));
        }
    }

    /**
     * Tests invokeAll method for set of keys.
     *
     * @param cache Cache.
     * @param required Expected injected resources.
     * @param async Use async API.
     * @param oldAsync Use old async API.
     */
    private void checkResourceInjectionOnInvokeAll(IgniteCache<String, Integer> cache,
        Collection<ResourceType> required, boolean async, boolean oldAsync) {
        Set<String> keys = new HashSet<>(Arrays.asList(UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()));

        Map<String, EntryProcessorResult<Integer>> results;

        if (async) {
            if (oldAsync) {
                IgniteCache<String, Integer> acache = cache.withAsync();

                acache.invokeAll(keys, new ResourceInjectionEntryProcessor());

                results = acache.<Map<String, EntryProcessorResult<Integer>>>future().get();
            }
            else
                results = cache.invokeAllAsync(keys, new ResourceInjectionEntryProcessor()).get();
        }
        else
            results = cache.invokeAll(keys, new ResourceInjectionEntryProcessor());

        assertEquals(keys.size(), results.size());

        for (EntryProcessorResult<Integer> res : results.values()) {
            Collection<ResourceType> notInjected1 = ResourceInfoSet.valueOf(res.get()).notInjected(required);

            if (!notInjected1.isEmpty())
                fail("Can't inject resource(s): " + Arrays.toString(notInjected1.toArray()));
        }
    }

    /**
     * Tests invoke for single key.
     *
     * @param cache Cache.
     * @param required Expected injected resources.
     * @param async Use async API.
     * @param oldAsync Use old async API.
     */
    private void checkResourceInjectionOnInvoke(IgniteCache<String, Integer> cache,
        Collection<ResourceType> required, boolean async, boolean oldAsync) {

        String key = UUID.randomUUID().toString();

        Integer flags;

        if (async) {
            if (oldAsync) {
                IgniteCache<String, Integer> acache = cache.withAsync();

                acache.invoke(key, new GridCacheAbstractFullApiSelfTest.ResourceInjectionEntryProcessor());

                flags = acache.<Integer>future().get();
            }
            else
                flags = cache.invokeAsync(key,
                    new GridCacheAbstractFullApiSelfTest.ResourceInjectionEntryProcessor()).get();
        }
        else
            flags = cache.invoke(key, new GridCacheAbstractFullApiSelfTest.ResourceInjectionEntryProcessor());

        if (cache.isAsync())
            flags = cache.<Integer>future().get();

        assertTrue("Processor result is null", flags != null);

        Collection<ResourceType> notInjected = ResourceInfoSet.valueOf(flags).notInjected(required);

        if (!notInjected.isEmpty())
            fail("Can't inject resource(s): " + Arrays.toString(notInjected.toArray()));
    }

    /**
     * Sets given value, returns old value.
     */
    public static final class SetValueProcessor implements EntryProcessor<String, Integer, Integer> {
        /** */
        private Integer newVal;

        /**
         * @param newVal New value to set.
         */
        SetValueProcessor(Integer newVal) {
            this.newVal = newVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<String, Integer> entry,
            Object... arguments) throws EntryProcessorException {
            Integer val = entry.getValue();

            entry.setValue(newVal);

            return val;
        }
    }

    /**
     *
     */
    public enum CacheStartMode {
        /** Start caches together nodes (not dynamically) */
        STATIC,

        /** */
        NODES_THEN_CACHES,

        /** */
        ONE_BY_ONE
    }

    /**
     *
     */
    private static class RemoveEntryProcessor implements EntryProcessor<String, Integer, String>, Serializable {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<String, Integer> e, Object... args) {
            assertNotNull(e.getKey());

            Integer old = e.getValue();

            e.remove();

            return String.valueOf(old);
        }
    }

    /**
     *
     */
    private static class IncrementEntryProcessor implements EntryProcessor<String, Integer, String>, Serializable {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<String, Integer> e, Object... args) {
            assertNotNull(e.getKey());

            Integer old = e.getValue();

            e.setValue(old == null ? 1 : old + 1);

            return String.valueOf(old);
        }
    }

    /**
     *
     */
    public static class ResourceInjectionEntryProcessor extends ResourceInjectionEntryProcessorBase<String, Integer> {
        /** */
        protected transient Ignite ignite;

        /** */
        protected transient String cacheName;

        /** */
        protected transient IgniteLogger log;

        /** */
        protected transient DummyService svc;

        /**
         * @param ignite Ignite.
         */
        @IgniteInstanceResource
        public void setIgnite(Ignite ignite) {
            assert ignite != null;

            checkSet();

            infoSet.set(ResourceType.IGNITE_INSTANCE, true);

            this.ignite = ignite;
        }

        /**
         * @param cacheName Cache name.
         */
        @CacheNameResource
        public void setCacheName(String cacheName) {
            checkSet();

            infoSet.set(ResourceType.CACHE_NAME, true);

            this.cacheName = cacheName;
        }

        /**
         * @param log Logger.
         */
        @LoggerResource
        public void setLoggerResource(IgniteLogger log) {
            assert log != null;

            checkSet();

            infoSet.set(ResourceType.LOGGER, true);

            this.log = log;
        }

        /**
         * @param svc Service.
         */
        @ServiceResource(serviceName = SERVICE_NAME1)
        public void setDummyService(DummyService svc) {
            assert svc != null;

            checkSet();

            infoSet.set(ResourceType.SERVICE, true);

            this.svc = svc;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {
            Integer oldVal = e.getValue();

            e.setValue(ThreadLocalRandom.current().nextInt() + (oldVal == null ? 0 : oldVal));

            return super.process(e, args);
        }
    }

    /**
     *
     */
    private static class CheckEntriesTask extends TestIgniteIdxRunnable {
        /** Keys. */
        private final Collection<String> keys;

        /**
         * @param keys Keys.
         */
        public CheckEntriesTask(Collection<String> keys) {
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(DEFAULT_CACHE_NAME).context();

            int size = 0;

            if (ctx.isNear())
                ctx = ctx.near().dht().context();

            for (String key : keys) {
                if (ctx.affinity().keyLocalNode(key, ctx.discovery().topologyVersionEx())) {
                    GridCacheEntryEx e = ctx.cache().entryEx(key);

                    assert e != null : "Entry is null [idx=" + idx + ", key=" + key + ", ctx=" + ctx + ']';
                    assert !e.deleted() : "Entry is deleted: " + e;

                    size++;

                    e.touch();
                }
            }

            assertEquals("Incorrect size on cache #" + idx, size, ignite.cache(ctx.name()).localSize(ALL));
        }
    }

    /**
     *
     */
    private static class CheckCacheSizeTask extends TestIgniteIdxRunnable {
        /** */
        private final Map<String, Integer> map;

        /**
         * @param map Map.
         */
        CheckCacheSizeTask(Map<String, Integer> map) {
            this.map = map;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(DEFAULT_CACHE_NAME).context();

            int size = 0;

            for (String key : map.keySet())
                if (ctx.affinity().keyLocalNode(key, ctx.discovery().topologyVersionEx()))
                    size++;

            assertEquals("Incorrect key size on cache #" + idx, size, ignite.cache(ctx.name()).localSize(ALL));
        }
    }

    /**
     *
     */
    private static class CheckPrimaryKeysTask implements TestCacheCallable<String, Integer, List<String>> {
        /** Start from. */
        private final int startFrom;

        /** Count. */
        private final int cnt;

        /**
         * @param startFrom Start from.
         * @param cnt Count.
         */
        public CheckPrimaryKeysTask(int startFrom, int cnt) {
            this.startFrom = startFrom;
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public List<String> call(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
            List<String> found = new ArrayList<>();

            Affinity<Object> aff = ignite.affinity(cache.getName());

            for (int i = startFrom; i < startFrom + 100_000; i++) {
                String key = "key" + i;

                if (aff.isPrimary(ignite.cluster().localNode(), key)) {
                    found.add(key);

                    if (found.size() == cnt)
                        return found;
                }
            }

            throw new IgniteException("Unable to find " + cnt + " keys as primary for cache.");
        }
    }

    /**
     *
     */
    public static class EntryTtlTask implements TestCacheCallable<String, Integer, IgnitePair<Long>> {
        /** Entry key. */
        private final String key;

        /** Check cache for nearness, use DHT cache if it is near. */
        private final boolean useDhtForNearCache;

        /**
         * @param key Entry key.
         * @param useDhtForNearCache Check cache for nearness, use DHT cache if it is near.
         */
        public EntryTtlTask(String key, boolean useDhtForNearCache) {
            this.key = key;
            this.useDhtForNearCache = useDhtForNearCache;
        }

        /** {@inheritDoc} */
        @Override public IgnitePair<Long> call(Ignite ignite, IgniteCache<String, Integer> cache) throws Exception {
            GridCacheAdapter<?, ?> internalCache = internalCache0(cache);

            if (useDhtForNearCache && internalCache.context().isNear())
                internalCache = internalCache.context().near().dht();

            GridCacheEntryEx entry = internalCache.entryEx(key);

            entry.unswap();

            IgnitePair<Long> pair = new IgnitePair<>(entry.ttl(), entry.expireTime());

            if (!entry.isNear())
                entry.context().cache().removeEntry(entry);

            return pair;
        }
    }

    /**
     *
     */
    private static class CheckIteratorTask extends TestIgniteIdxCallable<Void> {
        /**
         * @param idx Index.
         */
        @Override public Void call(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(DEFAULT_CACHE_NAME).context();
            GridCacheQueryManager queries = ctx.queries();

            ConcurrentMap<UUID, Map<Long, GridFutureAdapter<?>>> map = GridTestUtils.getFieldValue(queries,
                GridCacheQueryManager.class, "qryIters");

            for (Map<Long, GridFutureAdapter<?>> map1 : map.values())
                assertTrue("Iterators not removed for grid " + idx, map1.isEmpty());

            return null;
        }
    }

    /**
     *
     */
    private static class PrintIteratorStateTask extends TestIgniteIdxCallable<Void> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param idx Index.
         */
        @Override public Void call(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(DEFAULT_CACHE_NAME).context();
            GridCacheQueryManager queries = ctx.queries();

            ConcurrentMap<UUID, Map<Long, GridFutureAdapter<?>>> map = GridTestUtils.getFieldValue(queries,
                GridCacheQueryManager.class, "qryIters");

            for (Map<Long, GridFutureAdapter<?>> map1 : map.values()) {
                if (!map1.isEmpty()) {
                    log.warning("Iterators leak detected at grid: " + idx);

                    for (Map.Entry<Long, GridFutureAdapter<?>> entry : map1.entrySet())
                        log.warning(entry.getKey() + "; " + entry.getValue());
                }
            }

            return null;
        }
    }

    /**
     *
     */
    public static class RemoveAndReturnNullEntryProcessor implements
        EntryProcessor<String, Integer, Integer>, Serializable {

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {
            e.remove();

            return null;
        }
    }

    /**
     *
     */
    private static class CheckEntriesDeletedTask extends TestIgniteIdxRunnable {
        /** */
        private final int cnt;

        /**
         * @param cnt Keys count.
         */
        public CheckEntriesDeletedTask(int cnt) {
            this.cnt = cnt;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            for (int i = 0; i < cnt; i++) {
                String key = String.valueOf(i);

                GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(DEFAULT_CACHE_NAME).context();

                GridCacheEntryEx entry = ctx.isNear() ? ctx.near().dht().peekEx(key) : ctx.cache().peekEx(key);

                if (ignite.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key).contains(((IgniteKernal)ignite).localNode())) {
                    assertNotNull(entry);
                    assertTrue(entry.deleted());
                }
                else
                    assertNull(entry);
            }
        }
    }

    /**
     *
     */
    private static class CheckKeySizeTask extends TestIgniteIdxRunnable {
        /** Keys. */
        private final Collection<String> keys;

        /**
         * @param keys Keys.
         */
        public CheckKeySizeTask(Collection<String> keys) {
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public void run(int idx) throws Exception {
            GridCacheContext<String, Integer> ctx = ((IgniteKernal)ignite).<String, Integer>internalCache(DEFAULT_CACHE_NAME).context();

            int size = 0;

            for (String key : keys)
                if (ctx.affinity().keyLocalNode(key, ctx.discovery().topologyVersionEx()))
                    size++;

            assertEquals("Incorrect key size on cache #" + idx, size, ignite.cache(DEFAULT_CACHE_NAME).localSize(ALL));
        }
    }

    /**
     *
     */
    private static class FailedEntryProcessor implements EntryProcessor<String, Integer, Integer>, Serializable {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<String, Integer> e, Object... args) {
            throw new EntryProcessorException("Test entry processor exception.");
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        TestValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof TestValue))
                return false;

            TestValue val = (TestValue)o;

            if (this.val != val.val)
                return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     * Dummy Service.
     */
    public interface DummyService {
        /**
         *
         */
        public void noop();
    }

    /**
     * No-op test service.
     */
    public static class DummyServiceImpl implements DummyService, Service {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void noop() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            System.out.println("Cancelling service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            System.out.println("Initializing service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            System.out.println("Executing service: " + ctx.name());
        }
    }

    /**
     *
     */
    public static class CacheEventListener implements IgniteBiPredicate<UUID, CacheEvent>, IgnitePredicate<CacheEvent> {
        /** */
        public final LinkedBlockingQueue<CacheEvent> evts = new LinkedBlockingQueue<>();

        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, CacheEvent evt) {
            evts.add(evt);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(CacheEvent evt) {
            evts.add(evt);

            return true;
        }
    }
}
