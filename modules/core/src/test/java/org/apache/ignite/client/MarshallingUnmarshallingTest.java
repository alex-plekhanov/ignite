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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.testframework.MvccFeatureChecker.Feature.NEAR_CACHE;
import static org.junit.Assert.assertArrayEquals;

/**
 * Marshalling/unmarshalling tests.
 */
public class MarshallingUnmarshallingTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Client connector address. */
    private static final String CLIENT_CONN_ADDR = "127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);

        awaitPartitionMapExchange();
    }

    /**
     * Test cache operations with different data types.
     */
    @Test
    public void testDataTypes() throws Exception {
        Ignite ignite = grid(0);

        try (IgniteClient client = startClient()) {
            ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

            Person person = new Person(1, "name");

            // Primitive and built-in types.
            checkDataType(client, ignite, (byte)1);
            checkDataType(client, ignite, (short)1);
            checkDataType(client, ignite, 1);
            checkDataType(client, ignite, 1L);
            checkDataType(client, ignite, 1.0f);
            checkDataType(client, ignite, 1.0d);
            checkDataType(client, ignite, 'c');
            checkDataType(client, ignite, true);
            checkDataType(client, ignite, "string");
            checkDataType(client, ignite, UUID.randomUUID());
            checkDataType(client, ignite, new Date());

            // Enum.
            checkDataType(client, ignite, CacheAtomicityMode.ATOMIC);

            // Binary object.
            checkDataType(client, ignite, person);

            // Arrays.
            checkDataType(client, ignite, new byte[] {(byte)1});
            checkDataType(client, ignite, new short[] {(short)1});
            checkDataType(client, ignite, new int[] {1});
            checkDataType(client, ignite, new long[] {1L});
            checkDataType(client, ignite, new float[] {1.0f});
            checkDataType(client, ignite, new double[] {1.0d});
            checkDataType(client, ignite, new char[] {'c'});
            checkDataType(client, ignite, new boolean[] {true});
            checkDataType(client, ignite, new String[] {"string"});
            checkDataType(client, ignite, new UUID[] {UUID.randomUUID()});
            checkDataType(client, ignite, new Date[] {new Date()});
            checkDataType(client, ignite, new int[][] {new int[] {1}});

            checkDataType(client, ignite, new CacheAtomicityMode[] {CacheAtomicityMode.ATOMIC});

            checkDataType(client, ignite, new Person[] {person});
            checkDataType(client, ignite, new Person[][] {new Person[] {person}});
            checkDataType(client, ignite, new Object[] {1, "string", person, new Person[] {person}});

            // Lists.
            checkDataType(client, ignite, Collections.emptyList());
            checkDataType(client, ignite, Collections.singletonList(person));
            checkDataType(client, ignite, Arrays.asList(person, person));
            checkDataType(client, ignite, new ArrayList<>(Arrays.asList(person, person)));
            checkDataType(client, ignite, new LinkedList<>(Arrays.asList(person, person)));
            checkDataType(client, ignite, Arrays.asList(Arrays.asList(person, person), person));

            // Sets.
            checkDataType(client, ignite, Collections.emptySet());
            checkDataType(client, ignite, Collections.singleton(person));
            checkDataType(client, ignite, new HashSet<>(Arrays.asList(1, 2)));
            checkDataType(client, ignite, new HashSet<>(Arrays.asList(Arrays.asList(person, person), person)));
            checkDataType(client, ignite, new HashSet<>(new ArrayList<>(Arrays.asList(Arrays.asList(person,
                person), person))));

            // Maps.
            checkDataType(client, ignite, Collections.emptyMap());
            checkDataType(client, ignite, Collections.singletonMap(1, person));
            checkDataType(client, ignite, F.asMap(1, person));
            checkDataType(client, ignite, new HashMap<>(F.asMap(1, person)));
            checkDataType(client, ignite, new HashMap<>(F.asMap(new HashSet<>(Arrays.asList(1, 2)),
                Arrays.asList(person, person))));
        }
    }

    @Test
    public void testHandles() throws Exception {
        Ignite ignite = grid(0);

        try (IgniteClient client = startClient()) {
            ClientCache<Integer, Object[]> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

            Person person = new Person(0, "name");

            cache.put(0, new Object[] {person, person} );

            Object[] res = cache.get(0);

            assertEquals(person, res[0]);

            assertTrue(res[0] == res[1]);
        }
    }

    /**
     *
     */
    @Test
    public void testCacheObjectReturningOperationsDebug() throws Exception {
        CacheAtomicityMode atomicityMode = TRANSACTIONAL;
        CacheMode cacheMode = CacheMode.LOCAL;
        int nearFlag = 0;

        Ignite ignite = grid(0);

        try (IgniteClient client = startClient()) {
            String cacheName = DEFAULT_CACHE_NAME;

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName)
                .setAtomicityMode(atomicityMode)
                .setCacheMode(cacheMode);

            if (nearFlag == 1)
                ccfg.setNearConfiguration(new NearCacheConfiguration<>());

            ignite.createCache(ccfg);

            awaitPartitionMapExchange();

            ClientCache<Integer, Object[]> cache = client.cache(cacheName);

            TransactionConcurrency concurrency = null;//TransactionConcurrency.OPTIMISTIC;
            TransactionIsolation isolation = null;//TransactionIsolation.READ_COMMITTED;

            if (concurrency == null || isolation == null)
                checkCacheObjectReturningOperations(cache);
            else {
                try (ClientTransaction tx = client.transactions().txStart(concurrency, isolation)) {
                    checkCacheObjectReturningOperations(cache);
                }
            }
        }
    }

    /**
     * Test operations which should return CacheObject on server-side, to avoid double marshalling/unmarshalling.
     */
    @Test
    public void testCacheObjectReturningOperations() throws Exception {
        Ignite ignite = grid(0);

        try (IgniteClient client = startClient()){
            int cacheNum = 0;

            for (CacheMode cacheMode : CacheMode.values()) {
                for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
                    if (isMvcc(atomicityMode) && !MvccFeatureChecker.isSupported(cacheMode))
                        continue;

                    for (int nearFlag = 0; nearFlag < 2; nearFlag++) {
                        if (nearFlag == 1) {
                            if (isMvcc(atomicityMode) && !MvccFeatureChecker.isSupported(NEAR_CACHE))
                                continue;

                            if (cacheMode == CacheMode.LOCAL)
                                continue;
                        }

                        String cacheName = "TEST_CACHE_OBJECTS_" + cacheNum++;

                        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName)
                            .setAtomicityMode(atomicityMode)
                            .setCacheMode(cacheMode);

                        if (nearFlag == 1)
                            ccfg.setNearConfiguration(new NearCacheConfiguration<>());

                        ignite.createCache(ccfg);

                        awaitPartitionMapExchange();

                        ClientCache<Integer, Object[]> cache = client.cache(cacheName);

                        log.info("Test operations for cache " + cacheName + " [cacheMode=" + cacheMode +
                            ", atomicityMode=" + atomicityMode + ", nearFlag=" + nearFlag + ']');

                        // Check atomic operations and implicit transactions.
                        checkCacheObjectReturningOperations(cache);

                        // For transactional caches test explicit transactions with different modes.
                        if (atomicityMode == TRANSACTIONAL || atomicityMode == TRANSACTIONAL_SNAPSHOT) {
                            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                                    if (isMvcc(atomicityMode) && !MvccFeatureChecker.isSupported(concurrency, isolation))
                                        continue;

                                    log.info("Test operations under explicit tx for cache " + cacheName +
                                        " [cacheMode=" + cacheMode + ", atomicityMode=" + atomicityMode + ", nearFlag="
                                        + nearFlag + "txConcurrency=" + concurrency + ", txIsolation=" + isolation +  ']');

                                    try (ClientTransaction tx = client.transactions().txStart(concurrency, isolation)) {
                                        checkCacheObjectReturningOperations(cache);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * @param cache Cache.
     */
    private void checkCacheObjectReturningOperations(ClientCache<Integer, Object[]> cache) {
        for (int key = 0; key < 10; key++) {
            Person person = new Person(key, "name " + key);

            Object[] val = new Object[] {person, person};

            cache.put(key, val);

            checkResultWithHandles(val, cache.get(key));

            checkResultWithHandles(val, cache.getAndReplace(key, val));
        }
    }

    /**
     * Check objects array returned by cache operation (there should be only references to the same instance in array).
     *
     * @param exp Expected objects array.
     * @param act Actual objects array.
     */
    private void checkResultWithHandles(Object[] exp, Object[] act) {
        assertArrayEquals(exp, act);

        for (int i = 1; i < act.length; i++)
            assertTrue(act[0] == act[i]);
    }

    /**
     * @param mode Mode.
     */
    private static boolean isMvcc(CacheAtomicityMode mode) {
        return mode == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
    }

    /**
     * Check that we get the same value from the cache as we put before.
     *
     * @param client Thin client.
     * @param ignite Ignite node.
     * @param obj Value of data type to check.
     */
    private void checkDataType(IgniteClient client, Ignite ignite, Object obj) {
        IgniteCache<Object, Object> thickCache = ignite.cache(DEFAULT_CACHE_NAME);
        ClientCache<Object, Object> thinCache = client.cache(DEFAULT_CACHE_NAME);

        Integer key = 1;

        thinCache.put(key, obj);

        assertTrue(thinCache.containsKey(key));

        Object cachedObj = thinCache.get(key);

        assertEqualsArraysAware(obj, cachedObj);

        assertEqualsArraysAware(obj, thickCache.get(key));

        assertEquals(client.binary().typeId(obj.getClass().getName()), ignite.binary().typeId(obj.getClass().getName()));

        if (!obj.getClass().isArray()) { // TODO IGNITE-12578
            // Server-side comparison with the original object.
            assertTrue(thinCache.replace(key, obj, obj));

            // Server-side comparison with the restored object.
            assertTrue(thinCache.remove(key, cachedObj));
        }
    }

    /**
     * Assert values equals (deep equals for arrays).
     *
     * @param exp Expected value.
     * @param actual Actual value.
     */
    private void assertEqualsArraysAware(Object exp, Object actual) {
        if (exp instanceof Object[])
            assertArrayEquals((Object[])exp, (Object[])actual);
        else if (U.isPrimitiveArray(exp))
            assertArrayEquals(new Object[] {exp}, new Object[] {actual}); // Hack to compare primitive arrays.
        else
            assertEquals(exp, actual);
    }

    /**
     *
     */
    private IgniteClient startClient() {
        return Ignition.startClient(new ClientConfiguration().setAddresses(CLIENT_CONN_ADDR));
    }
}
