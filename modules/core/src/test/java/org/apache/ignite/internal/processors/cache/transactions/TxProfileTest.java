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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.PartitionTxUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests transactions profiling.
 */
public class TxProfileTest extends GridCommonAbstractTest {
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected long getTestTimeout() {
        return 3_000_000L;
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                ).setWalMode(WALMode.BACKGROUND)
            )
            .setEventStorageSpi(new NoopEventStorageSpi())
            .setIncludeEventTypes()
            .setClientFailureDetectionTimeout(60_000L);
    }

    /**
     * Tests transaction labels.
     */
    @Test
    public void testTxs() throws Exception {
        int gridsCnt = 3;
        int putMapsCnt = 100;
        int keysCnt = 256;
        Random rnd = new Random();

        Map<Integer, Map<Integer, Integer>> putMaps = new HashMap<>();

        for (int i = 0; i < putMapsCnt; i++) {
            Map<Integer, Integer> putMap = new TreeMap<>();

            for (int j = 0; j < keysCnt; j++)
                putMap.put(rnd.nextInt(100_000), j);

            putMaps.put(i, putMap);
        }

        Ignite ignite = startGrids(gridsCnt);

        ignite.cluster().active(true);

        IgniteCache cache = ignite.getOrCreateCache(defaultCacheConfiguration()
            .setAffinity(new RendezvousAffinityFunction().setPartitions(10)).setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));

        Ignition.setClientMode(true);
        for (int i = 0; i < gridsCnt; i++)
            startGrid(i + gridsCnt).cache(DEFAULT_CACHE_NAME).put(0, 0);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        System.out.println("Starting test. Waiting 15 seconds...");
        //doSleep(15_000L);
        System.out.println("Test started...");
        long t0 = System.currentTimeMillis();

        AtomicInteger cnt = new AtomicInteger();
        GridTestUtils.runMultiThreaded(() -> {
            Random rnd0 = new Random();

            Ignite ignite0 = grid(cnt.getAndIncrement() % gridsCnt + gridsCnt);

            IgniteCache cache0 = ignite0.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 2_000L; i++)
                cache0.put(rnd0.nextInt(500), rnd0.nextInt(500));

                /*
                for (int i = 0; i < putMapsCnt; i++)
                    cache0.putAll(putMaps.get(rnd0.nextInt(putMapsCnt)));
*/
        },50, "put-thread");

/*
        for (int i = 0; i < putMapsCnt; i++)
            cache.putAll(putMaps.get(i));
*/

/*
        for (int i = 0; i < 10_000; i++) {
            try(Transaction tx = ignite.transactions().txStart()) {
                cache.put(i % 100, i % 100);

                tx.commit();
            }
        }
*/

        System.out.println("Page write locks: " + PageMemoryImpl.writeLockCounter.get());
        System.out.println("Page read locks: " + PageMemoryImpl.readLockCounter.get());
        System.out.println("Init0 cnt: " + GridCacheOffheapManager.init0cnt.get());
        System.out.println("Init0 time: " + GridCacheOffheapManager.init0time.get()/1_000_000L);
        System.out.println("ApplyUpdCntr cnt: " + IgniteTxHandler.updCntrCnt.get());
        System.out.println("ApplyUpdCntr time: " + IgniteTxHandler.updCntrTime.get()/1_000_000L);
        System.out.println("PartUpdCntr maxSize: " + PartitionTxUpdateCounterImpl.maxSize);
        System.out.println(">>>> Total test time: " + (System.currentTimeMillis() - t0));
        System.out.println("Test completed. Waiting 10 seconds...");
        //doSleep(10_000L);
    }
}
