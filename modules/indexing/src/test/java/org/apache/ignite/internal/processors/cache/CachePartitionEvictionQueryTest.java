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

import java.util.Iterator;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CachePartitionEvictionQueryTest extends GridCommonAbstractTest {
    private static final String[] IDS = new String[] {
        "RubinNT_grid960.delta.sbrf.ru",
        "RubinNT_grid952.delta.sbrf.ru",
        "RubinNT_grid956.delta.sbrf.ru",
        "RubinNT_grid959.delta.sbrf.ru"
    };

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        if (name.startsWith(getTestIgniteInstanceName())) {
            int nodeIdx = getTestIgniteInstanceIndex(name);

            cfg.setConsistentId(IDS[nodeIdx]);
        }
        else
            cfg.setConsistentId(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(512L * 1024 * 1024)
            ));

        return cfg;
    }

    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Override protected long getTestTimeout() {
        return 3_000_000L;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQuery() throws Exception {
        startGrids(4);

        grid(0).cluster().active(true);

        grid(0).getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setBackups(1));

        try (IgniteDataStreamer<Integer, Integer> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 10_000; i++)
                streamer.addData(i, i);
        }

        stopAllGrids();

        startGrid(0);
        startGrid(1);

        grid(0).cluster().active(true);

        startGrid(2);
        startGrid(3);

        grid(0).cluster().baselineAutoAdjustEnabled(true);
        grid(0).cluster().baselineAutoAdjustTimeout(10L);

        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));

        while (true) {
            stopGrid(1);

            //grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

            //awaitPartitionMapExchange();

            doSleep(10_000L);

            stopGrid(2);

            //grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

            //awaitPartitionMapExchange();
            doSleep(10_000L);

            startGrid(1);

            //grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

            //awaitPartitionMapExchange();
            doSleep(10_000L);

            IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(() -> {
                while (true)
                    for (Object obj : client.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>().setPageSize(1)))
                        doSleep(10);
            }, 20, "query");

            doSleep(5_000L);

            startGrid(2);

            //grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

            //awaitPartitionMapExchange();

            forceCheckpoint();

            doSleep(10_000L);

            fut.get();
        }
    }

    @Test
    public void testQuery1() throws Exception {
        startGrids(1);

        grid(0).cluster().active(true);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        grid(0).getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setBackups(0));

        try (IgniteDataStreamer<Integer, Integer> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 10_000; i++)
                streamer.addData(i, i);
        }

        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));

        Iterator iter = client.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>().setPageSize(1)).iterator();

        if (iter.hasNext())
            iter.next();

        startGrid(2);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        doSleep(5_000L);

        while (iter.hasNext())
            iter.next();
    }

    @Test
    public void testQuery2() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0)).setConsistentId("1"));

        grid(0).cluster().active(true);

        grid(0).cluster().baselineAutoAdjustEnabled(true);
        grid(0).cluster().baselineAutoAdjustTimeout(0);

        IgniteCache cache = grid(0).getOrCreateCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setBackups(0)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(2))
        );

        cache.put(0, 0);
        cache.put(1, 1);

        Iterator iter = grid(0).cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>().setPageSize(1)).iterator();

        if (iter.hasNext())
            iter.next();

        startGrid(getConfiguration(getTestIgniteInstanceName(1)).setConsistentId("0"));

        awaitPartitionMapExchange();

        forceCheckpoint(grid(0));

        while (iter.hasNext())
            iter.next();
    }
}
