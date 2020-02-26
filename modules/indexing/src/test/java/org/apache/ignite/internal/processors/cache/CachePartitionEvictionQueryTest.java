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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
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
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setBackups(0)
            .setCacheMode(CacheMode.PARTITIONED)
            .setIndexedTypes(Integer.class, Integer.class)
        );

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));


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
        startGrids(3);

        grid(0).cluster().active(true);
/*
        grid(0).cluster().baselineAutoAdjustEnabled(true);
        grid(0).cluster().baselineAutoAdjustTimeout(5_000L);
*/

        try (IgniteDataStreamer<Integer, Integer> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 10_000; i++)
                streamer.addData(i, i);
        }

        IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));

        IgniteInternalFuture fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (true)
                for (Object obj : client.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>().setPageSize(1)))
                    doSleep(10);
        }, 20, "query");

        while (true) {
            stopGrid(2);

            grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

            awaitPartitionMapExchange();

            //doSleep(10_000L);

            startGrid(2);

            grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

            awaitPartitionMapExchange();

            forceCheckpoint();
            //doSleep(40_000L);
        }
    }
}
