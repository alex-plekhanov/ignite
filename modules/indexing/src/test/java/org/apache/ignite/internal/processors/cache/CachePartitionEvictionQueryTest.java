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
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CachePartitionEvictionQueryTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name).setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testQuery2() throws Exception {
        startGrid(getConfiguration(getTestIgniteInstanceName(0)).setConsistentId("1"));

        grid(0).cluster().active(true);

        grid(0).cluster().baselineAutoAdjustEnabled(true);
        grid(0).cluster().baselineAutoAdjustTimeout(0);

        IgniteCache<Integer, Integer> cache = grid(0).getOrCreateCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setBackups(0)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(2)));

        cache.put(0, 0);
        cache.put(1, 1);

        Iterator iter = grid(0).cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>().setPageSize(1)).iterator();

        iter.next();

        startGrid(getConfiguration(getTestIgniteInstanceName(1)).setConsistentId("0"));

        awaitPartitionMapExchange();

        forceCheckpoint(grid(0));

        iter.next();
    }
}
