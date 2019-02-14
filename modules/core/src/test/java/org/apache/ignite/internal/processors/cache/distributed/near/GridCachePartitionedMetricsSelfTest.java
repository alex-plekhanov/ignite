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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheTransactionalAbstractMetricsSelfTest;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Partitioned cache metrics test.
 */
public class GridCachePartitionedMetricsSelfTest extends GridCacheTransactionalAbstractMetricsSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.METRICS);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setRebalanceMode(SYNC);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }


    @Test
    public void testPuts() throws Exception {
        IgniteEx cl = startGrid(getConfiguration("client").setClientMode(true));

        final IgniteCache<Integer, Integer> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);
        final IgniteCache<Integer, Integer> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);
        final IgniteCache<Integer, Integer> cacheCl = cl.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 500; i++)
            cache0.put(i, i);

        cache1.put(0, 0);
        //cacheCl.put(0, 0);

        Transaction tx = cl.transactions().txStart();

        cacheCl.put(0, 0);

        doSleep(500L);

        tx.commit();

        doSleep(1_000L);

        log.info(">>>> Puts0: " + cache0.localMetrics().getCachePuts());
        log.info(">>>> Puts1: " + cache1.localMetrics().getCachePuts());
        log.info(">>>> PutsCl: " + cacheCl.localMetrics().getCachePuts());
        log.info(">>>> AvgPuts0: " + cache0.localMetrics().getAveragePutTime());
        log.info(">>>> AvgPuts1: " + cache1.localMetrics().getAveragePutTime());
        log.info(">>>> AvgPutsCl: " + cacheCl.localMetrics().getAveragePutTime());
        log.info(">>>> Commit0: " + cache0.localMetrics().getCacheTxCommits());
        log.info(">>>> Commit1: " + cache1.localMetrics().getCacheTxCommits());
        log.info(">>>> CommitCl: " + cacheCl.localMetrics().getCacheTxCommits());
        log.info(">>>> AvgCommit0: " + cache0.localMetrics().getAverageTxCommitTime());
        log.info(">>>> AvgCommit1: " + cache1.localMetrics().getAverageTxCommitTime());
        log.info(">>>> AvgCommitCl: " + cacheCl.localMetrics().getAverageTxCommitTime());
    }

}
