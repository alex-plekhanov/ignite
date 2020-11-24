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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test node restart.
 */
public class PutAndRestartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<>("cache")
                .setBackups(1)
                .setEvictionPolicyFactory(() -> new LruEvictionPolicy<>().setMaxSize(100))
                .setOnheapCacheEnabled(true)
                .setNearConfiguration(new NearCacheConfiguration<>())
                .setAffinity(new RendezvousAffinityFunction(false, 10))
            );
    }

    @Test
    public void test() throws Exception {
        startGrids(4);

        try {
            AtomicInteger gridIdx = new AtomicInteger();

            long ts = U.currentTimeMillis();

            GridTestUtils.runMultiThreadedAsync(() -> {
                IgniteCache<Integer, Integer> cache = grid(gridIdx.getAndIncrement()).cache("cache");

                while (U.currentTimeMillis() - ts < 150_000L)
                    cache.put(ThreadLocalRandom.current().nextInt(100_000), 0);
            }, 2, "put-worker");

            while (U.currentTimeMillis() - ts < 150_000L) {
                stopGrid(2);
                startGrid(2);
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
