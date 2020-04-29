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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class PageEvictionUnderLoadTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU)
                    .setMaxSize(512L * 1024 * 1024)))
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 10))));
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 1_000_000L;
    }

    /**
     *
     */
    @Test
    public void testEvictionUnderLoad() throws Exception {
        Ignite ignite = startGrid(0);

        AtomicLong progress = new AtomicLong();
        AtomicBoolean stop = new AtomicBoolean();

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            Random rnd = new Random();

            while (!stop.get()) {
                progress.incrementAndGet();

                cache.put(rnd.nextInt(500_000), new byte[rnd.nextInt(4096)]);

                cache.get(rnd.nextInt(500_000));
            }
        }, 25, "put-thread");

        long lastProgress = 0;

        for (int i = 0; i < 500; i++) {
            long curProgress = progress.get();

            log.info("Progress: " + curProgress + " (" + (curProgress - lastProgress) + ')');

            lastProgress = curProgress;

            doSleep(1_000L);
        }

        stop.set(true);

        fut.get();
    }
}
