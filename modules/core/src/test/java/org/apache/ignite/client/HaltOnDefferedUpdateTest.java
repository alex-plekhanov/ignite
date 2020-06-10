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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class HaltOnDefferedUpdateTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 1_000_000L;
    }

    /** Ip finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder().setShared(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        //return super.getConfiguration(igniteInstanceName).setFailureHandler(new StopNodeOrHaltFailureHandler());
        return new IgniteConfiguration().setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder)).setIgniteInstanceName(igniteInstanceName);
    }

    /** */
    @Test
    public void testHalt() throws Exception {
        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).createCache(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.REPLICATED)
        );

        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 1_000; i++)
            map.put(i, i);

        AtomicBoolean stopped = new AtomicBoolean();

        GridTestUtils.runMultiThreadedAsync(()  -> {
            while (!stopped.get()) {
                //grid(0).cache(DEFAULT_CACHE_NAME).putAll(map);
                //grid(1).cache(DEFAULT_CACHE_NAME).putAll(map);

                for (int i = 0; i < 10; i++) {
                    try {
                        grid(0).cache(DEFAULT_CACHE_NAME).put(i, i);
                        grid(1).cache(DEFAULT_CACHE_NAME).put(i + 10, i + 10);
                    }
                    catch (Exception ignore) {
                        // No-op.
                    }
                }
            }
        }, 5 ,"put");

        for (int i = 0; i < 100; i++) {
            startGrid(1);
            awaitPartitionMapExchange();
            //stopGrid(0, true);
            //awaitPartitionMapExchange();
            //doSleep(600L); // DEFERRED_UPDATE_RESPONSE_TIMEOUT + 100 ms
            //startGrid(0);
            //awaitPartitionMapExchange();
            stopGrid(1, true);
            awaitPartitionMapExchange();
            //doSleep(600L); // DEFERRED_UPDATE_RESPONSE_TIMEOUT + 100 ms
        }

        stopped.set(true);
    }
}
