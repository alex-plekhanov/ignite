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

package org.apache.ignite.failure;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Out of memory error failure handler test.
 */
public class OomFailureHandlerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new DummyFailureHandler();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test OOME in IgniteCompute.
     */
    public void testComputeOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        try {
            IgniteFuture<Boolean> res = ignite0.compute(ignite0.cluster().forNodeId(ignite1.cluster().localNode().id()))
                .callAsync(new IgniteCallable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        throw new OutOfMemoryError();
                    }
                });

            res.get();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertFalse(dummyFailureHandler(ignite0).failure());
        assertTrue(dummyFailureHandler(ignite1).failure());
    }

    /**
     * Test OOME in EntryProcessor.
     */
    public void testEntryProcessorOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Integer key = primaryKey(cache1);

        cache1.put(key, key);

        try {
            IgniteFuture fut = cache0.invokeAsync(key, new EntryProcessor<Integer, Integer, Object>() {
                @Override public Object process(MutableEntry<Integer, Integer> entry,
                    Object... arguments) throws EntryProcessorException {
                    throw new OutOfMemoryError();
                }
            });

            fut.get();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertFalse(dummyFailureHandler(ignite0).failure());
        assertTrue(dummyFailureHandler(ignite1).failure());
    }

    /**
     * Test OOME in service method invocation.
     */
    public void testServiceInvokeOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Integer key = primaryKey(cache1);

        ignite0.services().deployKeyAffinitySingleton("fail-invoke-service", new FailServiceImpl(false),
            DEFAULT_CACHE_NAME, key);

        FailService svc = ignite0.services().serviceProxy("fail-invoke-service", FailService.class, false);

        try {
            svc.fail();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertFalse(dummyFailureHandler(ignite0).failure());
        assertTrue(dummyFailureHandler(ignite1).failure());
    }

    /**
     * Test OOME in service execute.
     */
    public void testServiceExecuteOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Integer key = primaryKey(cache1);

        ignite0.services().deployKeyAffinitySingleton("fail-execute-service", new FailServiceImpl(true),
            DEFAULT_CACHE_NAME, key);

        ignite0.services().serviceProxy("fail-execute-service", FailService.class, false);

        assertFalse(dummyFailureHandler(ignite0).failure());
        assertTrue(dummyFailureHandler(ignite1).failure());
    }

    /**
     * Gets dummy failure handler for ignite instance.
     *
     * @param ignite Ignite.
     */
    private static DummyFailureHandler dummyFailureHandler(Ignite ignite) {
        return (DummyFailureHandler)ignite.configuration().getFailureHandler();
    }

    /**
     *
     */
    private static class DummyFailureHandler implements FailureHandler {
        /** Failure. */
        private boolean failure;

        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            failure = true;

            return true;
        }

        /**
         * @return Failure.
         */
        public boolean failure() {
            return failure;
        }
    }

    /**
     *
     */
    private interface FailService extends Service {
        /**
         * Fail.
         */
        void fail();
    }

    /**
     *
     */
    private static class FailServiceImpl implements FailService {
        /** Fail on execute. */
        private final boolean failOnExec;

        /**
         * @param failOnExec Fail on execute.
         */
        private FailServiceImpl(boolean failOnExec) {
            this.failOnExec = failOnExec;
        }

        /** {@inheritDoc} */
        @Override public void fail() {
            throw new OutOfMemoryError();
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            if (failOnExec)
                throw new OutOfMemoryError();
        }
    }
}
