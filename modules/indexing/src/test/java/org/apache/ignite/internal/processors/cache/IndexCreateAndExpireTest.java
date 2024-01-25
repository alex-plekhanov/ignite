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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class IndexCreateAndExpireTest extends GridCommonAbstractTest {
    /** */
    private static final long TTL = 2_000L;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(2L * 1024 * 1024 * 1024)));

        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, TTL)))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, TestValue.class);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testIndexCreateAndExpire() throws Exception {
        Ignite ignite = startGrid();
        ignite.cluster().state(ClusterState.ACTIVE);

        long ts = U.currentTimeMillis();

        int cnt = 0;

        ignite.cache(DEFAULT_CACHE_NAME).query(
            new SqlFieldsQuery("CREATE INDEX TESTIDXINT ON \"" + DEFAULT_CACHE_NAME + "\".TestValue(intVal) INLINE_SIZE 1")
        ).getAll();

        try (IgniteDataStreamer<Integer, TestValue> ds = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            while (U.currentTimeMillis() - ts < TTL)
                ds.addData(++cnt, new TestValue(cnt));

        }
        log.info(">>>> Inserted " + cnt);

        int keys = cnt;
        GridTestUtils.runAsync(() -> {
            for (int i = keys; i >= 0; i--)
                ignite.cache(DEFAULT_CACHE_NAME).put(ThreadLocalRandom.current().nextInt(keys), new TestValue(ThreadLocalRandom.current().nextInt(keys)));
        });

/*
        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            doSleep(500L);

            log.info(">>>> Index dropping");

            ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("DROP INDEX TESTIDX")).getAll();

            log.info(">>>> Index dropped");
        });
*/

        ignite.cache(DEFAULT_CACHE_NAME).query(
            new SqlFieldsQuery("CREATE INDEX TESTIDX ON \"" + DEFAULT_CACHE_NAME + "\".TestValue(strVal) INLINE_SIZE 1")
        ).getAll();

        log.info(">>>> Index created");
        log.info(">>>> Cache size = " + ignite.cache(DEFAULT_CACHE_NAME).size());

        //fut.get();
    }

    /** */
    private static class TestValue {
        /** */
        @QuerySqlField
        private final String strVal;

        /** */
        @QuerySqlField
        private final int intVal;

        private final byte[] bytesVal = new byte[4500];


        /** */
        private TestValue(int intVal) {
            this.strVal = "val" + intVal;
            this.intVal = intVal;
        }
    }
}
