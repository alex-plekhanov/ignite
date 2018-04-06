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

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

/**
 * Failure handler hang test.
 */
public class FailureProcessorHangTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true).setName("default"));

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setFailureHandler(new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
                return true;
            }
        });

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        return cfg;
    }

    /**
     * Test PDS hang.
     */
    public void testPdsHang() throws Exception {
        cleanPersistenceDir();

        IgniteEx ignite = startGrid(0);
        //startGrid(1);

        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        try (Transaction tx = ignite.transactions().txStart()){
            ignite.getOrCreateCache(DEFAULT_CACHE_NAME).put(0, 0);

            ignite.context().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, null));

            tx.commit();
        }
        catch (Throwable ignore) {

        }


        stopAllGrids();
    }
}
