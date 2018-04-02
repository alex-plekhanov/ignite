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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * IgniteOutOfMemoryError failure handler test.
 */
public class IoomHangTest extends GridCommonAbstractTest {
    /** Offheap size for memory policy. */
    private static final int SIZE = 10 * 1024 * 1024;

    /** Page size. */
    static final int PAGE_SIZE = 2048;

    /** Number of entries. */
    static final int ENTRIES = 2_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();

        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(SIZE);
        dfltPlcCfg.setMaxSize(SIZE);
        dfltPlcCfg.setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);
        dsCfg.setPageSize(PAGE_SIZE);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setFailureHandler(new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
                return true;
            }
        });

        return cfg;
    }

    /**
     * Test IgniteOutOfMemoryException handling with PDS.
     */
    public void testIoomErrorPdsHandling() throws Exception {
        cleanPersistenceDir();

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache cache = ignite1.getOrCreateCache("TEST");

        Map<Integer, Object> entries = new HashMap<>();

        for (int i = 0; i < ENTRIES; i++)
            entries.put(i, new byte[PAGE_SIZE * 2 / 3]);

        cache.putAll(entries);

        ignite1.context().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, null));

        stopGrid(0);
        stopGrid(1);
    }
}
