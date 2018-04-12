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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * PME hangs test.
 */
public class PmeHangsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )
        );

        //cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /**
     * PME hangs test
     */
    public void testPmeHangs() throws Exception {
        cleanPersistenceDir();

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);
        IgniteEx ignite2 = startGrid(2);

        ((GridCacheDatabaseSharedManager)ignite1.context().cache().context().database()).addCheckpointListener(
            new DbCheckpointListener() {
                @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    doSleep(300_000L);
                }
            }
        );

        ignite2.cluster().active(true);

        startGrid(3);


        ((GridCacheDatabaseSharedManager)ignite1.context().cache().context().database()).addCheckpointListener(
            new DbCheckpointListener() {
                @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    throw new IgniteException();
                }
            }
        );


        ignite0.cluster().active(true);
    }
}
