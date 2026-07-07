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

package org.apache.ignite.internal.processors.cache.index;

import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class SchemaChangeDeadlockTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrids(2);

        QueryEntity qe = new QueryEntity().setTableName("TEST").setValueType("TEST");
        qe.addQueryField("fld", Integer.class.getName(), "FLD");

        ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(qe)));

        QueryIndex idx = new QueryIndex().setName("idx").setFieldNames(F.asList("FLD"), true);

        IgniteInternalFuture<?> fut = grid(0).context().query()
            .dynamicIndexCreate(DEFAULT_CACHE_NAME, DEFAULT_CACHE_NAME, "TEST", idx, true, 0);

        doSleep(100);

        stopGrid(1);

        fut.get();
    }
}
