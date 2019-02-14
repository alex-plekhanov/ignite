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

package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class LongQueriesSelfTest extends GridCommonAbstractTest {
    public void testLongQuery() throws Exception {
        Ignite ignite = startGrid();

        IgniteCache cache = ignite.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache")
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))
            .setSqlFunctionClasses(TestSQLFunctions.class)
        );

        cache.query(new SqlFieldsQuery("SELECT sleep(10000) FROM dual")).getAll();
    }

    public static class TestSQLFunctions {
        @QuerySqlFunction
        public static long sleep(long x) {
            doSleep(x);

            return x;
        }
    }

}
