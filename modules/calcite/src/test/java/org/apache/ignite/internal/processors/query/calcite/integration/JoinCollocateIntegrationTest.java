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

package org.apache.ignite.internal.processors.query.calcite.integration;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class JoinCollocateIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 3;
    }

    /** */
    @Test
    public void test() throws Exception {
        client.addCacheConfiguration(new CacheConfiguration<>()
            .setName("SqlDistributedProfileTemplate")
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setStatisticsEnabled(true)
        );

/*
        CREATE TABLE order_items (id varchar, orderId int, sku varchar, PRIMARY KEY (id, orderId) WITH "affinity_key=test");
        CREATE TABLE order_items (id varchar, orderId int, sku varchar, PRIMARY KEY (id));
        CREATE TABLE orders (id int, address varchar,  PRIMARY KEY (id));
        CREATE INDEX order_items_search_by_orderId ON order_items (orderId ASC)
        SELECT * FROM order_items i JOIN orders o ON o.id=i.orderId WHERE o.id=1
*/

        sql("CREATE TABLE order_items (\n" +
            "    id varchar,\n" +
            "    orderId int,\n" +
            "    sku varchar,\n" +
            "    price decimal,\n" +
            "    amount int,\n" +
            "    PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=order_items," +
            "          template=SqlDistributedProfileTemplate" +
            //",affinity_key=ORDERID" +
            "\"");

        sql("CREATE TABLE orders (\n" +
            "    id int,\n" +
            "    region varchar,\n" +
            "    itemsCount int,\n" +
            "    PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=orders,template=SqlDistributedProfileTemplate" +
            "\"");

        sql("CREATE INDEX order_items_search_by_orderId ON order_items (orderId ASC)");
        sql("CREATE INDEX order_items_search_by_price ON order_items (price ASC)");
        sql("CREATE INDEX orders_search_by_region ON orders (region ASC)");

        for (int i = 0; i < 30; i++) {
            sql("INSERT INTO orders VALUES(?, ?, ?)", i, "region" + i % 10, i);
            for (int j = 0; j < 20; j++)
                sql("INSERT INTO order_items VALUES(?, ?, ?, ?, ?)", i + "_" + j, i, "sku" + j + '_' + i, i / 10.0, j % 10);
        }

        //String sql = "SELECT sum(o.itemsCount) FROM orders o WHERE o.region = ?";

        String sql = "SELECT sum(i.price * i.amount)" +
            " FROM order_items i JOIN orders o ON o.id=i.orderId" +
            " WHERE o.region = ?";

/*
        assertQuery(sql)
            .withParams(1)
            .matches(QueryChecker.containsTableScan("test", "test"))
            .check();
*/

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; i++)
                sql(sql, i % 10);
        }, 10, "t");
    }

    /** */
    @Test
    public void testJoin() throws Exception {
        stopAllGrids();

        startGrids(2);
        sql(grid(0), "CREATE TABLE t1(id int, val varchar, PRIMARY KEY(id)) WITH template=REPLICATED");
        sql(grid(0), "CREATE TABLE t2(id int, val varchar, PRIMARY KEY(id))");
        sql(grid(0), "CREATE INDEX t1_idx ON t1(val)");
        sql(grid(0), "CREATE INDEX t2_idx ON t2(val)");
        //sql(grid(0), "INSERT INTO t1 VALUES (1, '1'), (2, '2'), (3, '3')");

        for (int i = 0; i < 10_000; i++)
            sql(grid(0), "INSERT INTO t2 VALUES (?, ?)", i, i);

        sql(grid(0), "SELECT t1.val FROM t1 JOIN t2 ON t1.val = t2.val ORDER BY t1.val");
        //assertQuery(grid(0), "SELECT t1.val FROM t1 JOIN t2 ON t1.val = t2.val ORDER BY t1.val").matches(QueryChecker.containsTableScan("test", "test")).check();
    }
}
