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

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.junit.Test;

/** */
public class JoinCollocateIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(
                new IndexingQueryEngineConfiguration()
                //new CalciteQueryEngineConfiguration()
            ));
    }

    /** */
    @Test
    public void testCorrelatesAssignedBeforeAccess() throws InterruptedException {
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

        client.getOrCreateCache("test");
        client.cache("test").query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS order_items (\n" +
            "    id varchar,\n" +
            "    orderId int,\n" +
            "    sku varchar,\n" +
            "    PRIMARY KEY (id, orderId))\n" +
            "    WITH \"cache_name=order_items," +
            "          template=SqlDistributedProfileTemplate," +
            "          key_type=org.apache.ignite.internal.processors.query.calcite.integration.JoinCollocateIntegrationTest.OrderItemKey," +
            "          value_type=org.apache.ignite.internal.processors.query.calcite.integration.JoinCollocateIntegrationTest.OrderItem" +
            ",affinity_key=ORDERID" +
            "\""));

        client.cache("test").query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS orders (\n" +
            "    id int,\n" +
            "    address varchar,\n" +
            "    PRIMARY KEY (id))\n" +
            "    WITH \"cache_name=orders," +
            "          template=SqlDistributedProfileTemplate," +
            "          value_type=org.apache.ignite.internal.processors.query.calcite.integration.JoinCollocateIntegrationTest.Order\""));

        client.cache("test").query(new SqlFieldsQuery("CREATE INDEX IF NOT EXISTS order_items_search_by_orderId ON order_items (orderId ASC)"));

        for (int i = 0; i < 100; i++) {
            client.cache("orders").put(i, new Order(i, "addr " + i));
            for (int j = 0; j < 20; j++)
                client.cache("order_items").put(new OrderItemKey(Integer.toString(j), i), new OrderItem(Integer.toString(j), i, "sku" + j + i));
        }

        assertQuery("SELECT count(*) FROM order_items i JOIN orders o ON o.id=i.orderId")
            .matches(QueryChecker.containsTableScan("test", "test"))
            .check();
    }

    /**
     * Order item model class.
     */
    class OrderItem {
        String id;
        int orderId;
        String sku;

        public OrderItem(String id, int orderId, String sku) {
            this.id = id;
            this.orderId = orderId;
            this.sku = sku;
        }
    }

    /**
     * Order item affinity key.
     */
    class OrderItemKey {
        String id;

        @AffinityKeyMapped
        int orderId;

        public OrderItemKey(String id, int orderId) {
            this.id = id;
            this.orderId = orderId;
        }
    }

    /**
     * Order model class.
     */
    class Order{
        int id;
        String address;

        public Order(int id, String address) {
            this.id = id;
            this.address = address;
        }
    }
}
