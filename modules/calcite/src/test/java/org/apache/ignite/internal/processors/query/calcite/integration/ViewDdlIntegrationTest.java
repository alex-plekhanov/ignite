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

import org.junit.Test;

/** */
public class ViewDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /** Cache name. */
    private static final String CACHE_NAME = "my_cache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sql("create table my_table(id int, val_int int, val_str varchar) with cache_name=\"" + CACHE_NAME + "\"");
        sql("insert into my_table values (0, 0, '0')");
        sql("insert into my_table values (1, 1, '1')");
        sql("insert into my_table values (2, 2, '2')");
    }

    /**
     * Creates and drops view.
     */
    @Test
    public void createDropViewSimpleCase() {
        //assertNull(findIndex(CACHE_NAME, "my_index"));

        //sql("create view my_view as select id, val_int, val_str from my_table");
        sql("create or replace view my_view as select * from my_table");

        assertQuery("select * from my_view").resultSize(3).check();

        //assertNotNull(findIndex(CACHE_NAME, "my_index"));

        sql("drop view my_view");

        //assertNull(findIndex(CACHE_NAME, "my_index"));

        //int cnt = indexes(CACHE_NAME).size();

        //sql("create index on my_table(val_int)");

        //assertEquals(cnt + 1, indexes(CACHE_NAME).size());
    }
}
