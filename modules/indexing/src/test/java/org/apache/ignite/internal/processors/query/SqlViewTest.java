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

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;

/**
 *
 */
public class SqlViewTest extends AbstractIndexingCommonTest {
    /** */
    @Test
    public void testSqlView() throws Exception {
        startGrid(0);

        sql("CREATE TABLE TEST_TBL(F1 VARCHAR PRIMARY KEY, F2 VARCHAR, F3 VARCHAR)");

        sql("INSERT INTO TEST_TBL VALUES ('11', '12', '13')");
        sql("INSERT INTO TEST_TBL VALUES ('21', '22', '23')");
        sql("INSERT INTO TEST_TBL VALUES ('31', '32', '33')");

        sql("CREATE VIEW TEST_VIEW AS SELECT * FROM TEST_TBL");

        List<List<?>> res = sql("SELECT * FROM TEST_VIEW");

        assertEquals(3, res.size());

        res = sql("SELECT T.F1, F2, * FROM TEST_VIEW T");

        assertEquals(3, res.size());
    }

    /** */
    private List<List<?>> sql(String sql) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql), true).getAll();
    }
}
