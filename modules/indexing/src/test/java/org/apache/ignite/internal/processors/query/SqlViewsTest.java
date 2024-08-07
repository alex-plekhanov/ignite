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
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class SqlViewsTest extends AbstractIndexingCommonTest {
    /** */
    @Test
    public void testSqlView() throws Exception {
        startGrids(3);

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

    /**
     * Tests views on not existing schema.
     */
    @Test
    public void testNotExistingSchema() throws Exception {
        startGrids(3);

        sql("CREATE TABLE my_table(id INT PRIMARY KEY, val VARCHAR)");

        GridTestUtils.assertThrowsAnyCause(log,
            () -> sql("CREATE VIEW my_schema.my_view AS SELECT * FROM public.my_table"),
            IgniteSQLException.class, "Schema doesn't exist: MY_SCHEMA");
    }

    /** */
    @Test
    public void testRecursiveView() throws Exception {
        startGrid(0);

        sql("CREATE TABLE TEST_VIEW0 (ID INT PRIMARY KEY, VAL VARCHAR)");
        sql("CREATE VIEW TEST_VIEW1 AS SELECT * FROM TEST_VIEW0");
        sql("DROP TABLE TEST_VIEW0");
        sql("CREATE VIEW TEST_VIEW0 AS SELECT * FROM TEST_VIEW1");

        sql("SELECT * FROM TEST_VIEW0");
    }

    /** */
    @Test
    public void testSysSchema() throws Exception {
        String msg = "DDL statements are not supported on SYS schema";

        startGrid(0);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> sql("CREATE OR REPLACE VIEW sys.views AS SELECT * FROM sys.tables"), IgniteSQLException.class, msg);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> sql("DROP VIEW sys.views"), IgniteSQLException.class, msg);
    }

    /** */
    private List<List<?>> sql(String sql) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql), true).getAll();
    }
}
