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

import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 * Tests correlated queries.
 */
public class CorrelatesIntegrationTest extends AbstractBasicIntegrationTest {
    /**
     * Checks correlates are assigned before access.
     */
    @Test
    public void testCorrelatesAssignedBeforeAccess() {
        sql("create table test_tbl(v INTEGER)");
        sql("INSERT INTO test_tbl VALUES (1)");

        assertQuery("SELECT t0.v, (SELECT t0.v + t1.v FROM test_tbl t1) AS j FROM test_tbl t0")
            .returns(1, 2)
            .check();
    }

    /**
     * Checks that correlates can't be moved under the table spool.
     */
    @Test
    public void testCorrelatesWithTableSpool() {
        sql("CREATE TABLE test(i1 INT, i2 INT)");
        sql("INSERT INTO test VALUES (1, 1), (2, 2)");

        assertQuery("SELECT (SELECT t1.i1 + t1.i2 + t0.i2 FROM test t1 WHERE i1 = 1) FROM test t0")
            .matches(QueryChecker.containsSubPlan("IgniteTableSpool"))
            .returns(3)
            .returns(4)
            .check();
    }

    @Test
    public void testJoinWithCorrelatedExpressionInFilter() throws Exception {
        sql("CREATE TABLE test(i INTEGER)");
        sql("INSERT INTO test VALUES (1), (2), (3), NULL");

        assertQuery("SELECT i, (SELECT s1.i FROM test s1, test s2 WHERE s1.i=s2.i AND s1.i=4-i1.i) AS j FROM test i1 ORDER BY i NULLS FIRST")
            .ordered()
            .returns(null, null)
            .returns(1, 3)
            .returns(2, 2)
            .returns(3, 1)
            .check();

        assertQuery("SELECT i, (SELECT s1.i FROM test s1 INNER JOIN test s2 ON s1.i=s2.i AND s1.i=4-i1.i) AS j FROM test i1 ORDER BY i NULLS FIRST;")
            .ordered()
            .returns(null, null)
            .returns(1, 3)
            .returns(2, 2)
            .returns(3, 1)
            .check();

        assertQuery("SELECT i, (SELECT i FROM test WHERE i IS NOT NULL EXCEPT SELECT i FROM test WHERE i<>i1.i) AS j FROM test i1 WHERE i IS NOT NULL ORDER BY i;")
            .ordered()
            .returns(1, 1)
            .returns(2, 2)
            .returns(3, 3)
            .check();

        assertQuery("SELECT (SELECT 4 EXCEPT SELECT test.i) FROM test")
            .returns(4)
            .returns(4)
            .returns(4)
            .returns(4)
            .check();
    }
}
