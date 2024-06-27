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

package org.apache.ignite.internal.processors.query.calcite.planner;

import org.apache.ignite.internal.processors.query.calcite.schema.IgniteSchema;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.junit.Test;

/**
 *
 */
public class UserDefinedViewsPlannerTest extends AbstractPlannerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testView() throws Exception {
        IgniteSchema schema = createSchema(
            createTable("T1", IgniteDistributions.single(), "C1", Integer.class, "C2", Integer.class, "C3", Integer.class),
            createTable("T2", IgniteDistributions.single(), "C1", Integer.class, "C2", Integer.class, "C3", Integer.class)
        );

        String viewSql = "SELECT T1.C1 AS C1_1, T1.C2 AS C1_2, T2.C1 AS C2_1, T2.C2 AS C2_2 FROM T1 JOIN T2 ON (T1.C3 = T2.C3)";

        schema.addView("V", viewSql);

        String sql = "select * from v where c1_1 = 1";

        assertPlan(sql, schema, hasChildThat(isTableScan("T1")).and(hasChildThat(isTableScan("T2"))));
    }
}
