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

package org.apache.ignite.internal.processors.query.calcite;

import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableSet;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test SQL data types.
 */
public class DataTypesTest extends GridCommonAbstractTest {
    /** */
    private static QueryEngine qryEngine;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx grid = startGrid();

        qryEngine = Commons.lookupComponent(grid.context(), QueryEngine.class);
    }

    /** */
    @Test
    public void testUnicodeStrings() {
        grid().getOrCreateCache(new CacheConfiguration<Integer, String>()
            .setName("string_cache")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, String.class).setTableName("string_table")))
        );

        String[] values = new String[] {"Кирилл", "Müller", "我是谁", "ASCII"};

        int key = 0;

        // Insert as inlined values.
        for (String val : values)
            executeSql("INSERT INTO string_table (_key, _val) VALUES (" + key++ + ", '" + val + "')");

        List<List<?>> rows = executeSql("SELECT _val FROM string_table");

        assertEquals(ImmutableSet.copyOf(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        executeSql("DELETE FROM string_table");

        // Insert as parameters.
        for (String val : values)
            executeSql("INSERT INTO string_table (_key, _val) VALUES (?, ?)", key++, val);

        rows = executeSql("SELECT _val FROM string_table");

        assertEquals(ImmutableSet.copyOf(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        rows = executeSql("SELECT substring(_val, 1, 2) FROM string_table");

        assertEquals(ImmutableSet.of("Ки", "Mü", "我是", "AS"),
            rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        for (String val : values) {
            rows = executeSql("SELECT char_length(_val) FROM string_table WHERE _val = ?", val);

            assertEquals(1, rows.size());
            assertEquals(val.length(), rows.get(0).get(0));
        }
    }

    /**
     * Test intraval data types.
     */
    @Test
    public void testIntervals() {
        executeSql("CREATE TABLE test(k INT PRIMARY KEY, v INT)");
        executeSql("INSERT INTO test(k, v) VALUES (1, 2)");
        System.out.println(">>>> " + executeSql("SELECT typeof(k) FROM test").get(0).get(0));
/*
        executeSql("CREATE TABLE test(k INTERVAL PRIMARY KEY, v INTERVAL)");
        executeSql("INSERT INTO test(k, v) VALUES (INTERVAL 10 MONTHS, INTERVAL 10 HOURS)");

        Duration.ofSeconds(10);
        Period.ofMonths(10);

*/
        assertEquals(Duration.ofSeconds(1), eval("INTERVAL 1 SECONDS"));
        assertEquals(Duration.ofMinutes(2), eval("INTERVAL 2 MINUTES"));
        assertEquals(Duration.ofHours(3), eval("INTERVAL 3 HOURS"));
        assertEquals(Duration.ofDays(4), eval("INTERVAL 4 DAYS"));
        assertEquals(Period.ofMonths(5), eval("INTERVAL 5 MONTHS"));
        assertEquals(Period.ofYears(6), eval("INTERVAL 6 YEARS"));

/*
        assertEquals(10L, eval("INTERVAL 10 SECONDS"));
        assertEquals(10L, eval("INTERVAL 10 MINUTES"));
        assertEquals(10L, eval("INTERVAL 10 HOURS"));
        assertEquals(10L, eval("INTERVAL 10 DAYS"));
        assertEquals(10L, eval("INTERVAL 10 MONTHS"));
        assertEquals(10L, eval("INTERVAL 10 YEARS"));
        assertEquals(14L, eval("INTERVAL '1-2' YEAR TO MONTH"));
        assertEquals(26L, eval("INTERVAL '1 2' DAY TO HOUR"));
        assertEquals(26, eval("CAST((INTERVAL '26' DAYS) AS INT)"));
*/
        //assertEquals(26, eval("CAST((INTERVAL '1 2' DAY TO HOUR) AS INT)"));
    }

    /** */
    public Object eval(String exp) {
        return executeSql("SELECT " + exp).get(0).get(0);
    }

    /** */
    public List<List<?>> executeSql(String sql, Object... params) {
        return qryEngine.query(null, "PUBLIC", sql, params).get(0).getAll();
    }
}
