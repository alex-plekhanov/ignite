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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.cleanPerformanceStatisticsDir;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.startCollectStatistics;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.stopCollectStatisticsAndRead;
import static org.apache.ignite.internal.processors.query.QueryParserMetricsHolder.QUERY_PARSER_METRIC_GROUP_NAME;
import static org.apache.ignite.internal.processors.query.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

/**
 * Test SQL diagnostic tools.
 */
public class SqlDiagnosticIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String jdbcUrl = "jdbc:ignite:thin://127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()))
            .setIncludeEventTypes(EventType.EVT_SQL_QUERY_EXECUTION);
    }

    /** */
    @Override protected int nodeCount() {
        return 2;
    }

    /** */
    @Test
    public void testParserMetrics() {
        MetricRegistry mreg0 = grid(0).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        MetricRegistry mreg1 = grid(1).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        mreg0.reset();
        mreg1.reset();

        LongMetric hits0 = mreg0.findMetric("hits");
        LongMetric hits1 = mreg1.findMetric("hits");
        LongMetric misses0 = mreg0.findMetric("misses");
        LongMetric misses1 = mreg1.findMetric("misses");

        // Parse and plan on client.
        sql("CREATE TABLE test_parse(a INT)");

        assertEquals(0, hits0.value());
        assertEquals(0, hits1.value());
        assertEquals(0, misses0.value());
        assertEquals(0, misses1.value());

        for (int i = 0; i < 10; i++)
            sql(grid(0), "INSERT INTO test_parse VALUES (?)", i);

        assertEquals(9, hits0.value());
        assertEquals(0, hits1.value());
        assertEquals(1, misses0.value());
        assertEquals(0, misses1.value());

        for (int i = 0; i < 10; i++)
            sql(grid(1), "SELECT * FROM test_parse WHERE a = ?", i);

        assertEquals(9, hits0.value());
        assertEquals(9, hits1.value());
        assertEquals(1, misses0.value());
        assertEquals(1, misses1.value());
    }

    /** */
    @Test
    public void testBatchParserMetrics() throws Exception {
        MetricRegistry mreg0 = grid(0).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        MetricRegistry mreg1 = grid(1).context().metric().registry(QUERY_PARSER_METRIC_GROUP_NAME);
        mreg0.reset();
        mreg1.reset();

        LongMetric hits0 = mreg0.findMetric("hits");
        LongMetric hits1 = mreg1.findMetric("hits");
        LongMetric misses0 = mreg0.findMetric("misses");
        LongMetric misses1 = mreg1.findMetric("misses");

        sql("CREATE TABLE test_batch(a INT)");

        assertEquals(0, hits0.value());
        assertEquals(0, hits1.value());
        assertEquals(0, misses0.value());
        assertEquals(0, misses1.value());

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setSchema("PUBLIC");

            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < 10; i++)
                    stmt.addBatch(String.format("INSERT INTO test_batch VALUES (%d)", i));

                stmt.executeBatch();

                assertEquals(0, hits0.value());
                assertEquals(0, hits1.value());
                assertEquals(10, misses0.value());
                assertEquals(0, misses1.value());
            }

            String sql = "INSERT INTO test_batch VALUES (?)";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 10; i < 20; i++) {
                    stmt.setInt(1, i);
                    stmt.addBatch();
                }

                stmt.executeBatch();

                assertEquals(0, hits0.value());
                assertEquals(0, hits1.value());
                assertEquals(11, misses0.value()); // Only one increment per batch.
                assertEquals(0, misses1.value());
            }

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 20; i < 30; i++) {
                    stmt.setInt(1, i);
                    stmt.addBatch();
                }

                stmt.executeBatch();

                assertEquals(1, hits0.value()); // Only one increment per batch.
                assertEquals(0, hits1.value());
                assertEquals(11, misses0.value());
                assertEquals(0, misses1.value());
            }
        }
    }

    /** */
    @Test
    public void testCancel() {
        MetricRegistry mreg0 = grid(0).context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        MetricRegistry mreg1 = grid(1).context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg0.reset();
        mreg1.reset();

        FieldsQueryCursor<?> cur = grid(0).getOrCreateCache("test_cancel")
            .query(new SqlFieldsQuery("SELECT * FROM table(system_range(1, 10000))"));

        assertTrue(cur.iterator().hasNext());

        cur.close();

        System.out.println("test");
    }

    /** */
    @Test
    public void testUserQueriesMetrics() throws Exception {
        sql(grid(0), "CREATE TABLE test_metric (a INT)");

        MetricRegistry mreg0 = grid(0).context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        MetricRegistry mreg1 = grid(1).context().metric().registry(SQL_USER_QUERIES_REG_NAME);
        mreg0.reset();
        mreg1.reset();

        AtomicInteger qryCnt = new AtomicInteger();
        grid(0).context().query().runningQueryManager().registerQueryFinishedListener(q -> qryCnt.incrementAndGet());

        sql(grid(0), "INSERT INTO test_metric VALUES (?)", 0);
        sql(grid(0), "SELECT * FROM test_metric WHERE a = ?", 0);

        try {
            sql(grid(0), "SELECT * FROM test_fail");

            fail();
        }
        catch (IgniteSQLException ignored) {
            // Expected.
        }

        FieldsQueryCursor<?> cur = grid(0).getOrCreateCache("test_metric")
            .query(new SqlFieldsQuery("SELECT * FROM table(system_range(1, 10000))"));

        assertTrue(cur.iterator().hasNext());

        cur.close();

        // Query unregistering is async process, wait for it before metrics check.
        assertTrue(GridTestUtils.waitForCondition(() -> qryCnt.get() == 4, 1_000L));

        assertEquals(2, ((LongMetric)mreg0.findMetric("success")).value());
        assertEquals(2, ((LongMetric)mreg0.findMetric("failed")).value()); // 1 error + 1 cancelled.
        assertEquals(1, ((LongMetric)mreg0.findMetric("canceled")).value());

        assertEquals(0, ((LongMetric)mreg1.findMetric("success")).value());
        assertEquals(0, ((LongMetric)mreg1.findMetric("failed")).value());
        assertEquals(0, ((LongMetric)mreg1.findMetric("canceled")).value());
    }

    // TODO check long running queries

    /** */
    @Test
    public void testPerformanceStatistics() throws Exception {
        cleanPerformanceStatisticsDir();
        startCollectStatistics();

        long startTime = U.currentTimeMillis();

        sql(grid(0), "CREATE TABLE test_perf_stat (a INT)");
        sql(grid(0), "SELECT * FROM table(system_range(1, 1000))");
        sql(grid(0), "SELECT * FROM test_perf_stat");

        AtomicInteger qryCnt = new AtomicInteger();
        AtomicInteger readsCnt = new AtomicInteger();
        Iterator<String> sqlIt = F.asList("CREATE", "INSERT", "SELECT").iterator();

        stopCollectStatisticsAndRead(new AbstractPerformanceStatisticsTest.TestHandler() {
            @Override public void query(
                UUID nodeId,
                GridCacheQueryType type,
                String text,
                long id,
                long qryStartTime,
                long duration,
                boolean success
            ) {
                qryCnt.incrementAndGet();

                assertTrue(nodeId.equals(grid(0).localNode().id()));
                assertEquals(SQL_FIELDS, type);
                assertTrue(text.startsWith(sqlIt.next()));
                assertTrue(qryStartTime >= startTime);
                assertTrue(duration >= 0);
                assertTrue(success);
            }

            @Override public void queryReads(
                UUID nodeId,
                GridCacheQueryType type,
                UUID queryNodeId,
                long id,
                long logicalReads,
                long physicalReads
            ) {
                readsCnt.incrementAndGet();
/*
                qryIds.add(id);
                readsNodes.remove(nodeId);

                assertTrue(expNodeIds.contains(queryNodeId));
                assertEquals(expType, type);
                assertTrue(logicalReads > 0);
                assertTrue(hasPhysicalReads ? physicalReads > 0 : physicalReads == 0);
*/
            }
        });

        assertEquals(3, qryCnt.get());
        //assertTrue("Query reads expected on nodes: " + readsNodes, readsNodes.isEmpty());
    }

    /** */
    @Test
    public void testSqlEvents() {
        // TODO cache query start event
        // TODO cache query entry event
        sql("CREATE TABLE test_event (a INT)");

        AtomicIntegerArray evts = new AtomicIntegerArray(2);

        grid(0).events().localListen(e -> evts.incrementAndGet(0) > 0, EventType.EVT_SQL_QUERY_EXECUTION);
        grid(1).events().localListen(e -> evts.incrementAndGet(1) > 0, EventType.EVT_SQL_QUERY_EXECUTION);

        sql(grid(0), "INSERT INTO test_event VALUES (?)", 0);
        sql(grid(0), "SELECT * FROM test_event WHERE a = ?", 0);

        assertEquals(2, evts.get(0));
        assertEquals(0, evts.get(1));
    }
}
