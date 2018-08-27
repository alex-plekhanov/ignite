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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

/**
 * Tests for ignite SQL system views.
 */
public class SqlSystemViewsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object ... args) {
        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }

    /**
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(String sql, Object ... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param sql Sql.
     */
    private void assertSqlError(final String sql) {
        Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() {
                execSql(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assert sqlE != null;

        assertEquals(IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlE.statusCode());
    }
    
    /**
     * Test system views modifications.
     */
    public void testModifications() throws Exception {
        startGrid();

        assertSqlError("DROP TABLE IGNITE.NODES");

        assertSqlError("TRUNCATE TABLE IGNITE.NODES");

        assertSqlError("ALTER TABLE IGNITE.NODES RENAME TO IGNITE.N");

        assertSqlError("ALTER TABLE IGNITE.NODES ADD COLUMN C VARCHAR");

        assertSqlError("ALTER TABLE IGNITE.NODES DROP COLUMN ID");

        assertSqlError("ALTER TABLE IGNITE.NODES RENAME COLUMN ID TO C");

        assertSqlError("CREATE INDEX IDX ON IGNITE.NODES(ID)");

        assertSqlError("INSERT INTO IGNITE.NODES (ID) VALUES ('-')");

        assertSqlError("UPDATE IGNITE.NODES SET ID = '-'");

        assertSqlError("DELETE IGNITE.NODES");
    }

    /**
     * Test different query modes.
     */
    public void testQueryModes() throws Exception {
        Ignite ignite = startGrid(0);
        startGrid(1);

        UUID nodeId = ignite.cluster().localNode().id();

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        String sql = "SELECT ID FROM IGNITE.NODES WHERE IS_LOCAL = true";

        SqlFieldsQuery qry;

        qry = new SqlFieldsQuery(sql).setDistributedJoins(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setReplicatedOnly(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setLocal(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));
    }

    /**
     * Test that we can't use cache tables and system views in the same query.
     */
    public void testCacheToViewJoin() throws Exception {
        Ignite ignite = startGrid();

        ignite.createCache(new CacheConfiguration<>().setName(DEFAULT_CACHE_NAME).setQueryEntities(
            Collections.singleton(new QueryEntity(Integer.class.getName(), String.class.getName()))));

        assertSqlError("SELECT * FROM \"" + DEFAULT_CACHE_NAME + "\".String JOIN IGNITE.NODES ON 1=1");
    }

    /**
     * @param rowData Row data.
     * @param colTypes Column types.
     */
    private void assertColumnTypes(List<?> rowData, Class<?> ... colTypes) {
        for (int i = 0; i < colTypes.length; i++) {
            if (rowData.get(i) != null)
                assertEquals("Column " + i + " type", colTypes[i], rowData.get(i).getClass());
        }
    }

    /**
     * Test nodes system view.
     *
     * @throws Exception If failed.
     */
    public void testNodesViews() throws Exception {
        Ignite ignite1 = startGrid(getTestIgniteInstanceName(), getConfiguration());
        Ignite ignite2 = startGrid(getTestIgniteInstanceName(1), getConfiguration().setClientMode(true));
        Ignite ignite3 = startGrid(getTestIgniteInstanceName(2), getConfiguration().setDaemon(true));

        awaitPartitionMapExchange();

        List<List<?>> resAll = execSql("SELECT ID, CONSISTENT_ID, VERSION, IS_LOCAL, IS_CLIENT, IS_DAEMON, " +
                "NODE_ORDER, ADDRESSES, HOSTNAMES FROM IGNITE.NODES");

        assertColumnTypes(resAll.get(0), UUID.class, String.class, String.class, Boolean.class, Boolean.class,
            Boolean.class, Integer.class, String.class, String.class);

        assertEquals(3, resAll.size());

        List<List<?>> resSrv = execSql(
            "SELECT ID, IS_LOCAL, NODE_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = FALSE AND IS_DAEMON = FALSE"
        );

        assertEquals(1, resSrv.size());

        assertEquals(ignite1.cluster().localNode().id(), resSrv.get(0).get(0));

        assertEquals(true, resSrv.get(0).get(1));

        assertEquals(1, resSrv.get(0).get(2));

        List<List<?>> resCli = execSql(
            "SELECT ID, IS_LOCAL, NODE_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = TRUE");

        assertEquals(1, resCli.size());

        assertEquals(ignite2.cluster().localNode().id(), resCli.get(0).get(0));

        assertEquals(false, resCli.get(0).get(1));

        assertEquals(2, resCli.get(0).get(2));

        List<List<?>> resDaemon = execSql(
            "SELECT ID, IS_LOCAL, NODE_ORDER FROM IGNITE.NODES WHERE IS_DAEMON = TRUE");

        assertEquals(1, resDaemon.size());

        assertEquals(ignite3.cluster().localNode().id(), resDaemon.get(0).get(0));

        assertEquals(false, resDaemon.get(0).get(1));

        assertEquals(3, resDaemon.get(0).get(2));

        // Check index on ID column.
        assertEquals(0, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = '-'").size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            ignite1.cluster().localNode().id()).size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            ignite3.cluster().localNode().id()).size());

        // Check index on IS_LOCAL column.
        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE IS_LOCAL = true").size());

        // Check index on IS_LOCAL column with disjunction.
        assertEquals(3, execSql("SELECT ID FROM IGNITE.NODES WHERE IS_LOCAL = true OR node_order=1 OR node_order=2 OR node_order=3").size());

        // Check quick-count.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM IGNITE.NODES").get(0).get(0));

        // Check joins
        assertEquals(ignite1.cluster().localNode().id(), execSql("SELECT N1.ID FROM IGNITE.NODES N1 JOIN " +
            "IGNITE.NODES N2 ON N1.IS_LOCAL = N2.IS_LOCAL JOIN IGNITE.NODES N3 ON N2.ID = N3.ID WHERE N3.IS_LOCAL = true")
            .get(0).get(0));

        // Check sub-query
        assertEquals(ignite1.cluster().localNode().id(), execSql("SELECT N1.ID FROM IGNITE.NODES N1 " +
            "WHERE NOT EXISTS (SELECT 1 FROM IGNITE.NODES N2 WHERE N2.ID = N1.ID AND N2.IS_LOCAL = false)")
            .get(0).get(0));

        // Check node attributes view
        UUID cliNodeId = ignite2.cluster().localNode().id();

        String cliAttrName = IgniteNodeAttributes.ATTR_CLIENT_MODE;

        assertColumnTypes(execSql("SELECT NODE_ID, NAME, VALUE FROM IGNITE.NODE_ATTRIBUTES").get(0),
            UUID.class, String.class, String.class);

        assertEquals(1,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NAME = ? AND VALUE = 'true'",
                cliAttrName).size());

        assertEquals(3,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NAME = ?", cliAttrName).size());

        assertEquals(1,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = ? AND VALUE = 'true'",
                cliNodeId, cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = '-' AND NAME = ?",
                cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM IGNITE.NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = '-'",
                cliNodeId).size());

        // Check node metrics view.
        List<List<?>> resMetrics = execSql("SELECT NODE_ID, LAST_UPDATE_TIME, " +
            "MAX_ACTIVE_JOBS, CUR_ACTIVE_JOBS, AVG_ACTIVE_JOBS, " +
            "MAX_WAITING_JOBS, CUR_WAITING_JOBS, AVG_WAITING_JOBS, " +
            "MAX_REJECTED_JOBS, CUR_REJECTED_JOBS, AVG_REJECTED_JOBS, TOTAL_REJECTED_JOBS, " +
            "MAX_CANCELED_JOBS, CUR_CANCELED_JOBS, AVG_CANCELED_JOBS, TOTAL_CANCELED_JOBS, " +
            "MAX_JOBS_WAIT_TIME, CUR_JOBS_WAIT_TIME, AVG_JOBS_WAIT_TIME, " +
            "MAX_JOBS_EXECUTE_TIME, CUR_JOBS_EXECUTE_TIME, AVG_JOBS_EXECUTE_TIME, TOTAL_JOBS_EXECUTE_TIME, " +
            "TOTAL_EXECUTED_JOBS, TOTAL_EXECUTED_TASKS, " +
            "TOTAL_BUSY_TIME, TOTAL_IDLE_TIME, CUR_IDLE_TIME, BUSY_TIME_PERCENTAGE, IDLE_TIME_PERCENTAGE, " +
            "TOTAL_CPU, CUR_CPU_LOAD, AVG_CPU_LOAD, CUR_GC_CPU_LOAD, " +
            "HEAP_MEMORY_INIT, HEAP_MEMORY_USED, HEAP_MEMORY_COMMITED, HEAP_MEMORY_MAX, HEAP_MEMORY_TOTAL, " +
            "NONHEAP_MEMORY_INIT, NONHEAP_MEMORY_USED, NONHEAP_MEMORY_COMMITED, NONHEAP_MEMORY_MAX, NONHEAP_MEMORY_TOTAL, " +
            "UPTIME, JVM_START_TIME, NODE_START_TIME, LAST_DATA_VERSION, " +
            "CUR_THREAD_COUNT, MAX_THREAD_COUNT, TOTAL_THREAD_COUNT, CUR_DAEMON_THREAD_COUNT, " +
            "SENT_MESSAGES_COUNT, SENT_BYTES_COUNT, RECEIVED_MESSAGES_COUNT, RECEIVED_BYTES_COUNT, " +
            "OUTBOUND_MESSAGES_QUEUE FROM IGNITE.NODE_METRICS");

        assertColumnTypes(resMetrics.get(0), UUID.class, Timestamp.class,
            Integer.class, Integer.class, Float.class, // Active jobs.
            Integer.class, Integer.class, Float.class, // Waiting jobs.
            Integer.class, Integer.class, Float.class, Integer.class, // Rejected jobs.
            Integer.class, Integer.class, Float.class, Integer.class, // Canceled jobs.
            Time.class, Time.class, Time.class, // Jobs wait time.
            Time.class, Time.class, Time.class, Time.class, // Jobs execute time.
            Integer.class, Integer.class, // Executed jobs/task.
            Time.class, Time.class, Time.class, Float.class, Float.class, // Busy/idle time.
            Integer.class, Double.class, Double.class, Double.class, // CPU.
            Long.class, Long.class, Long.class, Long.class, Long.class, // Heap memory.
            Long.class, Long.class, Long.class, Long.class, Long.class, // Nonheap memory.
            Time.class, Timestamp.class, Timestamp.class, Long.class, // Uptime.
            Integer.class, Integer.class, Long.class, Integer.class, // Threads.
            Integer.class, Long.class, Integer.class, Long.class, // Sent/received messages.
            Integer.class); // Outbound message queue.

        assertEquals(3, resAll.size());

        // Check join with nodes.
        assertEquals(3, execSql("SELECT NM.LAST_UPDATE_TIME FROM IGNITE.NODES N " +
            "JOIN IGNITE.NODE_METRICS NM ON N.ID = NM.NODE_ID").size());

        // Check index on NODE_ID column.
        assertEquals(1, execSql("SELECT LAST_UPDATE_TIME FROM IGNITE.NODE_METRICS WHERE NODE_ID = ?",
            cliNodeId).size());

        // Check malformed value for indexed column.
        assertEquals(0, execSql("SELECT LAST_UPDATE_TIME FROM IGNITE.NODE_METRICS WHERE NODE_ID = ?",
            "-").size());

        // Check quick-count.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM IGNITE.NODE_METRICS").get(0).get(0));
    }

    /**
     * Test baseline topology system view.
     */
    public void testBaselineViews() throws Exception {
        cleanPersistenceDir();

        Ignite ignite = startGrid(getTestIgniteInstanceName(), getPdsConfiguration("node0"));
        startGrid(getTestIgniteInstanceName(1), getPdsConfiguration("node1"));

        ignite.cluster().active(true);

        List<List<?>> res = execSql("SELECT CONSISTENT_ID, ONLINE FROM IGNITE.BASELINE_NODES ORDER BY CONSISTENT_ID");

        assertColumnTypes(res.get(0), String.class, Boolean.class);

        assertEquals(2, res.size());

        assertEquals("node0", res.get(0).get(0));
        assertEquals("node1", res.get(1).get(0));

        assertEquals(true, res.get(0).get(1));
        assertEquals(true, res.get(1).get(1));

        stopGrid(getTestIgniteInstanceName(1));

        res = execSql("SELECT CONSISTENT_ID FROM IGNITE.BASELINE_NODES WHERE ONLINE = false");

        assertEquals(1, res.size());

        assertEquals("node1", res.get(0).get(0));

        Ignite ignite2 = startGrid(getTestIgniteInstanceName(2), getPdsConfiguration("node2"));

        assertEquals(2, execSql(ignite2, "SELECT CONSISTENT_ID FROM IGNITE.BASELINE_NODES").size());

        res = execSql("SELECT CONSISTENT_ID FROM IGNITE.NODES N WHERE NOT EXISTS (SELECT 1 FROM " +
            "IGNITE.BASELINE_NODES B WHERE B.CONSISTENT_ID = N.CONSISTENT_ID)");

        assertEquals(1, res.size());

        assertEquals("node2", res.get(0).get(0));
    }

    /**
     * Test local transactions meta view.
     *
     * @throws Exception If failed.
     */
    public void testLocalTransactionsView() throws Exception {
        int txCnt = 3;

        final Ignite ignite = startGrid();

        IgniteCache cache = ignite.getOrCreateCache("cache");

        final CountDownLatch latchTxStart = new CountDownLatch(txCnt);

        final CountDownLatch latchTxBody = new CountDownLatch(1);

        final CountDownLatch latchTxEnd = new CountDownLatch(txCnt);

        multithreadedAsync(new Runnable() {
            @Override public void run() {
                try(Transaction tx = ignite.transactions().txStart()) {
                    tx.timeout(1_000_000L);

                    latchTxStart.countDown();

                    latchTxBody.await();

                    tx.commit();
                }
                catch (InterruptedException e) {
                    fail("Thread interrupted");
                }

                latchTxEnd.countDown();
            }
        }, txCnt);

        latchTxStart.await();

        List<List<?>> res = cache.query(
            new SqlFieldsQuery("SELECT XID, START_NODE_ID, START_TIME, TIMEOUT, TIMEOUT_MILLIS, IS_TIMED_OUT, " +
                "START_THREAD_ID, ISOLATION, CONCURRENCY, IMPLICIT, IS_INVALIDATE, STATE, SIZE, " +
                "STORE_ENABLED, STORE_WRITE_THROUGH, IO_POLICY, IMPLICIT_SINGLE, IS_EMPTY, OTHER_NODE_ID, " +
                "EVENT_NODE_ID, ORIGINATING_NODE_ID, IS_NEAR, IS_DHT, IS_COLOCATED, IS_LOCAL, IS_SYSTEM, " +
                "IS_USER, SUBJECT_ID, IS_DONE, IS_INTERNAL, IS_ONE_PHASE_COMMIT " +
                "FROM IGNITE.LOCAL_TRANSACTIONS"
            )
        ).getAll();

        assertColumnTypes(res.get(0), String.class, UUID.class, Timestamp.class, Time.class, Long.class, Boolean.class,
            Long.class, String.class, String.class, Boolean.class, Boolean.class, String.class, Integer.class,
            Boolean.class, Boolean.class, Byte.class, Boolean.class, Boolean.class, UUID.class,
            UUID.class, UUID.class, Boolean.class, Boolean.class, Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, UUID.class, Boolean.class, Boolean.class, Boolean.class
        );

        // Assert values.
        assertEquals(ignite.cluster().localNode().id(), res.get(0).get(1));

        assertTrue(U.currentTimeMillis() > ((Timestamp)res.get(0).get(2)).getTime());

        assertEquals("00:16:40" /* 1_000_000 ms */, res.get(0).get(3).toString());

        assertEquals(1_000_000L, res.get(0).get(4));

        assertEquals(false, res.get(0).get(5));

        // Assert row count.
        assertEquals(txCnt, res.size());

        latchTxBody.countDown();

        latchTxEnd.await();

        assertEquals(0, cache.query(
            new SqlFieldsQuery("SELECT XID FROM LOCAL_TRANSACTIONS").setSchema("IGNITE")
        ).getAll().size());
    }

    /**
     * Gets ignite configuration with persistence enabled.
     */
    private IgniteConfiguration getPdsConfiguration(String consistentId) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024L * 1024L).setPersistenceEnabled(true))
        );

        cfg.setConsistentId(consistentId);

        return cfg;
    }
}
