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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for ignite SQL meta views.
 */
public class IgniteSqlMetaViewsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(Ignite ignite, String sql, Object ... args) {
        IgniteCache cache = ignite.getOrCreateCache("cache");

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
            @Override public Void call() throws Exception {
                execSql(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assertEquals(IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlE.statusCode());
    }
    
    /**
     * Test meta views modifications.
     */
    public void testModifications() throws Exception {
        startGrid();

        assertSqlError("DROP TABLE IGNITE.LOCAL_TRANSACTIONS");

        assertSqlError("TRUNCATE TABLE IGNITE.LOCAL_TRANSACTIONS");

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS RENAME TO IGNITE.TX");

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS ADD COLUMN C VARCHAR");

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS DROP COLUMN XID");

        assertSqlError("ALTER TABLE IGNITE.LOCAL_TRANSACTIONS RENAME COLUMN XID TO C");

        assertSqlError("CREATE INDEX IDX ON IGNITE.LOCAL_TRANSACTIONS(XID)");

        assertSqlError("INSERT INTO IGNITE.LOCAL_TRANSACTIONS (XID) VALUES ('-')");

        try (Transaction tx = grid().transactions().txStart()) {
            assertSqlError("UPDATE IGNITE.LOCAL_TRANSACTIONS SET XID = '-'");

            assertSqlError("DELETE IGNITE.LOCAL_TRANSACTIONS");

            tx.commit();
        }
    }

    /**
     * @param rowData Row data.
     * @param colTypes Column types.
     */
    private void assertColumnTypes(List<?> rowData, Class<?> ... colTypes) {
        for (int i = 0; i < colTypes.length; i++) {
            if (rowData.get(i) != null)
                assertEquals("Column " + i + " type", rowData.get(i).getClass(), colTypes[i]);
        }
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
     * Test nodes and node attributes meta views.
     *
     * @throws Exception If failed.
     */
    public void testNodesViews() throws Exception {
        Ignite ignite1 = startGrid(getTestIgniteInstanceName(), getConfiguration());
        Ignite ignite2 = startGrid(getTestIgniteInstanceName(1), getConfiguration().setClientMode(true));
        Ignite ignite3 = startGrid(getTestIgniteInstanceName(2), getConfiguration().setDaemon(true));

        awaitPartitionMapExchange();

        List<List<?>> resAll = execSql("SELECT ID, CONSISTENT_ID, VERSION, IS_LOCAL, IS_CLIENT, IS_DAEMON, " +
                "STARTUP_ORDER, ADDRESSES, HOST_NAMES FROM IGNITE.NODES");

        assertColumnTypes(resAll.get(0), UUID.class, String.class, String.class, Boolean.class, Boolean.class,
            Boolean.class, Integer.class, String.class, String.class);

        assertEquals(3, resAll.size());

        List<List<?>> resSrv = execSql(
                "SELECT ID, IS_LOCAL, STARTUP_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = FALSE AND IS_DAEMON = FALSE"
        );

        assertEquals(1, resSrv.size());

        assertEquals(ignite1.cluster().localNode().id(), resSrv.get(0).get(0));

        assertEquals(true, resSrv.get(0).get(1));

        assertEquals(1, resSrv.get(0).get(2));

        List<List<?>> resCli = execSql(
            "SELECT ID, IS_LOCAL, STARTUP_ORDER FROM IGNITE.NODES WHERE IS_CLIENT = TRUE");

        assertEquals(1, resCli.size());

        assertEquals(ignite2.cluster().localNode().id(), resCli.get(0).get(0));

        assertEquals(false, resCli.get(0).get(1));

        assertEquals(2, resCli.get(0).get(2));

        List<List<?>> resDaemon = execSql(
            "SELECT ID, IS_LOCAL, STARTUP_ORDER FROM IGNITE.NODES WHERE IS_DAEMON = TRUE");

        assertEquals(1, resDaemon.size());

        assertEquals(ignite3.cluster().localNode().id(), resDaemon.get(0).get(0));

        assertEquals(false, resDaemon.get(0).get(1));

        assertEquals(3, resDaemon.get(0).get(2));

        // Check index on ID column
        assertEquals(0, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = '-'").size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            ignite1.cluster().localNode().id()).size());

        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE ID = ?",
            ignite3.cluster().localNode().id()).size());

        // Check index on IS_LOCAL column
        assertEquals(1, execSql("SELECT ID FROM IGNITE.NODES WHERE IS_LOCAL = true").size());

        // Check quick-count
        assertEquals(3L, execSql("SELECT COUNT(*) FROM IGNITE.NODES").get(0).get(0));

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
    }

    /**
     * Test tasks meta view.
     *
     * @throws Exception If failed.
     */
    public void testTasksView() throws Exception {
        startGrid();

        ComputeTask<Void, Void> task = new ComputeTask<Void, Void>() {
            @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                @Nullable Void arg) throws IgniteException {
                Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

                for (ClusterNode node : subgrid)
                    jobs.put(new ComputeJobAdapter() {
                        @Override public void cancel() {
                            super.cancel();

                            log.info("Job canceled");
                        }

                        @Override public Object execute() throws IgniteException {
                            doSleep(300_000L);

                            return null;
                        }
                    }, node);

                return jobs;
            }

            @Override public ComputeJobResultPolicy result(ComputeJobResult res,
                List<ComputeJobResult> rcvd) throws IgniteException {
                return ComputeJobResultPolicy.WAIT;
            }

            @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
                return null;
            }
        };

        grid().compute().executeAsync(task, null);

        assertEquals(1,
            execSql("SELECT ID FROM IGNITE.LOCAL_TASKS").size());

        doSleep(300_000);
    }
}
