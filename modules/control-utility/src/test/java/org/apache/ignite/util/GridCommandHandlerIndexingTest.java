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

package org.apache.ignite.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.GRID_NOT_IDLE_MSG;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.CACHE_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.GROUP_NAME;
import static org.apache.ignite.util.GridCommandHandlerIndexingUtils.createAndFillCache;

/**
 * If you not necessary create nodes for each test you can try use
 * {@link GridCommandHandlerIndexingClusterByClassTest}.
 */
public class GridCommandHandlerIndexingTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    public static final int GRID_CNT = 2;

    /** */
    @Test
    public void testValidateIndexesFailedOnNotIdleCluster() throws Exception {
        checkpointFreq = 100L;

        Ignite ignite = prepareGridForTest();

        AtomicBoolean stopFlag = new AtomicBoolean();

        IgniteCache<Integer, GridCommandHandlerIndexingUtils.Person> cache = ignite.cache(CACHE_NAME);

        Thread loadThread = new Thread(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stopFlag.get()) {
                int id = rnd.nextInt();

                cache.put(id, new GridCommandHandlerIndexingUtils.Person(id, "name" + id));

                if (Thread.interrupted())
                    break;
            }
        });

        try {
            loadThread.start();

            doSleep(checkpointFreq);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", CACHE_NAME));
        }
        finally {
            stopFlag.set(true);

            loadThread.join();
        }

        String out = testOut.toString();

        assertContains(log, out, GRID_NOT_IDLE_MSG + "[\"" + GROUP_NAME + "\"]");
    }

    /** Check that metadata remove command also removes class mapping. */
    @Test
    public void testRemoveMetadataAndRecreateWithDifferentCase() throws Exception {
        IgniteEx ig = startGrids(2);

        ig.cluster().active(true);

        IgniteCache<?, ?> cache = grid(0).getOrCreateCache(CACHE_NAME);

        cache.query(new SqlFieldsQuery("CREATE TABLE t1(id INT PRIMARY KEY, str VARCHAR) " +
                "WITH \"cache_name=" + DEFAULT_CACHE_NAME + ", key_type=CUSTOM_SQL_KEY_TYPE, value_type=CUSTOM_SQL_VALUE_TYPE\"")
                .setSchema("PUBLIC")).getAll();

        cache.query(new SqlFieldsQuery("INSERT INTO PUBLIC.t1 VALUES(1, '1')")).getAll();

        cache.query(new SqlFieldsQuery("DROP TABLE PUBLIC.t1")).getAll();

        assertEquals(EXIT_CODE_OK, execute("--meta", "remove", "--typeName", "CUSTOM_SQL_KEY_TYPE"));
        assertEquals(EXIT_CODE_OK, execute("--meta", "remove", "--typeName", "CUSTOM_SQL_VALUE_TYPE"));

        cache.query(new SqlFieldsQuery("CREATE TABLE t1(id INT PRIMARY KEY, str VARCHAR) " +
                "WITH \"cache_name=" + DEFAULT_CACHE_NAME + ", key_type=CUSTOM_SQL_KEY_TYPE, value_type=CUSTOM_SQL_VALUE_type\"")
                .setSchema("PUBLIC")).getAll();

        for (int i = 0; i < 10; ++i) {
            cache.query(new SqlFieldsQuery("INSERT INTO PUBLIC.t1 VALUES(" + i + ", '1')")).getAll();
        }

        List<List<?>> res = cache.query(new SqlFieldsQuery("SELECT * FROM PUBLIC.t1")).getAll();

        assertEquals(10, res.size());
    }


    /** Run index validation check on busy cluster. */
    @Test
    public void testIdleVerifyCheckFailsOnNotIdleClusterWithOverwriteWithPers() throws Exception {
        runIdleVerifyCheckCrcFailsOnNotIdleCluster(true);
    }

    /** Run index validation check on busy cluster. */
    @Test
    public void testIdleVerifyCheckFailsOnNotIdleClusterWithOverwriteWithoutPers() throws Exception {
        persistenceEnable(false);

        runIdleVerifyCheckCrcFailsOnNotIdleCluster(true);
    }

    /** Run index validation check on busy cluster. */
    @Test
    public void testIdleVerifyCheckFailsOnNotIdleClusterWithoutOverwriteWithPers() throws Exception {
        runIdleVerifyCheckCrcFailsOnNotIdleCluster(false);
    }

    /** Run index validation check on busy cluster. */
    @Test
    public void testIdleVerifyCheckFailsOnNotIdleClusterWithoutOverwriteWithoutPers() throws Exception {
        persistenceEnable(false);

        runIdleVerifyCheckCrcFailsOnNotIdleCluster(false);
    }

    /**
     * Check idle on busy cluster.
     *
     * @param allowOverwrite Overwrite param for datastreamer.
     * @throws Exception
     */
    public void runIdleVerifyCheckCrcFailsOnNotIdleCluster(boolean allowOverwrite) throws Exception {
        IgniteEx ig = startGrids(2);

        ig.cluster().state(ClusterState.ACTIVE);

        int cntPreload = 100;

        int maxItems = 100000;

        createCacheAndPreload(ig, DEFAULT_CACHE_NAME, cntPreload, 1, new CachePredicate(F.asList(ig.name())));

        if (persistenceEnable()) {
            forceCheckpoint();

            enableCheckpoints(G.allGrids(), false);
        }

        AtomicBoolean stopFlag = new AtomicBoolean();

        CountDownLatch startLoading = new CountDownLatch(1);

        IgniteInternalFuture f = runAsync(() -> {
            try (IgniteDataStreamer<Object, Object> ldr = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
                ldr.allowOverwrite(allowOverwrite);

                ldr.perThreadBufferSize(1);

                boolean addFlag = true;

                int i = cntPreload;

                while (!stopFlag.get()) {
                    if (addFlag)
                        ldr.addData(i, i);
                    else
                        ldr.removeData(i);

                    if (i == maxItems / 2)
                        startLoading.countDown();

                    if (i % 10 == 0)
                        ldr.flush();

                    if (++i == maxItems) {
                        addFlag = !addFlag;

                        i = 0;
                    }
                }
            }
        });

        injectTestSystemOut();

        startLoading.await();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", "--check-sizes"));

        stopFlag.set(true);

        f.get();

        String out = testOut.toString();

        assertContains(log, out, GRID_NOT_IDLE_MSG);

        testOut.reset();

        if (persistenceEnable())
            enableCheckpoints(G.allGrids(), true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", "--check-sizes"));

        out = testOut.toString();

        assertNotContains(log, out, GRID_NOT_IDLE_MSG);
    }

    /**
     *
     */
    static class CachePredicate implements IgnitePredicate<ClusterNode> {
        /** */
        private List<String> excludeNodes;

        /**
         * @param excludeNodes Nodes names.
         */
        public CachePredicate(List<String> excludeNodes) {
            this.excludeNodes = excludeNodes;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            String name = clusterNode.attribute(ATTR_IGNITE_INSTANCE_NAME).toString();

            return !excludeNodes.contains(name);
        }
    }

    /**
     * Tests with checkCrc=true that corrupted pages in the index partition are detected.
     */
    @Test
    public void testCorruptedIndexPartitionShouldFailValidationWithCrc() throws Exception {
        IgniteEx ignite = prepareGridForTest();

        forceCheckpoint();

        enableCheckpoints(ignite, false);

        corruptIndexPartition(indexPartition(ignite, CACHE_NAME), 1024, 4096);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", "--check-crc", CACHE_NAME));

        assertContains(log, testOut.toString(), "issues found (listed above)");
        assertContains(log, testOut.toString(), "CRC check of partition failed");
        assertNotContains(log, testOut.toString(), "Runtime failure on bounds");
    }

    /**
     * Tests with that corrupted pages in the index partition are detected.
     */
    @Test
    public void testCorruptedIndexPartitionShouldFailValidationWithoutCrc() throws Exception {
        IgniteEx ignite = prepareGridForTest();

        forceCheckpoint();

        File idxPath = indexPartition(ignite, CACHE_NAME);

        stopAllGrids();

        corruptIndexPartition(idxPath, 6, 47746);

        startGrids(GRID_CNT);

        awaitPartitionMapExchange();

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "validate_indexes", CACHE_NAME));

        assertContains(log, testOut.toString(), "Runtime failure on bounds");
        assertNotContains(log, testOut.toString(), "CRC validation failed");
    }

    /**
     * Create and fill nodes.
     *
     * @throws Exception
     */
    private IgniteEx prepareGridForTest() throws Exception {
        IgniteEx ignite = startGrids(GRID_CNT);

        ignite.cluster().state(ClusterState.ACTIVE);

        Ignite client = startGrid(CLIENT_NODE_NAME_PREFIX);

        createAndFillCache(client, CACHE_NAME, GROUP_NAME);

        return ignite;
    }

    /**
     * Get index partition file for specific node and cache.
     */
    private File indexPartition(IgniteEx ig, String cacheName) {
        return ig.context().pdsFolderResolver().fileTree().partitionFile(ig.cachex(cacheName).configuration(), INDEX_PARTITION);
    }

    /**
     * Write some random trash in index partition.
     */
    private void corruptIndexPartition(File path, int size, int offset) throws IOException {
        assertTrue(path.exists());

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        try (RandomAccessFile idx = new RandomAccessFile(path, "rw")) {
            byte[] trash = new byte[size];

            rand.nextBytes(trash);

            idx.seek(offset);

            idx.write(trash);
        }
    }
}
