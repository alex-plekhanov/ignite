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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/**
 * Test affinity awareness of thin client on stable topology.
 */
public class ThinClientAffinityAwarenessStableTopologyTest extends ThinClientAbstractAffinityAwarenessTest {
    /** Wait timeout. */
    private static final long WAIT_TIMEOUT = 1_000L;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(CLUSTER_SIZE - 1);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        String addrs[] = new String[CLUSTER_SIZE-1];

        // Add all nodes addresses to the list, except first one.
        for (int i = 0; i < CLUSTER_SIZE - 1; i++)
            addrs[i] = "127.0.0.1:" + (DFLT_PORT + i + 1);

        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(addrs).setAffinityAwarenessEnabled(true);

        client = new TcpIgniteClient(TestTcpClientChannel::new, clientCfg);

        // Wait until all channels initialized.
        assertTrue("Failed to wait for node1 channel init",
            GridTestUtils.waitForCondition(() -> channels[1] != null, WAIT_TIMEOUT));

        assertTrue("Failed to wait for node2 channel init",
            GridTestUtils.waitForCondition(() -> channels[2] != null, WAIT_TIMEOUT));

        // Request cache creation to determine default channel.
        client.getOrCreateCache(REPL_CACHE_NAME);

        T2<TestTcpClientChannel, ClientOperation> nextChOp = opsQueue.poll();

        assertNotNull(nextChOp);

        assertEquals(nextChOp.get2(), ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME);

        dfltCh = nextChOp.get1();
    }

    /**
     * Test that affinity awareness is not applicable for replicated cache.
     */
    @Test
    public void testReplicatedCache() {
        testNotApplicableCache(REPL_CACHE_NAME);
    }

    /**
     * Test that affinity awareness is not applicable for partitioned cache with custom affinity function.
     */
    @Test
    public void testPartitionedCustomAffinityCache() {
        testNotApplicableCache(PART_CUSTOM_AFFINITY_CACHE_NAME);
    }

    /**
     * Test affinity awareness for all applicable operation types for partitioned cache with primitive key.
     */
    @Test
    public void testPartitionedCachePrimitiveKey() {
        testApplicableCache(PART_CACHE_NAME, i -> i);
    }

    /**
     * Test affinity awareness for all applicable operation types for partitioned cache with complex key.
     */
    @Test
    public void testPartitionedCacheComplexKey() {
        testApplicableCache(PART_CACHE_NAME, i -> new TestComplexKey(i, i));
    }

    /**
     * Test affinity awareness for all applicable operation types for partitioned cache with annotated affinity mapped key.
     */
    @Test
    public void testPartitionedCacheAnnotatedAffinityKey() {
        testApplicableCache(PART_CACHE_NAME, i -> new TestAnnotatedAffinityKey(i, i));
    }

    /**
     * Test affinity awareness for all applicable operation types for partitioned cache with not annotated affinity
     * mapped key.
     */
    @Test
    public void testPartitionedCacheNotAnnotatedAffinityKey() {
        testApplicableCache(PART_CACHE_NAME, i -> new TestNotAnnotatedAffinityKey(new TestComplexKey(i, i), i));
    }

    /**
     * Test request to partition mapped to unknown for client node.
     */
    @Test
    public void testPartitionedCacheUnknownNode() {
        ClientCache<Object, Object> clientCache = client.cache(PART_CACHE_NAME);
        IgniteInternalCache<Object, Object> igniteCache = grid(0).context().cache().cache(PART_CACHE_NAME);

        // We don't included grid(0) address to list of addresses known for the client, so client don't have connection
        // with grid(0)
        UUID unknownNodeId = grid(0).localNode().id();

        Integer keyForUnknownNode = null;

        for (int i = 0; i < KEY_CNT; i++) {
            Collection<ClusterNode> nodes = igniteCache.affinity().mapKeyToPrimaryAndBackups(i);

            UUID keyNodeId = nodes.iterator().next().id();

            if (unknownNodeId.equals(keyNodeId)) {
                keyForUnknownNode = i;

                break;
            }
        }

        assertNotNull("Not found key for node " + unknownNodeId, keyForUnknownNode);

        clientCache.put(keyForUnknownNode, 0);

        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);
    }

    /**
     * @param cacheName Cache name.
     */
    private void testNotApplicableCache(String cacheName) {
        ClientCache<Object, Object> cache = client.cache(cacheName);

        // After first response we should send partitions request on default channel together with next request.
        cache.put(0, 0);

        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < KEY_CNT; i++) {
            cache.put(i, i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

            cache.get(i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_GET);
        }
    }

    /**
     * @param cacheName Cache name.
     * @param keyFactory Key factory function.
     */
    private void testApplicableCache(String cacheName, Function<Integer, Object> keyFactory) {
        ClientCache<Object, Object> clientCache = client.cache(cacheName);
        IgniteInternalCache<Object, Object> igniteCache = grid(0).context().cache().cache(cacheName);

        clientCache.put(keyFactory.apply(0), 0);

        TestTcpClientChannel opCh = affinityChannel(keyFactory.apply(0), igniteCache);

        // Default channel is the first who detects topology change, so next partition request will go through
        // the default channel.
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < KEY_CNT; i++) {
            Object key = keyFactory.apply(i);

            opCh = affinityChannel(key, igniteCache);

            clientCache.put(key, key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

            clientCache.get(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET);

            clientCache.containsKey(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_CONTAINS_KEY);

            clientCache.replace(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE);

            clientCache.replace(key, i, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE_IF_EQUALS);

            clientCache.remove(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_KEY);

            clientCache.remove(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_IF_EQUALS);

            clientCache.getAndPut(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_PUT);

            clientCache.getAndRemove(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REMOVE);

            clientCache.getAndReplace(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REPLACE);

            clientCache.putIfAbsent(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT_IF_ABSENT);
        }
    }
}
