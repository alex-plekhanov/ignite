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
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/**
 * Test affinity awareness of thin client.
 */
public class ThinClientAffinityAwarenessTest extends GridCommonAbstractTest {
    /** Cluster size. */
    private static final int CLUSTER_SIZE = 4;

    /** Wait timeout. */
    private static final long WAIT_TIMEOUT = 1_000L;

    /** Channels. */
    private final TestTcpClintChannel channels[] = new TestTcpClintChannel[CLUSTER_SIZE];

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg0 = new CacheConfiguration()
            .setName("replicated_cache")
            .setCacheMode(CacheMode.REPLICATED);

        CacheConfiguration ccfg1 = new CacheConfiguration()
            .setName("partitioned_custom_affinity_cache")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new CustomAffinityFunction());

        CacheConfiguration ccfg2 = new CacheConfiguration()
            .setName("partitioned_cache")
            .setCacheMode(CacheMode.PARTITIONED);

        return cfg.setCacheConfiguration(ccfg0, ccfg1, ccfg2);
    }

    /**
     *
     */
    @Test
    public void testAffinityAwareness() throws Exception {
        startGrids(CLUSTER_SIZE - 1);

        awaitPartitionMapExchange();

        String addrs[] = new String[CLUSTER_SIZE-1];

        // Add all nodes addresses to the list, except first one.
        for (int i = 0; i < CLUSTER_SIZE - 1; i++)
            addrs[i] = "127.0.0.1:" + (DFLT_PORT + i + 1);

        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(addrs).setAffinityAwarenessEnabled(true);

        IgniteClient client = new TcpIgniteClient(TestTcpClintChannel::new, clientCfg);

        // Wait until all channels initialized.
        assertTrue("Failed to wait for node1 channel init",
            GridTestUtils.waitForCondition(() -> channels[1] != null, WAIT_TIMEOUT));

        assertTrue("Failed to wait for node2 channel init",
            GridTestUtils.waitForCondition(() -> channels[2] != null, WAIT_TIMEOUT));

        TestTcpClintChannel node1ch = channels[1];
        TestTcpClintChannel node2ch = channels[2];

        // Create cache and determine default channel.
        ClientCache<Object, Object> clientCacheRepl = client.getOrCreateCache("replicated_cache");

        TestTcpClintChannel dfltCh = node1ch.nextOp() == ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME ? node1ch :
            node2ch.nextOp() == ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME ? node2ch : null;

        assertNotNull("Can't determine default channel", dfltCh);

        // After first response we should send partitions request on default channel together with next request.
        clientCacheRepl.put(0, 0);

        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < 100; i++) {
            clientCacheRepl.put(i, i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

            clientCacheRepl.get(i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_GET);
        }

        // Check partitioned cache with custom affinity.
        ClientCache<Object, Object> clientCachePartCustom = client.cache("partitioned_custom_affinity_cache");

        // Server send response for all caches with the same affinity even if we don't request it, so we already know
        // partitions information for partitioned cache with custom affinity function and doesn't send new partitions
        // request here.
        for (int i = 1; i < 100; i++) {
            clientCachePartCustom.put(i, i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

            clientCachePartCustom.get(i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_GET);
        }

        // Check partitioned cache.
        ClientCache<Object, Object> clientCachePart = client.cache("partitioned_cache");
        IgniteInternalCache<Object, Object> igniteCachePart = grid(0).context().cache().cache("partitioned_cache");

        clientCachePart.put(0, 0);

        TestTcpClintChannel opCh = affinityChannel(0, igniteCachePart, dfltCh);

        assertOpOnChannel(opCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < 100; i++) {
            opCh = affinityChannel(i, igniteCachePart, dfltCh);

            clientCachePart.put(i, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

            clientCachePart.get(i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET);
        }
    }

    /**
     * Checks that operation goes through specified channel.
     */
    private void assertOpOnChannel(TestTcpClintChannel ch, ClientOperation expectedOp) {
        for (int i = 0; i < channels.length; i++) {
            if (channels[i] != null) {
                ClientOperation actualOp = channels[i].nextOp();

                if (channels[i] == ch)
                    assertEquals("Unexpected operation on channel [ch=" + ch + ']', expectedOp, actualOp);
                else
                    assertNull("Unexpected operation on channel [ch=" + channels[i] + ", expCh=" + ch + ']', actualOp);
            }
        }
    }

    /**
     * Calculates affinity channel for cache and key.
     */
    private TestTcpClintChannel affinityChannel(Object key, IgniteInternalCache<Object, Object> cache, TestTcpClintChannel dfltCh) {
        Collection<ClusterNode> nodes = cache.affinity().mapKeyToPrimaryAndBackups(key);

        UUID nodeId = nodes.iterator().next().id();

        for (int i = 0; i < channels.length; i++) {
            if (channels[i] != null && nodeId.equals(channels[i].serverNodeId()))
                return channels[i];
        }

        return dfltCh;
    }

    /**
     * Test TCP client channel.
     */
    private class TestTcpClintChannel extends TcpClientChannel {
        /** Closed. */
        private volatile boolean closed;

        /** Operations queue. */
        private final Queue<ClientOperation> opsQueue = new ConcurrentLinkedQueue<>();

        /** Channel configuration. */
        private final ClientChannelConfiguration cfg;

        /**
         * @param cfg Config.
         */
        public TestTcpClintChannel(ClientChannelConfiguration cfg) {
            super(cfg);

            this.cfg = cfg;

            channels[cfg.getAddress().getPort() - DFLT_PORT] = this;
        }

        /** {@inheritDoc} */
        @Override public <T> T service(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader) throws ClientConnectionException, ClientAuthorizationException {
            opsQueue.offer(op);

            return super.service(op, payloadWriter, payloadReader);
        }

        /**
         * Gets next operation.
         */
        private ClientOperation nextOp() {
            return opsQueue.poll();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            super.close();

            closed = true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return cfg.getAddress().toString();
        }
    }

    /**
     *
     */
    private static class CustomAffinityFunction extends RendezvousAffinityFunction {
        // No-op.
    }
}
