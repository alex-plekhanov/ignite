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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
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

    /**
     *
     */
    @Test
    public void testAffinityAwareness() throws Exception {
        startGrids(CLUSTER_SIZE - 1);

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
        ClientCache<Integer, Integer> cache = client.getOrCreateCache("cache");

        TestTcpClintChannel dfltCh = node1ch.nextOp() == ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME ? node1ch :
            node2ch.nextOp() == ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME ? node2ch : null;

        assertNotNull("Can't determine default channel", dfltCh);

        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.put(4, 3);
        cache.put(5, 3);
        cache.put(6, 3);
        cache.put(7, 3);
        cache.put(8, 3);
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

        /**
         * @param op Op.
         */
        private void assertNextOp(ClientOperation op) {
            assertEquals("Wrong operation on channel " + cfg.getAddress(), op, nextOp());
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            super.close();

            closed = true;
        }
    }
}
