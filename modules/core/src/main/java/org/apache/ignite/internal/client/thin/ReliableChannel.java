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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.HostAndPortRange;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Adds failover to {@link ClientChannel}.
 */
final class ReliableChannel implements AutoCloseable {
    /** Client channel holders for each configured address. */
    private final ClientChannelHolder[] channels;

    /** Index of the current channel. */
    private int curChIdx;

    /** Affinity awareness enabled. */
    private final boolean affinityAwarenessEnabled;

    /** Cache affinity awareness context. */
    private final ClientCacheAffinityContext affinityCtx;

    /** Node channels. */
    private final Map<UUID, ClientChannelHolder> nodeChannels = new ConcurrentHashMap<>();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    /** Channels reinit was scheduled. */
    private final AtomicBoolean scheduledChannelsReinit = new AtomicBoolean();

    /** Channel is closed. */
    private boolean closed;

    /**
     * Constructor.
     */
    ReliableChannel(ClientConfiguration clientCfg) throws ClientException {
        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        List<InetSocketAddress> addrs = parseAddresses(clientCfg.getAddresses());

        channels = new ClientChannelHolder[addrs.size()];

        for (int i = 0; i < channels.length; i++)
            channels[i] = new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, addrs.get(i)));

        curChIdx = new Random().nextInt(channels.length); // We already verified there is at least one address.

        affinityAwarenessEnabled = clientCfg.affinityAwarenessEnabled() && channels.length > 1;

        affinityCtx = null; // TODO

        ClientConnectionException lastEx = null;

        for (int i = 0; i < channels.length; i++) {
            try {
                channels[curChIdx].getOrCreateChannel();

                if (affinityAwarenessEnabled)
                    initAllChannelsAsync();

                return;
            } catch (ClientConnectionException e) {
                lastEx = e;

                rollCurrentChannel();
            }
        }

        throw lastEx;
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        closed = true;

        for (ClientChannelHolder hld : channels)
            hld.closeChannel();
    }

    /**
     * Send request and handle response.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException {
        ClientConnectionException failure = null;

        for (int i = 0; i < channels.length; i++) {
            ClientChannel ch = null;

            try {
                ch = channel();

                return ch.service(op, payloadWriter, payloadReader);
            }
            catch (ClientConnectionException e) {
                if (failure == null)
                    failure = e;
                else
                    failure.addSuppressed(e);

                onChannelFailure(ch);
            }
        }

        throw failure;
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOperation op, Function<PayloadInputChannel, T> payloadReader)
        throws ClientException {
        return service(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<PayloadOutputChannel> payloadWriter) throws ClientException {
        service(op, payloadWriter, null);
    }

    /**
     * Send request to affinity node and handle response.
     */
    public <T> T affinityService(
        int cacheId,
        Object key,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException {
        if (!affinityAwarenessEnabled || nodeChannels.isEmpty())
            return service(op, payloadWriter, payloadReader);

/*
        affinityCtx.updateAffinityIfNeeded(cacheId);

        if (affinityCtx.updateNeeded)
            service(
                ClientOperation.CACHE_PARTITIONS,
                ch -> ClientCacheAffinityMapping.writeRequest(ch, cacheId),
                ClientCacheAffinityMapping::readResponse
            );
*/
        // TODO
        ClientConnectionException failure = null;

        for (int i = 0; i < channels.length; i++) {
            ClientChannel ch = null;

            try {
                ch = channel();

                return ch.service(op, payloadWriter, payloadReader);
            }
            catch (ClientConnectionException e) {
                if (failure == null)
                    failure = e;
                else
                    failure.addSuppressed(e);

                onChannelFailure(ch);
            }
        }

        throw failure;
    }

    /**
     * @return host:port_range address lines parsed as {@link InetSocketAddress}.
     */
    private static List<InetSocketAddress> parseAddresses(String[] addrs) throws ClientException {
        if (F.isEmpty(addrs))
            throw new ClientException("Empty addresses");

        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            try {
                ranges.add(HostAndPortRange.parse(
                    a,
                    ClientConnectorConfiguration.DFLT_PORT,
                    ClientConnectorConfiguration.DFLT_PORT + ClientConnectorConfiguration.DFLT_PORT_RANGE,
                    "Failed to parse Ignite server address"
                ));
            }
            catch (IgniteCheckedException e) {
                throw new ClientException(e);
            }
        }

        return ranges.stream()
            .flatMap(r -> IntStream
                .rangeClosed(r.portFrom(), r.portTo()).boxed()
                .map(p -> new InetSocketAddress(r.host(), p))
            )
            .collect(Collectors.toList());
    }

    /** */
    private synchronized ClientChannel channel() {
        if (closed)
            throw new ClientException("Channel is closed");

        try {
            return channels[curChIdx].getOrCreateChannel();
        }
        catch (ClientConnectionException e) {
            rollCurrentChannel();

            throw e;
        }
    }

    /** */
    private synchronized void rollCurrentChannel() {
        curChIdx = (curChIdx + 1) % channels.length;
    }

    /**
     * On current channel failure.
     */
    private synchronized void onChannelFailure(ClientChannel ch) {
        // There is nothing wrong if curChIdx was concurrently changed, since channel was closed if current index was
        // changed by another thread and no other channel will be closed because onChannelFailure checks channel binded
        // to the index before closing it.
        onChannelFailure(curChIdx, ch);
    }

    /**
     * On failure of the channel with the specified index.
     */
    private synchronized void onChannelFailure(int chIdx, ClientChannel ch) {
        if (ch == channels[chIdx].ch && ch != null) {
            channels[chIdx].closeChannel();

            if (chIdx == curChIdx)
                rollCurrentChannel();
        }
    }

    /**
     * Asynchronously try to establish a connection to all configured servers.
     */
    private void initAllChannelsAsync() {
        // Skip if there is already channels reinit scheduled.
        if (scheduledChannelsReinit.compareAndSet(false, true)) {
            executor.submit(
                () -> {
                    scheduledChannelsReinit.set(false);

                    for (ClientChannelHolder hld : channels) {
                        try {
                            hld.getOrCreateChannel();
                        }
                        catch (Exception ignore) {
                            // No-op.
                        }
                    }
                }
            );
        }
    }

    /**
     * Topology version change detected on channel.
     *
     * @param ch Channel.
     */
    private void onTopologyChanged(ClientChannel ch) {
        AffinityTopologyVersion topVer = ch.serverTopologyVersion();

        if (affinityAwarenessEnabled && topVer.compareTo(topVer) > 0) // TODO Check max topology version.
            initAllChannelsAsync();
    }

    /**
     * Channels holder.
     */
    private class ClientChannelHolder {
        /** Channel configuration. */
        private final ClientChannelConfiguration chCfg;

        /** Channel. */
        private volatile ClientChannel ch;

        /**
         * @param chCfg Channel config.
         */
        private ClientChannelHolder(ClientChannelConfiguration chCfg) {
            this.chCfg = chCfg;
        }

        /**
         * Get or create channel.
         */
        private synchronized ClientChannel getOrCreateChannel() {
            if (ch == null) {
                ch = new TcpClientChannel(chCfg);

                if (ch.serverNodeId() != null) {
                    ch.addTopologyChangeListener(ReliableChannel.this::onTopologyChanged);

                    nodeChannels.putIfAbsent(ch.serverNodeId(), this);
                }
            }

            return ch;
        }

        /**
         * Close channel.
         */
        private synchronized void closeChannel() {
            if (ch != null) {
                if (ch.serverNodeId() != null)
                    nodeChannels.remove(ch.serverNodeId(), this);

                U.closeQuiet(ch);
            }

            ch = null;
        }
    }
}
