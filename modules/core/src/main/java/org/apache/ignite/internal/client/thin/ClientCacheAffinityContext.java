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
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.jetbrains.annotations.NotNull;

/**
 * Client cache affinity awareness context.
 */
public class ClientCacheAffinityContext {
    /** Binary data processor. */
    private final IgniteBinary binary;

    /** Contains last topology version and known nodes of this version. */
    private final AtomicReference<TopologyNodes> lastTop = new AtomicReference<>();

    /** Current affinity mapping. */
    private final AtomicReference<ClientCacheAffinityMapping> affinityMapping = new AtomicReference<>();

    /**
     * @param binary Binary data processor.
     */
    public ClientCacheAffinityContext(IgniteBinary binary) {
        this.binary = binary;
    }

    /**
     * Update topology version if it's greater then current version and store nodes for last topology.
     *
     * @param topVer Topology version.
     * @param nodeId Node id.
     * @return {@code True} if last topology was updated to the new version.
     */
    public boolean updateLastTopologyVersion(AffinityTopologyVersion topVer, UUID nodeId) {
        while (true) {
            TopologyNodes lastTop = this.lastTop.get();

            if (lastTop == null || topVer.compareTo(lastTop.topVer) > 0) {
                if (this.lastTop.compareAndSet(lastTop, new TopologyNodes(topVer, nodeId)))
                    return true;
            }
            else if (topVer.equals(lastTop.topVer)) {
                lastTop.nodes.add(nodeId);

                return false;
            }
            else
                return false;
        }
    }

    /**
     * Is affinity update required for given cache.
     *
     * @param cacheId Cache id.
     */
    public boolean affinityUpdateRequired(int cacheId) {
        // TODO store cacheId

        TopologyNodes top = lastTop.get();

        if (top == null) // Don't know current topology.
            return false;

        ClientCacheAffinityMapping mapping = affinityMapping.get();

        if (mapping == null)
            return true;

        if (top.topVer.compareTo(mapping.topologyVersion()) > 0)
            return true;

        // TODO if cache not in mapping return true else return false.

        return true;
    }

    /**
     * @param ch Payload output channel.
     */
    public void writePartitionsUpdateRequest(PayloadOutputChannel ch) {
        // TODO
    }

    /**
     * @param ch Payload input channel.
     */
    public boolean readPartitionsUpdateResponse(PayloadInputChannel ch) {
        ClientCacheAffinityMapping newMapping = ClientCacheAffinityMapping.readResponse(ch);

        while (true) {
            ClientCacheAffinityMapping oldMapping = affinityMapping.get();

            if (oldMapping == null || newMapping.topologyVersion().compareTo(oldMapping.topologyVersion()) > 0) {
                if (affinityMapping.compareAndSet(oldMapping, newMapping))
                    // TODO update pending caches (remove)
                    return true;
                else
                    continue;
            }

            if (newMapping.topologyVersion().equals(oldMapping.topologyVersion())) {
                if (affinityMapping.compareAndSet(oldMapping, ClientCacheAffinityMapping.merge(oldMapping, newMapping)))
                    // TODO update pending caches (remove)
                    return true;
                else
                    continue;
            }

            // Obsolete mapping.
            return true;
        }
    }

    /**
     * Gets nodes of last topology.
     */
    public Iterable<UUID> lastTopologyNodes() {
        TopologyNodes top = lastTop.get();

        if (top == null)
            return Collections.emptyList();

        // Create Iterable which will abort iterations when topology changed.
        return new Iterable<UUID>() {
            @NotNull @Override public Iterator<UUID> iterator() {
                return new Iterator<UUID>() {
                    Iterator<UUID> delegate = top.nodes.iterator();

                    @Override public boolean hasNext() {
                        return top == lastTop.get() && delegate.hasNext();
                    }

                    @Override public UUID next() {
                        return delegate.next();
                    }
                };
            }
        };
    }

    /**
     * Calculates affinity node for given cache and key.
     *
     * @param cacheId Cache ID.
     * @param key Key.
     * @return Affinity node id or {@code null} if affinity node can't be determined for given cache and key.
     */
    public UUID affinityNode(int cacheId, Object key) {
        TopologyNodes top = lastTop.get();

        if (top == null)
            return null;

        ClientCacheAffinityMapping mapping = affinityMapping.get();

        if (mapping == null)
            return null;

        if (top.topVer.compareTo(mapping.topologyVersion()) > 0)
            return null;

        // TODO
        return null;
    }

    /**
     * Holder for list of nodes for topology version.
     */
    private static class TopologyNodes {
        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Nodes. */
        private final Collection<UUID> nodes = new ConcurrentLinkedQueue<>();

        /**
         * @param topVer Topology version.
         * @param nodeId Node id.
         */
        private TopologyNodes(AffinityTopologyVersion topVer, UUID nodeId) {
            this.topVer = topVer;

            nodes.add(nodeId);
        }
    }
}
