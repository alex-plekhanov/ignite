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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ClientCacheAffinityMapping {
    /** Empty partition mapping. */
    private static UUID[] EMPTY_PART_MAPPING = new UUID[0];

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Cache key configuration. */
    private final Map<Integer, Map<Integer, Integer>> cacheKeyCfg = new HashMap<>();

    /** Cache partition mapping. */
    private final Map<Integer, UUID[]> cachePartMapping = new HashMap<>();

    /**
     * @param topVer Topology version.
     */
    private ClientCacheAffinityMapping(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * Gets topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * Writes caches affinity request to the output channel.
     *
     * @param ch Output channel.
     * @param cacheIds Cache IDs.
     */
    public static void writeRequest(PayloadOutputChannel ch, int ... cacheIds) {
        BinaryOutputStream out = ch.out();

        out.writeInt(cacheIds.length);

        for (int cacheId : cacheIds)
            out.writeInt(cacheId);
    }

    /**
     * Reads caches affinity response from the input channel and creates {@code ClientCacheAffinityMapping} instance
     * from this response.
     *
     * @param ch Input channel.
     */
    public static ClientCacheAffinityMapping readResponse(PayloadInputChannel ch) {
        try (BinaryReaderExImpl in = new BinaryReaderExImpl(null, ch.in(), null, true)) {
            long topVer = in.readLong();
            int minorTopVer = in.readInt();

            ClientCacheAffinityMapping aff = new ClientCacheAffinityMapping(new AffinityTopologyVersion(topVer, minorTopVer));

            int mappingsCnt = in.readInt();

            for (int i = 0; i < mappingsCnt; i++) {
                boolean applicable = in.readBoolean();

                int cachesCnt = in.readInt();

                if (applicable) { // Affinity awareness is applicable for this caches.
                    int cacheIds[] = new int[cachesCnt];

                    for (int j = 0; j < cachesCnt; j++) {
                        int cacheId = in.readInt();

                        cacheIds[j] = cacheId;

                        aff.cacheKeyCfg.put(cacheId, readCacheKeyConfiguration(in));
                    }

                    UUID[] partToNode = readNodePartitions(in);

                    for (int cacheId : cacheIds)
                        aff.cachePartMapping.put(cacheId, partToNode);
                }
                else { // Affinity awareness is not applicable for this caches.
                    for (int j = 0; j < cachesCnt; j++) {
                        int cacheId = in.readInt();

                        aff.cacheKeyCfg.put(cacheId, Collections.emptyMap());
                        aff.cachePartMapping.put(cacheId, EMPTY_PART_MAPPING);
                    }
                }
            }

            return aff;
        }
        catch (IOException e) {
            throw new ClientError(e);
        }
    }

    /**
     * @param in Input reader.
     */
    private static Map<Integer, Integer> readCacheKeyConfiguration(BinaryReaderExImpl in) {
        int keyCfgCnt = in.readInt();

        Map<Integer, Integer> keyCfg = U.newHashMap(keyCfgCnt);

        for (int i = 0; i < keyCfgCnt; i++)
            keyCfg.put(in.readInt(), in.readInt());

        return keyCfg;
    }

    /**
     * @param in Input reader.
     */
    private static UUID[] readNodePartitions(BinaryReaderExImpl in) {
        int nodesCnt = in.readInt();

        int maxPart = -1;

        UUID[] partToNode = new UUID[1024];

        for (int i = 0; i < nodesCnt; i++) {
            UUID nodeId = in.readUuid();

            int partCnt = in.readInt();

            for (int j = 0; j < partCnt; j++) {
                int part = in.readInt();

                if (part > maxPart) {
                    maxPart = part;

                    // Expand partToNode if needed.
                    if (part >= partToNode.length)
                        partToNode = Arrays.copyOf(partToNode, U.ceilPow2(part + 1));
                }

                partToNode[part] = nodeId;
            }
        }

        return Arrays.copyOf(partToNode, maxPart + 1);
    }

    /**
     * TODO
     */
    private static class CacheAffinityInfo {
        /** Key configuration. */
        private final Map<Integer, Integer> keyCfg;

        /** Partition mapping. */
        private final UUID[] partMapping;

        /** Affinity mask. */
        private final int affinityMask;

        private CacheAffinityInfo(Map<Integer, Integer> keyCfg, UUID[] partMapping) {
            this.keyCfg = keyCfg;
            this.partMapping = partMapping;
            affinityMask = partMapping != null ? RendezvousAffinityFunction.calculateMask(partMapping.length) : 0;
        }
    }
}
