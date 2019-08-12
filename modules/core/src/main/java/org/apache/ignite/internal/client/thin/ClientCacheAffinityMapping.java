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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ClientCacheAffinityMapping {
    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Affinity information for each cache. */
    private final Map<Integer, CacheAffinityInfo> cacheAffinity = new HashMap<>();

    /** Binary data processor. */
    private final IgniteBinary binary = null; // TODO

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
     * Calculates affinity node for given cache and key.
     *
     * @param cacheId Cache ID.
     * @param key Key.
     */
    public UUID nodeForKey(int cacheId, Object key) {
        CacheAffinityInfo affinityInfo = cacheAffinity.get(cacheId);

        if (affinityInfo == null || affinityInfo.keyCfg == null || affinityInfo.partMapping == null)
            return null;

        if (!affinityInfo.keyCfg.isEmpty()) {
            int typeId = binary.typeId(key.getClass().getName());

            Integer fieldId = affinityInfo.keyCfg.get(typeId);

            if (fieldId != null) {
                BinaryObject obj = binary.toBinary(key);

                if (obj instanceof BinaryObjectExImpl)
                    key = ((BinaryObjectExImpl)obj).field(fieldId);
                else
                    return null; // TODO Warning?
            }
        }

        return affinityInfo.nodeForKey(key);
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
                    Map<Integer, Map<Integer, Integer>> cacheKeyCfg = U.newHashMap(cachesCnt);

                    for (int j = 0; j < cachesCnt; j++)
                        cacheKeyCfg.put(in.readInt(), readCacheKeyConfiguration(in));

                    UUID[] partToNode = readNodePartitions(in);

                    for (Map.Entry<Integer, Map<Integer, Integer>> keyCfg : cacheKeyCfg.entrySet())
                        aff.cacheAffinity.put(keyCfg.getKey(), new CacheAffinityInfo(keyCfg.getValue(), partToNode));
                }
                else { // Affinity awareness is not applicable for this caches.
                    for (int j = 0; j < cachesCnt; j++)
                        aff.cacheAffinity.put(in.readInt(), new CacheAffinityInfo(null, null));
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
     * Class to store affinity information for cache.
     */
    private static class CacheAffinityInfo {
        /** Key configuration. */
        private final Map<Integer, Integer> keyCfg;

        /** Partition mapping. */
        private final UUID[] partMapping;

        /** Affinity mask. */
        private final int affinityMask;

        /**
         * @param keyCfg Cache key configuration or {@code null} if affinity awareness is not applicable for this cache.
         * @param partMapping Partition to node mapping or {@code null} if affinity awareness is not applicable for
         * this cache.
         */
        private CacheAffinityInfo(Map<Integer, Integer> keyCfg, UUID[] partMapping) {
            this.keyCfg = keyCfg;
            this.partMapping = partMapping;
            affinityMask = partMapping != null ? RendezvousAffinityFunction.calculateMask(partMapping.length) : 0;
        }

        /**
         * Calculates node for given key.
         *
         * @param key Key.
         */
        private UUID nodeForKey(Object key) {
            assert partMapping != null;

            int part = RendezvousAffinityFunction.calculatePartition(key, affinityMask, partMapping.length);

            return partMapping[part];
        }
    }
}
