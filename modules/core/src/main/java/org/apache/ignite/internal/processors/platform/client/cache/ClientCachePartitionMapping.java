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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.binary.BinaryRawWriter;

/**
 * Cache partition mapping.
 */
public class ClientCachePartitionMapping {
    /** Partitions map for caches. */
    private final Map<UUID, Set<Integer>> partitionMap;

    /**
     * @param partitionMap Partition mapping.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ClientCachePartitionMapping(Map<UUID, Set<Integer>> partitionMap) {
        this.partitionMap = partitionMap;
    }

    /**
     * Write mapping using binary writer.
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer) {
        writer.writeInt(partitionMap.size());

        for (Map.Entry<UUID, Set<Integer>> nodeParts: partitionMap.entrySet()) {
            UUID nodeUuid = nodeParts.getKey();
            Set<Integer> parts = nodeParts.getValue();

            writer.writeUuid(nodeUuid);

            writer.writeInt(parts.size());
            for (int part : parts)
                writer.writeInt(part);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ClientCachePartitionMapping mapping = (ClientCachePartitionMapping)o;

        return Objects.equals(partitionMap, mapping.partitionMap);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(partitionMap);
    }
}
