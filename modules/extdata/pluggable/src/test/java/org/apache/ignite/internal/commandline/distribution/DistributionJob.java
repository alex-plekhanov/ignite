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

package org.apache.ignite.internal.commandline.distribution;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/** Distribution one-node job. */
@GridInternal
public class DistributionJob extends VisorJob<DistributionArgs, DistributionResult> {
    /**
     * Create job with specified argument.
     *
     * @param arg   Job argument.
     * @param debug Flag indicating whether debug information should be printed into node log.
     */
    protected DistributionJob(@Nullable DistributionArgs arg, boolean debug) {
        super(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected DistributionResult run(@Nullable DistributionArgs arg) throws IgniteException {
        if (!ignite.cluster().state().active())
            throw new IgniteException("Cluster is not active");

        Set<String> caches = arg.caches();

        Collection<CacheGroupDescriptor> cacheGrps;

        if (F.isEmpty(caches))
            cacheGrps = ignite.context().cache().cacheGroupDescriptors().values();
        else {
            cacheGrps = new HashSet<>();

            for (String cache : caches) {
                DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cache);

                if (desc != null)
                    cacheGrps.add(desc.groupDescriptor());
                else
                    throw new IgniteException("Cache not found: " + cache);
            }
        }

        Map<String, DistributionResult.CacheInfo> cachesInfo = new HashMap<>();

        try {
            for (CacheGroupDescriptor grpDesc : cacheGrps) {
                Map<Integer, DistributionResult.PartInfo> partsInfo = new HashMap<>();

                CacheGroupContext ctx = ignite.context().cache().cacheGroup(grpDesc.groupId());

                GridCacheDatabaseSharedManager db = grpDesc.persistenceEnabled() ?
                    (GridCacheDatabaseSharedManager)ctx.shared().database() : null;

                long dataSize = 0;

                for (GridDhtLocalPartition part : ctx.topology().localPartitions()) {
                    PageStore pageStore = db == null ? null : db.getPageStore(grpDesc.groupId(), part.id());

                    long partSize = pageStore == null ? 0 : pageStore.pages() * pageStore.getPageSize();

                    dataSize += partSize;

                    DistributionResult.PartInfo partInfo = new DistributionResult.PartInfo(
                        part.id(), part.state().name(), partSize
                    );

                    partsInfo.put(part.id(), partInfo);
                }

                PageStore pageStore = db == null ? null : db.getPageStore(grpDesc.groupId(), INDEX_PARTITION);

                long idxSize = pageStore == null ? 0 : pageStore.pages() * pageStore.getPageSize();

                DistributionResult.CacheInfo cacheInfo = new DistributionResult.CacheInfo(
                    grpDesc.caches().keySet(), ctx.config().getCacheMode(), grpDesc.persistenceEnabled(),
                    ctx.affinity().partitions(), partsInfo, idxSize,
                    dataSize == 0 ? 0 : 1d * idxSize / dataSize);

                cachesInfo.put(grpDesc.cacheOrGroupName(), cacheInfo);
            }

            DistributionResult.NodeInfo nodeInfo = new DistributionResult.NodeInfo(
                ignite.localNode().consistentId().toString(), cachesInfo);

            return new DistributionResult(Collections.singletonMap(nodeInfo.consistentId(), nodeInfo));
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException("Failed to collect partitions distribution: " + ex.getMessage(), ex);
        }
    }
}
