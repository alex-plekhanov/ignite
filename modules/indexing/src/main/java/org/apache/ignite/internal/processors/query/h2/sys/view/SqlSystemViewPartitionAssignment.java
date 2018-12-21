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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Meta view: partition assignment map.
 */
public class SqlSystemViewPartitionAssignment extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewPartitionAssignment(GridKernalContext ctx) {
        super("PARTITION_ASSIGNMENT", "Partitions assignment map", ctx, "CACHE_GROUP_ID,PARTITION",
            newColumn("CACHE_GROUP_ID", Value.INT),
            newColumn("TOPOLOGY_VERSION"),
            newColumn("PARTITION", Value.INT),
            newColumn("NODE_ID", Value.UUID),
            newColumn("IS_PRIMARY", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(final Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition grpIdCond = conditionForColumn("CACHE_GROUP_ID", first, last);
        SqlSystemViewColumnCondition partCond = conditionForColumn("PARTITION", first, last);

        Integer grpIdFilter = grpIdCond.isEquality() ? grpIdCond.valueForEquality().getInt() : null;
        Integer partFilter = partCond.isEquality() ? partCond.valueForEquality().getInt() : null;

        AtomicLong rowKey = new AtomicLong();

        AtomicInteger partNum = new AtomicInteger();

        AtomicBoolean isPrimary = new AtomicBoolean();

        return F.concat(F.iterator(grpAffAssignment(grpIdFilter).entrySet(),
            grpAssignment -> {
                partNum.set(partFilter == null ? -1 : partFilter - 1);

                return F.concat(F.iterator(partsAssignment(grpAssignment.getValue(), partFilter),
                    partAssignment -> {
                        partNum.incrementAndGet();

                        isPrimary.set(true);

                        return F.iterator(partAssignment,
                            partNode -> createRow(ses,
                                rowKey.incrementAndGet(),
                                grpAssignment.getKey(),
                                grpAssignment.getValue().topologyVersion(),
                                partNum.get(),
                                partNode.id(),
                                isPrimary.compareAndSet(true, false)
                            ), true);
                    }, true));
            }, true));
    }

    /**
     * Filtered map of cache group affinity assignment.
     *
     * @param grpIdFilter Group id or {@code null} if filter don't needed.
     */
    private Map<Integer, AffinityAssignment> grpAffAssignment(Integer grpIdFilter) {
        AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

        if (grpIdFilter == null) {
            return ctx.cache().cacheGroups().stream().collect(Collectors.toMap(CacheGroupContext::groupId,
                gctx -> gctx.affinity().cachedAffinity(topVer)));
        }

        CacheGroupContext gctx = ctx.cache().cacheGroup(grpIdFilter);

        return gctx == null ? Collections.emptyMap() : F.asMap(grpIdFilter, gctx.affinity().cachedAffinity(topVer));
    }

    /**
     * Filtered set of partitions assignment.
     *
     * @param affAssignment All partitions affinity assignment.
     * @param partFilter Partition number or {@code null} if filter don't needed.
     */
    private List<List<ClusterNode>> partsAssignment(AffinityAssignment affAssignment, Integer partFilter) {
        if (partFilter == null)
            return affAssignment.assignment();

        List<ClusterNode> assignments = partFilter >= 0 ? affAssignment.get(partFilter) : null;

        return assignments == null ? Collections.emptyList() : Collections.singletonList(assignments);
    }
}
