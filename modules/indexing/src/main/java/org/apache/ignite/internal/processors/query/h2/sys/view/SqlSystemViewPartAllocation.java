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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Meta view: partition allocation map.
 */
public class SqlSystemViewPartAllocation extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewPartAllocation(GridKernalContext ctx) {
        super("PART_ALLOCATION", "Partition allocation map", ctx, "CACHE_GROUP_ID,NODE_ID,PARTITION",
            newColumn("CACHE_GROUP_ID"),
            newColumn("NODE_ID", Value.UUID),
            newColumn("PARTITION"),
            newColumn("STATE")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(final Session ses, SearchRow first, SearchRow last) {
        SqlSystemViewColumnCondition idCond = conditionForColumn("CACHE_GROUP_ID", first, last);

        Collection<CacheGroupContext> cacheGroups;

        if (idCond.isEquality()) {
            log.debug("Get part allocation map: filter by group id");

            CacheGroupContext grp = ctx.cache().cacheGroup(idCond.valueForEquality().getInt());

            cacheGroups = grp == null ? Collections.emptySet() : Collections.<CacheGroupContext>singleton(grp);
        }
        else {
            log.debug("Get part allocation map: full group scan");

            cacheGroups = ctx.cache().cacheGroups();
        }

        SqlSystemViewColumnCondition partCond = conditionForColumn("PARTITION", first, last);
        SqlSystemViewColumnCondition nodeCond = conditionForColumn("NODE_ID", first, last);

        final int partFilter = partCond.isEquality() ? partCond.valueForEquality().getInt() : -1;
        final UUID nodeFilter = nodeCond.isEquality() ? uuidFromValue(nodeCond.valueForEquality()) : null;

/*
        return new ParentChildRowIterable<CacheGroupContext, PartitionAllocation>(
            ses, cacheGroups,
            new IgniteClosure<CacheGroupContext, Iterator<PartitionAllocation>>() {
                @Override public Iterator<PartitionAllocation> apply(CacheGroupContext grp) {
                    GridDhtPartitionFullMap partFullMap = grp.topology().partitionMap(false);

                    Iterator<Map.Entry<UUID, GridDhtPartitionMap>> partMapIter;

                    if (nodeFilter == null)
                        partMapIter = partFullMap.entrySet().iterator();
                    else {
                        if (partFullMap.containsKey(nodeFilter))
                            partMapIter = new IgniteBiTuple<UUID, GridDhtPartitionMap>(nodeFilter,
                                partFullMap.get(nodeFilter)).entrySet().iterator();
                        else
                            partMapIter = Collections.emptyIterator();
                    }

                    return new PartitionAllocationIterator(partMapIter, partFilter);
                }
            },
            new IgniteBiClosure<CacheGroupContext, PartitionAllocation, Object[]>() {
                @Override public Object[] apply(CacheGroupContext grp,
                    PartitionAllocation partAllocation) {
                    return new Object[] {
                        grp.groupId(),
                        partAllocation.getNodeId(),
                        partAllocation.getPartition(),
                        partAllocation.getState()
                    };
                }
            }
        );
*/
        return null; // TODO
    }

/*
    */
/**
     * Partition allocation iterator.
     *//*

    private class PartitionAllocationIterator extends ParentChildIterator<Map.Entry<UUID, GridDhtPartitionMap>,
        Map.Entry<Integer, GridDhtPartitionState>, PartitionAllocation> {
        */
/**
         * @param nodeIter Node iterator.
         * @param partFilter
         *//*

        public PartitionAllocationIterator(final Iterator<Map.Entry<UUID, GridDhtPartitionMap>> nodeIter,
            final int partFilter) {
            super(nodeIter,
                new IgniteClosure<Map.Entry<UUID, GridDhtPartitionMap>,
                    Iterator<Map.Entry<Integer, GridDhtPartitionState>>>() {
                    @Override public Iterator<Map.Entry<Integer, GridDhtPartitionState>> apply(Map.Entry<UUID, GridDhtPartitionMap> nodeParts) {
                        if (partFilter > 0) {
                            if (nodeParts.getValue().containsKey(partFilter))
                                return new IgniteBiTuple<Integer, GridDhtPartitionState>(partFilter,
                                    nodeParts.getValue().get(partFilter)).entrySet().iterator();
                            else
                                return Collections.emptyIterator();
                        }

                        return nodeParts.getValue().entrySet().iterator();
                    }
                },
                new IgniteBiClosure<Map.Entry<UUID, GridDhtPartitionMap>, Map.Entry<Integer, GridDhtPartitionState>, PartitionAllocation>() {
                    @Override public PartitionAllocation apply(Map.Entry<UUID, GridDhtPartitionMap> nodeParts,
                        Map.Entry<Integer, GridDhtPartitionState> partState) {
                        return new PartitionAllocation(nodeParts.getKey(), partState.getKey(), partState.getValue());
                    }
                }
            );
        }
    }
*/

    /**
     * Partition allocation row.
     */
    private class PartitionAllocation {
        /** Partition. */
        private Integer part;

        /** Node id. */
        private UUID nodeId;

        /** State. */
        private GridDhtPartitionState state;

        /**
         * @param nodeId Node id.
         * @param part Partition.
         * @param state State.
         */
        public PartitionAllocation(UUID nodeId, Integer part, GridDhtPartitionState state) {
            this.nodeId = nodeId;
            this.part = part;
            this.state = state;
        }

        /**
         * Gets node id.
         */
        public UUID getNodeId() {
            return nodeId;
        }

        /**
         * Gets partition.
         */
        public Integer getPartition() {
            return part;
        }

        /**
         * Gets state.
         */
        public GridDhtPartitionState getState() {
            return state;
        }
    }
}
