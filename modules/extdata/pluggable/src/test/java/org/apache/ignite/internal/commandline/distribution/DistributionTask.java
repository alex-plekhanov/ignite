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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/** */
@GridInternal
public class DistributionTask extends VisorMultiNodeTask<DistributionArgs, DistributionResult, DistributionResult> {
    /** {@inheritDoc} */
    @Override protected VisorJob<DistributionArgs, DistributionResult> job(DistributionArgs arg) {
        return new DistributionJob(arg, false);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected DistributionResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<String, DistributionResult.NodeInfo> nodesInfo = new HashMap<>();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            DistributionResult data = res.getData();

            nodesInfo.putAll(data.nodesInfo());
        }

        if (taskArg.baseline() != null) {
            if (!ignite.cluster().state().active())
                throw new IgniteException("Cluster is not active");

            // Enrich with estimation for baseline change.
            List<ClusterNode> baseline = new ArrayList<>();

            for (Map.Entry<String, Map<String, String>> entry : taskArg.baseline().entrySet()) {
                String consId = entry.getKey();

                BaselineTopology blt = ignite.context().state().clusterState().baselineTopology();
                ClusterNode curNode = blt != null ? blt.baselineNode(consId) : null;

                Map<String, Object> attrs;

                if (curNode != null) {
                    attrs = new HashMap<>(curNode.attributes());
                    // Enrich current node attributes with provided in the parameter.
                    attrs.putAll(entry.getValue());
                }
                else
                    attrs = new HashMap<>(entry.getValue());

                baseline.add(new DetachedClusterNode(consId, attrs));
            }

            baseline.sort(NodeOrderComparator.getInstance());

            // Cache names, partition sizes and index-to-data rate for each cache group.
            // We will store minimal partition size per each OWNING node since partition can contain gaps
            // and this gaps will be shrinked during rebalancing to another node.
            Map<String, T3<DistributionResult.CacheInfo, long[], Double>> grps = new HashMap<>();

            for (DistributionResult.NodeInfo nodeInfo : nodesInfo.values()) {
                // Mark all nodes as removed, will mark it as remained later.
                nodeInfo.diff(DistributionResult.Diff.REMOVED);

                for (Map.Entry<String, DistributionResult.CacheInfo> cacheInfoEntry : nodeInfo.cachesInfo().entrySet()) {
                    String grpName = cacheInfoEntry.getKey();

                    DistributionResult.CacheInfo cacheInfo = cacheInfoEntry.getValue();

                    T3<DistributionResult.CacheInfo, long[], Double> cache = grps.computeIfAbsent(grpName,
                        k -> new T3<>(cacheInfo, new long[cacheInfo.parts()], cacheInfo.indexToDataRation()));

                    long[] cachePartSizes = cache.get2();;

                    for (DistributionResult.PartInfo partInfo : cacheInfo.partsInfo().values()) {
                        if (GridDhtPartitionState.OWNING.name().equals(partInfo.state())) {
                            cachePartSizes[partInfo.partId()] = cachePartSizes[partInfo.partId()] > 0 ?
                                Math.min(cachePartSizes[partInfo.partId()], partInfo.size()) : partInfo.size();
                        }

                        // Mark all partitions as removed, will mark it as remained later.
                        partInfo.diff(DistributionResult.Diff.REMOVED);
                    }

                    if (cache.get3() == 0 || cache.get3() > cacheInfo.indexToDataRation())
                        cache.set3(cacheInfo.indexToDataRation());
                }
            }

            for (Map.Entry<String, T3<DistributionResult.CacheInfo, long[], Double>> entry : grps.entrySet()) {
                String grpName = entry.getKey();
                DistributionResult.CacheInfo cacheInfo = entry.getValue().get1();
                long[] cachePartSizes = entry.getValue().get2();
                double idxToDataRate = entry.getValue().get3();

                CacheGroupContext ctx = ignite.context().cache().cacheGroup(CU.cacheId(grpName));

                if (ctx == null) {
                    ignite.cache(F.first(cacheInfo.cacheNames())); // Get any cache from the group to initiate cache group context.
                    ctx = ignite.context().cache().cacheGroup(CU.cacheId(grpName));

                    if (ctx == null)
                        throw new IgniteException("Can't get context for cache group: " + grpName);
                }

                IgnitePredicate<ClusterNode> nodeFilter = ctx.nodeFilter();

                List<ClusterNode> affNodes = nodeFilter == null ? new ArrayList<>(baseline) :
                    baseline.stream().filter(n -> CU.affinityNode(n, nodeFilter)).collect(Collectors.toList());

                List<List<ClusterNode>> assignment = ctx.affinityFunction().assignPartitions(
                    new GridAffinityFunctionContextImpl(
                        affNodes,
                        ctx.affinity().assignments(ctx.affinity().lastVersion()),
                        null,
                        ctx.affinity().lastVersion(),
                        ctx.config().getBackups()
                ));

                for (int partId = 0; partId < assignment.size(); partId++) {
                    for (ClusterNode node : assignment.get(partId)) {
                        String consId = node.consistentId().toString();
                        DistributionResult.NodeInfo nodeInfo = nodesInfo.compute(consId,
                            (k, v) -> v == null ?
                                new DistributionResult.NodeInfo(consId, new HashMap<>())
                                    .diff(DistributionResult.Diff.ADDED) :
                                v.diff() == DistributionResult.Diff.ADDED ? v : v.diff(DistributionResult.Diff.REMAINED));

                        DistributionResult.CacheInfo cacheInfo0 = nodeInfo.cachesInfo().computeIfAbsent(grpName,
                            k -> new DistributionResult.CacheInfo(cacheInfo.cacheNames(), cacheInfo.cacheMode(),
                                cacheInfo.persistent(), cacheInfo.parts(), new TreeMap<>(), 0, idxToDataRate));

                        int partId0 = partId;
                        cacheInfo0.partsInfo().compute(partId,
                            (k, v) -> v == null ?
                                new DistributionResult.PartInfo(partId0, "NEW", cachePartSizes[partId0])
                                    .diff(DistributionResult.Diff.ADDED) :
                                v.diff(DistributionResult.Diff.REMAINED)
                        );
                    }
                }
            }
        }

        return new DistributionResult(nodesInfo);
    }
}
