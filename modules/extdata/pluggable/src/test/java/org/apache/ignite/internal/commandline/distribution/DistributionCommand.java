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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;

import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;

/** */
public class DistributionCommand extends AbstractCommand<DistributionArgs> {
    /** */
    public static final String CACHES_ARG = "--caches";

    /** */
    public static final String BASELINE_ARG = "--baseline";

    /** */
    private DistributionArgs args;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = client.compute().nodes().stream()
                .filter(GridClientNode::connectable).findFirst();

            if (firstNodeOpt.isPresent()) {
                VisorTaskArgument<?> arg = new VisorTaskArgument<>(
                    client.compute().nodes().stream().filter(node -> !node.isClient()).map(GridClientNode::nodeId)
                        .collect(Collectors.toList()),
                    args,
                    false
                );

                DistributionResult res = client.compute()
                        .projection(firstNodeOpt.get())
                        .execute(DistributionTask.class.getName(), arg);

                printResult(res, log);
            }
            else
                log.warning("No nodes found in topology, command won't be executed.");
        }
        catch (Throwable t) {
            log.error("Failed to execute distribution command:");
            log.error(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    /** */
    private void printResult(DistributionResult res, IgniteLogger log) {
        Map<String, DistributionResult.NodeInfo> nodesInfo = new TreeMap<>(res.nodesInfo());

        for (DistributionResult.NodeInfo nodeInfo : nodesInfo.values()) {
            log.info(nodeInfo.diff().sign() + nodeInfo.consistentId() + ':');

            long nodeAddedSize = 0;
            long nodeRemovedSize = 0;
            long nodeRemainedSize = 0;

            Map<String, DistributionResult.CacheInfo> cachesInfo = new TreeMap<>(nodeInfo.cachesInfo());

            for (Map.Entry<String, DistributionResult.CacheInfo> cacheInfoEntry : cachesInfo.entrySet()) {
                DistributionResult.CacheInfo cacheInfo = cacheInfoEntry.getValue();
                log.info(INDENT + cacheInfoEntry.getKey() +
                    " (caches: " + cacheInfo.cacheNames() +
                    ", mode: " + cacheInfo.cacheMode().name() +
                    ", persistent: " + cacheInfo.persistent() + "):");

                long cacheAddedSize = 0;
                long cacheRemovedSize = 0;
                long cacheRemainedSize = 0;
                int cacheAddedParts = 0;
                int cacheRemovedParts = 0;
                int cacheRemainedParts = 0;

                Map<Integer, DistributionResult.PartInfo> partsInfo = new TreeMap<>(cacheInfo.partsInfo());
                for (DistributionResult.PartInfo partInfo : partsInfo.values()) {
                    if (partInfo.diff() == DistributionResult.Diff.ADDED) {
                        cacheAddedSize += partInfo.size();
                        cacheAddedParts++;
                    }
                    else if (partInfo.diff() == DistributionResult.Diff.REMOVED){
                        cacheRemovedSize += partInfo.size();
                        cacheRemovedParts++;
                    }
                    else {
                        cacheRemainedSize += partInfo.size();
                        cacheRemainedParts++;
                    }

                    log.info(DOUBLE_INDENT + partInfo.diff().sign() + partInfo.partId() + ", state: " +
                        partInfo.state() + (cacheInfo.persistent() ?
                        ", size: " + U.readableSize(partInfo.size(), false) : ""));
                }

                if (cacheAddedParts > 0 || cacheRemovedParts > 0) {
                    long idxAddedSize = (long)(cacheAddedSize * cacheInfo.indexToDataRation());
                    long idxRemovedSize = (long)(cacheRemovedSize * cacheInfo.indexToDataRation());

                    if (cacheInfo.persistent()) {
                        log.info(DOUBLE_INDENT + "index, size: " + U.readableSize(cacheInfo.indexSize(), false) +
                            " (+" + U.readableSize(idxAddedSize, false) +
                            "/-" + U.readableSize(idxRemovedSize, false) + ')');
                    }

                    cacheAddedSize += idxAddedSize;
                    cacheRemovedSize += idxRemovedSize;
                    cacheRemainedSize += cacheInfo.indexSize();

                    log.info(INDENT + "Total for " + cacheInfoEntry.getKey() +
                        ", parts: " + (cacheRemainedParts + cacheRemovedParts) +
                        " (+" + cacheAddedParts + "/-" + cacheRemovedParts + ')' +
                        (cacheInfo.persistent() ?
                            ", size: " + U.readableSize(cacheRemainedSize + cacheRemovedSize, false) +
                            " (+" + U.readableSize(cacheAddedSize, false) +
                            "/-" + U.readableSize(cacheRemovedSize, false) + ')'
                            : ""));
                }
                else {
                    if (cacheInfo.persistent())
                        log.info(DOUBLE_INDENT + "index, size: " + U.readableSize(cacheInfo.indexSize(), false));

                    cacheRemainedSize += cacheInfo.indexSize();

                    log.info(INDENT + "Total for " + cacheInfoEntry.getKey() +
                        ", parts: " + cacheRemainedParts +
                        (cacheInfo.persistent() ? ", size: " + U.readableSize(cacheRemainedSize, false) : ""));
                }

                nodeAddedSize += cacheAddedSize;
                nodeRemovedSize += cacheRemovedSize;
                nodeRemainedSize += cacheRemainedSize;

                log.info("");
            }

            if (nodeAddedSize > 0 || nodeRemovedSize > 0) {
                log.info("Total for " + nodeInfo.consistentId() + ", size: " +
                    U.readableSize(nodeRemainedSize + nodeRemovedSize, false) +
                    " (+" + U.readableSize(nodeAddedSize, false) +
                    "/-" + U.readableSize(nodeRemovedSize, false) + ')');
            }
            else {
                log.info("Total for " + nodeInfo.consistentId() + ", size: " +
                    U.readableSize(nodeRemainedSize + nodeRemovedSize, false));
            }

            log.info("");
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> caches = null;
        String baselineUnparsed = null;

        while (argIter.hasNextArg()) {
            String cmdArg = argIter.nextArg("");

            Set<String> baseline = null;

            if (CACHES_ARG.equals(cmdArg) && caches == null)
                caches = argIter.nextStringSet("cache names");
            else if (BASELINE_ARG.equals(cmdArg) && baseline == null)
                baselineUnparsed = argIter.nextArg("target baseline information");
            else
                throw new IllegalArgumentException("Invalid argument \"" + cmdArg + "\".");
        }

        args = new DistributionArgs(caches, parseBaseline(baselineUnparsed));
    }

    /** Parse consistent IDs with nodes attributes in format `consId1[:{attr1:val1,attr2:val2,...}],...` */
    private Map<String, Map<String, String>> parseBaseline(String baselineUnparsed) {
        if (baselineUnparsed == null)
            return null;

        Map<String, Map<String, String>> baseline = new HashMap<>();

        String parseNodesErr = "Failed to parse baseline. Unexpected input after position ";
        String parseAttrsErr = "Failed to parse attributes. Unexpected input after position ";


        Pattern nodesPattern = Pattern.compile("\\s*([^:,]+)\\s*(?::\\s*\\{([^{}]+)})?\\s*,");
        Pattern attrsPattern = Pattern.compile("\\s*([^:,]+)\\s*:\\s*([^:,]+)\\s*,");

        // Add ',' to simplify parsing by regexp pattern.
        Matcher nodesMatcher = nodesPattern.matcher(baselineUnparsed + ',');

        int prevNodeIdx = 0;

        while (nodesMatcher.find()) {
            if (nodesMatcher.start() != prevNodeIdx)
                throw new IllegalArgumentException(parseNodesErr + prevNodeIdx);

            prevNodeIdx = nodesMatcher.end();

            String consId = nodesMatcher.group(1);
            String attrsUnparsed = nodesMatcher.group(2);
            Map<String, String> attrs = new HashMap<>();

            if (!F.isEmpty(attrsUnparsed)) {
                Matcher attrsMatcher = attrsPattern.matcher(attrsUnparsed + ',');

                int prevAttrIdx = 0;

                while (attrsMatcher.find()) {
                    if (attrsMatcher.start() != prevAttrIdx)
                        throw new IllegalArgumentException(parseAttrsErr + prevAttrIdx);

                    prevAttrIdx = attrsMatcher.end();

                    String attrName = attrsMatcher.group(1);
                    String attrVal = attrsMatcher.group(2);

                    if (attrs.put(attrName, attrVal) != null)
                        throw new IllegalArgumentException("Duplicated baseline node attribute: " + attrName);
                }

                if (attrsUnparsed.length() + 1 != prevAttrIdx)
                    throw new IllegalArgumentException(parseAttrsErr + prevAttrIdx);
            }

            if (baseline.put(consId, attrs) != null)
                throw new IllegalArgumentException("Duplicated baseline node consistent ID: " + consId);
        }

        if (baselineUnparsed.length() + 1 != prevNodeIdx)
            throw new IllegalArgumentException(parseNodesErr + prevNodeIdx);

        return baseline;
    }

    /** {@inheritDoc} */
    @Override public DistributionArgs arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        String cachesDesc = "cache1" + optional(",...,cacheN");
        String baselineDesc = "consistentId1" + optional(":{attr1:val1[,...,attrN:valN]}")
            + optional(",...,consistentIdN" + optional("..."));

        usage(log, "Print current partitions distribution or differences between distributions ", name(),
            F.asMap(CACHES_ARG, "Caches to process. If not specified information for all caches will be printed.",
                BASELINE_ARG, "Target baseline topology (list of nodes consistent IDs and their attributes). " +
                    "Node attributes will be filled implicitly, if node with such a consistent ID exists in the cluster, " +
                    "attributes explicitly provided as parameter will overwrite attributes from the cluster. " +
                    "If baseline specified, difference between target baseline and current baseline will be printed."),
            optional(CACHES_ARG, cachesDesc), optional(BASELINE_ARG, baselineDesc));
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "--distribution";
    }
}
