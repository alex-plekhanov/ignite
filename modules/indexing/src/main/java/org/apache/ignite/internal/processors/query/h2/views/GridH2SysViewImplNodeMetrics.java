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

package org.apache.ignite.internal.processors.query.h2.views;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: node metrics.
 */
public class GridH2SysViewImplNodeMetrics extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplNodeMetrics(GridKernalContext ctx) {
        super("NODE_METRICS", "Nodes metrics", ctx, "NODE_ID",
            newColumn("NODE_ID", Value.UUID),
            newColumn("LAST_UPDATE_TIME", Value.TIMESTAMP),
            newColumn("MAX_ACTIVE_JOBS", Value.INT),
            newColumn("CUR_ACTIVE_JOBS", Value.INT),
            newColumn("AVG_ACTIVE_JOBS", Value.FLOAT),
            newColumn("MAX_WAITING_JOBS", Value.INT),
            newColumn("CUR_WAITING_JOBS", Value.INT),
            newColumn("AVG_WAITING_JOBS", Value.FLOAT),
            newColumn("MAX_REJECTED_JOBS", Value.INT),
            newColumn("CUR_REJECTED_JOBS", Value.INT),
            newColumn("AVG_REJECTED_JOBS", Value.FLOAT),
            newColumn("TOTAL_REJECTED_JOBS", Value.INT),
            newColumn("MAX_CANCELED_JOBS", Value.INT),
            newColumn("CUR_CANCELED_JOBS", Value.INT),
            newColumn("AVG_CANCELED_JOBS", Value.FLOAT),
            newColumn("TOTAL_CANCELED_JOBS", Value.INT),
            newColumn("TOTAL_EXECUTED_JOBS", Value.INT),
            newColumn("MAX_JOBS_WAIT_TIME", Value.TIME),
            newColumn("CUR_JOBS_WAIT_TIME", Value.TIME),
            newColumn("AVG_JOBS_WAIT_TIME", Value.TIME),
            newColumn("MAX_JOBS_EXEC_TIME", Value.TIME),
            newColumn("CUR_JOBS_EXEC_TIME", Value.TIME),
            newColumn("AVG_JOBS_EXEC_TIME", Value.TIME),
            newColumn("TOTAL_JOBS_EXEC_TIME", Value.TIME),
            newColumn("TOTAL_EXECUTED_TASKS", Value.INT),
            newColumn("TOTAL_BUSY_TIME", Value.TIME),
            newColumn("TOTAL_IDLE_TIME", Value.TIME),
            newColumn("CUR_IDLE_TIME", Value.TIME),
            newColumn("BUSY_TIME_PERCENTAGE", Value.FLOAT),
            newColumn("IDLE_TIME_PERCENTAGE", Value.FLOAT),
            newColumn("TOTAL_CPU", Value.INT),
            newColumn("CUR_CPU_LOAD", Value.DOUBLE),
            newColumn("AVG_CPU_LOAD", Value.DOUBLE),
            newColumn("CUR_GC_CPU_LOAD", Value.DOUBLE),
            newColumn("HEAP_MEMORY_INIT", Value.LONG),
            newColumn("HEAP_MEMORY_USED", Value.LONG),
            newColumn("HEAP_MEMORY_COMMITED", Value.LONG),
            newColumn("HEAP_MEMORY_MAX", Value.LONG),
            newColumn("HEAP_MEMORY_TOTAL", Value.LONG),
            newColumn("NONHEAP_MEMORY_INIT", Value.LONG),
            newColumn("NONHEAP_MEMORY_USED", Value.LONG),
            newColumn("NONHEAP_MEMORY_COMMITED", Value.LONG),
            newColumn("NONHEAP_MEMORY_MAX", Value.LONG),
            newColumn("NONHEAP_MEMORY_TOTAL", Value.LONG),
            newColumn("UPTIME", Value.TIME),
            newColumn("JVM_START_TIME", Value.TIMESTAMP),
            newColumn("NODE_START_TIME", Value.TIMESTAMP),
            newColumn("CUR_THREAD_CNT", Value.INT),
            newColumn("MAX_THREAD_CNT", Value.INT),
            newColumn("TOTAL_THREAD_CNT", Value.LONG),
            newColumn("CUR_DAEMON_THREAD_CNT", Value.INT),
            newColumn("LAST_DATA_VERSION", Value.LONG),
            newColumn("SENT_MSG_CNT", Value.INT),
            newColumn("SENT_BYTES_CNT", Value.LONG),
            newColumn("RECEIVED_MSG_CNT", Value.INT),
            newColumn("RECEIVED_BYTES_CNT", Value.LONG),
            newColumn("OUTBOUND_MSG_QUEUE", Value.INT)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Collection<ClusterNode> nodes;

        ColumnCondition idCond = conditionForColumn("NODE_ID", first, last);

        if (idCond.isEquality()) {
            try {
                log.debug("Get node metrics: node id");

                UUID nodeId = UUID.fromString(idCond.getValue().getString());

                nodes = Collections.singleton(ctx.grid().cluster().node(nodeId));
            }
            catch (Exception e) {
                log.warning("Failed to get node by nodeId: " + idCond.getValue().getString(), e);

                nodes = Collections.emptySet();
            }
        }
        else {
            log.debug("Get node metrics: full scan");

            nodes = ctx.grid().cluster().nodes();
        }

        for (ClusterNode node : nodes) {
            if (node != null) {
                ClusterMetrics metrics = node.metrics();

                rows.add(
                    createRow(ses, rows.size(),
                        node.id(),
                        valueTimestampFromMillis(metrics.getLastUpdateTime()),
                        metrics.getMaximumActiveJobs(),
                        metrics.getCurrentActiveJobs(),
                        metrics.getAverageActiveJobs(),
                        metrics.getMaximumWaitingJobs(),
                        metrics.getCurrentWaitingJobs(),
                        metrics.getAverageWaitingJobs(),
                        metrics.getMaximumRejectedJobs(),
                        metrics.getCurrentRejectedJobs(),
                        metrics.getAverageRejectedJobs(),
                        metrics.getTotalRejectedJobs(),
                        metrics.getMaximumCancelledJobs(),
                        metrics.getCurrentCancelledJobs(),
                        metrics.getAverageCancelledJobs(),
                        metrics.getTotalCancelledJobs(),
                        metrics.getTotalExecutedJobs(),
                        valueTimeFromMillis(metrics.getMaximumJobWaitTime()),
                        valueTimeFromMillis(metrics.getCurrentJobWaitTime()),
                        valueTimeFromMillis((long)metrics.getAverageJobWaitTime()),
                        valueTimeFromMillis(metrics.getMaximumJobExecuteTime()),
                        valueTimeFromMillis(metrics.getCurrentJobExecuteTime()),
                        valueTimeFromMillis((long)metrics.getAverageJobExecuteTime()),
                        valueTimeFromMillis(metrics.getTotalJobsExecutionTime()),
                        metrics.getTotalExecutedTasks(),
                        valueTimeFromMillis(metrics.getTotalBusyTime()),
                        valueTimeFromMillis(metrics.getTotalIdleTime()),
                        valueTimeFromMillis(metrics.getCurrentIdleTime()),
                        metrics.getBusyTimePercentage(),
                        metrics.getIdleTimePercentage(),
                        metrics.getTotalCpus(),
                        metrics.getCurrentCpuLoad(),
                        metrics.getAverageCpuLoad(),
                        metrics.getCurrentGcCpuLoad(),
                        metrics.getHeapMemoryInitialized(),
                        metrics.getHeapMemoryUsed(),
                        metrics.getHeapMemoryCommitted(),
                        metrics.getHeapMemoryMaximum(),
                        metrics.getHeapMemoryTotal(),
                        metrics.getNonHeapMemoryInitialized(),
                        metrics.getNonHeapMemoryUsed(),
                        metrics.getNonHeapMemoryCommitted(),
                        metrics.getNonHeapMemoryMaximum(),
                        metrics.getNonHeapMemoryTotal(),
                        valueTimeFromMillis(metrics.getUpTime()),
                        valueTimestampFromMillis(metrics.getStartTime()),
                        valueTimestampFromMillis(metrics.getNodeStartTime()),
                        metrics.getCurrentThreadCount(),
                        metrics.getMaximumThreadCount(),
                        metrics.getTotalStartedThreadCount(),
                        metrics.getCurrentDaemonThreadCount(),
                        metrics.getLastDataVersion(),
                        metrics.getSentMessagesCount(),
                        metrics.getSentBytesCount(),
                        metrics.getReceivedMessagesCount(),
                        metrics.getReceivedBytesCount(),
                        metrics.getOutboundMessagesQueueSize()
                    )
                );
            }
        }

        return rows;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return ctx.grid().cluster().nodes().size();
    }
}
