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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;

/**
 * Distributed meta view adapter.
 */
public class IgniteSqlDistributedMetaViewAdapter extends IgniteSqlAbstractMetaView {
    /** Delegate view. */
    protected final IgniteSqlMetaView delegate;

    /** Message topic. */
    protected final Object msgTopic;

    /** Message listener. */
    protected final GridMessageListener msgLsnr;

    /**
     * @param cols Columns.
     */
    private static Column[] addDistributedColumns(Column[] cols) {
        Column[] newCols = new Column[cols.length + 1];

        newCols[0] = newColumn("NODE_ID", Value.UUID);

        System.arraycopy(cols, 0, newCols, 1, cols.length);

        return newCols;
    }

    /**
     * @param idxs Indexes.
     */
    private static String[] addDistributedIndexes(String[] idxs) {
        String[] newIdxs = new String[idxs.length];

        for (int i = 0; i < idxs.length; i++)
            newIdxs[i] = "NODE_ID," + idxs[i];

        return idxs;
    }

    /**
     * @param tblName Table name.
     * @param desc Descriptor.
     * @param ctx Context.
     * @param delegate Local meta view.
     */
    public IgniteSqlDistributedMetaViewAdapter(String tblName, String desc, GridKernalContext ctx,
        IgniteSqlMetaView delegate) {
        super(tblName, desc, ctx, addDistributedColumns(delegate.getColumns()),
            addDistributedIndexes(delegate.getIndexes()));

        this.delegate = delegate;
        this.msgTopic = new IgniteBiTuple<>(GridTopic.TOPIC_QUERY, delegate.getTableName());

        msgLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                onMessage0(nodeId, msg);
            }
        };

        ctx.io().addMessageListener(msgTopic, msgLsnr);
    }

    /**
     * @param nodeId Source node Id.
     * @param msg Message.
     */
    private void onMessage0(UUID nodeId, Object msg) {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            return;

        try {
            if (msg instanceof MetaViewRowsRequest)
                onRowsRequest(node, (MetaViewRowsRequest)msg);
            else if (msg instanceof MetaViewRowsResponse)
                onRowsResponse(node, (MetaViewRowsResponse)msg);
        }
        catch (Throwable th) {
            U.error(log, "Failed to handle message [nodeId=" + nodeId + ", msg=" + msg + "]", th);

            if (th instanceof Error)
                throw th;
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    protected void onRowsRequest(ClusterNode node, MetaViewRowsRequest msg) {
        // TODO: get SearchRow from request, invoke getRows, send response
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    protected void onRowsResponse(ClusterNode node, MetaViewRowsResponse msg) {
        // TODO: update future
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        // TODO: send request, get future, create iterable based on future

/*
        for (ClusterNode node : ctx.discovery().allNodes()) {
            try {
                ctx.io().sendOrderedMessage(node, msgTopic, new MetaViewRowsRequest(), GridIoPolicy.QUERY_POOL, 0, false);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        }
*/

        return null;
    }
}
