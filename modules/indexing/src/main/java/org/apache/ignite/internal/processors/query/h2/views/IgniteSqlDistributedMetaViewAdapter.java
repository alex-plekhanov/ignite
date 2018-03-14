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

import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Distributed meta view adapter.
 */
public class IgniteSqlDistributedMetaViewAdapter extends IgniteSqlAbstractMetaView {
    /** Delegate view. */
    protected final IgniteSqlMetaView delegate;

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
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        return new NodeRowIterable(delegate.getRows(ses, first, last), ctx.localNodeId(), ses);
    }

    /**
     * Row iterable with nodeId column.
     */
    private static class NodeRowIterable implements Iterable<Row> {
        /** Delegate. */
        private final Iterable<Row> delegate;

        /** Node id. */
        private final UUID nodeId;

        /** Session. */
        private final Session ses;

        /**
         * Default constructor.
         */
        public NodeRowIterable(Iterable<Row> delegate, UUID nodeId, Session ses) {
            this.delegate = delegate;
            this.nodeId = nodeId;
            this.ses = ses;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Row> iterator() {
            return new NodeRowIterator(delegate.iterator(), nodeId, ses);
        }
    }

    /**
     * Row iterator with nodeId column.
     */
    private static class NodeRowIterator implements Iterator<Row> {
        /** Delegate. */
        private final Iterator<Row> delegate;

        /** Node id. */
        private final UUID nodeId;

        /** Session. */
        private final Session ses;

        /**
         * @param delegate Delegate.
         * @param nodeId Node id.
         * @param ses Database session
         */
        private NodeRowIterator(Iterator<Row> delegate, UUID nodeId, Session ses) {
            this.delegate = delegate;
            this.nodeId = nodeId;
            this.ses = ses;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return delegate.hasNext();
        }

        /** {@inheritDoc} */
        @Override public Row next() {
            Row row = delegate.next();

            if (row == null)
                return null;

            Value[] oldValues = row.getValueList();
            Value[] newValues = new Value[oldValues.length + 1];

            newValues[0] = ValueUuid.get(nodeId.toString());

            System.arraycopy(oldValues, 0, newValues, 1, oldValues.length);

            return ses.getDatabase().createRow(newValues, 1);
        }
    }
}
