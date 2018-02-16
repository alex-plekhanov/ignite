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

import org.apache.ignite.internal.GridKernalContext;
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

    /**
     * Adds first element to array.
     *
     * @param arr Array.
     * @param element Element.
     */
    private static <T> T[] addFirstElement(T[] arr, T element) {
        T[] newArr = (T[])new Object[arr.length + 1];

        newArr[0] = element;

        System.arraycopy(arr, 0, newArr, 1, arr.length);

        return newArr;
    }

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
        String[] newIdxs = new String[idxs.length + 1];

        newIdxs[0] = "NODE_ID";

        System.arraycopy(idxs, 0, newIdxs, 1, idxs.length);

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
        return null;
    }
}
