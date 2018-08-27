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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * Meta view: system views.
 */
public class SqlSystemViewViews extends SqlAbstractLocalSystemView {
    /** Views. */
    private final Collection<SqlSystemView> views;

    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewViews(GridKernalContext ctx, Collection<SqlSystemView> views) {
        super("SYSTEM_VIEWS", "Registered system views", ctx,
            newColumn("SCHEMA"),
            newColumn("NAME"),
            newColumn("DESCRIPTION")
        );

        this.views = views;
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        log.debug("Get system views: full scan");

        for (SqlSystemView view : views) {
            if (view != null)
                rows.add(
                    createRow(ses, rows.size(),
                        QueryUtils.SCHEMA_SYS,
                        view.getTableName(),
                        view.getDescription()
                    )
                );
        }

        return rows.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return views.size();
    }
}
