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
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Meta view: queries.
 */
public class SqlSystemViewQueries extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewQueries(GridKernalContext ctx) {
        super("QUERIES", "Queries", ctx,
            newColumn("ID", Value.LONG),
            newColumn("QUERY_TEXT"),
            newColumn("QUERY_TYPE"),
            newColumn("SCHEMA_NAME"),
            newColumn("START_TIME", Value.TIMESTAMP),
            newColumn("IS_CANCELABLE", Value.BOOLEAN),
            newColumn("IS_LOCAL", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        Collection<GridRunningQueryInfo> qryInfos = ctx.query().runningQueries(0L);

        for (GridRunningQueryInfo info : qryInfos) {
                rows.add(
                    createRow(ses, rows.size(),
                        info.id(),
                        info.query(),
                        info.queryType(),
                        info.schemaName(),
                        valueTimestampFromMillis(info.startTime()),
                        info.cancelable(),
                        info.local()
                    )
                );
        }

        return rows.iterator();
    }
}
