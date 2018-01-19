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
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Sys view caches.
 */
public class GridH2SysViewCaches extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewCaches(GridKernalContext ctx) {
        super("CACHES", ctx, "NAME",
            newColumn("NAME"),
            newColumn("CACHE_MODE"),
            newColumn("GROUP_NAME"),
            newColumn("ATOMICITY_MODE"),
            newColumn("BACKUPS", Value.INT),
            newColumn("REBALANCE_MODE"),
            newColumn("REBALANCE_DELAY", Value.LONG),
            newColumn("SQL_SCHEMA")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        for(IgniteInternalCache<?, ?> cache : ctx.cache().caches()) {
            rows.add(
                createRow(ses, rows.size(),
                    cache.name(),
                    cache.configuration().getCacheMode().name(),
                    cache.configuration().getGroupName(),
                    cache.configuration().getAtomicityMode().name(),
                    cache.configuration().getBackups(),
                    cache.configuration().getRebalanceMode().name(),
                    cache.configuration().getRebalanceDelay(),
                    cache.configuration().getSqlSchema()
                )
            );
        }

        return rows;
    }
}
