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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Meta view: cache groups.
 */
public class SqlSystemViewCacheGroups extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewCacheGroups(GridKernalContext ctx) {
        super("CACHE_GROUPS", "Cache groups", ctx, "ID",
            newColumn("ID"),
            newColumn("NAME"),
            newColumn("CACHE_OR_GROUP_NAME"),
            newColumn("CACHES_COUNT", Value.INT),
            newColumn("PARTITIONS_COUNT", Value.INT),
            newColumn("CONFIG_CACHE_MODE"),
            newColumn("CONFIG_ATOMICITY_MODE"),
            newColumn("CONFIG_BACKUPS", Value.INT),
            newColumn("CONFIG_REBALANCE_MODE"),
            newColumn("CONFIG_REBALANCE_DELAY", Value.LONG)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        SqlSystemViewColumnCondition idCond = conditionForColumn("ID", first, last);

        Collection<CacheGroupContext> cacheGroups;


        if (idCond.isEquality()) {
            log.debug("Get cache groups: group id");

            CacheGroupContext cacheGrp = ctx.cache().cacheGroup(idCond.valueForEquality().getInt());

            cacheGroups = Collections.<CacheGroupContext>singleton(cacheGrp);
        }
        else {
            log.debug("Get cache groups: full scan");

            cacheGroups = ctx.cache().cacheGroups();;
        }

        for (CacheGroupContext grp : cacheGroups) {
            if (grp != null)
                rows.add(
                    createRow(ses, rows.size(),
                        grp.groupId(),
                        grp.name(),
                        grp.cacheOrGroupName(),
                        grp.caches().size(),
                        grp.topology().partitions(),
                        grp.config().getCacheMode().name(),
                        grp.config().getAtomicityMode().name(),
                        grp.config().getBackups(),
                        grp.config().getRebalanceMode().name(),
                        grp.config().getRebalanceDelay()
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
        return ctx.cache().cacheGroups().size();
    }
}
