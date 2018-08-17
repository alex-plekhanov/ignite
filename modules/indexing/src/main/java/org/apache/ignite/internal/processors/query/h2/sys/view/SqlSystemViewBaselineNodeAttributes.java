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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * System view: node attributes.
 */
public class SqlSystemViewBaselineNodeAttributes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewBaselineNodeAttributes(GridKernalContext ctx) {
        super("BLT_NODE_ATTRIBUTES", "Baseline topology node attributes", ctx, new String[] {"CONSISTENT_ID,NAME", "NAME"},
            newColumn("CONSISTENT_ID"),
            newColumn("NAME"),
            newColumn("VALUE")
        );
    }

    /** {@inheritDoc} */
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        if (!ctx.state().clusterState().hasBaselineTopology())
            return Collections.emptyIterator();

        BaselineTopology blt = ctx.state().clusterState().baselineTopology();

        SqlSystemViewColumnCondition idCond = conditionForColumn("CONSISTENT_ID", first, last);
        SqlSystemViewColumnCondition nameCond = conditionForColumn("NAME", first, last);

        Collection<Object> consistentIds = blt.consistentIds();

        if (idCond.isEquality()) {
            String idCondVal = idCond.valueForEquality().getString();

            if (consistentIds.contains(idCondVal))
                consistentIds = Collections.singleton(idCondVal);
            else {
                Object consistentId = F.find(consistentIds, null, new IgnitePredicate<Object>() {
                        @Override public boolean apply(Object id) {
                            return F.eq(idCondVal, id.toString());
                        }
                    });

                if (consistentId != null)
                    consistentIds = Collections.singleton(consistentId);
            }
        }

        AtomicLong rowKey = new AtomicLong();

        if (nameCond.isEquality()) {
            String attrName = nameCond.valueForEquality().getString();

            return F.iterator(consistentIds,
                consistentId -> createRow(ses,
                        rowKey.incrementAndGet(),
                        consistentId,
                        attrName,
                        blt.attributes(consistentId).get(attrName)),
                true,
                consistentId -> blt.attributes(consistentId) != null && blt.attributes(consistentId).containsKey(attrName)
            );
        }
        else {
            return F.concat(F.iterator(consistentIds,
                consistentId -> F.iterator(blt.attributes(consistentId).entrySet(),
                    attr -> createRow(ses,
                        rowKey.incrementAndGet(),
                        consistentId,
                        attr.getKey(),
                        attr.getValue()),
                    true),
                true,
                consistentId -> blt.attributes(consistentId) != null));
        }
    }
}
