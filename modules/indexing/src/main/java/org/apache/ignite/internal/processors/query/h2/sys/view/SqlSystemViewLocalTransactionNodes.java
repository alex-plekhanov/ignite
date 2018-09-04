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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Meta view: transaction nodes.
 */
public class SqlSystemViewLocalTransactionNodes extends SqlAbstractLocalSystemView {
    /**
     * @param ctx Grid context.
     */
    public SqlSystemViewLocalTransactionNodes(GridKernalContext ctx) {
        super("LOCAL_TRANSACTION_NODES", "Transaction nodes", ctx, "XID",
            newColumn("XID"),
            newColumn("NODE_ID", Value.UUID),
            newColumn("PRIMARY", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        List<Row> rows = new ArrayList<>();

        // TODO: Check for thread safety

        Collection<IgniteInternalTx> txs = ctx.cache().context().tm().activeTransactions();

        SqlSystemViewColumnCondition xidCond = conditionForColumn("XID", first, last);

        if (xidCond.isEquality()) {
            try {
                log.debug("Get transaction entities: filter by xid");

                final String xid = xidCond.valueForEquality().getString();

                txs = F.view(txs, new IgnitePredicate<IgniteInternalTx>() {
                    @Override public boolean apply(IgniteInternalTx tx) {
                        return xid != null && xid.equals(tx.xid().toString());
                    }
                });

            }
            catch (Exception e) {
                log.warning("Failed to get transactions by xid: " + xidCond.valueForEquality().getString(), e);

                txs = Collections.emptySet();
            }
        }
        else
            log.debug("Get transaction entities: transactions full scan");

        AtomicLong rowKey = new AtomicLong();

        return F.concat(F.iterator(txs,
            tx -> F.iterator(tx.transactionNodes().entrySet(),
                nodeMap -> createRow(ses,
                    rowKey.incrementAndGet(),
                    tx.xid(),
                    nodeMap.getKey(),
                    true),
                true).iterator(),
            true, tx -> !F.isEmpty(tx.transactionNodes())));
    }
}
