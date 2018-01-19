package org.apache.ignite.internal.processors.query.h2.views;

import org.apache.ignite.internal.GridKernalContext;

/**
 * Meta view: cluster transactions.
 */
public class SqlMetaViewClusterTransactions extends SqlDistributedMetaViewAdapter {
    /**
     * @param ctx Context.
     */
    public SqlMetaViewClusterTransactions(GridKernalContext ctx) {
        super("TRANSACTIONS", "Cluster active transactions", ctx,
            new SqlMetaViewLocalTransactions(ctx));
    }
}
