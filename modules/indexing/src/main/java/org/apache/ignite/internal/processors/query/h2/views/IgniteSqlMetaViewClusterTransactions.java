package org.apache.ignite.internal.processors.query.h2.views;

import org.apache.ignite.internal.GridKernalContext;

/**
 * Meta view: cluster transactions.
 */
public class IgniteSqlMetaViewClusterTransactions extends IgniteSqlDistributedMetaViewAdapter {
    /**
     * @param ctx Context.
     */
    public IgniteSqlMetaViewClusterTransactions(GridKernalContext ctx) {
        super("TRANSACTIONS", "Cluster active transactions", ctx,
            new IgniteSqlMetaViewLocalTransactions(ctx));
    }
}
