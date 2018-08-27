package org.apache.ignite.internal.processors.query.h2.sys.view;

import org.apache.ignite.internal.GridKernalContext;

/**
 * Meta view: cluster transactions.
 */
public class SqlSystemViewClusterTransactions extends SqlDistributedSystemViewAdapter {
    /**
     * @param ctx Context.
     */
    public SqlSystemViewClusterTransactions(GridKernalContext ctx) {
        super("TRANSACTIONS", "Cluster active transactions", ctx,
            new SqlSystemViewLocalTransactions(ctx));
    }
}
