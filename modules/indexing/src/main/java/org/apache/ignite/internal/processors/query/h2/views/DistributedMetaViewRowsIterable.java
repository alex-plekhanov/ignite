package org.apache.ignite.internal.processors.query.h2.views;

import java.util.Iterator;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.h2.result.Row;
import org.jetbrains.annotations.NotNull;

/**
 * Distributed meta view rows iterable.
 */
public class DistributedMetaViewRowsIterable implements Iterable<Row> {
    /** Future. */
    final private IgniteInternalFuture<?> fut;

    /**
     * @param fut Future.
     */
    public DistributedMetaViewRowsIterable(IgniteInternalFuture<?> fut) {
        this.fut = fut;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Row> iterator() {
        return null;
    }
}
