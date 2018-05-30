package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.IgniteCache;

/**
 *
 */
public class WalDeltaConsistencyTest extends AbstractWalDeltaConsistencyTest {
    /** {@inheritDoc} */
    @Override public void prepare() {
        super.prepare();

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);
        for (int i = 0; i < 1_000; i++)
            cache.put(i, "Cache value " + i);
    }

    /** {@inheritDoc} */
    @Override public void process() {
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 3_000; i++)
            cache.put(i, "Changed cache value " + i);

        for (int i = 500; i < 1_500; i++)
            cache.remove(i);
    }
}
