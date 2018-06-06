package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;

/**
 *
 */
public class WalDeltaConsistencyTest extends AbstractWalDeltaConsistencyTest {
    /**
     *
     */
    public final void testWalDeltaConsistency() throws Exception {
        IgniteEx ignite = startGrid(0);

        PageMemoryTracker tracker = trackPageMemory(ignite);

        ignite.cluster().active(true);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 5_000; i++)
            cache.put(i, "Cache value " + i);

        for (int i = 1_000; i < 2_000; i++)
            cache.put(i, i);

        for (int i = 500; i < 1_500; i++)
            cache.remove(i);

        dbMgr.forceCheckpoint("Page snapshot's tracking").finishFuture().get();

        for (int i = 3_000; i < 10_000; i++)
            cache.put(i, "Changed cache value " + i);

        for (int i = 4_000; i < 7_000; i++)
            cache.remove(i);

        assertTrue(tracker.checkPages(true));
    }
}
