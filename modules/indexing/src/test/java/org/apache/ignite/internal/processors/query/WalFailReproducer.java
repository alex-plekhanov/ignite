package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalDeltaConsistencyTest;

public class WalFailReproducer extends AbstractWalDeltaConsistencyTest {
    @Override protected boolean checkPagesOnCheckpoint() {
        return true;
    }

    public final void testPutRemoveCacheDestroy() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>("cache0");
        ccfg.setIndexedTypes(Integer.class, Integer.class);

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache0 = ignite.getOrCreateCache(ccfg);

        for (int i = 0; i < 5_000; i++)
            cache0.put(i, i);

        forceCheckpoint();

        for (int i = 1_000; i < 4_000; i++)
            cache0.remove(i);

        forceCheckpoint();

        stopAllGrids();
    }


}
