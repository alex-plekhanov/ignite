package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class SizeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>();
        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setBackups(1);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     *
     */
    public void testSize() throws Exception {
        startGrid(0);
        startGrid(1);

        IgniteCache cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        awaitPartitionMapExchange(true, true, null);

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    //startGrid(2);
                    //doSleep(3000L);
                    //stopGrid(2);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                while (true) {
                    try {
                        startGrid(2);
                        awaitPartitionMapExchange(true, true, null);
                        stopGrid(2);
                        awaitPartitionMapExchange(true, true, null);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }

            }
        });

        while (true)
            assertEquals(1000, cache.size(CachePeekMode.PRIMARY));
    }
}
