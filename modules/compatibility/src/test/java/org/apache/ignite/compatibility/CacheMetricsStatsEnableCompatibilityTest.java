package org.apache.ignite.compatibility;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 *
 */
public class CacheMetricsStatsEnableCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Cache name. */
    private final static String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setPeerClassLoadingEnabled(true);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStatisticsEnable() throws Exception {
        IgniteEx ig1 = startGrid(1, "2.3.0", new ConfigurationClosure());
        IgniteEx ig0 = startGrid(0);
        IgniteEx ig2 = startGrid(2, "2.3.0", new ConfigurationClosure());
        IgniteEx ig3 = startGrid(3);

        IgniteCache cache = ig0.getOrCreateCache(CACHE_NAME);

        assertFalse(cache.metrics().isStatisticsEnabled());

        doSleep(500500);

        ig0.context().cache().enableStatistics("cache1", true);

        awaitPartitionMapExchange();

        assertTrue(cache.metrics().isStatisticsEnabled());
        assertTrue(ig3.cache(CACHE_NAME).getConfiguration(CacheConfiguration.class).isStatisticsEnabled());
        //assertFalse(ig1.cache(CACHE_NAME).getConfiguration(CacheConfiguration.class).isStatisticsEnabled());
    }

        /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, new CO<Message>() {
                @Override public Message apply() {
                    return new GridTestMessage();
                }
            });

            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }
    }

}
