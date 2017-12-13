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

package org.apache.ignite.internal.processors.cache;

import java.lang.management.ManagementFactory;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheMetricsEnableRuntimeTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String GROUP = "group1";

    /** Wait condition timeout. */
    private static final long WAIT_CONDITION_TIMEOUT = 3_000L;

    /** Persistence. */
    private boolean persistence = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Gets CacheGroupMetricsMXBean for given node and group name.
     *
     * @param nodeIdx Node index.
     * @param cacheName Cache name.
     * @return MBean instance.
     */
    private CacheMetricsMXBean mxBean(int nodeIdx, String cacheName, Class<? extends CacheMetricsMXBean> clazz)
        throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeCacheMBeanName(getTestIgniteInstanceName(nodeIdx), cacheName,
            clazz.getName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, CacheMetricsMXBean.class,
            true);
    }


    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration cacheCfg = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setCacheConfiguration(cacheCfg);

        if (persistence)
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(WALMode.LOG_ONLY));

        return cfg;
    }

    /**
     * Check cache statistics enabled/disabled flag for all nodes
     *
     * @param cacheName Cache name.
     * @param enabled Enabled.
     */
    private boolean checkStatisticsMode(String cacheName, boolean enabled) {
        for (Ignite ignite : G.allGrids())
            if (ignite.cache(cacheName).metrics().isStatisticsEnabled() != enabled)
                return false;

        return true;
    }

    /**
     * @param statisticsEnabledCache1 Statistics enabled for cache 1.
     * @param statisticsEnabledCache2 Statistics enabled for cache 2.
     */
    private void assertCachesStatisticsMode(final boolean statisticsEnabledCache1, final boolean statisticsEnabledCache2) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return checkStatisticsMode(CACHE1, statisticsEnabledCache1)
                    && checkStatisticsMode(CACHE2, statisticsEnabledCache2);
            }
        }, WAIT_CONDITION_TIMEOUT));
    }

    /**
     * @param persistence Persistence.
     */
    private void testJmxStatisticsEnable(boolean persistence) throws Exception {
        this.persistence = persistence;

        Ignite ig1 = startGrid(1);
        Ignite ig2 = startGrid(2);

        CacheConfiguration cacheCfg2 = new CacheConfiguration(ig1.cache(CACHE1).getConfiguration(
            CacheConfiguration.class));

        cacheCfg2.setName(CACHE2);
        cacheCfg2.setStatisticsEnabled(true);

        IgniteCache cache1 = ig2.cache(CACHE1);
        IgniteCache cache2 = ig2.getOrCreateCache(cacheCfg2);

        CacheMetricsMXBean mxBeanCache1 = mxBean(2, CACHE1, CacheClusterMetricsMXBeanImpl.class);
        CacheMetricsMXBean mxBeanCache2 = mxBean(2, CACHE2, CacheClusterMetricsMXBeanImpl.class);
        CacheMetricsMXBean mxBeanCache1loc = mxBean(2, CACHE1, CacheLocalMetricsMXBeanImpl.class);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.disableStatistics();

        assertCachesStatisticsMode(true, false);

        stopGrid(1);

        startGrid(3);

        assertCachesStatisticsMode(true, false);

        mxBeanCache1loc.disableStatistics();

        assertCachesStatisticsMode(false, false);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.enableStatistics();

        // Start node 1 again.
        startGrid(1);

        assertCachesStatisticsMode(true, true);

        cache1.put(1, 1);
        cache2.put(1, 1);

        cache1.get(1);
        cache2.get(1);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite ignite : G.allGrids()) {
                    CacheMetrics metrics1 = ignite.cache(CACHE1).metrics();
                    CacheMetrics metrics2 = ignite.cache(CACHE2).metrics();

                    if (metrics1.getCacheGets() < 1 || metrics2.getCacheGets() <1
                        || metrics1.getCachePuts() < 1 || metrics2.getCachePuts() < 1)
                        return false;
                }

                return true;
            }
        }, WAIT_CONDITION_TIMEOUT));
    }

    /**
     * @throws Exception If failed.
     */
    public void testJmxNoPdsStatisticsEnable() throws Exception {
        testJmxStatisticsEnable(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJmxPdsStatisticsEnable() throws Exception {
        testJmxStatisticsEnable(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheManagerStatisticsEnable() throws Exception {
        final CacheManager mgr1 = Caching.getCachingProvider().getCacheManager();
        final CacheManager mgr2 = Caching.getCachingProvider().getCacheManager();

        CacheConfiguration cfg1 = new CacheConfiguration()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        mgr1.createCache(CACHE1, cfg1);

        CacheConfiguration cfg2 = new CacheConfiguration(cfg1)
            .setName(CACHE2)
            .setStatisticsEnabled(true);

        mgr1.createCache(CACHE2, cfg2);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !mgr1.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && !mgr2.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && mgr1.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && mgr2.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));

        mgr1.enableStatistics(CACHE1, true);
        mgr2.enableStatistics(CACHE2, false);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return mgr1.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && mgr2.getCache(CACHE1).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && !mgr1.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled()
                    && !mgr2.getCache(CACHE2).getConfiguration(CacheConfiguration.class).isStatisticsEnabled();
            }
        }, WAIT_CONDITION_TIMEOUT));
    }
}
