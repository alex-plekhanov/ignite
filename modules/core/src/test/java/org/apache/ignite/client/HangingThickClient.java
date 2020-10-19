package org.apache.ignite.client;

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class HangingThickClient extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        Set<String> addresses = Collections.singleton("127.0.0.1:" + (cfg.isClientMode() ? "10900" : "47500..47509"));

        cfg.setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder(true).setAddresses(addresses))
                .setJoinTimeout(-1)
        );

        return cfg;
    }

    @Test
    public void hangingThickClientTest() throws Exception {
        Ignite server = startGrid(0);
        Ignite client = startClientGrid(1);
    }
}