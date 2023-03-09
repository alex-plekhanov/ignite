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

package org.apache.ignite.internal.commandline;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeColocatedBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.distribution.DistributionArgs;
import org.apache.ignite.internal.commandline.distribution.DistributionCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests control-utility distribution command.
 */
public class DistributionCommandTest extends GridCommandHandlerAbstractTest {
    /** */
    private IgniteConfiguration getConfiguration(String igniteInstanceName, Map<String, String> attrs) throws Exception {
        return getConfiguration(igniteInstanceName).setConsistentId(igniteInstanceName).setUserAttributes(attrs);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testBaselineParsing() {
        assertEquals(F.asMap("cons1", Collections.emptyMap()),
            parseArgs(DistributionCommand.BASELINE_ARG, "cons1").baseline());

        assertEquals(F.asMap("cons1", F.asMap("attr", "val")),
            parseArgs(DistributionCommand.BASELINE_ARG, "cons1:{attr:val}").baseline());

        assertEquals(F.asMap("cons1", Collections.emptyMap(), "cons2", Collections.emptyMap()),
            parseArgs(DistributionCommand.BASELINE_ARG, "cons1,cons2").baseline());

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1,cons2,");

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1,,cons2");

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1:{attr},cons2");

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val,},cons2");

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val,,attr2:val},cons2");

        assertEquals(F.asMap("cons1", F.asMap("attr1", "val"), "cons2", Collections.emptyMap()),
            parseArgs(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val},cons2").baseline());

        assertEquals(F.asMap("cons1", F.asMap("attr1", "val", "attr2", "val"),
            "cons2", Collections.emptyMap()),
            parseArgs(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val,attr2:val},cons2").baseline());

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val,attr2:val},cons2,cons3:{attr1:val");

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val,attr2:val},cons2,cons3{attr1:val}");

        assertParseThrows(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val,attr2:val},cons2,cons3:{attr1:{val}}");

        assertEquals(F.asMap("cons1", F.asMap("attr1", "val", "attr2", "val"),
            "cons2", Collections.emptyMap(),
            "cons3", F.asMap("attr1", "val")),
            parseArgs(DistributionCommand.BASELINE_ARG, "cons1:{attr1:val,attr2:val},cons2,cons3:{attr1:val}").baseline());
    }

    /** */
    private DistributionArgs parseArgs(String... args) {
        Command<DistributionArgs> cmd = CommandsProviderExtImpl.DISTRIBUTION_COMMAND;
        cmd.parseArguments(new CommandArgIterator(F.asList(args).iterator(),
            Collections.emptySet(), Collections.emptyMap()));

        return cmd.arg();
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    private void assertParseThrows(String... args) {
        assertThrowsWithCause(() -> parseArgs(args), IllegalArgumentException.class);
    }

    /**
     * Tests command on persistent cluster.
     */
    @Test
    public void testExecutePersistent() throws Exception {
        persistenceEnable(true);

        doTestDistribution();
    }

    /**
     * Tests command on inmemory cluster.
     */
    @Test
    public void testExecuteInMemory() throws Exception {
        persistenceEnable(false);

        doTestDistribution();
    }

    /** */
    private void doTestDistribution() throws Exception {
        autoConfirmation = false;

        injectTestSystemOut();

        IgniteEx ignite = startGrid(getConfiguration("cons1", F.asMap("cell", "01")));
        startGrid(getConfiguration("cons2", F.asMap("cell", "01")));
        startGrid(getConfiguration("cons3", F.asMap("cell", "02")));
        startGrid(getConfiguration("cons4", F.asMap("cell", "02")));

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Object>("testPart").setBackups(1)
                .setGroupName("partGrp")
                .setNodeFilter(n -> !"cons7".equals(n.consistentId()))
                .setAffinity(new RendezvousAffinityFunction().setPartitions(10)
                    .setAffinityBackupFilter(new ClusterNodeAttributeColocatedBackupFilter("cell"))));

        for (int i = 0; i < 10000; i++)
            cache.put(i, new byte[1024]);

        cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Object>("testRepl").setCacheMode(CacheMode.REPLICATED)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(10)));

        for (int i = 0; i < 10000; i++)
            cache.put(i, new byte[1024]);

        if (persistenceEnable())
            forceCheckpoint();

        assertEquals(EXIT_CODE_OK, execute(CommandsProviderExtImpl.DISTRIBUTION_COMMAND.name()));

        assertEquals(EXIT_CODE_OK, execute(CommandsProviderExtImpl.DISTRIBUTION_COMMAND.name(),
            DistributionCommand.CACHES_ARG, "testPart,testRepl",
            DistributionCommand.BASELINE_ARG,
            "cons1:{cell:01},cons2,cons3:{cell:02},cons5:{cell:03},cons6:{cell:03},cons7:{cell:04}"));

        String testOutStr = testOut.toString();

        // Attributes for cons2 are not set explicitly, but taken from the cluster,
        // so distribution should remain the same.
        String totalCons1 = findLine(testOutStr, "Total for cons1");
        String totalCons2 = findLine(testOutStr, "Total for cons2");

        assertEquals(totalCons1.replace("cons1", ""), totalCons2.replace("cons2", ""));

        // Removed cons4, added cons5 and cons6 nodes.
        assertTrue(testOutStr.contains("-cons4:"));
        assertTrue(testOutStr.contains("+cons5:"));
        assertTrue(testOutStr.contains("+cons6:"));

        // Nodes cons5 and cons6 have the same distribution, since in the same cell.
        String totalCons5 = findLine(testOutStr, "Total for cons5");
        String totalCons6 = findLine(testOutStr, "Total for cons6");

        assertEquals(totalCons5.replace("cons5", ""), totalCons6.replace("cons6", ""));

        // Cache group partGrp for cons7 is filtered by node filter.
        int pos = testOutStr.indexOf("+cons7:");

        assertTrue(pos > 0);

        assertTrue(testOutStr.indexOf("testPart", pos) < 0);
    }


    /** */
    private String findLine(String out, String pattern) {
        int pos = out.indexOf(pattern);

        if (pos < 0)
            return null;

        int posEnd = out.indexOf(System.lineSeparator(), pos);

        if (posEnd < 0)
            posEnd = out.length();

        int posBeg = out.lastIndexOf(System.lineSeparator(), pos);

        if (posBeg < 0)
            posBeg = 0;
        else
            posBeg += System.lineSeparator().length();

        return out.substring(posBeg, posEnd);
    }
}
