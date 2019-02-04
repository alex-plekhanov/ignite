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
package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskV2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgnitePartitionConsistencyOnBaselineChangeTest extends GridCommonAbstractTest {
    /** Initial grids count. */
    private static int GRIDS_COUNT = 6;

    /** Transactional cache name. */
    protected static final String TX_CACHE_NAME = "txCache";

    /** Atomic cache name. */
    protected static final String ATOMIC_CACHE_NAME = "atomicCache";

    /** Keys count. */
    protected static final int KEYS_CNT = 50;

    /** Stop tx load flag. */
    protected static final AtomicBoolean txStop = new AtomicBoolean();

    /** Stop atomic load flag. */
    protected static final AtomicBoolean atomicStop = new AtomicBoolean();

    /** Account value bound. */
    protected static final long ACCOUNT_VAL_BOUND = 1000;

    /** Account value origin. */
    protected static final long ACCOUNT_VAL_ORIGIN = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(100L * 1024 * 1024)
                )
        );

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration<Integer, Long> txCacheCfg = new CacheConfiguration<Integer, Long>(TX_CACHE_NAME)
            .setBackups(2)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            //.setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(FULL_SYNC);
            //.setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Long.class)));

        CacheConfiguration<Integer, Long> atomicCacheCfg = new CacheConfiguration<Integer, Long>(ATOMIC_CACHE_NAME)
            .setBackups(3)
            .setAtomicityMode(ATOMIC)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(FULL_SYNC)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Long.class)));

        cfg.setCacheConfiguration(txCacheCfg, atomicCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param threads Threads.
     * @param ignite Load source instance.
     */
    @SuppressWarnings({"SameParameterValue", "StatementWithEmptyBody"})
    protected IgniteInternalFuture startTxLoad(int threads, Ignite ignite) {
        txStop.set(false);

        return GridTestUtils.runMultiThreadedAsync(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!txStop.get()) {
                    Transaction tx = null;

                    IgniteCache<Integer, Long> cache = ignite.cache(TX_CACHE_NAME);

                    TransactionConcurrency concurrency = rnd.nextBoolean() ? PESSIMISTIC : OPTIMISTIC;

                    TransactionIsolation isolation = REPEATABLE_READ;

                    try (Transaction tx0 = tx = ignite.transactions().txStart(concurrency, isolation, 0, 100)) {
                        int acc0 = rnd.nextInt(KEYS_CNT);

                        int acc1;

                        while ((acc1 = rnd.nextInt(KEYS_CNT)) == acc0)
                            ;

                        // Avoid deadlocks.
                        if (acc0 > acc1) {
                            int tmp = acc0;
                            acc0 = acc1;
                            acc1 = tmp;
                        }

                        long val0 = cache.get(acc0);
                        long val1 = cache.get(acc1);

                        long delta = rnd.nextLong(Math.max(val0, val1));

                        if (val0 < val1) {
                            cache.put(acc0, val0 + delta);
                            cache.put(acc1, val1 - delta);
                        }
                        else {
                            cache.put(acc0, val0 - delta);
                            cache.put(acc1, val1 + delta);
                        }

                        if (rnd.nextInt(10) == 0)
                            tx.rollback();
                        else
                            tx.commit();
                    }
                    catch (Throwable e) {
                        //log.error("Unexpected error [tx=" + tx + "]", e);
                    }
                }
            },
            threads, "tx-load-thread"
        );
    }

    /**
     * Starts load for transactional cache.
     *
     * @param threads Threads.
     * @param ignite Load source instance.
     */
    @SuppressWarnings("SameParameterValue")
    protected IgniteInternalFuture startAtomicLoad(int threads, Ignite ignite) {
        atomicStop.set(false);

        return GridTestUtils.runMultiThreadedAsync(() -> {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!atomicStop.get()) {
                    IgniteCache<Integer, Long> cache = ignite.cache(ATOMIC_CACHE_NAME);

                    int cnt = rnd.nextInt(KEYS_CNT / 10);

                    for (int i = 0; i < cnt; i++) {
                        try {
                            cache.put(rnd.nextInt(KEYS_CNT), rnd.nextLong(ACCOUNT_VAL_ORIGIN, ACCOUNT_VAL_BOUND + 1));
                        }
                        catch (Throwable e) {
                            //log.error("Unexpected error", e);
                        }
                    }
                }
            },
            threads, "atomic-load-thread"
        );
    }

    /**
     * @param fut Future for completion awaiting.
     */
    protected void stopTxLoad(IgniteInternalFuture fut) throws IgniteCheckedException {
        txStop.set(true);

        fut.get();
    }

    /**
     * @param fut Future for completion awaiting.
     */
    protected void stopAtomicLoad(IgniteInternalFuture fut) throws IgniteCheckedException {
        atomicStop.set(true);

        fut.get();
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @return Sum of all cache values.
     */
    protected long populateData(Ignite ignite, String cacheName) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long total = 0;

        try (IgniteDataStreamer<Integer, Long> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.allowOverwrite(false);

            for (int i = 0; i < KEYS_CNT; i++) {
                long val = rnd.nextLong(ACCOUNT_VAL_ORIGIN, ACCOUNT_VAL_BOUND + 1);

                dataStreamer.addData(i, val);

                total += val;
            }

            dataStreamer.flush();
        }

        log.info("Total sum for cache '" + cacheName + "': " + total);

        return total;
    }

    /**
     * Restart node in BLT, add new node to BLT.
     */
    @Test
    public void testRestartNodeAddNewNodeToBaseline() throws Exception {
        startGrids(GRIDS_COUNT);

        Ignite client = startGrid(getConfiguration("client").setClientMode(true));

        grid(0).cluster().active(true);

        populateData(client, ATOMIC_CACHE_NAME);

        populateData(client, TX_CACHE_NAME);

        IgniteInternalFuture futTx = startTxLoad(5, client);

        IgniteInternalFuture futAtomic = startAtomicLoad(5, client);

        doSleep(10_000L);

        int restartNodeIdx = 2;

        String restartNodeConsistentId = grid(restartNodeIdx).cluster().localNode().consistentId().toString();

        stopGrid(restartNodeIdx);

        doSleep(10_000L);

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), restartNodeConsistentId,false));

        startGrid(restartNodeIdx);

        doSleep(10_000L);

        startGrid(GRIDS_COUNT);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        stopTxLoad(futTx);

        stopAtomicLoad(futAtomic);

        assertTrue(idleVerify(grid(0)));
    }

    /**
     * Verifies checksums of backup partitions.
     *
     * @param ignite Ignite instance.
     * @return {@code true} if there is no conflict.
     */
    private boolean idleVerify(IgniteEx ignite) {
        Set<String> caches = new HashSet<>();

        caches.add(TX_CACHE_NAME);

        VisorIdleVerifyTaskArg arg = new VisorIdleVerifyTaskArg(caches);

        IdleVerifyResultV2 res = ignite.compute().execute(VisorIdleVerifyTaskV2.class.getName(),
            new VisorTaskArgument<>(ignite.cluster().localNode().id(), arg, false));

        if (res.hasConflicts()) {
            StringBuilder sb = new StringBuilder(">>>> ");

            res.print(sb::append);

            log.info(sb.toString());
        }

        return !res.hasConflicts();
    }
}
